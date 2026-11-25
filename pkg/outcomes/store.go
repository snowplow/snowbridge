/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.1
 * located at https://docs.snowplow.io/limited-use-license-1.1
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package outcomes

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	log "github.com/sirupsen/logrus"
)

// ErrorSample is the latest sample of an error code and description
type ErrorSample struct {
	SeenAt       time.Time `json:"seenAt"`
	Input        *string   `json:"input"`
	LatestState  *string   `json:"latestState"`
	ErrorMessage *string   `json:"errorMessage"`
	ErrorType    *string   `json:"errorType"`
}

// ErrorRow is one invalid error group of one metadata reporting period, with its sample
type ErrorRow struct {
	PeriodStart time.Time
	PeriodEnd   time.Time
	Code        string
	Description string
	Count       int
	Sample      []byte
}

// ErrorKey is a code and description pair that has a sample
type ErrorKey struct {
	Code        string `json:"code"`
	Description string `json:"description"`
}

// Store reads and writes ef_errors
type Store interface {
	ErrorsWithSample(ctx context.Context, from time.Time, limit int) ([]ErrorKey, error)
	LatestSample(ctx context.Context, code, description string) (*ErrorSample, error)
	Insert(ctx context.Context, rows []ErrorRow) error
	Ping(ctx context.Context) error
}

// PostgresStore keeps error groups in ef_errors, one row per group per metadata reporting period
type PostgresStore struct {
	pool *pgxpool.Pool
}

// NewPostgresStore connects to Postgres
func NewPostgresStore(ctx context.Context, dsn string) (*PostgresStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &PostgresStore{pool: pool}, nil
}

// Close closes the connection pool
func (s *PostgresStore) Close() {
	s.pool.Close()
}

// Ping checks the database is reachable
func (s *PostgresStore) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

const insertQuery = `
insert into ef_errors (period_start, period_end, category, code, description, count, sample)
values ($1, $2, 'invalid', $3, $4, $5, $6)`

// Insert writes the error groups of one reporting period
func (s *PostgresStore) Insert(ctx context.Context, rows []ErrorRow) error {
	batch := &pgx.Batch{}
	for _, r := range rows {
		batch.Queue(insertQuery, r.PeriodStart, r.PeriodEnd, r.Code, r.Description, r.Count, string(r.Sample))
	}
	return s.pool.SendBatch(ctx, batch).Close()
}

const errorsWithSampleQuery = `
select code, description
from ef_errors
where period_start >= $1
group by code, description
order by sum(count) desc, code, description
limit $2`

// ErrorsWithSample returns the distinct code and description pairs with a sample in a period that started at or after from, most frequent first
func (s *PostgresStore) ErrorsWithSample(ctx context.Context, from time.Time, limit int) ([]ErrorKey, error) {
	rows, err := s.pool.Query(ctx, errorsWithSampleQuery, from, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (ErrorKey, error) {
		var k ErrorKey
		err := row.Scan(&k.Code, &k.Description)
		return k, err
	})
}

const latestSampleQuery = `
select period_end,
       sample -> 'data' ->> 'payload',
       sample -> 'data' -> 'failure' ->> 'latestState',
       sample -> 'data' -> 'failure' ->> 'errorMessage',
       sample -> 'data' -> 'failure' ->> 'errorType'
from ef_errors
where code = $1 and description = $2
  and sample is not null
order by period_start desc
limit 1`

// LatestSample returns the latest sample of an error code and description, or nil if there is none
func (s *PostgresStore) LatestSample(ctx context.Context, code, description string) (*ErrorSample, error) {
	var sample ErrorSample
	err := s.pool.QueryRow(ctx, latestSampleQuery, code, description).
		Scan(&sample.SeenAt, &sample.Input, &sample.LatestState, &sample.ErrorMessage, &sample.ErrorType)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	sample.SeenAt = sample.SeenAt.UTC()
	return &sample, nil
}

// DeleteOlderThan removes error groups of periods that started more than ttlDays ago
func (s *PostgresStore) DeleteOlderThan(ctx context.Context, ttlDays int) (int64, error) {
	tag, err := s.pool.Exec(ctx, "delete from ef_errors where period_start < now() - make_interval(days => $1)", ttlDays)
	return tag.RowsAffected(), err
}

// RunTTL deletes expired error groups now and then every hour, until ctx is done
func (s *PostgresStore) RunTTL(ctx context.Context, ttlDays int) {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		if n, err := s.DeleteOlderThan(ctx, ttlDays); err != nil {
			log.WithError(err).Warn("outcomes TTL delete failed")
		} else if n > 0 {
			log.Infof("Outcomes TTL deleted %d error groups older than %d days", n, ttlDays)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
