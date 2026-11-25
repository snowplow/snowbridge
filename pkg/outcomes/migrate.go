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
	"embed"
	"io/fs"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
	log "github.com/sirupsen/logrus"
)

//go:embed migrations/*.sql
var embeddedMigrations embed.FS

// Migrate applies the embedded schema migrations that the database doesn't have yet.
// A Postgres advisory lock serialises replicas that start together.
// If the database is at a newer version than this binary knows, it logs a warning and applies nothing.
func (s *PostgresStore) Migrate(ctx context.Context) error {
	migrations, err := fs.Sub(embeddedMigrations, "migrations")
	if err != nil {
		return err
	}
	locker, err := lock.NewPostgresSessionLocker(lock.WithLockTimeout(1, 300))
	if err != nil {
		return err
	}
	db := stdlib.OpenDBFromPool(s.pool)
	defer func() { _ = db.Close() }()

	provider, err := goose.NewProvider(goose.DialectPostgres, db, migrations, goose.WithSessionLocker(locker))
	if err != nil {
		return err
	}

	current, err := provider.GetDBVersion(ctx)
	if err != nil {
		return err
	}
	sources := provider.ListSources()
	latest := sources[len(sources)-1].Version
	log.Infof("Outcomes schema at version %d, this build knows up to version %d", current, latest)

	if current > latest {
		log.Warnf("Outcomes schema version %d is newer than version %d known to this build, serving without migrating", current, latest)
		return nil
	}

	results, err := provider.Up(ctx)
	for _, r := range results {
		if r.Error == nil {
			log.Infof("Outcomes schema migration %d applied (%s)", r.Source.Version, r.Duration)
		}
	}
	return err
}
