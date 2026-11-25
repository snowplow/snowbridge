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
	"net/url"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func freshDatabase(t *testing.T, name string) string {
	t.Helper()
	if testing.Short() {
		t.Skip("end-to-end test needs Docker")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, e2ePostgresDSN)
	require.NoError(t, err)
	defer func() { _ = conn.Close(ctx) }()
	_, err = conn.Exec(ctx, "create database "+name)
	require.NoError(t, err)

	dsn, err := url.Parse(e2ePostgresDSN)
	require.NoError(t, err)
	dsn.Path = "/" + name
	return dsn.String()
}

func migratedStore(t *testing.T, dsn string) *PostgresStore {
	t.Helper()
	store, err := NewPostgresStore(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(store.Close)
	return store
}

func appliedVersions(t *testing.T, store *PostgresStore) map[int64]int {
	t.Helper()
	rows, err := store.pool.Query(context.Background(), "select version_id, count(*) from goose_db_version where is_applied group by version_id")
	require.NoError(t, err)
	versions := map[int64]int{}
	for rows.Next() {
		var version int64
		var count int
		require.NoError(t, rows.Scan(&version, &count))
		versions[version] = count
	}
	require.NoError(t, rows.Err())
	return versions
}

func TestE2EConcurrentMigrationsApplyEachVersionOnce(t *testing.T) {
	dsn := freshDatabase(t, "outcomes_concurrent_start")

	stores := make([]*PostgresStore, 6)
	for i := range stores {
		stores[i] = migratedStore(t, dsn)
	}
	errs := make([]error, len(stores))
	var wg sync.WaitGroup
	for i, store := range stores {
		wg.Go(func() { errs[i] = store.Migrate(context.Background()) })
	}
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}

	assert.Equal(t, map[int64]int{0: 1, 1: 1}, appliedVersions(t, stores[0]))
}

func TestE2EMigrateKeepsServingWhenDatabaseIsNewer(t *testing.T) {
	dsn := freshDatabase(t, "outcomes_rolled_back")
	store := migratedStore(t, dsn)
	require.NoError(t, store.Migrate(context.Background()))
	_, err := store.pool.Exec(context.Background(), "insert into goose_db_version (version_id, is_applied) values (2, true)")
	require.NoError(t, err)

	require.NoError(t, store.Migrate(context.Background()))

	assert.Equal(t, map[int64]int{0: 1, 1: 1, 2: 1}, appliedVersions(t, store))
}
