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
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"
)

// Run serves the outcomes read and ingest APIs, configured from OUTCOMES_* environment variables, until ctx is done
func Run(ctx context.Context) error {
	cfg, err := ConfigFromEnv()
	if err != nil {
		return err
	}

	store, err := NewPostgresStore(ctx, cfg.PostgresDSN)
	if err != nil {
		return err
	}
	defer store.Close()

	authz := NewConsoleAuthorizer("https://"+cfg.ConsoleDomain, cfg.OrganizationID, cfg.AuthzResource, cfg.AuthzAction)

	if err := store.Migrate(ctx); err != nil {
		return err
	}
	go store.RunTTL(ctx, cfg.TTLDays)

	servers := []*http.Server{
		{Addr: cfg.ListenAddr, Handler: NewHandler(store, authz, cfg.CORSOrigin), ReadHeaderTimeout: 10 * time.Second},
		{Addr: cfg.IngestAddr, Handler: NewIngestHandler(store), ReadHeaderTimeout: 10 * time.Second},
	}

	errs := make(chan error, len(servers))
	for _, server := range servers {
		go func() {
			log.Infof("Outcomes server listening on %s", server.Addr)
			errs <- server.ListenAndServe()
		}()
	}

	select {
	case err = <-errs:
	case <-ctx.Done():
		log.Info("Outcomes server shutting down")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, server := range servers {
		if shutdownErr := server.Shutdown(shutdownCtx); shutdownErr != nil && !errors.Is(shutdownErr, http.ErrServerClosed) {
			log.WithError(shutdownErr).Warn("outcomes server shutdown failed")
		}
	}
	return err
}
