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
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"
)

const maxIngestBody = 10 << 20

// metadataEvent holds the fields the outcomes server reads from a metadata reporter event
type metadataEvent struct {
	Data struct {
		PeriodStart   string `json:"periodStart"`
		PeriodEnd     string `json:"periodEnd"`
		InvalidErrors []struct {
			Code        string          `json:"code"`
			Description string          `json:"description"`
			Count       int             `json:"count"`
			Sample      json.RawMessage `json:"sample"`
		} `json:"invalidErrors"`
	} `json:"data"`
}

// NewIngestHandler accepts metadata reporter events on POST /collect and stores their error groups
func NewIngestHandler(store Store) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /collect", func(w http.ResponseWriter, r *http.Request) {
		var event metadataEvent
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxIngestBody)).Decode(&event); err != nil {
			writeJSON(w, http.StatusBadRequest, apiError{"invalid metadata event"})
			return
		}
		start, errStart := time.Parse(time.RFC3339, event.Data.PeriodStart)
		end, errEnd := time.Parse(time.RFC3339, event.Data.PeriodEnd)
		if errStart != nil || errEnd != nil {
			writeJSON(w, http.StatusBadRequest, apiError{"invalid period"})
			return
		}

		var rows []ErrorRow
		for _, e := range event.Data.InvalidErrors {
			if len(e.Sample) == 0 || bytes.Equal(e.Sample, []byte("null")) {
				continue
			}
			rows = append(rows, ErrorRow{start, end, e.Code, e.Description, e.Count, e.Sample})
		}
		if len(rows) == 0 {
			w.WriteHeader(http.StatusOK)
			return
		}

		if err := store.Insert(r.Context(), rows); err != nil {
			log.WithError(err).Error("cannot write errors")
			writeJSON(w, http.StatusInternalServerError, apiError{"cannot write errors"})
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	return mux
}
