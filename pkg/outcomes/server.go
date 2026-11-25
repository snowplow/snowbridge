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
	"encoding/json"
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/snowplow/snowplow-golang-analytics-sdk/analytics"
)

// sampleResponse is the sample endpoint response, with enriched event TSVs turned into JSON
type sampleResponse struct {
	SeenAt       time.Time `json:"seenAt"`
	Input        any       `json:"input"`
	LatestState  any       `json:"latestState"`
	ErrorMessage *string   `json:"errorMessage"`
	ErrorType    *string   `json:"errorType"`
}

// asJSON returns an enriched event TSV as a JSON object, valid JSON as is, and anything else as a string.
// The event goes through encoding/json rather than the SDK's ToJson, whose keys come out in random order,
// so its fields are always sorted by name.
func asJSON(s *string) any {
	if s == nil {
		return nil
	}
	if event, err := analytics.ParseEvent(*s); err == nil {
		if m, err := event.ToMap(); err == nil {
			if j, err := json.Marshal(m); err == nil {
				return json.RawMessage(j)
			}
		}
	}
	if json.Valid([]byte(*s)) {
		return json.RawMessage(*s)
	}
	return *s
}

type errorsResponse struct {
	Errors []ErrorKey `json:"errors"`
}

const (
	errorsDefaultWindow = 7 * 24 * time.Hour
	errorsLimit         = 100
)

type apiError struct {
	Message string `json:"message"`
}

// NewHandler serves the error sample read API
func NewHandler(store Store, authz Authorizer, corsOrigin string) http.Handler {
	h := &handler{store: store, authz: authz}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", h.healthz)
	mux.Handle("GET /event-forwarding/v1/errors", h.authed(h.errorList))
	mux.Handle("GET /event-forwarding/v1/errors/sample", h.authed(h.errorSample))

	return cors(corsOrigin, mux)
}

type handler struct {
	store Store
	authz Authorizer
}

func (h *handler) healthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := h.store.Ping(ctx); err != nil {
		log.WithError(err).Warn("outcomes healthz: database unreachable")
		writeJSON(w, http.StatusServiceUnavailable, apiError{"database unreachable"})
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("healthy"))
}

func (h *handler) authed(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ok, err := h.authz.Authorize(r.Context(), r.Header.Get("Authorization"))
		if err != nil {
			log.WithError(err).Warn("outcomes authorization failed")
		}
		if !ok {
			writeJSON(w, http.StatusForbidden, apiError{"Authentication denied"})
			return
		}
		next(w, r)
	})
}

func (h *handler) errorList(w http.ResponseWriter, r *http.Request) {
	from := time.Now().Add(-errorsDefaultWindow)
	if f := r.URL.Query().Get("from"); f != "" {
		t, err := time.Parse(time.RFC3339, f)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, apiError{"from must be an RFC 3339 timestamp"})
			return
		}
		from = t
	}

	keys, err := h.store.ErrorsWithSample(r.Context(), from, errorsLimit)
	if err != nil {
		h.internalError(w, "cannot list errors", err)
		return
	}
	if keys == nil {
		keys = []ErrorKey{}
	}
	writeJSON(w, http.StatusOK, errorsResponse{Errors: keys})
}

func (h *handler) errorSample(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if !q.Has("code") || !q.Has("description") {
		writeJSON(w, http.StatusBadRequest, apiError{"code and description are required"})
		return
	}

	code := q.Get("code")
	sample, err := h.store.LatestSample(r.Context(), code, q.Get("description"))
	if err != nil {
		h.internalError(w, "cannot read error sample", err)
		return
	}
	if sample == nil {
		writeJSON(w, http.StatusNotFound, apiError{"no sample found for error code '" + code + "'"})
		return
	}
	writeJSON(w, http.StatusOK, sampleResponse{
		SeenAt:       sample.SeenAt,
		Input:        asJSON(sample.Input),
		LatestState:  asJSON(sample.LatestState),
		ErrorMessage: sample.ErrorMessage,
		ErrorType:    sample.ErrorType,
	})
}

func (h *handler) internalError(w http.ResponseWriter, msg string, err error) {
	log.WithError(err).Error(msg)
	writeJSON(w, http.StatusInternalServerError, apiError{msg})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.WithError(err).Warn("failed to write outcomes response")
	}
}

func cors(origin string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		if origin != "*" {
			w.Header().Add("Vary", "Origin")
		}
		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
			w.Header().Set("Access-Control-Max-Age", "600")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
