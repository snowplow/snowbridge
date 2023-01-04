//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
)

// Note that on MacOs, firewall rules may prevent a network connection to the server. Must be allowlisted on server startup.

func main() {
	mux := http.NewServeMux()
	s := &http.Server{
		Addr:    ":8999",
		Handler: mux,
	}
	mux.HandleFunc("/hello", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
	})

	mux.HandleFunc("/shutdown", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("Shutting down server")
		s.Shutdown(context.Background())
	})

	fmt.Printf("Starting server at port 8999\n")
	if err := s.ListenAndServeTLS("../rootCA.crt", "../rootCA.key"); err != nil {
		log.Fatal(err)
	}

}
