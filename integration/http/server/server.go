/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

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
