package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
)

// Note that on MacOs, firewall rules may prevent a network connection to the server. Remedy this by allowing the `server` binary in this directory, in Firewall options.

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
