//
// Copyright (c) 2020-present Snowplow Analytics Ltd. All rights reserved.
//
// This program is licensed to you under the Snowplow Community License Version 1.0,
// and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
// You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0

package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net/http"
)

func main() {

	cert, err := tls.LoadX509KeyPair("../localhost.crt", "../localhost.key")
	if err != nil {
		fmt.Println(err)
	}

	caCert, err := ioutil.ReadFile("../rootCA.crt")
	if err != nil {
		fmt.Println(err)
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	transport := http.Transport{TLSClientConfig: &tls.Config{
		InsecureSkipVerify: false,
		Certificates:       []tls.Certificate{cert},
		ServerName:         "localhost",
		RootCAs:            caCertPool,
	}}
	var DefaultClient = &http.Client{
		Transport: &transport,
	}

	_, err1 := DefaultClient.Get("https://localhost:8999/shutdown")
	if err1 != nil {
		fmt.Println(err1)
	}

}
