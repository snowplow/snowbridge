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

package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
)

func main() {

	cert, err := tls.LoadX509KeyPair("../localhost.crt", "../localhost.key")
	if err != nil {
		fmt.Println(err)
	}

	caCert, err := os.ReadFile("../rootCA.crt")
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
