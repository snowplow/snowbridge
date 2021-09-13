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
