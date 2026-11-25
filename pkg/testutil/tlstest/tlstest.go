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

// Package tlstest generates TLS material for tests at test time, so no
// certificates or keys need to be committed. It imports only the standard
// library, so any Snowbridge package's tests can use it without an import cycle.
package tlstest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Files holds the paths of PEM files written by GenerateFiles.
type Files struct {
	CACert     string // CA certificate, which signed both certificates below
	ServerCert string // server certificate for localhost and 127.0.0.1
	ServerKey  string
	ClientCert string // client certificate
	ClientKey  string
}

// GenerateFiles creates a fresh CA, a server certificate for localhost and
// 127.0.0.1 and a client certificate, both signed by the CA, writes them as PEM
// files to t.TempDir() and returns their paths. Each call creates a new CA.
func GenerateFiles(t testing.TB) Files {
	t.Helper()
	dir := t.TempDir()

	caKey := newKey(t)
	caTemplate := &x509.Certificate{
		SerialNumber:          newSerial(t),
		Subject:               pkix.Name{CommonName: "snowbridge test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("creating CA certificate: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parsing CA certificate: %v", err)
	}

	issue := func(name string, usage x509.ExtKeyUsage, dnsNames []string, ips []net.IP) (string, string) {
		key := newKey(t)
		template := &x509.Certificate{
			SerialNumber: newSerial(t),
			Subject:      pkix.Name{CommonName: name},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(24 * time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{usage},
			DNSNames:     dnsNames,
			IPAddresses:  ips,
		}
		der, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
		if err != nil {
			t.Fatalf("creating %s certificate: %v", name, err)
		}
		certPath := filepath.Join(dir, name+".crt")
		keyPath := filepath.Join(dir, name+".key")
		writePEM(t, certPath, "CERTIFICATE", der)
		writePEM(t, keyPath, "PRIVATE KEY", marshalKey(t, key))
		return certPath, keyPath
	}

	files := Files{CACert: filepath.Join(dir, "ca.crt")}
	writePEM(t, files.CACert, "CERTIFICATE", caDER)
	files.ServerCert, files.ServerKey = issue("server", x509.ExtKeyUsageServerAuth, []string{"localhost"}, []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback})
	files.ClientCert, files.ClientKey = issue("client", x509.ExtKeyUsageClientAuth, nil, nil)
	return files
}

func newKey(t testing.TB) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating key: %v", err)
	}
	return key
}

func newSerial(t testing.TB) *big.Int {
	t.Helper()
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("generating serial number: %v", err)
	}
	return serial
}

func marshalKey(t testing.TB, key *ecdsa.PrivateKey) []byte {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshalling key: %v", err)
	}
	return der
}

func writePEM(t testing.TB, path string, blockType string, der []byte) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}), 0o600); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
}
