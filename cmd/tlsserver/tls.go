package main

import (
	"crypto/tls"
	"net"

	_ "embed"
)

var (
	//go:embed cert.pem
	certPEM []byte

	//go:embed key.pem
	keyPEM []byte
)

func tlsListen(network, address string) (net.Listener, error) {
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}

	var certificates []tls.Certificate = []tls.Certificate{cert}

	return tls.Listen(network, address, &tls.Config{
		Certificates: certificates,
	})
}
