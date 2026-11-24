package dbmux

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"testing"
)

type nopLogger struct{}

func (nopLogger) Debugf(string, ...any) {}
func (nopLogger) Warnf(string, ...any)  {}

func TestNewRequiresVerifiedClientCertificates(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lis.Close() }()

	for name, cfg := range map[string]*tls.Config{
		"no client auth":   {},
		"unverified":       {ClientAuth: tls.RequireAnyClientCert, ClientCAs: x509.NewCertPool()},
		"no client CA set": {ClientAuth: tls.RequireAndVerifyClientCert},
	} {
		if _, err := New(lis, cfg, []string{"a"}, nopLogger{}); err == nil {
			t.Errorf("%s: New accepted the TLS config", name)
		}
	}
	if _, err := New(lis, &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: x509.NewCertPool()}, []string{"a", "a"}, nopLogger{}); err == nil {
		t.Error("New accepted a duplicate database")
	}
}
