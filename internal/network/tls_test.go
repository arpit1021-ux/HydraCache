package network

import (
	"context"
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

// testCA generates a self-signed CA and can mint leaf certificates
// signed by it, entirely in memory/temp files — no external tooling —
// so TLS/mTLS tests are self-contained and deterministic.
type testCA struct {
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
	certPEM []byte
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return &testCA{cert: cert, key: key, certPEM: certPEM}
}

// issue mints a leaf certificate for "127.0.0.1" signed by the CA, and
// writes both it and its key as PEM files under dir, returning their
// paths.
func (ca *testCA) issue(t *testing.T, dir, name string) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create leaf cert: %v", err)
	}

	certFile = filepath.Join(dir, name+"-cert.pem")
	keyFile = filepath.Join(dir, name+"-key.pem")

	certOut, err := os.Create(certFile)
	if err != nil {
		t.Fatalf("create cert file: %v", err)
	}
	defer certOut.Close()
	if err = pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
		t.Fatalf("write cert PEM: %v", err)
	}

	keyBytes, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal leaf key: %v", err)
	}
	keyOut, err := os.Create(keyFile)
	if err != nil {
		t.Fatalf("create key file: %v", err)
	}
	defer keyOut.Close()
	if err := pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}); err != nil {
		t.Fatalf("write key PEM: %v", err)
	}
	return certFile, keyFile
}

func (ca *testCA) writeCAFile(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(path, ca.certPEM, 0o644); err != nil {
		t.Fatalf("write CA file: %v", err)
	}
	return path
}

func TestBuildServerTLSConfig_RequiresCertAndKey(t *testing.T) {
	if _, err := BuildServerTLSConfig(TLSMaterial{}); err == nil {
		t.Fatal("expected an error with no cert/key configured")
	}
}

func TestBuildServerTLSConfig_MissingFileErrors(t *testing.T) {
	_, err := BuildServerTLSConfig(TLSMaterial{CertFile: "/nope/cert.pem", KeyFile: "/nope/key.pem"})
	if err == nil {
		t.Fatal("expected an error for a nonexistent cert file")
	}
}

func TestTLS_ServerRejectsPlainTCPClient(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	certFile, keyFile := ca.issue(t, dir, "server")

	serverTLS, err := BuildServerTLSConfig(TLSMaterial{CertFile: certFile, KeyFile: keyFile})
	if err != nil {
		t.Fatalf("BuildServerTLSConfig: %v", err)
	}

	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0", MaxConns: 10, TLSConfig: serverTLS}, newTestCache())
	if err = srv.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(srv.Shutdown)

	// A plain (non-TLS) dial completes at the TCP level, but any read/
	// write must fail once the server expects a TLS handshake.
	conn, err := net.DialTimeout("tcp", srv.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("plain dial: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	_, werr := conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	if werr == nil {
		buf := make([]byte, 64)
		if _, rerr := conn.Read(buf); rerr == nil {
			t.Fatal("expected a plain-TCP client talking to a TLS listener to fail, got a readable response")
		}
	}
}

func TestTLS_ClientServerRoundTrip(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	serverCert, serverKey := ca.issue(t, dir, "server")

	serverTLS, err := BuildServerTLSConfig(TLSMaterial{CertFile: serverCert, KeyFile: serverKey})
	if err != nil {
		t.Fatalf("BuildServerTLSConfig: %v", err)
	}

	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0", MaxConns: 10, TLSConfig: serverTLS}, newTestCache())
	if err = srv.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(srv.Shutdown)

	clientTLS, err := BuildClientTLSConfig(TLSMaterial{CAFile: ca.writeCAFile(t, dir)})
	if err != nil {
		t.Fatalf("BuildClientTLSConfig: %v", err)
	}
	t.Cleanup(func() { SetClientTLSConfig(nil) })
	SetClientTLSConfig(clientTLS)

	client := NewClient(srv.Addr().String())
	if err = client.Connect(); err != nil {
		t.Fatalf("TLS client connect: %v", err)
	}
	defer client.Close()

	resp, err := client.Send("PING")
	if err != nil {
		t.Fatalf("PING over TLS: %v", err)
	}
	if resp != "PONG" {
		t.Errorf("resp = %q, want PONG", resp)
	}
}

func TestTLS_MutualTLS_RejectsClientWithoutCert(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	serverCert, serverKey := ca.issue(t, dir, "server")
	caFile := ca.writeCAFile(t, dir)

	serverTLS, err := BuildServerTLSConfig(TLSMaterial{
		CertFile: serverCert, KeyFile: serverKey,
		CAFile: caFile, RequireClientCert: true,
	})
	if err != nil {
		t.Fatalf("BuildServerTLSConfig: %v", err)
	}

	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0", MaxConns: 10, TLSConfig: serverTLS}, newTestCache())
	if err = srv.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(srv.Shutdown)

	// Client trusts the CA but presents no client certificate of its own.
	clientTLS, err := BuildClientTLSConfig(TLSMaterial{CAFile: caFile})
	if err != nil {
		t.Fatalf("BuildClientTLSConfig: %v", err)
	}
	t.Cleanup(func() { SetClientTLSConfig(nil) })
	SetClientTLSConfig(clientTLS)

	// The handshake failure from a missing client certificate isn't
	// always surfaced by Connect() itself (TLS 1.3 can complete the
	// client's side of the handshake before the server's rejection
	// alert arrives) — it reliably surfaces on the first real use of the
	// connection, which is the observable behavior that actually matters
	// to a caller.
	client := NewClient(srv.Addr().String())
	connectErr := client.Connect()
	if connectErr == nil {
		defer client.Close()
		if _, sendErr := client.Send("PING"); sendErr == nil {
			t.Fatal("expected mutual-TLS server to reject a client with no certificate")
		}
	}
}

func TestTLS_MutualTLS_AcceptsClientWithValidCert(t *testing.T) {
	dir := t.TempDir()
	ca := newTestCA(t)
	serverCert, serverKey := ca.issue(t, dir, "server")
	clientCert, clientKey := ca.issue(t, dir, "client")
	caFile := ca.writeCAFile(t, dir)

	serverTLS, err := BuildServerTLSConfig(TLSMaterial{
		CertFile: serverCert, KeyFile: serverKey,
		CAFile: caFile, RequireClientCert: true,
	})
	if err != nil {
		t.Fatalf("BuildServerTLSConfig: %v", err)
	}

	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0", MaxConns: 10, TLSConfig: serverTLS}, newTestCache())
	if err = srv.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(srv.Shutdown)

	clientTLS, err := BuildClientTLSConfig(TLSMaterial{
		CertFile: clientCert, KeyFile: clientKey, CAFile: caFile,
	})
	if err != nil {
		t.Fatalf("BuildClientTLSConfig: %v", err)
	}
	t.Cleanup(func() { SetClientTLSConfig(nil) })
	SetClientTLSConfig(clientTLS)

	client := NewClient(srv.Addr().String())
	if err = client.Connect(); err != nil {
		t.Fatalf("mutual-TLS client connect: %v", err)
	}
	defer client.Close()

	resp, err := client.Send("PING")
	if err != nil {
		t.Fatalf("PING over mutual TLS: %v", err)
	}
	if resp != "PONG" {
		t.Errorf("resp = %q, want PONG", resp)
	}
}

func TestTLS_NoClientConfigDialsPlainTCP(t *testing.T) {
	// Regression guard: with no SetClientTLSConfig call (the default for
	// every existing test and caller), Client.Connect must still dial
	// plain TCP against a plain (non-TLS) server.
	srv := NewServer(ServerConfig{Addr: "127.0.0.1:0", MaxConns: 10}, newTestCache())
	if err := srv.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(srv.Shutdown)

	client := NewClient(srv.Addr().String())
	if err := client.Connect(); err != nil {
		t.Fatalf("plain connect: %v", err)
	}
	defer client.Close()
	if resp, err := client.Send("PING"); err != nil || resp != "PONG" {
		t.Fatalf("PING = %q, %v", resp, err)
	}
}
