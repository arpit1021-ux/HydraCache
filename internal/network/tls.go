package network

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync/atomic"
)

// TLSMaterial names the files backing a node's TLS identity. CertFile and
// KeyFile are this node's own certificate/key. CAFile, if set, is the CA
// used both to verify a peer's certificate (server-side: enables mutual
// TLS: RequireClientCert; client-side: trusts the CA that issued other
// nodes' certs) — a single shared CA across the cluster, matching how a
// cluster mTLS deployment is normally operated (one CA, one cert per
// node), not a per-node CA.
type TLSMaterial struct {
	CertFile          string
	KeyFile           string
	CAFile            string
	RequireClientCert bool
}

// BuildServerTLSConfig loads this node's cert/key into a *tls.Config
// suitable for tls.Listen. If CAFile is set, incoming connections are
// verified against it — RequireClientCert controls whether presenting a
// valid client certificate is mandatory (true mutual TLS) or merely
// verified-if-offered.
func BuildServerTLSConfig(m TLSMaterial) (*tls.Config, error) {
	if m.CertFile == "" || m.KeyFile == "" {
		return nil, fmt.Errorf("tls: cert_file and key_file are both required")
	}
	cert, err := tls.LoadX509KeyPair(m.CertFile, m.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("tls: load server cert/key: %w", err)
	}
	tc := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	if m.CAFile != "" {
		pool, err := loadCAPool(m.CAFile)
		if err != nil {
			return nil, err
		}
		tc.ClientCAs = pool
		if m.RequireClientCert {
			tc.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			tc.ClientAuth = tls.VerifyClientCertIfGiven
		}
	}
	return tc, nil
}

// BuildClientTLSConfig loads this node's own cert/key (to present to
// peers when they require mutual TLS) and, if CAFile is set, trusts that
// CA when verifying a peer's certificate.
func BuildClientTLSConfig(m TLSMaterial) (*tls.Config, error) {
	tc := &tls.Config{MinVersion: tls.VersionTLS12}
	if m.CertFile != "" && m.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(m.CertFile, m.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("tls: load client cert/key: %w", err)
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	if m.CAFile != "" {
		pool, err := loadCAPool(m.CAFile)
		if err != nil {
			return nil, err
		}
		tc.RootCAs = pool
	}
	return tc, nil
}

func loadCAPool(caFile string) (*x509.CertPool, error) {
	data, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("tls: read CA file %s: %w", caFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("tls: no valid certificates found in %s", caFile)
	}
	return pool, nil
}

// clientTLSConfig is the TLS config every outbound inter-node connection
// this process makes (replication, gossip, election RPCs, migration,
// heartbeat pings) uses — all of them dial via NewClient/NewClientWithTimeout,
// so a single process-wide default here is what lets every one of those
// call sites (spread across the network, cluster, election, and
// heartbeat packages) pick up TLS without individually threading a
// *tls.Config parameter through each of them. Same pattern as
// net/http's DefaultTransport: a per-process default for something that
// is, in practice, singular per process. nil (the default) dials plain
// TCP — every existing test and caller that never touches this keeps
// working unchanged.
var clientTLSConfig atomic.Pointer[tls.Config]

// SetClientTLSConfig sets the TLS config used by every subsequent
// outbound inter-node connection. Pass nil to go back to plain TCP.
// Intended to be called once, at startup, before the node starts
// dialing peers.
func SetClientTLSConfig(cfg *tls.Config) {
	clientTLSConfig.Store(cfg)
}

func getClientTLSConfig() *tls.Config {
	return clientTLSConfig.Load()
}
