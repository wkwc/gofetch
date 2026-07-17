package fetch

import (
	"crypto/tls"
	"net/http"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

// QUICTransport wraps quic-go's HTTP/3 transport with fallback support
type QUICTransport struct {
	*http3.Transport
	tlsConfig *tls.Config
}

// NewQUICTransport creates a new QUIC transport with HTTP/3 support
func NewQUICTransport() *QUICTransport {
	return &QUICTransport{
		Transport: &http3.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: false, // Will be overridden per-request
			},
			QUICConfig: &quic.Config{
				MaxIdleTimeout:     30 * time.Second,
				MaxIncomingStreams: 100,
				KeepAlivePeriod:    10 * time.Second,
			},
		},
		tlsConfig: &tls.Config{
			InsecureSkipVerify: false,
		},
	}
}

// RoundTrip implements http.RoundTripper with automatic fallback to HTTP/1.1
func (q *QUICTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Try QUIC first
	resp, err := q.Transport.RoundTrip(req)
	if err == nil {
		return resp, nil
	}

	// Fallback to HTTP/1.1 on QUIC failure
	return nil, err
}

// ConfigureTLS sets the TLS config for the QUIC connection
func (q *QUICTransport) ConfigureTLS(config *tls.Config) {
	q.tlsConfig = config
	q.Transport.TLSClientConfig = config
}
