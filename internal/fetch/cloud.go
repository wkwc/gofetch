package fetch

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// CloudProvider is the interface for cloud storage backends
type CloudProvider interface {
	// Get returns a reader for the object
	Get(ctx context.Context, bucket, key string) (io.ReadCloser, error)

	// Head returns metadata without downloading
	Head(ctx context.Context, bucket, key string) (ObjectInfo, error)

	// PresignURL generates a pre-signed URL for GET
	PresignURL(ctx context.Context, bucket, key string, expiry time.Duration) (string, error)

	// Name returns the provider name
	Name() string
}

type ObjectInfo struct {
	Size         int64
	LastModified time.Time
	ETag         string
	ContentType  string
	Metadata     map[string]string
}

// S3Provider implements CloudProvider for AWS S3
type S3Provider struct {
	region    string
	accessKey string
	secretKey string
	endpoint  string
	client    *http.Client
}

func NewS3Provider(accessKey, secretKey, region string) *S3Provider {
	return &S3Provider{
		region:    region,
		accessKey: accessKey,
		secretKey: secretKey,
		endpoint:  "s3.amazonaws.com",
		client:    &http.Client{Timeout: 30 * time.Second},
	}
}

func (p *S3Provider) Get(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	url := p.buildURL(bucket, key, nil)
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	p.signRequest(req)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("S3 GET failed: %s", resp.Status)
	}
	return resp.Body, nil
}

func (p *S3Provider) Head(ctx context.Context, bucket, key string) (ObjectInfo, error) {
	url := p.buildURL(bucket, key, nil)
	req, _ := http.NewRequestWithContext(ctx, "HEAD", url, nil)
	p.signRequest(req)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return ObjectInfo{}, fmt.Errorf("HEAD failed: %s", resp.Status)
	}

	return ObjectInfo{
		Size:         resp.ContentLength,
		LastModified: parseTime(resp.Header.Get("Last-Modified")),
		ETag:         strings.Trim(resp.Header.Get("ETag"), `"`),
		ContentType:  resp.Header.Get("Content-Type"),
	}, nil
}

func (p *S3Provider) PresignURL(ctx context.Context, bucket, key string, expiry time.Duration) (string, error) {
	params := url.Values{
		"X-Amz-Algorithm":     {"AWS4-HMAC-SHA256"},
		"X-Amz-Credential":    {p.accessKey + "/" + p.credentialScope()},
		"X-Amz-Date":          {time.Now().UTC().Format("20060102T150405Z")},
		"X-Amz-Expires":       {fmt.Sprintf("%d", int(expiry.Seconds()))},
		"X-Amz-SignedHeaders": {"host"},
	}

	url := fmt.Sprintf("https://%s.%s/%s?%s", bucket, p.endpoint, key, params.Encode())
	sig := p.signature("GET", "/"+bucket+"/"+key, params)
	url += "&X-Amz-Signature=" + sig
	return url, nil
}

func (p *S3Provider) Name() string { return "S3" }

func (p *S3Provider) buildURL(bucket, key string, params url.Values) string {
	if len(params) > 0 {
		return fmt.Sprintf("https://%s.%s/%s?%s", bucket, p.endpoint, key, params.Encode())
	}
	return fmt.Sprintf("https://%s.%s/%s", bucket, p.endpoint, key)
}

func (p *S3Provider) signRequest(req *http.Request) {
	// AWS Signature Version 4 implementation
	// Simplified for brevity - production would use AWS SDK
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=...")
}

func (p *S3Provider) sign(method, path string, params url.Values) string {
	// Simplified - use AWS SDK in production
	return "signature"
}

func (p *S3Provider) credentialScope() string {
	now := time.Now().UTC()
	return fmt.Sprintf("%s/%s/s3/aws4_request", now.Format("20060102"), "us-east-1")
}

func (p *S3Provider) signature(method, path string, params url.Values) string {
	// Simplified - use AWS SDK in production
	return "signature"
}

// GSProvider implements CloudProvider for Google Cloud Storage
type GSProvider struct {
	projectID   string
	credentials string
	client      *http.Client
}

func NewGSProvider(projectID, credentialsFile string) *GSProvider {
	return &GSProvider{
		projectID: projectID,
		client:    &http.Client{Timeout: 30 * time.Second},
	}
}

func (p *GSProvider) Get(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	url := fmt.Sprintf("https://storage.googleapis.com/%s/%s", bucket, key)
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	req.Header.Set("Authorization", "Bearer "+p.getToken())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (p *GSProvider) Head(ctx context.Context, bucket, key string) (ObjectInfo, error) {
	url := fmt.Sprintf("https://storage.googleapis.com/%s/%s", bucket, key)
	req, _ := http.NewRequestWithContext(ctx, "HEAD", url, nil)
	req.Header.Set("Authorization", "Bearer "+p.getToken())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer resp.Body.Close()
	return ObjectInfo{
		Size:         resp.ContentLength,
		ContentType:  resp.Header.Get("Content-Type"),
		ETag:         strings.Trim(resp.Header.Get("ETag"), `"`),
		LastModified: parseTime(resp.Header.Get("Last-Modified")),
	}, nil
}

func (p *GSProvider) PresignURL(ctx context.Context, bucket, key string, expiry time.Duration) (string, error) {
	return fmt.Sprintf("https://storage.googleapis.com/%s/%s?signed=true&expires=%d", bucket, key, int64(time.Now().Add(expiry).Unix())), nil
}

func (p *GSProvider) Name() string { return "GS" }

func (p *GSProvider) getToken() string {
	// Use gcloud auth or service account
	return "token"
}

// AzureProvider implements CloudProvider for Azure Blob Storage
type AzureProvider struct {
	accountName string
	accountKey  string
	client      *http.Client
}

func NewAzureProvider(accountName, accountKey string) *AzureProvider {
	return &AzureProvider{
		accountName: accountName,
		accountKey:  accountKey,
		client:      &http.Client{Timeout: 30 * time.Second},
	}
}

func (p *AzureProvider) Get(ctx context.Context, container, blob string) (io.ReadCloser, error) {
	url := fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", p.accountName, container, blob)
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
	p.signRequest(req)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (p *AzureProvider) Head(ctx context.Context, container, blob string) (ObjectInfo, error) {
	url := fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s", p.accountName, container, blob)
	req, _ := http.NewRequestWithContext(ctx, "HEAD", url, nil)
	p.signRequest(req)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return ObjectInfo{}, err
	}
	defer resp.Body.Close()
	return ObjectInfo{
		Size:         resp.ContentLength,
		ContentType:  resp.Header.Get("Content-Type"),
		ETag:         strings.Trim(resp.Header.Get("ETag"), `"`),
		LastModified: parseTime(resp.Header.Get("Last-Modified")),
	}, nil
}

func (p *AzureProvider) PresignURL(ctx context.Context, container, blob string, expiry time.Duration) (string, error) {
	// Generate SAS token
	sas := generateSAS(p.accountName, p.accountKey, container, blob, expiry)
	return fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s?%s", p.accountName, container, blob, sas), nil
}

func (p *AzureProvider) Name() string { return "Azure" }

func (p *AzureProvider) signRequest(req *http.Request) {
	// Azure Shared Key or SAS auth
}

func parseTime(s string) time.Time {
	t, _ := time.Parse(http.TimeFormat, s)
	return t
}

func generateSAS(account, key, container, blob string, expiry time.Duration) string {
	// Generate Azure SAS token
	return "sas=token"
}

// MultiCloudProvider aggregates multiple cloud providers
type MultiCloudProvider struct {
	providers map[string]CloudProvider
}

func NewMultiCloudProvider() *MultiCloudProvider {
	return &MultiCloudProvider{providers: make(map[string]CloudProvider)}
}

func (m *MultiCloudProvider) Add(provider CloudProvider) {
	m.providers[provider.Name()] = provider
}

func (m *MultiCloudProvider) Get(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	// Try each provider until one succeeds
	for _, p := range m.providers {
		rc, err := p.Get(ctx, "", "")
		if err == nil {
			return rc, nil
		}
	}
	return nil, fmt.Errorf("no provider succeeded")
}

func (m *MultiCloudProvider) Head(ctx context.Context, bucket, key string) (ObjectInfo, error) {
	for _, p := range m.providers {
		info, err := p.Head(ctx, "", "")
		if err == nil {
			return info, nil
		}
	}
	return ObjectInfo{}, fmt.Errorf("not found")
}

func (m *MultiCloudProvider) PresignURL(ctx context.Context, bucket, key string, expiry time.Duration) (string, error) {
	for _, p := range m.providers {
		url, err := p.PresignURL(ctx, "", "", expiry)
		if err == nil {
			return url, nil
		}
	}
	return "", fmt.Errorf("no provider could presign")
}

func (m *MultiCloudProvider) Name() string { return "MultiCloud" }
