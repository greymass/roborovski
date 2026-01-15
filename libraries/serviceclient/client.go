package serviceclient

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/greymass/roborovski/libraries/encoding"
)

type Client struct {
	baseURL    string
	httpClient *http.Client
}

func New(backendURL string, timeout time.Duration) *Client {
	parsedURL, err := url.Parse(backendURL)
	if err != nil {
		return &Client{
			baseURL:    backendURL,
			httpClient: &http.Client{Timeout: timeout},
		}
	}

	if parsedURL.Scheme == "unix" {
		return &Client{
			baseURL: "http://localhost",
			httpClient: &http.Client{
				Timeout: timeout,
				Transport: &http.Transport{
					DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
						return net.Dial("unix", parsedURL.Path)
					},
				},
			},
		}
	}

	return &Client{
		baseURL:    backendURL,
		httpClient: &http.Client{Timeout: timeout},
	}
}

func (c *Client) Post(ctx context.Context, path string, req, resp any) error {
	body, err := encoding.JSONiter.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(httpResp.Body)
		return &ServiceError{
			StatusCode: httpResp.StatusCode,
			Message:    http.StatusText(httpResp.StatusCode),
			Body:       bodyBytes,
		}
	}

	if resp != nil {
		if err := encoding.JSONiter.NewDecoder(httpResp.Body).Decode(resp); err != nil {
			return fmt.Errorf("decode response: %w", err)
		}
	}

	return nil
}
