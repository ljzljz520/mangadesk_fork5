package core

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// PageClient : Source of a chapter's page images.
type PageClient interface {
	// Pages : Filenames of every page in the chapter, in reading order.
	Pages() []string
	// GetPage : Download a single page. The context controls the whole request,
	// including an in-flight response body.
	GetPage(ctx context.Context, filename string) ([]byte, error)
}

// AtHomeProvider : Resolve a chapter into a PageClient.
type AtHomeProvider interface {
	NewClient(ctx context.Context, req DownloadRequest) (PageClient, error)
}

// MDHomeBaseURL : MangaDex@Home server information endpoint.
const MDHomeBaseURL = "https://api.mangadex.org"

// mdHomeServerResponse : Response payload for the at-home server request.
type mdHomeServerResponse struct {
	Result  string        `json:"result"`
	BaseURL string        `json:"baseUrl"`
	Chapter mdHomeChapter `json:"chapter"`
}

type mdHomeChapter struct {
	Hash      string   `json:"hash"`
	Data      []string `json:"data"`
	DataSaver []string `json:"dataSaver"`
}

// DexAtHome : AtHomeProvider backed by the MangaDex API.
type DexAtHome struct {
	baseURL string
	client  *http.Client
}

// NewDexAtHome : Create the MangaDex@Home provider.
func NewDexAtHome() *DexAtHome {
	return &DexAtHome{
		baseURL: MDHomeBaseURL,
		client:  &http.Client{Timeout: 0},
	}
}

// NewClient : Request an at-home server for the chapter and build a page
// client. The context cancels the server lookup request.
func (d *DexAtHome) NewClient(ctx context.Context, req DownloadRequest) (PageClient, error) {
	u, err := url.Parse(d.baseURL)
	if err != nil {
		return nil, err
	}
	u.Path = fmt.Sprintf("/at-home/server/%s", url.PathEscape(req.ChapterID))
	q := u.Query()
	q.Set("forcePort443", strconv.FormatBool(req.ForcePort443))
	u.RawQuery = q.Encode()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := d.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code getting at-home server: %d", resp.StatusCode)
	}

	var r mdHomeServerResponse
	if err = json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, err
	}

	pages := r.Chapter.Data
	if req.Quality == "data-saver" {
		pages = r.Chapter.DataSaver
	}

	return &dexPageClient{
		client:  d.client,
		baseURL: strings.TrimRight(r.BaseURL, "/"),
		quality: req.Quality,
		hash:    r.Chapter.Hash,
		pages:   pages,
	}, nil
}

// dexPageClient : PageClient for a MangaDex@Home server.
type dexPageClient struct {
	client  *http.Client
	baseURL string
	quality string
	hash    string
	pages   []string
}

func (c *dexPageClient) Pages() []string {
	return c.pages
}

// GetPage : Download one page. Cancellation of ctx aborts a request that is
// still in flight, including while the response body is being read.
func (c *dexPageClient) GetPage(ctx context.Context, filename string) ([]byte, error) {
	address := strings.Join([]string{c.baseURL, c.quality, c.hash, filename}, "/")

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code downloading page: %d", resp.StatusCode)
	}

	type readResult struct {
		data []byte
		err  error
	}
	done := make(chan readResult, 1)
	go func() {
		data, err := io.ReadAll(resp.Body)
		done <- readResult{data, err}
	}()

	select {
	case result := <-done:
		return result.data, result.err
	case <-ctx.Done():
		// Closing the body unblocks the concurrent read.
		_ = resp.Body.Close()
		<-done
		return nil, ctx.Err()
	}
}
