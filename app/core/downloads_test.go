package core

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// chapterStats : Per-chapter fake server counters.
type chapterStats struct {
	started  int64
	canceled int64
}

// hangingServer : Fake image service. Every image request blocks until the
// chapter is released or the client abandons the request. It records how many
// requests started, how many were aborted via context cancellation, and the
// peak number of simultaneous requests.
type hangingServer struct {
	srv *httptest.Server

	mu       sync.Mutex
	releases map[string]chan struct{}
	stats    map[string]*chapterStats

	active    int64
	maxActive int64
}

func newHangingServer(t *testing.T, chapters ...string) *hangingServer {
	t.Helper()
	h := &hangingServer{
		releases: map[string]chan struct{}{},
		stats:    map[string]*chapterStats{},
	}
	for _, chapter := range chapters {
		h.releases[chapter] = make(chan struct{})
		h.stats[chapter] = &chapterStats{}
	}

	h.srv = httptest.NewServer(http.HandlerFunc(h.handle))
	t.Cleanup(h.srv.Close)
	return h
}

func (h *hangingServer) handle(w http.ResponseWriter, r *http.Request) {
	chapter := r.URL.Query().Get("chapter")

	h.mu.Lock()
	stat := h.stats[chapter]
	release := h.releases[chapter]
	h.mu.Unlock()
	if stat == nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	atomic.AddInt64(&stat.started, 1)
	active := atomic.AddInt64(&h.active, 1)
	for {
		old := atomic.LoadInt64(&h.maxActive)
		if active <= old || atomic.CompareAndSwapInt64(&h.maxActive, old, active) {
			break
		}
	}
	defer atomic.AddInt64(&h.active, -1)

	select {
	case <-release:
		_, _ = io.WriteString(w, "fake-image")
	case <-r.Context().Done():
		// Client cancelled the in-flight request.
		atomic.AddInt64(&stat.canceled, 1)
	}
}

func (h *hangingServer) release(chapter string) {
	h.mu.Lock()
	ch := h.releases[chapter]
	h.mu.Unlock()
	if ch != nil {
		select {
		case <-ch:
		default:
			close(ch)
		}
	}
}

func (h *hangingServer) startedCount(chapter string) int64 {
	h.mu.Lock()
	stat := h.stats[chapter]
	h.mu.Unlock()
	if stat == nil {
		return 0
	}
	return atomic.LoadInt64(&stat.started)
}

func (h *hangingServer) canceledCount(chapter string) int64 {
	h.mu.Lock()
	stat := h.stats[chapter]
	h.mu.Unlock()
	if stat == nil {
		return 0
	}
	return atomic.LoadInt64(&stat.canceled)
}

// fakeProvider : AtHomeProvider backed by the hanging server. Page filenames
// are the full fake URLs. It also records which chapters were resolved (i.e.
// actually began downloading).
type fakeProvider struct {
	server          *hangingServer
	pagesPerChapter int

	newClientsMu sync.Mutex
	newClients   map[string]int
}

func (p *fakeProvider) NewClient(_ context.Context, req DownloadRequest) (PageClient, error) {
	p.newClientsMu.Lock()
	p.newClients[req.ChapterID]++
	p.newClientsMu.Unlock()

	pages := make([]string, p.pagesPerChapter)
	for i := range pages {
		pages[i] = fmt.Sprintf("%s/img?chapter=%s&page=%d",
			p.server.srv.URL, req.ChapterID, i+1)
	}
	// Disable keep-alives so the hanging test server can shut down promptly.
	return &fakePageClient{
		pages:  pages,
		client: &http.Client{Transport: &http.Transport{DisableKeepAlives: true}},
	}, nil
}

func (p *fakeProvider) clientCount(chapter string) int {
	p.newClientsMu.Lock()
	defer p.newClientsMu.Unlock()
	return p.newClients[chapter]
}

type fakePageClient struct {
	pages  []string
	client *http.Client
}

func (c *fakePageClient) Pages() []string { return c.pages }

func (c *fakePageClient) GetPage(ctx context.Context, filename string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, filename, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	return io.ReadAll(resp.Body)
}

// recorder : Simulates the UI success callback and tracks in-flight callbacks.
type recorder struct {
	total    int64
	inFlight int64

	mu       sync.Mutex
	statuses map[string]DownloadStatus
	success  map[string]int
}

func newRecorder() *recorder {
	return &recorder{
		statuses: map[string]DownloadStatus{},
		success:  map[string]int{},
	}
}

func (r *recorder) onChange(task *DownloadTask) {
	atomic.AddInt64(&r.total, 1)
	atomic.AddInt64(&r.inFlight, 1)
	defer atomic.AddInt64(&r.inFlight, -1)

	r.mu.Lock()
	defer r.mu.Unlock()
	r.statuses[task.Request.Key()] = task.Status()
	if task.Status() == StatusCompleted {
		r.success[task.Request.Key()]++
	}
}

func (r *recorder) status(key string) DownloadStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.statuses[key]
}

func (r *recorder) successCount(key string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.success[key]
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", msg)
}

func testRequest(downloadDir, manga, chapter string) DownloadRequest {
	return DownloadRequest{
		MangaID:       manga,
		MangaTitle:    "Manga " + manga,
		ChapterID:     chapter,
		ChapterNumber: chapter,
		ChapterTitle:  "Title " + chapter,
		Language:      "en",
		Quality:       "data",
		AsZip:         false,
		ZipType:       "zip",
		DownloadDir:   downloadDir,
	}
}

// TestDownloadQueueLifecycle verifies, against a hanging fake image service:
//   - duplicate submissions across "pages" merge while a task is active,
//   - cancelling a queued task starts no work,
//   - the shared concurrency limit is never exceeded across multiple manga,
//   - cancelling a running task aborts the in-flight request and never produces
//     a success notification,
//   - Close stops acceptance, cancels and waits for everything, and leaves no
//     UI callback waiting afterwards.
func TestDownloadQueueLifecycle(t *testing.T) {
	const concurrency = 2

	server := newHangingServer(t, "A", "B", "D", "E")
	provider := &fakeProvider{
		server:          server,
		pagesPerChapter: 1,
		newClients:      map[string]int{},
	}
	dir := t.TempDir()

	manager := NewDownloadManager(provider, concurrency)
	rec := newRecorder()
	manager.OnChange = rec.onChange

	reqA := testRequest(dir, "manga-1", "A")
	reqB := testRequest(dir, "manga-2", "B")
	reqC := testRequest(dir, "manga-1", "C")
	reqD := testRequest(dir, "manga-1", "D")
	reqE := testRequest(dir, "manga-2", "E")
	reqF := testRequest(dir, "manga-1", "F")

	// Two manga share the global limit: A and B start simultaneously.
	taskA, ok := manager.Submit(reqA)
	if !ok {
		t.Fatal("first submission of A should create a task")
	}
	if _, ok := manager.Submit(reqB); !ok {
		t.Fatal("submission of B should create a task")
	}

	waitFor(t, time.Second, func() bool {
		info, _ := manager.Task(reqA.Key())
		return info.Status == StatusRunning
	}, "A to start running")
	waitFor(t, time.Second, func() bool {
		return server.startedCount("A") == 1 && server.startedCount("B") == 1
	}, "fake server to receive exactly one image request for A and B")
	waitFor(t, time.Second, func() bool {
		return atomic.LoadInt64(&server.active) == concurrency
	}, "both download slots to be in use")

	// Cross-page duplicate submission: leaving and re-entering the manga page
	// and submitting A again must merge into the single active task.
	for i := 0; i < 2; i++ {
		merged, isNew := manager.Submit(reqA)
		if isNew {
			t.Fatal("resubmission of active task must merge, not create a new task")
		}
		if merged != taskA {
			t.Fatal("merged submission must return the existing task")
		}
	}
	if got := len(manager.Tasks()); got != 2 {
		t.Fatalf("expected 2 tracked tasks after merges, got %d", got)
	}
	time.Sleep(50 * time.Millisecond)
	if got := server.startedCount("A"); got != 1 {
		t.Fatalf("merged submissions must not start extra work, A started=%d", got)
	}

	// C queues behind the two running downloads.
	taskC, ok := manager.Submit(reqC)
	if !ok {
		t.Fatal("submission of C should create a task")
	}
	waitFor(t, time.Second, func() bool {
		info, _ := manager.Task(reqC.Key())
		return info.Status == StatusQueued
	}, "C to be queued")
	time.Sleep(50 * time.Millisecond)
	if got := server.startedCount("C"); got != 0 {
		t.Fatalf("queued task C must not have started image work, started=%d", got)
	}
	if got := provider.clientCount("C"); got != 0 {
		t.Fatalf("queued task C must not have resolved a page client, got %d", got)
	}

	// Cancel C while still queued: no work is ever done.
	if !manager.Cancel(reqC.Key()) {
		t.Fatal("cancelling queued task C should succeed")
	}
	waitFor(t, time.Second, func() bool {
		info, _ := manager.Task(reqC.Key())
		return info.Status == StatusCancelled
	}, "C to become cancelled")
	time.Sleep(50 * time.Millisecond)
	if got := server.startedCount("C"); got != 0 {
		t.Fatalf("cancelled queued task C must never start, started=%d", got)
	}
	if got := rec.successCount(reqC.Key()); got != 0 {
		t.Fatalf("cancelled task C must never report success, got %d", got)
	}

	// Release B: it completes and fires exactly one success notification.
	server.release("B")
	waitFor(t, time.Second, func() bool {
		return rec.status(reqB.Key()) == StatusCompleted
	}, "B to complete after release")
	if got := rec.successCount(reqB.Key()); got != 1 {
		t.Fatalf("completed B should fire exactly one success callback, got %d", got)
	}
	if got := server.startedCount("B"); got != 1 {
		t.Fatalf("B should download once, started=%d", got)
	}
	if _, err := os.Stat(filepath.Join(DownloadFolder(reqB), "0001")); err != nil {
		t.Fatalf("completed chapter file should exist: %v", err)
	}

	// Cancelling running A must abort the in-flight request and never succeed.
	if !manager.Cancel(reqA.Key()) {
		t.Fatal("cancelling running task A should succeed")
	}
	waitFor(t, time.Second, func() bool {
		return rec.status(reqA.Key()) == StatusCancelled
	}, "A to become cancelled")
	waitFor(t, time.Second, func() bool {
		return server.canceledCount("A") == 1
	}, "fake server to observe A's in-flight request aborted")
	if got := rec.successCount(reqA.Key()); got != 0 {
		t.Fatalf("cancelled task A must never report success, got %d", got)
	}
	if _, err := os.Stat(DownloadFolder(reqA)); !os.IsNotExist(err) {
		t.Fatalf("partial download for cancelled A should be removed, stat err=%v", err)
	}
	_ = taskC

	// A finished task is no longer part of the "active" merge window:
	// resubmitting B creates a new task that completes immediately.
	newB, isNew := manager.Submit(reqB)
	if !isNew {
		t.Fatal("resubmission of a completed chapter should create a new task")
	}
	waitFor(t, time.Second, func() bool {
		info, _ := manager.Task(reqB.Key())
		return info.Status == StatusCompleted
	}, "resubmitted B to complete")
	if newB.Status() != StatusCompleted {
		t.Fatalf("expected new B task completed, got %s", newB.Status())
	}

	// No excess work at any point: peak simultaneous image requests equals the
	// shared limit even with tasks spanning different manga.
	if got := atomic.LoadInt64(&server.maxActive); got > concurrency {
		t.Fatalf("download concurrency limit exceeded: maxActive=%d, limit=%d",
			got, concurrency)
	}

	// Exit scenario: two running downloads and one queued. Close must reject
	// new submissions, cancel everything, abort hanging requests and wait for
	// all task goroutines to finish.
	manager.Submit(reqD)
	manager.Submit(reqE)
	waitFor(t, time.Second, func() bool {
		return server.startedCount("D") == 1 && server.startedCount("E") == 1
	}, "D and E to start before shutdown")

	if _, ok := manager.Submit(reqF); !ok {
		t.Fatal("submission of F before Close should be accepted")
	}
	waitFor(t, time.Second, func() bool {
		info, _ := manager.Task(reqF.Key())
		return info.Status == StatusQueued
	}, "F to be queued before shutdown")

	closed := make(chan struct{})
	go func() {
		manager.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("Close must return after cancelling in-flight requests, not wait for them to finish")
	}

	// Queue no longer accepts work.
	if task, ok := manager.Submit(testRequest(dir, "manga-9", "G")); ok || task != nil {
		t.Fatal("no task may be accepted after Close")
	}

	for key, req := range map[string]DownloadRequest{
		"D": reqD,
		"E": reqE,
		"F": reqF,
	} {
		info, exists := manager.Task(req.Key())
		if !exists {
			t.Fatalf("task %s should still be visible after Close", key)
		}
		if info.Status != StatusCancelled {
			t.Fatalf("task %s should be cancelled after Close, got %s", key, info.Status)
		}
		if got := rec.successCount(req.Key()); got != 0 {
			t.Fatalf("cancelled-on-exit task %s must never report success, got %d", key, got)
		}
	}
	if got := server.startedCount("F"); got != 0 {
		t.Fatalf("queued task F must never start on exit, started=%d", got)
	}
	waitFor(t, time.Second, func() bool {
		return server.canceledCount("D") == 1 && server.canceledCount("E") == 1
	}, "D and E in-flight requests to be aborted on exit")

	// No callback may remain in flight after Close, and none may arrive later.
	if got := atomic.LoadInt64(&rec.inFlight); got != 0 {
		t.Fatalf("no callback should be waiting after Close, inFlight=%d", got)
	}
	callbacksAtClose := atomic.LoadInt64(&rec.total)
	time.Sleep(100 * time.Millisecond)
	if got := atomic.LoadInt64(&rec.total); got != callbacksAtClose {
		t.Fatalf("no callbacks should fire after Close, before=%d after=%d",
			callbacksAtClose, got)
	}

	// Close is idempotent.
	manager.Close()

	if got := atomic.LoadInt64(&server.maxActive); got > concurrency {
		t.Fatalf("download concurrency limit exceeded overall: maxActive=%d, limit=%d",
			got, concurrency)
	}
}

// TestRequestKey : Identity must cover the frozen dimensions that change the
// downloaded artifact.
func TestRequestKey(t *testing.T) {
	base := testRequest(t.TempDir(), "m1", "c1")
	cases := []struct {
		name string
		mut  func(*DownloadRequest)
		same bool
	}{
		{"identical", func(r *DownloadRequest) {}, true},
		{"different download dir", func(r *DownloadRequest) { r.DownloadDir = "/elsewhere" }, true},
		{"different manga", func(r *DownloadRequest) { r.MangaID = "m2" }, false},
		{"different chapter", func(r *DownloadRequest) { r.ChapterID = "c2" }, false},
		{"different quality", func(r *DownloadRequest) { r.Quality = "data-saver" }, false},
		{"different format", func(r *DownloadRequest) { r.AsZip = true }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			other := base
			tc.mut(&other)
			if got := other.Key() == base.Key(); got != tc.same {
				t.Fatalf("key equality=%v, want %v (base=%q other=%q)",
					got, tc.same, base.Key(), other.Key())
			}
		})
	}
}
