package core

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// DefaultDownloadConcurrency : Maximum number of chapter downloads that may run
// at the same time. The limit is shared by every manga in the application.
const DefaultDownloadConcurrency = 3

// DownloadStatus : Lifecycle state of a download task.
type DownloadStatus int

const (
	// StatusQueued : Task is accepted and waiting for a download slot.
	StatusQueued DownloadStatus = iota
	// StatusRunning : Task is actively downloading.
	StatusRunning
	// StatusCompleted : Task finished successfully.
	StatusCompleted
	// StatusFailed : Task finished with an error.
	StatusFailed
	// StatusCancelled : Task was cancelled before or during the download.
	StatusCancelled
)

// String : Human readable status.
func (s DownloadStatus) String() string {
	switch s {
	case StatusQueued:
		return "Waiting"
	case StatusRunning:
		return "Downloading"
	case StatusCompleted:
		return "Done"
	case StatusFailed:
		return "Failed"
	case StatusCancelled:
		return "Cancelled"
	default:
		return "Unknown"
	}
}

// Active reports whether the task may still perform work.
func (s DownloadStatus) Active() bool {
	return s == StatusQueued || s == StatusRunning
}

// DownloadRequest : Fully frozen description of a chapter download. Values are
// captured at submission time, so a task is unaffected by later configuration
// changes or by the user leaving the manga page.
type DownloadRequest struct {
	MangaID       string
	MangaTitle    string
	ChapterID     string
	ChapterNumber string
	ChapterTitle  string
	Language      string

	// Frozen download settings.
	Quality      string // "data" or "data-saver".
	ForcePort443 bool
	AsZip        bool
	ZipType      string
	DownloadDir  string
}

// Key : Identity of a task. Two requests with the same key describe the same
// download while the task is still active.
func (r DownloadRequest) Key() string {
	return strings.Join([]string{
		r.MangaID,
		r.ChapterID,
		r.Quality,
		fmt.Sprintf("%t", r.AsZip),
		r.ZipType,
	}, "|")
}

// DownloadTask : A chapter download owned by the application.
type DownloadTask struct {
	Request   DownloadRequest
	Submitted time.Time

	status DownloadStatus
	err    error

	ctx    context.Context
	cancel context.CancelFunc
}

// Status : Current lifecycle state of the task.
func (t *DownloadTask) Status() DownloadStatus {
	return t.status
}

// ErrText : Error message if the task failed.
func (t *DownloadTask) ErrText() string {
	if t.err == nil {
		return ""
	}
	return t.err.Error()
}

// TaskInfo : Snapshot of a task safe for use outside the manager.
type TaskInfo struct {
	Key     string
	Request DownloadRequest
	Status  DownloadStatus
	Err     string
}

// DownloadManager : Application-owned bounded download queue. Tasks submitted
// from any manga page share the same concurrency limit and keep running after
// the page that submitted them is closed.
type DownloadManager struct {
	provider    AtHomeProvider
	concurrency int
	sem         chan struct{}

	mu        sync.Mutex
	tasks     map[string]*DownloadTask
	order     []string // Submission order of the keys currently in tasks.
	accepting bool
	closed    bool
	seq       uint64

	wg sync.WaitGroup // Tracks every task goroutine.

	// OnChange is invoked (without holding manager locks) whenever a task
	// transitions state. The callback must not block.
	OnChange func(*DownloadTask)
}

// NewDownloadManager : Create a download manager backed by the given provider.
// The manager starts accepting tasks immediately.
func NewDownloadManager(provider AtHomeProvider, concurrency int) *DownloadManager {
	if concurrency < 1 {
		concurrency = 1
	}
	m := &DownloadManager{
		provider:    provider,
		concurrency: concurrency,
		sem:         make(chan struct{}, concurrency),
		tasks:       map[string]*DownloadTask{},
		accepting:   true,
	}
	return m
}

// Concurrency : Return the shared download concurrency limit.
func (m *DownloadManager) Concurrency() int {
	return m.concurrency
}

// Submit : Enqueue a chapter download. If an identical task (same manga,
// chapter, quality and format) is still active, it is merged and the existing
// task is returned with ok=false. Returns ok=true when a new task was created.
// After Close, no new tasks are accepted.
func (m *DownloadManager) Submit(req DownloadRequest) (task *DownloadTask, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.accepting {
		return nil, false
	}

	key := req.Key()
	if existing, exists := m.tasks[key]; exists && existing.status.Active() {
		return existing, false
	}

	m.seq++
	ctx, cancel := context.WithCancel(context.Background())
	task = &DownloadTask{
		Request:   req,
		Submitted: time.Now(),
		status:    StatusQueued,
		ctx:       ctx,
		cancel:    cancel,
	}
	if _, exists := m.tasks[key]; !exists {
		m.order = append(m.order, key)
	}
	m.tasks[key] = task

	m.wg.Add(1)
	go m.run(task)
	return task, true
}

// Cancel : Cancel a queued or running task. Cancelling a queued task guarantees
// its download never starts; cancelling a running task propagates the
// cancellation into its in-flight HTTP requests.
func (m *DownloadManager) Cancel(key string) bool {
	m.mu.Lock()
	task, ok := m.tasks[key]
	if !ok || !task.status.Active() {
		m.mu.Unlock()
		return false
	}
	m.mu.Unlock()

	task.cancel()
	return true
}

// Tasks : Return a snapshot of all known tasks in submission order.
func (m *DownloadManager) Tasks() []TaskInfo {
	m.mu.Lock()
	defer m.mu.Unlock()

	snapshot := make([]TaskInfo, 0, len(m.order))
	for _, key := range m.order {
		task, ok := m.tasks[key]
		if !ok {
			continue
		}
		snapshot = append(snapshot, TaskInfo{
			Key:     key,
			Request: task.Request,
			Status:  task.status,
			Err:     task.ErrText(),
		})
	}
	return snapshot
}

// Task : Look up the current state of a single task.
func (m *DownloadManager) Task(key string) (TaskInfo, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	task, ok := m.tasks[key]
	if !ok {
		return TaskInfo{}, false
	}
	return TaskInfo{
		Key:     key,
		Request: task.Request,
		Status:  task.status,
		Err:     task.ErrText(),
	}, true
}

// Prune : Remove tasks that have reached a terminal state. Active tasks are
// kept.
func (m *DownloadManager) Prune() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	pruned := 0
	kept := make([]string, 0, len(m.order))
	for _, key := range m.order {
		task, ok := m.tasks[key]
		if ok && task.status.Active() {
			kept = append(kept, key)
			continue
		}
		delete(m.tasks, key)
		pruned++
	}
	m.order = kept
	return pruned
}

// Close : Stop accepting new tasks, cancel every active task and wait for all
// task goroutines to finish. Safe to call multiple times.
func (m *DownloadManager) Close() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		m.wg.Wait()
		return
	}
	m.closed = true
	m.accepting = false

	// Cancel all active tasks. Queued tasks exit without doing any work,
	// running tasks have their in-flight requests aborted.
	tasks := make([]*DownloadTask, 0, len(m.tasks))
	for _, task := range m.tasks {
		if task.status.Active() {
			tasks = append(tasks, task)
		}
	}
	m.mu.Unlock()

	for _, task := range tasks {
		task.cancel()
	}

	// Wait for every task goroutine to end before returning. No UI callback
	// can be in flight after this returns.
	m.wg.Wait()
}

// run : Goroutine for a single task. It acquires a slot from the shared
// semaphore, so at most the configured number of chapter downloads run at the
// same time across all manga.
func (m *DownloadManager) run(task *DownloadTask) {
	defer m.wg.Done()

	// Wait for a free download slot, but abort immediately if the task was
	// cancelled while still queued (or the application is shutting down).
	select {
	case m.sem <- struct{}{}:
		defer func() { <-m.sem }()
	case <-task.ctx.Done():
		m.transition(task, StatusCancelled, nil)
		return
	}

	if task.ctx.Err() != nil {
		m.transition(task, StatusCancelled, nil)
		return
	}

	m.transition(task, StatusRunning, nil)

	err := m.download(task.ctx, task.Request)
	switch {
	case err == nil:
		m.transition(task, StatusCompleted, nil)
	case task.ctx.Err() != nil || errors.Is(err, context.Canceled):
		// Remove partial output so a cancelled download never looks complete.
		_ = os.RemoveAll(DownloadFolder(task.Request))
		m.transition(task, StatusCancelled, err)
	default:
		_ = os.RemoveAll(DownloadFolder(task.Request))
		m.transition(task, StatusFailed, err)
	}
}

// transition : Move a task to a new state and notify listeners. Only forward
// transitions out of an active state are applied.
func (m *DownloadManager) transition(task *DownloadTask, status DownloadStatus, err error) {
	m.mu.Lock()
	if !task.status.Active() {
		m.mu.Unlock()
		return
	}
	task.status = status
	task.err = err
	onChange := m.OnChange
	m.mu.Unlock()

	log.Printf("Download task %s: %s", task.Request.Key(), status)
	if onChange != nil {
		onChange(task)
	}
}

// download : Fetch and persist a single chapter. The supplied context is
// threaded into every request so that cancellation reaches in-flight calls.
func (m *DownloadManager) download(ctx context.Context, req DownloadRequest) error {
	client, err := m.provider.NewClient(ctx, req)
	if err != nil {
		return err
	}

	folder := DownloadFolder(req)
	if err = os.MkdirAll(folder, os.ModePerm); err != nil {
		return err
	}

	for num, page := range client.Pages() {
		if err = ctx.Err(); err != nil {
			return err
		}

		image, err := client.GetPage(ctx, page)
		if err != nil {
			return err
		}

		filename := fmt.Sprintf("%04d%s", num+1, filepath.Ext(page))
		if err = os.WriteFile(filepath.Join(folder, filename), image, os.ModePerm); err != nil {
			return err
		}
	}

	if req.AsZip {
		if err = ctx.Err(); err != nil {
			return err
		}
		if err = saveAsZip(folder); err != nil {
			return err
		}
	}
	return nil
}

// saveAsZip : Package a chapter folder's images into a zip archive, replacing
// the folder with the archive file.
func saveAsZip(chapterFolder string) error {
	tempZip := fmt.Sprintf("%s.%s", chapterFolder, "temp")

	zipFile, err := os.Create(tempZip)
	if err != nil {
		return err
	}
	w := zip.NewWriter(zipFile)

	if err = filepath.WalkDir(chapterFolder, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		fileOriginal, err := os.Open(path)
		if err != nil {
			return err
		}
		defer func() {
			_ = fileOriginal.Close()
		}()

		fh := zip.FileHeader{
			Name:     d.Name(),
			Modified: time.Now(),
			Method:   zip.Deflate,
		}
		fileZip, err := w.CreateHeader(&fh)
		if err != nil {
			return err
		}

		_, err = io.Copy(fileZip, fileOriginal)
		return err
	}); err != nil {
		return err
	}

	if err = w.Close(); err != nil {
		return err
	}
	if err = zipFile.Close(); err != nil {
		return err
	}
	if err = os.RemoveAll(chapterFolder); err != nil {
		return err
	}
	return os.Rename(tempZip, chapterFolder)
}

// DownloadFolder : Compute the on-disk path for a chapter download based on the
// frozen request parameters.
func DownloadFolder(req DownloadRequest) string {
	mangaName := req.MangaTitle
	chapterName := fmt.Sprintf("Chapter %s [%s-%s] %s - %s",
		req.ChapterNumber, req.Language, req.Quality,
		req.ChapterTitle, strings.SplitN(req.ChapterID, "-", 2)[0])

	restricted := []string{"<", ">", ":", "/", "|", "?", "*", "\"", "\\", "."}
	for _, c := range restricted {
		mangaName = strings.ReplaceAll(mangaName, c, "-")
		chapterName = strings.ReplaceAll(chapterName, c, "-")
	}

	folder := filepath.Join(req.DownloadDir, mangaName, chapterName)
	if req.AsZip {
		folder = fmt.Sprintf("%s.%s", folder, req.ZipType)
	}
	return folder
}
