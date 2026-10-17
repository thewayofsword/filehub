package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type TaskStatus string

const (
	StatusQueued      TaskStatus = "queued"
	StatusDownloading TaskStatus = "downloading"
	StatusPaused      TaskStatus = "paused"
	StatusVerifying   TaskStatus = "verifying" // 下载完做完整性校验
	StatusCompleted   TaskStatus = "completed"
	StatusExists      TaskStatus = "exists"  // 本地已有相同文件，跳过
	StatusCorrupt     TaskStatus = "corrupt" // 哈希不一致
	StatusCanceled    TaskStatus = "canceled"
	StatusFailed      TaskStatus = "failed"
)

var (
	errTaskNotFound       = fmt.Errorf("任务不存在")
	errTaskNotDownloading = fmt.Errorf("任务不在下载中")
	errTaskNotPaused      = fmt.Errorf("任务不在暂停状态")
)

// Task is a single download job. Path is the relative path (incl. filename)
// as seen in the peer's upload dir; Name is just the basename for display.
type Task struct {
	ID         string     `json:"id"`
	Peer       string     `json:"peer"`
	Port       int        `json:"port"`
	Path       string     `json:"path"`
	Name       string     `json:"name"`
	Size       int64      `json:"size"`
	Status     TaskStatus `json:"status"`
	Speed      int64      `json:"speed"`
	Error      string     `json:"error,omitempty"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt time.Time  `json:"finished_at"`

	downloaded atomic.Int64

	ctx    context.Context
	cancel context.CancelFunc
}

// TaskView is the JSON-serializable snapshot of a Task.
type TaskView struct {
	ID         string     `json:"id"`
	Peer       string     `json:"peer"`
	Port       int        `json:"port"`
	Path       string     `json:"path"`
	Name       string     `json:"name"`
	Size       int64      `json:"size"`
	Downloaded int64      `json:"downloaded"`
	Status     TaskStatus `json:"status"`
	Speed      int64      `json:"speed"`
	Error      string     `json:"error,omitempty"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt time.Time  `json:"finished_at"`
}

type DownloadManager struct {
	mu    sync.Mutex
	tasks map[string]*Task
	dir   string
	sem   chan struct{}
}

func NewDownloadManager(dir string) *DownloadManager {
	return &DownloadManager{
		tasks: make(map[string]*Task),
		dir:   dir,
		sem:   make(chan struct{}, 4), // max concurrent downloads
	}
}

func (dm *DownloadManager) Start(peer string, port int, rel string) *Task {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	t := &Task{
		ID:        fmt.Sprintf("%d", time.Now().UnixNano()),
		Peer:      peer,
		Port:      port,
		Path:      rel,
		Name:      filepath.Base(filepath.FromSlash(rel)),
		Status:    StatusQueued,
		StartedAt: time.Now(),
	}
	dm.tasks[t.ID] = t
	go dm.run(t)
	return t
}

func (dm *DownloadManager) List() []TaskView {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	out := make([]TaskView, 0, len(dm.tasks))
	for _, t := range dm.tasks {
		out = append(out, TaskView{
			ID:         t.ID,
			Peer:       t.Peer,
			Port:       t.Port,
			Path:       t.Path,
			Name:       t.Name,
			Size:       t.Size,
			Downloaded: t.downloaded.Load(),
			Status:     t.Status,
			Speed:      t.Speed,
			Error:      t.Error,
			StartedAt:  t.StartedAt,
			FinishedAt: t.FinishedAt,
		})
	}
	// Stable ordering by download start time so the list never jumps around.
	// Tie-break by ID (UnixNano, monotonically increasing) for same-timestamp tasks.
	sort.SliceStable(out, func(i, j int) bool {
		if !out[i].StartedAt.Equal(out[j].StartedAt) {
			return out[i].StartedAt.Before(out[j].StartedAt)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

func (dm *DownloadManager) Clear() {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	for id, t := range dm.tasks {
		switch t.Status {
		case StatusCompleted, StatusExists, StatusCorrupt, StatusFailed, StatusCanceled:
			delete(dm.tasks, id)
		}
	}
}

func (dm *DownloadManager) Pause(id string) error {
	dm.mu.Lock()
	t := dm.tasks[id]
	if t == nil {
		dm.mu.Unlock()
		return errTaskNotFound
	}
	if t.Status != StatusDownloading {
		dm.mu.Unlock()
		return errTaskNotDownloading
	}
	t.Status = StatusPaused
	cancel := t.cancel
	dm.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}

func (dm *DownloadManager) Resume(id string) error {
	dm.mu.Lock()
	t := dm.tasks[id]
	if t == nil {
		dm.mu.Unlock()
		return errTaskNotFound
	}
	if t.Status != StatusPaused {
		dm.mu.Unlock()
		return errTaskNotPaused
	}
	t.Status = StatusQueued
	dm.mu.Unlock()
	go dm.run(t)
	return nil
}

func (dm *DownloadManager) Cancel(id string) error {
	dm.mu.Lock()
	t := dm.tasks[id]
	if t == nil {
		dm.mu.Unlock()
		return errTaskNotFound
	}
	switch t.Status {
	case StatusQueued, StatusDownloading, StatusPaused:
		t.Status = StatusCanceled
	default:
		dm.mu.Unlock()
		return errTaskNotDownloading
	}
	cancel := t.cancel
	dm.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}

// run fetches metadata fast, then hands off to download under a semaphore
// slot so queue tasks carry a real size (progress shows 0% not 100%).
func (dm *DownloadManager) run(t *Task) {
	ctx, cancel := context.WithCancel(context.Background())
	dm.mu.Lock()
	t.ctx = ctx
	t.cancel = cancel
	dm.mu.Unlock()
	defer cancel()

	port := t.Port
	if port == 0 {
		port = 36774
	}
	base := fmt.Sprintf("http://%s:%d", t.Peer, port)

	// Fast metadata fetch: size only, no hashing (hashing is deferred to the end).
	remote, err := fetchRemoteFileInfo(base, t.Path)
	if err != nil {
		dm.fail(t, "获取远程文件信息失败: "+err.Error())
		return
	}
	t.Size = remote.Size

	// Acquire a download slot, honoring cancel while queued.
	select {
	case dm.sem <- struct{}{}:
	case <-ctx.Done():
		dm.markCanceled(t)
		return
	}
	defer func() { <-dm.sem }()

	dm.setStatus(t, StatusDownloading)
	dm.download(t, ctx, base, remote)
}

func (dm *DownloadManager) download(t *Task, ctx context.Context, base string, remote FileInfo) {
	final := filepath.Join(dm.dir, filepath.FromSlash(t.Path))
	// Ensure the target subdirectory exists before writing.
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		dm.fail(t, "创建目录失败: "+err.Error())
		return
	}
	part := final + ".part"

	// 1) Same-file check: a complete local file with matching hash -> skip.
	if st, err := os.Stat(final); err == nil && st.Size() == remote.Size {
		dm.setStatus(t, StatusVerifying)
		rh, _ := fetchRemoteHash(base, t.Path)
		lh, lerr := fileSHA256(final)
		if rh != "" && lerr == nil && lh == rh {
			t.downloaded.Store(remote.Size)
			dm.finish(t, StatusExists, remote.Size)
			return
		}
		dm.setStatus(t, StatusDownloading)
	}

	// 2) Determine resume offset from any existing .part file.
	var offset int64
	if st, err := os.Stat(part); err == nil {
		offset = st.Size()
	}
	if offset < 0 || offset >= remote.Size {
		if offset > remote.Size {
			_ = os.Remove(part)
		}
		offset = 0
	}
	// A .part file already full-sized: verify and finalize.
	if offset == remote.Size {
		dm.setStatus(t, StatusVerifying)
		rh, _ := fetchRemoteHash(base, t.Path)
		lh, lerr := fileSHA256(part)
		if rh != "" && lerr == nil && lh == rh {
			_ = os.Rename(part, final)
			dm.finish(t, StatusCompleted, remote.Size)
			return
		}
		dm.setStatus(t, StatusDownloading)
		offset = 0
		_ = os.Remove(part)
	}

	// 3) Issue request, resuming with a Range header when offset > 0.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		base+"/api/download?path="+url.QueryEscape(t.Path), nil)
	if err != nil {
		dm.fail(t, err.Error())
		return
	}
	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		select {
		case <-ctx.Done():
			dm.handleStop(t, part)
		default:
			dm.fail(t, "连接失败: "+err.Error())
		}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		dm.fail(t, "服务器返回 "+resp.Status)
		return
	}

	var f *os.File
	if offset > 0 && resp.StatusCode == http.StatusPartialContent {
		f, err = os.OpenFile(part, os.O_WRONLY|os.O_APPEND, 0o644)
	} else {
		f, err = os.OpenFile(part, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
		offset = 0
	}
	if err != nil {
		dm.fail(t, "创建临时文件失败: "+err.Error())
		return
	}
	defer f.Close()

	// Stream the hash as we write so we never re-read the whole file twice.
	h := sha256.New()
	if offset > 0 {
		if pf, err := os.Open(part); err == nil {
			_, _ = io.Copy(h, pf)
			_ = pf.Close()
		}
	}
	t.downloaded.Store(offset)

	last := t.downloaded.Load()
	lastT := time.Now()
	buf := make([]byte, 64*1024)

	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			if _, werr := f.Write(buf[:n]); werr != nil {
				dm.fail(t, "写入失败: "+werr.Error())
				return
			}
			_, _ = h.Write(buf[:n])
			done := t.downloaded.Add(int64(n))
			if time.Since(lastT) >= 500*time.Millisecond {
				el := time.Since(lastT).Seconds()
				if el > 0 {
					dm.setSpeed(t, int64(float64(done-last)/el))
				}
				last = done
				lastT = time.Now()
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			select {
			case <-ctx.Done():
				dm.handleStop(t, part)
			default:
				dm.fail(t, "下载中断: "+rerr.Error())
			}
			return
		}
	}

	total := t.downloaded.Load()
	if total != remote.Size {
		dm.fail(t, fmt.Sprintf("下载不完整: %d/%d 字节", total, remote.Size))
		return
	}
	_ = f.Close()

	// 4) Verify integrity, then promote .part to the final name.
	dm.finalize(t, base, part, final, hex.EncodeToString(h.Sum(nil)), remote.Size)
}

func (dm *DownloadManager) finalize(t *Task, base, part, final, localHash string, size int64) {
	dm.setStatus(t, StatusVerifying)
	rh, _ := fetchRemoteHash(base, t.Path)
	if rh != "" && rh != localHash {
		_ = os.Remove(part)
		dm.fail(t, "校验失败（文件损坏）")
		return
	}
	if err := os.Rename(part, final); err != nil {
		dm.fail(t, "重命名失败: "+err.Error())
		return
	}
	dm.finish(t, StatusCompleted, size)
}

func (dm *DownloadManager) handleStop(t *Task, part string) {
	dm.mu.Lock()
	st := t.Status
	dm.mu.Unlock()
	if st == StatusPaused {
		dm.markPaused(t)
	} else {
		_ = os.Remove(part)
		dm.markCanceled(t)
	}
}

func (dm *DownloadManager) setStatus(t *Task, s TaskStatus) {
	dm.mu.Lock()
	t.Status = s
	dm.mu.Unlock()
}

func (dm *DownloadManager) setSpeed(t *Task, sp int64) {
	dm.mu.Lock()
	t.Speed = sp
	dm.mu.Unlock()
}

func (dm *DownloadManager) fail(t *Task, msg string) {
	dm.mu.Lock()
	t.Status = StatusFailed
	t.Error = msg
	t.Speed = 0
	t.FinishedAt = time.Now()
	dm.mu.Unlock()
}

func (dm *DownloadManager) finish(t *Task, s TaskStatus, downloaded int64) {
	dm.mu.Lock()
	t.Status = s
	t.Speed = 0
	t.FinishedAt = time.Now()
	if downloaded > 0 {
		t.downloaded.Store(downloaded)
	}
	dm.mu.Unlock()
}

func (dm *DownloadManager) markPaused(t *Task) {
	dm.mu.Lock()
	t.Status = StatusPaused
	t.Speed = 0
	t.FinishedAt = time.Now()
	dm.mu.Unlock()
}

func (dm *DownloadManager) markCanceled(t *Task) {
	dm.mu.Lock()
	t.Status = StatusCanceled
	t.Speed = 0
	t.Error = ""
	t.FinishedAt = time.Now()
	dm.mu.Unlock()
}

// fetchRemoteFileInfo queries a remote FileHub for one file's metadata
// (size/mtime only — hashing is deferred via /api/hash).
func fetchRemoteFileInfo(base, rel string) (FileInfo, error) {
	client := &http.Client{Timeout: 8 * time.Second}
	resp, err := client.Get(base + "/api/file?path=" + url.QueryEscape(rel))
	if err != nil {
		return FileInfo{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return FileInfo{}, fmt.Errorf("状态 %s", resp.Status)
	}
	var fi FileInfo
	if err := json.NewDecoder(resp.Body).Decode(&fi); err != nil {
		return FileInfo{}, err
	}
	return fi, nil
}

// fetchRemoteHash asks the remote to compute (and memoize) a file's SHA-256.
func fetchRemoteHash(base, rel string) (string, error) {
	resp, err := http.Get(base + "/api/hash?path=" + url.QueryEscape(rel))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("状态 %s", resp.Status)
	}
	var m struct {
		SHA256 string `json:"sha256"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return "", err
	}
	return m.SHA256, nil
}
