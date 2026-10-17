package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

//go:embed web/index.html
var indexHTML []byte

// FileEntry describes one shared file (or folder) in the upload dir.
type FileEntry struct {
	Name  string `json:"name"`
	Size  int64  `json:"size"`
	MTime int64  `json:"mtime"` // unix seconds
	IsDir bool   `json:"is_dir"`
}

// FileInfo describes a single remote file including its content hash,
// used for same-file comparison and integrity verification.
type FileInfo struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	MTime  int64  `json:"mtime"`
	SHA256 string `json:"sha256"`
}

type Server struct {
	hostname    string
	version     string
	port        int
	uploadDir   string
	downloadDir string
	disc        *Discovery
	dm          *DownloadManager

	hashMu    sync.Mutex
	hashCache map[string]string
}

func (s *Server) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/api/info", s.handleInfo)
	mux.HandleFunc("/api/peers", s.handlePeers)
	mux.HandleFunc("/api/list", s.handleList)
	mux.HandleFunc("/api/file", s.handleFile)
	mux.HandleFunc("/api/hash", s.handleHash)
	mux.HandleFunc("/api/files", s.handleFilesProxy)
	mux.HandleFunc("/api/download", s.handleDownload)
	mux.HandleFunc("/api/download/start", s.handleDownloadStart)
	mux.HandleFunc("/api/download/pause", s.handleDownloadPause)
	mux.HandleFunc("/api/download/resume", s.handleDownloadResume)
	mux.HandleFunc("/api/download/cancel", s.handleDownloadCancel)
	mux.HandleFunc("/api/downloads", s.handleDownloads)
	mux.HandleFunc("/api/downloads/clear", s.handleDownloadsClear)
	return mux
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(indexHTML)
}

func (s *Server) handleInfo(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]interface{}{
		"hostname": s.hostname,
		"ip":       primaryLocalIP(),
		"port":     s.port,
		"version":  s.version,
	})
}

func (s *Server) handlePeers(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.disc.Peers())
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	sub := r.URL.Query().Get("path")
	if !validSubdir(sub) {
		http.Error(w, "非法路径", http.StatusBadRequest)
		return
	}
	entries, err := s.listUpload(sub)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, entries)
}

// listUpload lists the contents of a subdirectory of the upload dir.
// sub == "" means the upload root.
func (s *Server) listUpload(sub string) ([]FileEntry, error) {
	dir := s.uploadDir
	if sub != "" {
		dir = filepath.Join(dir, filepath.FromSlash(sub))
	}
	dirs, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	out := make([]FileEntry, 0, len(dirs))
	for _, d := range dirs {
		info, err := d.Info()
		if err != nil {
			continue
		}
		out = append(out, FileEntry{
			Name:  d.Name(),
			Size:  info.Size(),
			MTime: info.ModTime().Unix(),
			IsDir: d.IsDir(),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].IsDir != out[j].IsDir {
			return out[i].IsDir // directories first
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

// handleFile returns fast metadata (name/size/mtime) only — no hashing,
// so starting a download is not blocked by hashing a large file.
func (s *Server) handleFile(w http.ResponseWriter, r *http.Request) {
	rel := r.URL.Query().Get("path")
	if !safeRelPath(rel) {
		http.Error(w, "非法文件名", http.StatusBadRequest)
		return
	}
	path := s.resolveUpload(rel)
	st, err := os.Stat(path)
	if err != nil {
		http.Error(w, "文件不存在", http.StatusNotFound)
		return
	}
	writeJSON(w, FileInfo{
		Name:  filepath.Base(rel),
		Size:  st.Size(),
		MTime: st.ModTime().Unix(),
	})
}

// handleHash computes (and memoizes) a file's SHA-256. Called lazily at the
// end of a download for integrity verification.
func (s *Server) handleHash(w http.ResponseWriter, r *http.Request) {
	rel := r.URL.Query().Get("path")
	if !safeRelPath(rel) {
		http.Error(w, "非法文件名", http.StatusBadRequest)
		return
	}
	path := s.resolveUpload(rel)
	st, err := os.Stat(path)
	if err != nil {
		http.Error(w, "文件不存在", http.StatusNotFound)
		return
	}
	writeJSON(w, map[string]interface{}{
		"name":   filepath.Base(rel),
		"size":   st.Size(),
		"sha256": s.cachedHash(rel, path, st),
	})
}

// resolveUpload maps a validated relative path to an absolute path inside the
// upload dir. Callers must pass a safeRelPath-validated value.
func (s *Server) resolveUpload(rel string) string {
	return filepath.Join(s.uploadDir, filepath.FromSlash(rel))
}

// cachedHash computes (and memoizes) a file's SHA-256 keyed by relpath+size+mtime
// so we hash each file at most once per content version.
func (s *Server) cachedHash(rel, path string, st os.FileInfo) string {
	key := fmt.Sprintf("%s|%d|%d", rel, st.Size(), st.ModTime().UnixNano())
	s.hashMu.Lock()
	if h, ok := s.hashCache[key]; ok {
		s.hashMu.Unlock()
		return h
	}
	s.hashMu.Unlock()

	h, err := fileSHA256(path)
	if err != nil {
		return ""
	}

	s.hashMu.Lock()
	s.hashCache[key] = h
	s.hashMu.Unlock()
	return h
}

// handleFilesProxy lists files either locally (no peer) or by proxying to a
// remote FileHub instance, so the browser only ever talks to its own node.
func (s *Server) handleFilesProxy(w http.ResponseWriter, r *http.Request) {
	peer := strings.TrimSpace(r.URL.Query().Get("peer"))
	port := strings.TrimSpace(r.URL.Query().Get("port"))
	sub := r.URL.Query().Get("path")

	if peer == "" || peer == primaryLocalIP() {
		if !validSubdir(sub) {
			http.Error(w, "非法路径", http.StatusBadRequest)
			return
		}
		entries, err := s.listUpload(sub)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, entries)
		return
	}

	if port == "" {
		port = fmt.Sprintf("%d", s.port)
	}
	base := fmt.Sprintf("http://%s:%s", peer, port)
	resp, err := http.Get(base + "/api/list?path=" + url.QueryEscape(sub))
	if err != nil {
		http.Error(w, "无法连接 "+peer+": "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_, _ = io.Copy(w, resp.Body)
}

// handleDownload serves a shared file. http.ServeContent handles Range
// requests, which enables client-side resumable downloads.
func (s *Server) handleDownload(w http.ResponseWriter, r *http.Request) {
	rel := r.URL.Query().Get("path")
	if !safeRelPath(rel) {
		http.Error(w, "非法文件名", http.StatusBadRequest)
		return
	}
	path := s.resolveUpload(rel)
	f, err := os.Open(path)
	if err != nil {
		http.Error(w, "文件不存在", http.StatusNotFound)
		return
	}
	defer f.Close()

	st, err := f.Stat()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	name := filepath.Base(rel)
	w.Header().Set("Content-Disposition", `attachment; filename*=UTF-8''`+url.PathEscape(name))
	http.ServeContent(w, r, name, st.ModTime(), f)
}

func (s *Server) handleDownloadStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "仅支持 POST", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Peer string `json:"peer"`
		Port int    `json:"port"`
		Path string `json:"path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "请求格式错误", http.StatusBadRequest)
		return
	}
	if !safeRelPath(req.Path) || req.Peer == "" {
		http.Error(w, "参数错误", http.StatusBadRequest)
		return
	}
	if req.Port == 0 {
		req.Port = s.port
	}
	t := s.dm.Start(req.Peer, req.Port, req.Path)
	writeJSON(w, map[string]string{"id": t.ID})
}

func (s *Server) handleDownloads(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.dm.List())
}

func (s *Server) handleDownloadsClear(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "仅支持 POST", http.StatusMethodNotAllowed)
		return
	}
	s.dm.Clear()
	writeJSON(w, map[string]bool{"ok": true})
}

func (s *Server) handleDownloadPause(w http.ResponseWriter, r *http.Request) {
	s.handleTaskControl(w, r, func(id string) error { return s.dm.Pause(id) })
}

func (s *Server) handleDownloadResume(w http.ResponseWriter, r *http.Request) {
	s.handleTaskControl(w, r, func(id string) error { return s.dm.Resume(id) })
}

func (s *Server) handleDownloadCancel(w http.ResponseWriter, r *http.Request) {
	s.handleTaskControl(w, r, func(id string) error { return s.dm.Cancel(id) })
}

func (s *Server) handleTaskControl(w http.ResponseWriter, r *http.Request, fn func(id string) error) {
	if r.Method != http.MethodPost {
		http.Error(w, "仅支持 POST", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "请求格式错误", http.StatusBadRequest)
		return
	}
	if err := fn(req.ID); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, map[string]bool{"ok": true})
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(v)
}
