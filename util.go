package main

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path"
	"strings"
)

// safeRelPath validates a forward-slash relative path used to address files
// and folders inside the upload/download dirs. It rejects absolute paths,
// backslashes, ".." segments, and empty/duplicated segments.
func safeRelPath(p string) bool {
	if p == "" || strings.Contains(p, "\\") || strings.HasPrefix(p, "/") {
		return false
	}
	c := path.Clean(p)
	if c != p || c == "." || c == ".." {
		return false
	}
	for _, seg := range strings.Split(c, "/") {
		if seg == ".." {
			return false
		}
	}
	return true
}

// validSubdir is like safeRelPath but also accepts "" (the upload root).
func validSubdir(sub string) bool {
	if sub == "" {
		return true
	}
	return safeRelPath(sub)
}

// fileSHA256 returns the hex-encoded SHA-256 of the file at filePath.
func fileSHA256(filePath string) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
