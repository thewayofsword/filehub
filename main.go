package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const Version = "0.1.3"

func main() {
	port := flag.Int("port", 36774, "HTTP 服务端口")
	upload := flag.String("upload", "", "共享目录（默认：当前目录下的 upload）")
	download := flag.String("download", "", "下载目录（默认：当前目录下的 download）")
	flag.Parse()

	workdir, _ := os.Getwd()
	uploadDir := *upload
	if uploadDir == "" {
		uploadDir = filepath.Join(workdir, "upload")
	}
	downloadDir := *download
	if downloadDir == "" {
		downloadDir = filepath.Join(workdir, "download")
	}

	if err := os.MkdirAll(uploadDir, 0o755); err != nil {
		log.Fatalf("创建 upload 目录失败: %v", err)
	}
	if err := os.MkdirAll(downloadDir, 0o755); err != nil {
		log.Fatalf("创建 download 目录失败: %v", err)
	}

	hostname, _ := os.Hostname()

	disc := NewDiscovery(hostname, Version, *port)
	if err := disc.Start(); err != nil {
		log.Fatalf("启动发现服务失败（UDP %d）: %v", discoveryPort, err)
	}

	srv := &Server{
		hostname:    hostname,
		version:     Version,
		port:        *port,
		uploadDir:   uploadDir,
		downloadDir: downloadDir,
		disc:        disc,
		dm:          NewDownloadManager(downloadDir),
		hashCache:   make(map[string]string),
	}

	log.Printf("FileHub v%s 已启动", Version)
	log.Printf("  本机地址: %s", strings.Join(localIPList(), ", "))
	log.Printf("  HTTP 端口: %d", *port)
	log.Printf("  共享目录 (upload):   %s", uploadDir)
	log.Printf("  下载目录 (download): %s", downloadDir)
	log.Printf("  访问地址: http://%s:%d", primaryLocalIP(), *port)

	if err := http.ListenAndServe(":"+strconv.Itoa(*port), srv.routes()); err != nil {
		log.Fatalf("HTTP 服务启动失败: %v", err)
	}
}
