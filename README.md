# FileHub — 局域网文件共享

简单、零配置的局域网点对点文件共享工具。解决 Windows 共享目录经常找不到、
权限混乱的痛点：每台电脑跑一个单文件可执行程序，自动发现同网段其他机器，
点一下就下载。

## 快速开始

1. 把 `upload` 目录（首次启动自动创建）里的文件就是要分享给别人的。
2. 双击运行 `filehub.exe`（Windows）或 `filehub`（macOS/Linux）。
3. 浏览器打开 `http://本机IP:36774`（启动日志会打印地址）。
4. 左侧「在线机器」自动列出同网段其他 FileHub；点击查看其共享文件，点「下载」。

- **服务端口**：`36774`（TCP，Web UI / 下载）；`36774`（UDP，机器发现）。
- 下载的文件保存到本机 `download` 目录；同名同内容自动跳过（秒传），
  未传完自动断点续传，传完校验 SHA-256，损坏会明确标红。
- **子目录**：upload 里的子目录可点击进入浏览；下载子目录里的文件会
  在 download 下保留同样的相对路径（如 `upload/docs/a.txt` → `download/docs/a.txt`）。

## 目录结构

```
filehub/
├── main.go          # 入口：端口/目录 flag、目录初始化、启动
├── discovery.go     # UDP 广播自动发现同网段机器
├── server.go        # HTTP 服务 + Web UI + 文件列表/下载接口
├── download.go      # 下载管理器：断点续传 + 哈希校验 + 进度
├── util.go          # 文件名安全校验、SHA-256
├── web/index.html   # 内嵌 Web 界面
├── docs/PLAN.md     # 需求分析与版本规划
├── release/         # 编译产物（发布到 Y:\temp\filehub）
└── go.mod
```

## 编译

```bash
# Windows (amd64)
GOOS=windows GOARCH=amd64 go build -trimpath -ldflags "-s -w" -o release/filehub-windows-amd64.exe .

# macOS (arm64 / amd64)
GOOS=darwin GOARCH=arm64 go build -trimpath -ldflags "-s -w" -o release/filehub-darwin-arm64 .
GOOS=darwin GOARCH=amd64 go build -trimpath -ldflags "-s -w" -o release/filehub-darwin-amd64 .

# Linux (amd64)
GOOS=linux GOARCH=amd64 go build -trimpath -ldflags "-s -w" -o release/filehub-linux-amd64 .
```

## 命令行参数

| 参数 | 默认 | 说明 |
|------|------|------|
| `-port` | `36774` | HTTP 服务端口 |
| `-upload` | `./upload` | 共享目录 |
| `-download` | `./download` | 下载目录 |

## API 一览

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/` | Web UI |
| GET | `/api/info` | 本机信息 |
| GET | `/api/peers` | 已发现的机器 |
| GET | `/api/list` | 本机共享文件列表（`?path=` 子目录） |
| GET | `/api/file?path=` | 单文件元信息（size/mtime，即时返回） |
| GET | `/api/hash?path=` | 单文件 SHA-256（惰性计算+缓存） |
| GET | `/api/files?peer=&port=&path=` | 本机/远程文件列表（代理） |
| GET | `/api/download?path=` | 下载文件（支持 Range 断点续传） |
| POST | `/api/download/start` | 发起下载 `{peer,port,path}` |
| POST | `/api/download/pause` | 暂停任务 `{id}` |
| POST | `/api/download/resume` | 继续任务 `{id}` |
| POST | `/api/download/cancel` | 终止任务 `{id}` |
| GET | `/api/downloads` | 下载任务状态 |
| POST | `/api/downloads/clear` | 清除已结束任务 |

## 防火墙说明

Windows 首次运行会弹出防火墙提示，请**允许**。需要放行：
- TCP `36774`（Web 界面 + 文件下载）
- UDP `36774`（机器发现）
