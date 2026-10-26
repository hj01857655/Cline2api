// Cline2api-go：OpenAI 兼容反代 → api.cline.bot（ClinePass 通道，锁免费模型）。
// Python 版 Cline2api 的 Go 迁移，结构对应：
//
//	config/   server.py 的常量与 .env
//	cred/     credential.py + oauth.py（凭据、刷新、WorkOS 设备流登录）
//	freelist/ server.py 的免费模型清单（recommended-models / free_cache.json）
//	dump/     server.py 的请求/响应原文落盘
//	upstream/ server.py 的 _upstream_headers + routes/chat.py 的 _post_chat（401 自愈）
//	adapters/ adapters/anthropic.py + adapters/responses.py（协议桥）
//	handler/  server.py 调度 + routes/ 三协议路由
//
// 用法：cline2api [login]  —— 不带参数启动服务，login 走 WorkOS 设备流。
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"cline2api/internal/config"
	"cline2api/internal/cred"
	"cline2api/internal/dump"
	"cline2api/internal/freelist"
	"cline2api/internal/handler"
	"cline2api/internal/upstream"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "login" {
		if _, err := cred.Login(); err != nil {
			fmt.Printf("[oauth] ❌ 登录失败: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("[oauth] 把上面文件内容迁到状态目录 credentials.json（或设 CLINE2API_STATE_DIR）后即可启动服务")
		return
	}

	cfg := config.Load()
	logger := handler.NewLogger(cfg.StateDir)
	logi := func(msg string, args ...any) { logger.Info(msg, args...) }

	store := cred.NewStore(cfg.StateDir)
	info, err := store.Load()
	if err != nil {
		fmt.Printf("凭证不可用: %v\n", err)
		fmt.Println("请先登录：cline2api login")
		os.Exit(1)
	}

	// 启动预检：磁盘 token 已临期就先续上，别等用户第一个请求吃 401 才刷
	// （失败不阻断启动，交给 401 自愈路径兜底）
	refreshed, err := store.EnsureFresh()
	switch {
	case err != nil:
		logger.Warn("启动预检刷新失败（不阻断，将由 401 自愈兜底）", slog.String("err", err.Error()))
	case refreshed:
		logi("启动预检：token 临期，已提前刷新")
	default:
		logi("启动预检：token 仍新鲜，跳过刷新")
	}

	free := freelist.New(config.APIBase, cfg.StateFilePath("free_cache.json"))
	free.LoadCache() // 先用上次落盘的清单兜底，冷启动不从空清单开始
	free.Fetch(true) // 启动只试一次 4s；补拉交给后台循环
	go free.RefreshLoop(context.Background())

	// 后台 token 续期：每 30 分钟查一次，临期才刷（EnsureFresh 内部判定）
	go func() {
		for {
			time.Sleep(30 * time.Minute)
			if _, err := store.EnsureFresh(); err != nil {
				logger.Warn("[token] 后台刷新失败（下次循环再试）", slog.String("err", err.Error()))
			}
		}
	}()

	dw := dump.New(cfg.DumpOn, cfg.DumpDir, cfg.DumpMaxFiles)
	up := upstream.New(config.APIBase, config.ChatPath, store)
	srv := &handler.Server{Cfg: cfg, Cred: store, Free: free, Dump: dw, Up: up, Log: logger}

	addr := fmt.Sprintf("%s:%d", config.ListenHost, cfg.Port)
	logger.Info("Cline2api 已启动",
		slog.String("addr", "http://"+addr),
		slog.String("provider", info.Provider),
		slog.Int64("expiresAt", info.ExpiresAt))
	logger.Info("OpenAI Base URL（仅免费模型）", slog.String("url", fmt.Sprintf("http://%s/v1", addr)))

	server := &http.Server{Addr: addr, Handler: srv.Handler()}
	if err := server.ListenAndServe(); err != nil {
		logger.Error("服务退出", slog.String("err", err.Error()))
		os.Exit(1)
	}
}
