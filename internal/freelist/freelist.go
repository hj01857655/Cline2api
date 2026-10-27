// Package freelist 对应 Python 版 server.py 的免费模型清单逻辑：
// GET recommended-models 的 body.free 数组即免费面（cline-free/* 前缀、*:free 后缀，
// 命名规则即免费面契约）；clinePass/* 是订阅内含面（无订阅一律 403），不放行。
//
// 清单拉不到时**保留旧清单**不清空（清单空了会把合法免费模型一并拒掉），
// 并进 30s 退避窗口；free_cache.json 落盘上次可用清单，冷启动不从空开始。
package freelist

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"
)

const fetchUA = "Cline2api/1.0-go"

// List 线程安全的免费清单。
type List struct {
	apiBase   string
	cachePath string
	client    *http.Client

	mu        sync.Mutex
	ids       []string
	models    map[string]json.RawMessage // id → 原始目录条目（free_cache.json 兼容）
	failUntil time.Time
}

// freeCacheFile 与 Python 版 free_cache.json 字节格式一致：{"at": <unix>, "models": [...]}
type freeCacheFile struct {
	At     int64             `json:"at"`
	Models []json.RawMessage `json:"models"`
}

func New(apiBase, cachePath string) *List {
	return &List{
		apiBase:   apiBase,
		cachePath: cachePath,
		client:    &http.Client{},
		ids:       []string{},
		models:    map[string]json.RawMessage{},
	}
}

// freeIDsFromBody 从 recommended-models 的 free 数组筛免费面 id 与目录。
func freeIDsFromBody(models []json.RawMessage) ([]string, map[string]json.RawMessage) {
	ids, catalog := []string{}, map[string]json.RawMessage{}
	for _, raw := range models {
		var m struct {
			ID string `json:"id"`
		}
		if json.Unmarshal(raw, &m) != nil || m.ID == "" {
			continue
		}
		if isFreeName(m.ID) {
			ids = append(ids, m.ID)
			catalog[m.ID] = raw
		}
	}
	return ids, catalog
}

func isFreeName(id string) bool {
	return len(id) >= len("cline-free/") && id[:len("cline-free/")] == "cline-free/" ||
		len(id) >= len(":free") && id[len(id)-len(":free"):] == ":free"
}

// LoadCache 读上次落盘的清单兜底；无缓存或损坏不是错误（首次启动本就如此）。
func (l *List) LoadCache() {
	raw, err := os.ReadFile(l.cachePath)
	if err != nil {
		return
	}
	var data freeCacheFile
	if json.Unmarshal(raw, &data) != nil {
		return
	}
	ids, catalog := freeIDsFromBody(data.Models)
	if len(ids) == 0 {
		return
	}
	l.mu.Lock()
	l.ids, l.models = ids, catalog
	l.mu.Unlock()
	slog.Info("[models] 用上次落盘清单兜底", slog.Int("count", len(ids)))
}

func (l *List) saveCache(models []json.RawMessage) {
	raw, err := json.Marshal(freeCacheFile{At: time.Now().Unix(), Models: models})
	if err != nil {
		return
	}
	tmp := l.cachePath + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		slog.Warn("[models] 清单落盘失败（不影响转发）", slog.String("err", err.Error()))
		return
	}
	if err := os.Rename(tmp, l.cachePath); err != nil {
		slog.Warn("[models] 清单落盘失败（不影响转发）", slog.String("err", err.Error()))
	}
}

// Fetch 拉一次 recommended-models。quick=true 给请求路径用：只试一次、
// 超时 4s、退避窗口内直接跳过——这条 GET 的失败是成串来的，重试三轮会挂在
// 用户请求上十几秒，比临时按命名兜底糟得多。非 quick：3 次 / 20s 超时，
// 重试间隔 0.5s、1s。
func (l *List) Fetch(quick bool) {
	l.mu.Lock()
	if quick && time.Now().Before(l.failUntil) {
		l.mu.Unlock()
		return
	}
	l.mu.Unlock()

	attempts, timeout := 3, 20*time.Second
	if quick {
		attempts, timeout = 1, 4*time.Second
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
		}
		ids, catalog, body, err := l.fetchOnce(timeout)
		if err != nil {
			lastErr = err
			continue
		}
		if len(ids) > 0 {
			l.mu.Lock()
			l.ids, l.models = ids, catalog
			l.failUntil = time.Time{}
			l.mu.Unlock()
			l.saveCache(modelsOf(catalog))
			slog.Info("[models] 免费模型", slog.Int("count", len(ids)),
				slog.String("ids", join(ids, ", ")))
		} else {
			// 联系上上游了但免费面是空的（全转付费/命名规则变了筛空）：同样是
			// 可信答案，进退避，否则每条请求都会因空清单再打一次上游。
			l.mu.Lock()
			l.failUntil = time.Now().Add(30 * time.Second)
			old := len(l.ids)
			l.mu.Unlock()
			slog.Warn("[models] free 列表为空；保留旧清单",
				slog.String("topKeys", topKeys(body)), slog.Int("kept", old))
		}
		return
	}
	l.mu.Lock()
	l.failUntil = time.Now().Add(30 * time.Second)
	old := len(l.ids)
	l.mu.Unlock()
	slog.Warn("[models] 拉取失败（保留旧清单）",
		slog.Int("attempts", attempts),
		slog.String("err", fmt.Sprintf("%v", lastErr)),
		slog.Int("kept", old))
}

func (l *List) fetchOnce(timeout time.Duration) (ids []string, catalog map[string]json.RawMessage, keys string, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, l.apiBase+"/api/v1/ai/cline/recommended-models", nil)
	if err != nil {
		return nil, nil, "", err
	}
	req.Header.Set("User-Agent", fetchUA)
	resp, err := l.client.Do(req)
	if err != nil {
		return nil, nil, "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	var body struct {
		Free []json.RawMessage `json:"free"`
	}
	if len(raw) > 0 {
		_ = json.Unmarshal(raw, &body)
	}
	ids, catalog = freeIDsFromBody(body.Free)
	return ids, catalog, topKeys(string(raw)), nil
}

// IsFree 免费面判定：仅 cline-free/*（或 *:free）。cline-pass/* 是订阅面，一律拒。
// 清单拉不到且无缓存时按命名规则兜底放行——订阅面 id 匹配不上前缀也不带
// :free 后缀，仍然被拒，不违背「宁拒不放」。
func (l *List) IsFree(modelID string) bool {
	l.mu.Lock()
	empty := len(l.ids) == 0
	l.mu.Unlock()
	if empty {
		l.Fetch(quick(true))
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.ids) > 0 {
		for _, id := range l.ids {
			if id == modelID {
				return true
			}
		}
	}
	return isFreeName(modelID)
}

// Status 面板/诊断用的清单快照：数量、id、空清单与退避窗口标记。
func (l *List) Status() map[string]any {
	l.mu.Lock()
	defer l.mu.Unlock()
	return map[string]any{
		"count":   len(l.ids),
		"ids":     append([]string(nil), l.ids...),
		"empty":   len(l.ids) == 0,
		"backoff": !l.failUntil.IsZero() && time.Now().Before(l.failUntil),
	}
}

// IDs 返回当前免费清单（/v1/models 用）；空时先快速补拉一次。
func (l *List) IDs() []string {
	l.mu.Lock()
	empty := len(l.ids) == 0
	l.mu.Unlock()
	if empty {
		l.Fetch(quick(true))
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.ids...)
}

// RefreshLoop 后台刷新：每 60s 醒一次。短周期轮询让失败后 60s 就能补上
// （这条链路没有 TTL 概念，60s 一次的上游清单请求成本可以忽略）。
func (l *List) RefreshLoop(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.Fetch(false)
		}
	}
}

func quick(b bool) bool { return b }

func modelsOf(catalog map[string]json.RawMessage) []json.RawMessage {
	out := make([]json.RawMessage, 0, len(catalog))
	// free_cache.json 的 models 数组顺序不影响判定，仅缓存用
	for _, raw := range catalog {
		out = append(out, raw)
	}
	return out
}

func join(vals []string, sep string) string {
	out := ""
	for i, v := range vals {
		if i > 0 {
			out += sep
		}
		out += v
	}
	return out
}

// topKeys 取响应顶层键名前几个（空清单告警排查用）。
func topKeys(body string) string {
	var m map[string]json.RawMessage
	if json.Unmarshal([]byte(body), &m) != nil {
		return ""
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	// map 迭代无序；只取前 6 个做诊断即可，排序保证日志稳定
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[j] < keys[i] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	if len(keys) > 6 {
		keys = keys[:6]
	}
	return fmt.Sprintf("%v", keys)
}
