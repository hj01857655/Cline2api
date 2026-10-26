// Package upstream 对应 Python 版 server.py 的 _upstream_headers + routes/chat.py
// 的 _post_chat：上游 chat POST + 401 自愈（刷新一次重发）。
//
// 身份头全套：上游 2025-09 起 Firebase→WorkOS 迁移后，Authorization 必须带
// `workos:` 前缀（社区实锤，OmniRoute #3321 同款问题），否则 401 要求重登录。
package upstream

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"cline2api/internal/cred"
)

// Client 上游通道。
type Client struct {
	Base string // https://api.cline.bot
	Path string // /api/v1/chat/completions
	Cred *cred.Store
	HTTP *http.Client
}

func New(base, path string, c *cred.Store) *Client {
	return &Client{
		Base: base,
		Path: path,
		Cred: c,
		HTTP: &http.Client{Timeout: 180 * time.Second},
	}
}

// Header 组装上游身份头。
func (c *Client) Header(token string) http.Header {
	h := http.Header{}
	h.Set("Authorization", "Bearer "+cred.WithWorkosPrefix(token))
	h.Set("Content-Type", "application/json")
	h.Set("HTTP-Referer", "https://cline.bot")
	h.Set("X-Title", "Cline")
	h.Set("X-CLIENT-TYPE", "cline-sdk")
	h.Set("User-Agent", "Cline/2.6.9")
	h.Set("X-CLIENT-VERSION", "2.6.9")
	h.Set("X-PLATFORM", "cline-sdk")
	h.Set("X-PLATFORM-VERSION", "2.6.9")
	h.Set("X-IS-MULTIROOT", "false")
	return h
}

// Post 上游 chat POST；401 时刷新 token 重发一次（401 自愈）。
// 返回的 resp.Body 由调用方关闭与消费（流式场景直接逐块读）。
func (c *Client) Post(ctx context.Context, rid string, payload []byte) (*http.Response, error) {
	resp, err := c.postOnce(ctx, payload)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusUnauthorized {
		return resp, nil
	}
	resp.Body.Close()
	slog.Warn("上游 401，刷新 token 重发", slog.String("rid", rid))
	if err := c.Cred.Refresh(); err != nil {
		slog.Error("401 刷新失败", slog.String("rid", rid), slog.String("err", err.Error()))
		return nil, &UnauthorizedError{RefreshErr: err}
	}
	resp2, err := c.postOnce(ctx, payload)
	if err != nil {
		slog.Error("401 刷新后仍失败", slog.String("rid", rid), slog.String("err", err.Error()))
	}
	return resp2, err
}

// UnauthorizedError 表示刷新后仍不可用（路由层映射 502 upstream 401 after refresh）。
type UnauthorizedError struct {
	RefreshErr error
}

func (e *UnauthorizedError) Error() string {
	return fmt.Sprintf("upstream 401 after refresh: %v", e.RefreshErr)
}

func (c *Client) postOnce(ctx context.Context, payload []byte) (*http.Response, error) {
	token, err := c.Cred.Token()
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.Base+c.Path, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header = c.Header(token)
	return c.HTTP.Do(req)
}

// UnwrapDataLayer 上游非流式 chat 响应有时会多包一层 {"data":{...}}（Python 版
// 原样透传是已知缺陷，客户端拿不到顶层 choices）。内层确实像 chat completion
// （有 choices 或 object 字段）才解包，避免误伤正常响应。
func UnwrapDataLayer(body []byte) []byte {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return body
	}
	var outer struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(trimmed, &outer); err != nil || len(outer.Data) == 0 {
		return body
	}
	var inner struct {
		Choices json.RawMessage `json:"choices"`
		Object  string          `json:"object"`
	}
	if err := json.Unmarshal(outer.Data, &inner); err != nil {
		return body
	}
	if len(inner.Choices) == 0 && inner.Object == "" {
		return body
	}
	return outer.Data
}

// ReadAll 读完并关闭 body（非流式场景）。
func ReadAll(resp *http.Response) ([]byte, error) {
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}
