// Package cred 对应 Python 版 credential.py：读状态目录 credentials.json（唯一凭证来源）
// + refreshToken 自动刷新。缓存有效性按 token 真实 exp 判定（JWT exp 优先，
// expiresAt 兜底），留 FRESH_MARGIN 提前量；刷新结果原子写回。
package cred

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	refreshPath  = "/api/v1/auth/refresh"
	freshMargin  = 120 * time.Second
	upstreamBase = "https://api.cline.bot"
	reqUA        = "Cline2api/1.0-go"
)

type Store struct {
	path  string
	mu    sync.Mutex
	cache struct {
		token string
		exp   time.Time // zero = 未知有效期，从不主动刷
	}
}

// 凭据文件结构：字段名与 Python 版完全一致（同一份文件两边读写）。
type credentialsFile struct {
	AccessToken  string `json:"accessToken"`
	RefreshToken string `json:"refreshToken"`
	Provider     string `json:"provider"`
	ExpiresAt    int64  `json:"expiresAt"`
}

// AccountInfo 是账号快照（不含 token），对应 Python 版 load()。
type AccountInfo struct {
	Provider  string `json:"provider"`
	ExpiresAt int64  `json:"expiresAt"`
}

func NewStore(stateDir string) *Store {
	return &Store{path: filepath.Join(stateDir, "credentials.json")}
}

// Load 返回账号快照；凭据缺失/损坏时报错（对应启动时的可用性检查）。
func (s *Store) Load() (AccountInfo, error) {
	data, err := s.readFile()
	if err != nil {
		return AccountInfo{}, err
	}
	provider := data.Provider
	if provider == "" {
		provider = "cline-pass"
	}
	return AccountInfo{Provider: provider, ExpiresAt: data.ExpiresAt}, nil
}

// Token 返回当前可用 accessToken（裸值，不带 workos: 前缀——前缀由
// upstream.Header 统一拼一次。Python 版 token() 补前缀 + server 再拼一次，
// 冷启动首请求双前缀 401 后靠自愈刷新才正常，这里在归属层修掉）。
// 缓存有效性按 token 真实 exp 判定（留提前量），不再用固定 TTL：
// accessToken 实测只活 60 分钟，固定缓存会在临期时把过期 token 发给上游。
func (s *Store) Token() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cache.token != "" && isFresh(s.cache.exp) {
		return s.cache.token, nil
	}
	data, err := s.readFile()
	if err != nil {
		return "", err
	}
	tok := strings.TrimSpace(data.AccessToken)
	if tok == "" {
		return "", errors.New("凭据缺少 accessToken，请重新登录：cline2api login")
	}
	s.cache.token = tok
	s.cache.exp = expiryOf(data, tok)
	return tok, nil
}

// EnsureFresh 磁盘上的 token 临期则先刷新，返回是否发生了刷新（启动预检用）。
func (s *Store) EnsureFresh() (bool, error) {
	s.mu.Lock()
	cachedFresh := s.cache.token != "" && isFresh(s.cache.exp)
	s.mu.Unlock()
	if cachedFresh {
		return false, nil
	}
	data, err := s.readFile()
	if err != nil {
		return false, err
	}
	if isFresh(expiryOf(data, data.AccessToken)) {
		return false, nil
	}
	if err := s.Refresh(); err != nil {
		return false, err
	}
	return true, nil
}

// Refresh 用 refreshToken 换新 accessToken；成功则原子写回并更新缓存。
// 上游会把响应包一层 {"data": {...}}（register 如此，refresh 同形态兼容）。
func (s *Store) Refresh() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := s.readFile()
	if err != nil {
		return err
	}
	rt := data.RefreshToken
	if rt == "" {
		return errors.New("凭据无 refreshToken，请重新登录：cline2api login")
	}

	body, _ := json.Marshal(map[string]string{
		"refreshToken": rt,
		"grantType":    "refresh_token",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, upstreamBase+refreshPath, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("刷新请求构造失败：%w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", reqUA)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("刷新请求失败：%w", err)
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	var outer struct {
		Data struct {
			AccessToken   string `json:"accessToken"`
			AccessToken2  string `json:"access_token"`
			RefreshToken  string `json:"refreshToken"`
			RefreshToken2 string `json:"refresh_token"`
			ExpiresIn     int64  `json:"expiresIn"`
		} `json:"data"`
		AccessToken   string `json:"accessToken"`
		AccessToken2  string `json:"access_token"`
		RefreshToken  string `json:"refreshToken"`
		RefreshToken2 string `json:"refresh_token"`
		ExpiresIn     int64  `json:"expiresIn"`
	}
	_ = json.Unmarshal(raw, &outer)

	newAt := firstNonEmpty(outer.Data.AccessToken, outer.Data.AccessToken2,
		outer.AccessToken, outer.AccessToken2)
	if resp.StatusCode != 200 || newAt == "" {
		return fmt.Errorf("刷新失败 HTTP %d: %s", resp.StatusCode, trunc(string(raw), 200))
	}

	data.AccessToken = newAt
	if rt2 := firstNonEmpty(outer.Data.RefreshToken, outer.Data.RefreshToken2,
		outer.RefreshToken, outer.RefreshToken2); rt2 != "" {
		data.RefreshToken = rt2
	}
	// 新 token 的真实过期时刻：JWT exp 优先；磁盘 expiresAt 会残留旧值，
	// 不覆盖的话刷新完立刻又被判临期。
	if jwtExp := jwtExp(newAt); !jwtExp.IsZero() {
		data.ExpiresAt = jwtExp.Unix()
	} else if expiresIn := firstPositive(outer.Data.ExpiresIn, outer.ExpiresIn); expiresIn != 0 {
		data.ExpiresAt = time.Now().Add(time.Duration(expiresIn) * time.Second).Unix()
	}
	if err := s.writeFile(data); err != nil {
		return fmt.Errorf("刷新结果写回失败：%w", err)
	}
	s.cache.token = newAt
	s.cache.exp = expiryOf(data, newAt)
	expStr := "未知"
	if !s.cache.exp.IsZero() {
		expStr = s.cache.exp.Format("15:04:05")
	}
	slog.Info("令牌已刷新并写回 credentials.json", slog.String("validUntil", expStr))
	return nil
}

// ── 内部 ─────────────────────────────────────────────────────────────────────

func (s *Store) readFile() (credentialsFile, error) {
	raw, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return credentialsFile{}, fmt.Errorf("未找到 %s，请先登录：cline2api login", s.path)
		}
		return credentialsFile{}, fmt.Errorf("凭据文件无法读取：%w", err)
	}
	var data credentialsFile
	if err := json.Unmarshal(raw, &data); err != nil {
		return credentialsFile{}, fmt.Errorf("凭据文件无法解析：%w", err)
	}
	if data.AccessToken == "" {
		return credentialsFile{}, errors.New("凭据缺少 accessToken，请重新登录：cline2api login")
	}
	return data, nil
}

func (s *Store) writeFile(data credentialsFile) error {
	raw, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

// expiryOf token 真实过期时刻：expiresAt 优先，其次 JWT exp，都没有则零值（视为永不过期）。
func expiryOf(data credentialsFile, tok string) time.Time {
	if data.ExpiresAt > 0 {
		return time.Unix(data.ExpiresAt, 0)
	}
	return jwtExp(tok)
}

func isFresh(exp time.Time) bool {
	return exp.IsZero() || time.Until(exp) > freshMargin
}

// jwtExp 从 JWT payload 取 exp；非 JWT 或解析失败返回零值。
func jwtExp(tok string) time.Time {
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		return time.Time{}
	}
	seg := parts[1]
	if pad := len(seg) % 4; pad != 0 {
		seg += strings.Repeat("=", 4-pad)
	}
	raw, err := base64.URLEncoding.DecodeString(seg)
	if err != nil {
		return time.Time{}
	}
	var payload struct {
		Exp int64 `json:"exp"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil || payload.Exp == 0 {
		return time.Time{}
	}
	return time.Unix(payload.Exp, 0)
}

// WithWorkosPrefix 给裸 accessToken 补 workos: 前缀（已带则原样返回），
// 供 upstream 组装 Authorization 头：上游 2025-09 起 Firebase→WorkOS 迁移后
// 必须带此前缀，否则 401 要求重登录。
func WithWorkosPrefix(tok string) string {
	if strings.HasPrefix(strings.ToLower(tok), "workos:") {
		return tok
	}
	return "workos:" + tok
}
func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func firstPositive(vals ...int64) int64 {
	for _, v := range vals {
		if v > 0 {
			return v
		}
	}
	return 0
}

func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
