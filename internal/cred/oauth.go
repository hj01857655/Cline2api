// Package cred 的 OAuth 登录：WorkOS 设备流 → Cline register，
// 凭据落盘 exe 同目录 credentials.json（StateDir，不碰 ~/.cline 的 IDE 凭证），
// 之后迁入状态目录 credentials.json 供服务使用。
//
// 协议依据 cline 源码 sdk/packages/core/src/auth/cline.ts（useWorkOSDeviceAuth 路径）。
package cred

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	workosClientID = "client_01K3A541FN8TA3EPPHTD2325AR" // production（cline-environment.ts）
	workosDevice   = "https://api.workos.com/user_management/authorize/device"
	workosToken    = "https://api.workos.com/user_management/authenticate"
	registerPath   = "/api/v1/auth/register"
	deviceGrant    = "urn:ietf:params:oauth:grant-type:device_code"
)

// AuthStorePath 返回登录凭据落盘路径 ~/.cline/data/Cline2api_auth.json。
func AuthStorePath() string {
	// 落盘到网关自己项目（exe 同目录 = StateDir），不写 ~/.cline 的 IDE 凭证目录
	if exe, err := os.Executable(); err == nil {
		return filepath.Join(filepath.Dir(exe), "credentials.json")
	}
	return "credentials.json"
}

// Login 走完整设备流并把凭据写入 AUTH_STORE，返回该路径。
func Login() (string, error) {
	// 1. 申请 device code
	var dev struct {
		DeviceCode              string `json:"device_code"`
		UserCode                string `json:"user_code"`
		VerificationURI         string `json:"verification_uri"`
		VerificationURIComplete string `json:"verification_uri_complete"`
		ExpiresIn               int    `json:"expires_in"`
		Interval                int    `json:"interval"`
	}
	if err := postForm(workosDevice, map[string]string{
		"client_id":  workosClientID,
		"grant_type": deviceGrant,
	}, &dev); err != nil {
		return "", fmt.Errorf("device 授权失败：%w", err)
	}
	if dev.DeviceCode == "" {
		return "", fmt.Errorf("device 授权失败：响应缺少 device_code")
	}
	uri := dev.VerificationURIComplete
	if uri == "" {
		uri = dev.VerificationURI
	}
	fmt.Printf("[oauth] 浏览器打开并输入代码：%s\n", uri)
	fmt.Printf("[oauth] user_code = %s\n", dev.UserCode)

	// 2. 轮询 token（interval≥2，slow_down +5，超时 expires_in 或 600s）
	interval := time.Duration(dev.Interval) * time.Second
	if interval < 2*time.Second {
		interval = 2 * time.Second
	}
	deadline := time.Now().Add(time.Duration(dev.ExpiresIn) * time.Second)
	if dev.ExpiresIn <= 0 {
		deadline = time.Now().Add(600 * time.Second)
	}
	var tokens struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		Error        string `json:"error"`
	}
	for time.Now().Before(deadline) {
		time.Sleep(interval)
		tokens = struct {
			AccessToken  string `json:"access_token"`
			RefreshToken string `json:"refresh_token"`
			Error        string `json:"error"`
		}{}
		if err := postForm(workosToken, map[string]string{
			"client_id":   workosClientID,
			"grant_type":  deviceGrant,
			"device_code": dev.DeviceCode,
		}, &tokens); err != nil {
			// RFC 8628 §3.5：WorkOS 对 pending/slow_down 返回 HTTP 400，error 字段在
			// body 里。必须解析后继续轮询，而不是把 400 当致命错误（否则首次轮询
			// 即挂，用户授权永远追不上一个已死流程）。
			msg := err.Error()
			if strings.Contains(msg, "authorization_pending") {
				continue
			}
			if strings.Contains(msg, "slow_down") {
				interval += 5 * time.Second
				continue
			}
			return "", fmt.Errorf("token 轮询失败：%w", err)
		}
		if tokens.AccessToken != "" {
			break
		}
		switch tokens.Error {
		case "authorization_pending":
			continue
		case "slow_down":
			interval += 5 * time.Second
			continue
		case "":
			return "", fmt.Errorf("token 轮询失败：响应无 access_token")
		default:
			return "", fmt.Errorf("token 轮询失败：%s", tokens.Error)
		}
	}
	if tokens.AccessToken == "" {
		return "", fmt.Errorf("设备授权超时")
	}

	// 3. WorkOS token 换 Cline 自家凭据（register，与官方 CLI 同款）
	regBody, _ := json.Marshal(map[string]string{
		"accessToken":  tokens.AccessToken,
		"refreshToken": tokens.RefreshToken,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, upstreamBase+registerPath, bytes.NewReader(regBody))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", reqUA)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("register 失败：%w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)

	// 上游 2026-10 起把 register 响应包了一层 {"data": {...}}，两种形态都兼容
	var body struct {
		Data struct {
			AccessToken   string `json:"accessToken"`
			AccessToken2  string `json:"access_token"`
			RefreshToken  string `json:"refreshToken"`
			RefreshToken2 string `json:"refresh_token"`
			ExpiresIn     int    `json:"expiresIn"`
		} `json:"data"`
		AccessToken   string `json:"accessToken"`
		AccessToken2  string `json:"access_token"`
		RefreshToken  string `json:"refreshToken"`
		RefreshToken2 string `json:"refresh_token"`
		ExpiresIn     int    `json:"expiresIn"`
	}
	_ = json.Unmarshal(raw, &body)

	access := firstNonEmpty(body.Data.AccessToken, body.Data.AccessToken2,
		body.AccessToken, body.AccessToken2)
	if resp.StatusCode != 200 || access == "" {
		return "", fmt.Errorf("register 失败 HTTP %d: %s", resp.StatusCode, trunc(string(raw), 200))
	}
	refresh := firstNonEmpty(body.Data.RefreshToken, body.Data.RefreshToken2,
		body.RefreshToken, body.RefreshToken2, tokens.RefreshToken)
	expiresIn := int(firstPositive(int64(body.Data.ExpiresIn), int64(body.ExpiresIn)))

	// 4. 落盘 AUTH_STORE
	creds := credentialsFile{
		AccessToken:  access,
		RefreshToken: refresh,
		Provider:     "cline-pass",
		ExpiresAt:    time.Now().Add(time.Duration(expiresIn) * time.Second).Unix(),
	}
	store := AuthStorePath()
	if err := os.MkdirAll(filepath.Dir(store), 0o700); err != nil {
		return "", fmt.Errorf("凭据目录创建失败：%w", err)
	}
	rawCreds, _ := json.MarshalIndent(creds, "", "  ")
	if err := os.WriteFile(store, rawCreds, 0o600); err != nil {
		return "", fmt.Errorf("凭据写入失败：%w", err)
	}
	fmt.Printf("[oauth] ✅ 凭据已写入 %s\n", store)
	return store, nil
}

// postForm 发 form 编码 POST 并把 JSON 响应解析到 out（out 可为 nil）。
func postForm(u string, form map[string]string, out any) error {
	vals := url.Values{}
	for k, v := range form {
		vals.Set(k, v)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewBufferString(vals.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("User-Agent", reqUA)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, trunc(string(raw), 200))
	}
	if out == nil {
		return nil
	}
	if len(raw) == 0 {
		return nil
	}
	return json.Unmarshal(raw, out)
}
