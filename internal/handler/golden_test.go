// golden_test.go — /v1/responses 与 /v1/chat/completions 端到端验证
// （httptest 假上游，不连真实 cline 上游）。移植自 Python 版
// tests/test_responses_route.py，断言逐条对齐：
//  1. /v1/responses 流式：Responses SSE 合法（seq 连续、生命周期、usage、items）
//  2. /v1/responses 非流式：Responses JSON 合法
//  3. /v1/responses 上游 4xx：透传
//  4. /v1/chat/completions 流式回归
package handler

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"cline2api/internal/config"
	"cline2api/internal/cred"
	"cline2api/internal/dump"
	"cline2api/internal/freelist"
	"cline2api/internal/upstream"
)

const usageJSON = `{"prompt_tokens":10,"completion_tokens":7,"total_tokens":17,
"completion_tokens_details":{"reasoning_tokens":3},"prompt_tokens_details":{"cached_tokens":0}}`

func sseLine(t *testing.T, obj any) string {
	t.Helper()
	raw, err := json.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}
	return "data: " + string(raw)
}

type fakeUpstream struct {
	t       *testing.T
	mode    string // "stream" | "json" | "error"
	mu      sync.Mutex
	payload map[string]any // 最近一次收到的 chat 请求体
}

func (f *fakeUpstream) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.payload = nil
		_ = json.NewDecoder(r.Body).Decode(&f.payload)
		f.mu.Unlock()
		switch f.mode {
		case "error":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"message":"param invalid"}`))
		case "json":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(fmt.Sprintf(`{"id":"c2","object":"chat.completion","created":1,"model":"m",
"choices":[{"index":0,"message":{"role":"assistant","content":"Hi!","reasoning_content":"think ok"},"finish_reason":"stop"}],
"usage":%s}`, usageJSON)))
		default:
			w.Header().Set("Content-Type", "text/event-stream")
			chunk := func(delta map[string]any, finish any) string {
				return sseLine(f.t, map[string]any{
					"id": "c1", "object": "chat.completion.chunk", "created": 1, "model": "m",
					"choices": []any{map[string]any{"index": 0, "delta": delta, "finish_reason": finish}},
				}) + "\n\n"
			}
			fmt.Fprint(w, chunk(map[string]any{"role": "assistant", "reasoning_content": "think "}, nil))
			fmt.Fprint(w, chunk(map[string]any{"reasoning_content": "ok"}, nil))
			fmt.Fprint(w, chunk(map[string]any{"content": "Hi!"}, nil))
			fmt.Fprint(w, chunk(map[string]any{"tool_calls": []any{map[string]any{
				"index": 0, "id": "call_1", "type": "function",
				"function": map[string]any{"name": "get_weather", "arguments": `{"city": "BJ"}`},
			}}}, nil))
			fmt.Fprint(w, chunk(map[string]any{}, "tool_calls"))
			fmt.Fprint(w, sseLine(f.t, map[string]any{
				"id": "c1", "object": "chat.completion.chunk", "created": 1, "model": "m",
				"choices": []any{}, "usage": json.RawMessage(usageJSON),
			})+"\n\n")
			fmt.Fprint(w, "data: [DONE]\n\n")
		}
	})
	mux.HandleFunc("/api/v1/ai/cline/recommended-models", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"free":[{"id":"cline-free/test-model"}]}`))
	})
	return mux
}

func newTestServer(t *testing.T, mode string) (*Server, *fakeUpstream) {
	t.Helper()
	fake := &fakeUpstream{t: t, mode: mode}
	upSrv := httptest.NewServer(fake.handler())
	t.Cleanup(upSrv.Close)

	stateDir := t.TempDir()
	creds := `{"accessToken":"fake","refreshToken":"r","provider":"cline-pass","expiresAt":0}`
	if err := os.WriteFile(filepath.Join(stateDir, "credentials.json"), []byte(creds), 0o600); err != nil {
		t.Fatal(err)
	}
	store := cred.NewStore(stateDir)
	free := freelist.New(upSrv.URL, filepath.Join(stateDir, "free_cache.json"))
	cfg := &config.Config{StateDir: stateDir, DumpOn: false, DumpDir: stateDir, DumpMaxFiles: 200}
	srv := &Server{
		Cfg:  cfg,
		Cred: store,
		Free: free,
		Dump: dump.New(false, stateDir, 200),
		Up:   upstream.New(upSrv.URL, "/api/v1/chat/completions", store),
		Log:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	return srv, fake
}

func postJSON(t *testing.T, h http.Handler, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func parseFrames(t *testing.T, raw string) []map[string]any {
	t.Helper()
	var events []map[string]any
	for _, block := range strings.Split(raw, "\n\n") {
		block = strings.TrimSpace(block)
		if !strings.HasPrefix(block, "data:") {
			continue
		}
		payload := strings.TrimSpace(block[len("data:"):])
		if payload == "" || payload == "[DONE]" {
			continue
		}
		var ev map[string]any
		if err := json.Unmarshal([]byte(payload), &ev); err != nil {
			t.Fatalf("帧解析失败: %v\n%s", err, payload)
		}
		events = append(events, ev)
	}
	return events
}

const respReqFmt = `{"model":"cline-free/test-model","stream":%s,"instructions":"sys prompt",
"input":[{"type":"message","role":"user","content":"hello"}],
"tools":[{"type":"function","name":"get_weather","description":"w",
"parameters":{"type":"object","properties":{"city":{"type":"string"}}}}]}`

func TestResponsesRoute(t *testing.T) {
	// ---------- 1) /v1/responses 流式 ----------
	srv, fake := newTestServer(t, "stream")
	h := srv.Handler()
	rec := postJSON(t, h, "/v1/responses", fmt.Sprintf(respReqFmt, "true"))
	if rec.Code != 200 {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String()[:min(500, rec.Body.Len())])
	}
	if !strings.Contains(rec.Header().Get("Content-Type"), "text/event-stream") {
		t.Fatalf("Content-Type=%s", rec.Header().Get("Content-Type"))
	}

	fake.mu.Lock()
	cb := fake.payload
	fake.mu.Unlock()
	if cb == nil {
		t.Fatal("上游未收到请求体")
	}
	msgs := cb["messages"].([]any)
	if fmt.Sprint(msgs[0]) != fmt.Sprint(map[string]any{"role": "system", "content": "sys prompt"}) {
		t.Fatalf("messages[0]=%v", msgs[0])
	}
	if fmt.Sprint(msgs[1]) != fmt.Sprint(map[string]any{"role": "user", "content": "hello"}) {
		t.Fatalf("messages[1]=%v", msgs[1])
	}
	if cb["stream"] != true {
		t.Fatalf("stream=%v", cb["stream"])
	}
	tools := cb["tools"].([]any)
	fn0 := tools[0].(map[string]any)["function"].(map[string]any)
	if fn0["name"] != "get_weather" {
		t.Fatalf("tools[0].function.name=%v", fn0["name"])
	}

	events := parseFrames(t, rec.Body.String())
	seqs := []float64{}
	types := []string{}
	for _, e := range events {
		seqs = append(seqs, e["sequence_number"].(float64))
		types = append(types, e["type"].(string))
	}
	for i, s := range seqs {
		if s != float64(i) {
			t.Fatalf("seq 不连续: %v", seqs)
		}
	}
	if types[0] != "response.created" || types[1] != "response.in_progress" || types[len(types)-1] != "response.completed" {
		t.Fatalf("生命周期不对: %v", types)
	}
	final := events[len(events)-1]["response"].(map[string]any)
	for _, k := range []string{"id", "object", "created_at", "model", "status", "output"} {
		if _, ok := final[k]; !ok {
			t.Fatalf("final 缺字段 %s", k)
		}
	}
	if final["status"] != "completed" {
		t.Fatalf("status=%v", final["status"])
	}
	output := final["output"].([]any)
	gotTypes := []string{}
	for _, o := range output {
		gotTypes = append(gotTypes, o.(map[string]any)["type"].(string))
	}
	if fmt.Sprint(gotTypes) != "[reasoning message function_call]" {
		t.Fatalf("output types=%v", gotTypes)
	}
	usage := final["usage"].(map[string]any)
	if usage["total_tokens"] != float64(17) {
		t.Fatalf("total_tokens=%v", usage["total_tokens"])
	}
	fn := output[2].(map[string]any)
	if fn["call_id"] != "call_1" || fn["arguments"] != `{"city": "BJ"}` {
		t.Fatalf("function_call=%v", fn)
	}

	// ---------- 2) /v1/responses 非流式 ----------
	srv2, _ := newTestServer(t, "json")
	rec2 := postJSON(t, srv2.Handler(), "/v1/responses", fmt.Sprintf(respReqFmt, "false"))
	if rec2.Code != 200 {
		t.Fatalf("status=%d body=%s", rec2.Code, rec2.Body.String()[:min(500, rec2.Body.Len())])
	}
	var out map[string]any
	if err := json.Unmarshal(rec2.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out["object"] != "response" || out["status"] != "completed" {
		t.Fatalf("object/status=%v/%v", out["object"], out["status"])
	}
	outTypes := []string{}
	for _, o := range out["output"].([]any) {
		outTypes = append(outTypes, o.(map[string]any)["type"].(string))
	}
	if fmt.Sprint(outTypes) != "[reasoning message]" {
		t.Fatalf("output=%v", outTypes)
	}
	if out["usage"].(map[string]any)["total_tokens"] != float64(17) {
		t.Fatalf("usage=%v", out["usage"])
	}

	// ---------- 3) 上游 400 透传 ----------
	srv3, _ := newTestServer(t, "error")
	rec3 := postJSON(t, srv3.Handler(), "/v1/responses", fmt.Sprintf(respReqFmt, "true"))
	if rec3.Code != 400 {
		t.Fatalf("status=%d body=%s", rec3.Code, rec3.Body.String()[:min(300, rec3.Body.Len())])
	}
	if !strings.Contains(rec3.Body.String(), "param invalid") {
		t.Fatalf("body=%s", rec3.Body.String())
	}

	// ---------- 4) /v1/chat/completions 回归 ----------
	srv4, _ := newTestServer(t, "stream")
	rec4 := postJSON(t, srv4.Handler(), "/v1/chat/completions",
		`{"model":"cline-free/test-model","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	if rec4.Code != 200 {
		t.Fatalf("status=%d body=%s", rec4.Code, rec4.Body.String()[:min(300, rec4.Body.Len())])
	}
	if !strings.Contains(rec4.Body.String(), "data: ") || !strings.Contains(rec4.Body.String(), "[DONE]") {
		t.Fatal("chat 流缺 data: / [DONE]")
	}
}
