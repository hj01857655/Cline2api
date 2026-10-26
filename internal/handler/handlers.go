// Package handler 的三个协议路由，对应 Python 版 routes/chat.py、
// routes/anthropic.py、routes/responses.py。
package handler

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"cline2api/internal/adapters"
	"cline2api/internal/upstream"
)

// freeGate 免费清单门：非免费模型 400。返回 true = 放行。
func (s *Server) freeGate(rid, model string) bool {
	if model == "" || s.Free.IsFree(model) {
		return true
	}
	s.Log.Warn("拒绝非免费模型", slog.String("rid", rid), slog.String("model", model))
	return false
}

// chat POST /v1/chat/completions：近乎直通 + 401 自愈 + 断流保护。
func (s *Server) chat(w http.ResponseWriter, r *http.Request) {
	info := reqInfoFrom(r)
	p := info.payload
	model := str(p["model"])

	if !s.freeGate(info.rid, model) {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": map[string]any{
				"message": fmt.Sprintf("model %s 不在免费清单（Cline2api 只转发免费模型）", model),
				"type":    "invalid_request_error",
			},
		})
		return
	}
	payload := passthroughFilter(p, model)
	wantStream := false
	if b, ok := p["stream"].(bool); ok {
		wantStream = b
	}

	resp, err := s.Up.Post(r.Context(), info.rid, marshalCompact(payload))
	if err != nil {
		s.Log.Error("上游连接失败", slog.String("rid", info.rid), slog.String("err", err.Error()))
		writeJSON(w, http.StatusBadGateway, map[string]any{
			"error": map[string]any{"message": err.Error(), "type": "upstream_error"},
		})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		writeJSON(w, http.StatusBadGateway, map[string]any{
			"error": map[string]any{"message": "upstream 401 after refresh", "type": "upstream_error"},
		})
		return
	}
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512<<10))
		s.Log.Warn("上游错误", slog.String("rid", info.rid),
			slog.Int("status", resp.StatusCode), slog.String("body", truncStr(string(body), 400)))
		ct := resp.Header.Get("Content-Type")
		if ct == "" {
			ct = "application/json"
		}
		w.Header().Set("Content-Type", ct)
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(body)
		return
	}

	if !wantStream {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
		// 上游有时多包一层 {"data":{...}}，Python 版透传是已知缺陷，这里解开
		body = upstream.UnwrapDataLayer(body)
		s.doneLog(info.rid, "status=200", len(body), info.t0)
		s.Dump.Save(info.rid, "resp", string(body))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
		return
	}

	// 流式：裸字节转发；断流补错误帧 + [DONE]；客户端断开只记日志
	sseHeaders(w)
	flusher, _ := w.(http.Flusher)
	total := 0
	var dumpBuf []byte
	buf := make([]byte, 32*1024)
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			total += n
			if s.Dump.On && len(dumpBuf) <= 512*1024 {
				dumpBuf = append(dumpBuf, chunk...)
			}
			if _, werr := w.Write(chunk); werr != nil {
				s.Log.Warn("client 断开", slog.String("rid", info.rid),
					slog.String("line", fmt.Sprintf("stream %dB %.1fs", total, time.Since(info.t0).Seconds())))
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		if rerr != nil {
			if rerr == io.EOF {
				s.doneLog(info.rid, "stream", total, info.t0)
				if len(dumpBuf) > 0 {
					s.Dump.Save(info.rid, "resp", string(dumpBuf))
				}
				return
			}
			// 断流时把已收到的部分落盘（正常路径只在流末落，中途断了只剩 .req）
			s.Log.Error("stream 中断", slog.String("rid", info.rid),
				slog.String("err", rerr.Error()),
				slog.String("line", fmt.Sprintf("%dB %.1fs", total, time.Since(info.t0).Seconds())))
			if len(dumpBuf) > 0 {
				s.Dump.Save(info.rid, "resp_partial", string(dumpBuf))
			}
			// 裸掐断会让客户端把半截回答当完整结果，显式补错误帧 + [DONE]
			errFrame := marshalCompact(map[string]any{
				"error": map[string]any{
					"message": "upstream stream ended prematurely: " + rerr.Error(),
					"type":    "upstream_error",
				},
			})
			_, _ = w.Write(append(append([]byte("data: "), errFrame...), '\n', '\n'))
			_, _ = w.Write([]byte("data: [DONE]\n\n"))
			if flusher != nil {
				flusher.Flush()
			}
			return
		}
		select {
		case <-r.Context().Done():
			s.Log.Warn("client 断开", slog.String("rid", info.rid),
				slog.String("line", fmt.Sprintf("stream %dB %.1fs", total, time.Since(info.t0).Seconds())))
			return
		default:
		}
	}
}

// anthropic POST /v1/messages：Anthropic Messages ⇄ Chat 翻译。
func (s *Server) anthropic(w http.ResponseWriter, r *http.Request) {
	info := reqInfoFrom(r)
	p := info.payload
	if len(p) == 0 {
		writeJSON(w, http.StatusBadRequest, adapters.AnthropicErrorBody(
			"invalid_request_error", "invalid request: JSON object body required"))
		return
	}
	model := str(p["model"])
	if !s.freeGate(info.rid, model) {
		writeJSON(w, http.StatusBadRequest, adapters.AnthropicErrorBody(
			"invalid_request_error",
			fmt.Sprintf("model %s 不在免费清单（Cline2api 只转发免费模型）", model)))
		return
	}

	chatBody := adapters.AnthropicToChat(p)
	if msgs, ok := chatBody["messages"].([]any); !ok || len(msgs) == 0 {
		writeJSON(w, http.StatusBadRequest, adapters.AnthropicErrorBody(
			"invalid_request_error", "invalid request: 'messages' is required"))
		return
	}
	payload := passthroughFilter(chatBody, model)
	wantStream := false
	if b, ok := p["stream"].(bool); ok {
		wantStream = b
	}

	resp, err := s.Up.Post(r.Context(), info.rid, marshalCompact(payload))
	if err != nil {
		s.Log.Error("上游连接失败", slog.String("rid", info.rid), slog.String("err", err.Error()))
		writeJSON(w, http.StatusBadGateway,
			adapters.AnthropicErrorBody("api_error", err.Error()))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		writeJSON(w, http.StatusBadGateway,
			adapters.AnthropicErrorBody("api_error", "upstream 401 after refresh"))
		return
	}
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512<<10))
		s.Log.Warn("上游错误", slog.String("rid", info.rid),
			slog.Int("status", resp.StatusCode), slog.String("body", truncStr(string(body), 400)))
		s.Dump.Save(info.rid, "resp", string(body))
		writeJSON(w, resp.StatusCode, adapters.AnthropicErrorBody(
			adapters.AnthropicErrTypeForHTTP(resp.StatusCode), truncStr(string(body), 400)))
		return
	}

	if wantStream {
		s.streamAdapter(w, r, info, func(lines adapters.SSELines) func(func([]byte) bool) {
			return adapters.ChatSSEToAnthropic(lines.Seq, model)
		}, resp.Body, "messages stream")
		return
	}

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	// 上游非流式响应间歇性多包一层 {"data":{...}}（Python 版已知缺陷：
	// chat 路由透传、messages/responses 路由没解包），统一在此解掉
	body = upstream.UnwrapDataLayer(body)
	var chatResult map[string]any
	if json.Unmarshal(body, &chatResult) != nil || chatResult == nil {
		writeJSON(w, http.StatusBadGateway,
			adapters.AnthropicErrorBody("api_error", "upstream response parse failed"))
		return
	}
	out := adapters.ChatJSONToAnthropic(chatResult, model)
	outBody := marshalCompact(out)
	s.doneLog(info.rid, "messages", len(outBody), info.t0)
	s.Dump.Save(info.rid, "resp", string(outBody))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(outBody)
}

// responses POST /v1/responses：OpenAI Responses ⇄ Chat 翻译（每帧带 sequence_number）。
func (s *Server) responses(w http.ResponseWriter, r *http.Request) {
	info := reqInfoFrom(r)
	p := info.payload
	if len(p) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": map[string]any{
				"message": "invalid request: JSON object body required",
				"type":    "invalid_request_error",
			},
		})
		return
	}
	model := str(p["model"])
	if !s.freeGate(info.rid, model) {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": map[string]any{
				"message": fmt.Sprintf("model %s 不在免费清单（Cline2api 只转发免费模型）", model),
				"type":    "invalid_request_error",
			},
		})
		return
	}

	chatBody := adapters.ResponsesToChat(p)
	if msgs, ok := chatBody["messages"].([]any); !ok || len(msgs) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": map[string]any{
				"message": "invalid request: 'input' is required",
				"type":    "invalid_request_error",
			},
		})
		return
	}
	payload := passthroughFilter(chatBody, model)
	wantStream := false
	if b, ok := p["stream"].(bool); ok {
		wantStream = b
	}

	resp, err := s.Up.Post(r.Context(), info.rid, marshalCompact(payload))
	if err != nil {
		s.Log.Error("上游连接失败", slog.String("rid", info.rid), slog.String("err", err.Error()))
		writeJSON(w, http.StatusBadGateway, map[string]any{
			"error": map[string]any{"message": err.Error(), "type": "upstream_error"},
		})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		writeJSON(w, http.StatusBadGateway, map[string]any{
			"error": map[string]any{"message": "upstream 401 after refresh", "type": "upstream_error"},
		})
		return
	}
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512<<10))
		s.Log.Warn("上游错误", slog.String("rid", info.rid),
			slog.Int("status", resp.StatusCode), slog.String("body", truncStr(string(body), 400)))
		s.Dump.Save(info.rid, "resp", string(body))
		ct := resp.Header.Get("Content-Type")
		if ct == "" {
			ct = "application/json"
		}
		w.Header().Set("Content-Type", ct)
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(body)
		return
	}

	if wantStream {
		s.streamAdapter(w, r, info, func(lines adapters.SSELines) func(func([]byte) bool) {
			return adapters.ChatSSEToResponses(lines, model)
		}, resp.Body, "responses stream")
		return
	}

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	body = upstream.UnwrapDataLayer(body)
	var chatResult map[string]any
	if json.Unmarshal(body, &chatResult) != nil || chatResult == nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{
			"error": map[string]any{"message": "upstream response parse failed", "type": "upstream_error"},
		})
		return
	}
	out := adapters.ChatJSONToResponses(chatResult, model)
	outBody := marshalCompact(out)
	s.doneLog(info.rid, "responses", len(outBody), info.t0)
	s.Dump.Save(info.rid, "resp", string(outBody))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(outBody)
}

// streamAdapter 通用 adapter 流转发：上游 body → 行流 → adapter 帧流 → 客户端。
func (s *Server) streamAdapter(
	w http.ResponseWriter, r *http.Request, info reqInfo,
	build func(adapters.SSELines) func(func([]byte) bool),
	upstreamBody io.Reader, kind string,
) {
	sseHeaders(w)
	flusher, _ := w.(http.Flusher)
	lines, errRef := sseLineFeed(upstreamBody)
	total := 0
	for frame := range build(adapters.SSELines{Seq: lines, Err: errRef}) {
		if err := r.Context().Err(); err != nil {
			s.Log.Warn("client 断开", slog.String("rid", info.rid),
				slog.String("line", fmt.Sprintf("%s %dB %.1fs", kind, total, time.Since(info.t0).Seconds())))
			return
		}
		if _, werr := w.Write(frame); werr != nil {
			s.Log.Warn("client 断开", slog.String("rid", info.rid),
				slog.String("line", fmt.Sprintf("%s %dB %.1fs", kind, total, time.Since(info.t0).Seconds())))
			return
		}
		total += len(frame)
		if flusher != nil {
			flusher.Flush()
		}
	}
	s.doneLog(info.rid, kind, total, info.t0)
}

func truncStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
