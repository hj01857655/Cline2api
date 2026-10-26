# Cline2api-go

Python 版 [Cline2api](../Cline2api) 的 Go 迁移版：OpenAI 兼容反代 → api.cline.bot
（ClinePass 通道，锁免费模型）。零第三方依赖（纯标准库），单 exe 部署。

## 三协议路由

| 路由 | 协议 | 客户端 |
|---|---|---|
| `/v1/chat/completions` | OpenAI Chat（近直通） | 通用 OpenAI 客户端 |
| `/v1/responses` | OpenAI Responses（每帧带 sequence_number） | Codex / grok-build |
| `/v1/messages` | Anthropic Messages SSE | Claude Code / Kiro |

## 构建与运行

```
go build -o cline2api.exe .
```

```
cline2api.exe          # 启动服务（默认 127.0.0.1:8381）
cline2api.exe login    # WorkOS 设备流登录，凭据落 ~/.cline/data/Cline2api_auth.json
```

登录后把 `Cline2api_auth.json` 的内容放到状态目录 `credentials.json`
（或直接设 `CLINE2API_STATE_DIR` 指向已有 Python 版目录，无缝接管其
credentials.json / free_cache.json / dumps/）。

## 配置（环境变量或状态目录 `.env`）

| 变量 | 默认 | 说明 |
|---|---|---|
| `API_KEY` | 空 | 客户端鉴权（Bearer），空 = 不鉴权；`/health` 永远豁免 |
| `PORT` | `8381` | 监听端口（固定 127.0.0.1） |
| `CLINE2API_STATE_DIR` | exe 所在目录 | credentials.json / free_cache.json / request.log / dumps/ 位置 |
| `DUMP` | `1` | 请求/响应原文落盘开关（只落 body，绝不落 header） |
| `DUMP_DIR` | `<状态目录>/dumps` | 落盘目录 |
| `DUMP_MAX_FILES` | `200` | 超量按 mtime 删最旧，≤0 不轮转 |

## 与 Python 版的行为差异（有意修复）

1. **workos: 双前缀 401**：Python 版 `token()` 补前缀、`_upstream_headers()` 再拼一次，
   冷启动首个请求必 401 靠自愈刷新才恢复。Go 版 Token() 返回裸 token，前缀由
   upstream 层统一拼一次。
2. **非流式 `{"data":{...}}` 包装层**：上游非流式响应间歇性多包一层 data，
   Python 版 chat 路由原样透传（客户端拿不到顶层 choices）、messages/responses
   路由直接解析失败。Go 版三个路由统一解包（内层确有 choices/object 才解）。
3. Python 版 adapters/responses.py 的 InvokeSalvage（antml 文本工具调用回收）
   在 Cline2api 路由中未被引用，未移植。

## 状态文件兼容

`credentials.json`（accessToken/refreshToken/provider/expiresAt）与
`free_cache.json`（at/models）字节格式与 Python 版完全一致，两个版本可交替读写。

## 测试

```
go test ./...
```

`internal/handler/golden_test.go` 移植自 Python 版 `tests/test_responses_route.py`：
httptest 假上游覆盖 Responses 流式（seq 连续性、生命周期、usage、items 顺序）、
非流式、上游 4xx 透传、chat 流式回归。
