# Cline2api-go

![Go](https://img.shields.io/badge/Go-1.24-00ADD8?logo=go&logoColor=white) ![Platform](https://img.shields.io/badge/platform-Windows%20%7C%20Linux-lightgrey) ![Type](https://img.shields.io/badge/type-API%20Gateway-blue)

Python 版 [Cline2api](../Cline2api) 的 Go 迁移版：OpenAI 兼容反代 → api.cline.bot
（ClinePass 通道，锁免费模型）。零第三方依赖（纯标准库），单 exe 部署。

## 接口

| 端点 | 说明 |
|---|---|
| `GET /health` | 健康检查（永远免鉴权） |
| `GET /v1/models` | 模型列表（免费池实时清单，`owned_by: cline-free`） |
| `POST /v1/chat/completions` | OpenAI Chat Completions（近直通），通用 OpenAI 客户端 |
| `POST /v1/responses` | OpenAI Responses SSE（每帧带 sequence_number），Codex / grok-build |
| `POST /v1/messages` | Anthropic Messages SSE，Claude Code / Kiro |
| `GET /`、`/admin` | Web 管理面板（内置，开箱即用）：凭据状态、免费池状态、模型清单与健康诊断。状态目录放一份自定义 `panel.html` 可覆盖内置版 |

`/admin/api/overview` 面板数据源：凭据快照（`credential.valid` = accessToken 可读，不含 token 本体）、免费池状态（`free_pool.count/backoff/empty`）、模型清单与 `local_auth`（API_KEY 是否开启）。`API_KEY` 非空时 `/admin` 页面可匿名打开，`/admin/api/*` 与业务路由同样需要 Bearer。

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

## Docker 部署

```bash
# 配置环境变量后启动
cp .env.example .env   # 按需填写
docker compose up -d
```

镜像同时发布到 GHCR：`ghcr.io/hj01857655/cline2api`（跟随 v* 标签发布，含 latest，linux/amd64 + linux/arm64）。

二进制发版：推送 `v*` 标签后 Actions 自动构建 linux / windows / darwin 的 amd64 + arm64 六平台产物并发布 Release。
