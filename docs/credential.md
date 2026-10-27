# Cline2api 凭据

## 登录

- `cline2api.exe login`：WorkOS AuthKit 设备流（user_code 一次性，10 分钟窗口）。
- 轮询必须按 RFC 8628 处理 pending（HTTP 400 + body error=authorization_pending → continue），
  此前首轮即挂的 bug 已修（internal/cred/oauth.go）。
- 凭据落盘：`credentials.json`（exe 同目录，与 StateDir 一致）。不写 `~/.cline` 的 IDE 凭证目录。

## token

- accessToken 实测约 60 分钟过期；401 自愈刷新，缓存按 JWT exp 判定。
- 模型：`cline-free/nemotron-3.5-super-vl` 等 free 路由。
