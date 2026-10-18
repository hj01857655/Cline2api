# -*- coding: utf-8 -*-
"""Cline2api：OpenAI 兼容反代 → api.cline.bot（ClinePass 通道，锁免费模型）。

结构：单上游通道 + 三协议路由分包（本文件只做调度）：
  routes/chat.py        /v1/chat/completions（近乎直通 + 401 自愈 + 断流保护）
  routes/responses.py   /v1/responses（经 adapters/responses.py 翻译）
  routes/messages.py    /v1/messages（经 adapters/anthropic.py 翻译）
兼容别名 anthropic_bridge / responses_bridge 保留给旧探针。

上游协议（cline 源码 sdk/packages/llms）：
- chat 上游: POST {API_BASE}/api/v1/chat/completions（LiteLLM 形态，OpenAI 兼容）
- 免费清单: GET  {API_BASE}/api/v1/ai/cline/recommended-models
  → 免费 id：`cline-free/*`、`*:free`、catalog pricing 三项全 0
- 认证: Bearer accessToken（oauth.py 产出，credential.py 刷新）
"""
import json
import logging
import os
import sys
import threading
import time
import uuid
from logging.handlers import RotatingFileHandler

import requests
from dotenv import load_dotenv
from flask import Flask, Response, g, jsonify, request

import credential
from adapters import anthropic, responses
from routes import chat, messages, responses as routes_responses

# 兼容别名：旧探针/测试引用的旧模块名（anthropic_bridge / responses_bridge）
anthropic_bridge = anthropic
responses_bridge = responses

load_dotenv()

API_BASE = "https://api.cline.bot"
CHAT_PATH = "/api/v1/chat/completions"
MODELS_PATH = "/api/v1/ai/cline/recommended-models"
UA = "Cline2api/0.1"
API_KEY = os.environ.get("API_KEY", "").strip()
LISTEN_HOST = "127.0.0.1"
LISTEN_PORT = int(os.getenv("PORT", "8381"))

# ── 日志（qoder2api 同一标准：request.log 轮转 + rid + in:/done: 双行 + ua=）────
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "request.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        RotatingFileHandler(LOG_FILE, maxBytes=2 * 1024 * 1024, backupCount=3, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)
logging.getLogger("werkzeug").disabled = True

# ── 请求/响应原文落盘（默认 dumps/，DUMP_DIR 改路径，DUMP=0 关闭）─────────────
# 只落 body 原文，绝不落 HTTP header：Authorization 里是明文网关 key，且 body 需可直接重放
DUMP_DIR = os.environ.get("DUMP_DIR", "") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "dumps")
DUMP_ON = os.environ.get("DUMP", "1").strip() != "0"
DUMP_MAX_BYTES = 512 * 1024   # 单方向落盘上限，防超大响应撑爆磁盘
DUMP_MAX_FILES = int(os.environ.get("DUMP_MAX_FILES", "200"))  # ≤0 表示不轮转


def _dump_trim():
    """dump 数量超过 DUMP_MAX_FILES 时按 mtime 删最旧（明文提示词会无限增长）。"""
    if DUMP_MAX_FILES <= 0:
        return
    try:
        entries = sorted((e for e in os.scandir(DUMP_DIR) if e.is_file()),
                         key=lambda e: e.stat().st_mtime)
    except OSError:
        return
    for stale in entries[:len(entries) - DUMP_MAX_FILES]:
        try:
            os.remove(stale.path)
        except OSError:
            pass


# dump 文件名前缀启动段：rid 每进程从 r0001 重新计数，旧版同名 rid 跳重启后直接覆盖旧证据
_BOOT_TAG = time.strftime("%m%d-%H%M%S", time.localtime()) + "-p%d" % os.getpid()


def _dump_save(rid, kind, data):
    """把请求/响应原文落盘到 dumps/<rid>.<kind>.txt（DUMP=0 时跳过）。"""
    if not DUMP_ON or not data:
        return
    try:
        os.makedirs(DUMP_DIR, exist_ok=True)
        text = data if isinstance(data, str) else data.decode("utf-8", "replace")
        truncated = len(text) > DUMP_MAX_BYTES
        path = os.path.join(DUMP_DIR, "%s.%s.%s.txt%s" % (_BOOT_TAG, rid, kind, ".truncated" if truncated else ""))
        with open(path, "w", encoding="utf-8") as f:
            f.write(text[:DUMP_MAX_BYTES])
        _dump_trim()
    except OSError as e:
        logger.warning("[%s] 落盘失败: %s", rid, e)

# ── rid 单调计数器（替代 time-based 生成：旧式 time.time()*10%10000 每 1000 秒循环
# 撞名，r9983 曾在 09:43 与 09:59 各出现一次，导致按 rid 做的配对统计与 dump 落盘不可信）
_seq_lock = threading.Lock()
_req_seq = 0


def _next_req_id():
    global _req_seq
    with _seq_lock:
        _req_seq += 1
        return "r%04d" % _req_seq


app = Flask(__name__)

# ── 免费模型清单（启动拉一次，后台 10 分钟刷新；落盘上次可用清单，冷启动不从空开始）──
_free_models = {"ids": [], "models": {}, "fail_until": 0.0}
_HERE = os.path.dirname(os.path.abspath(__file__))
_FREE_CACHE = os.path.join(_HERE, "free_cache.json")


def _free_ids_from_body(models):
    """从 recommended-models 的 free 数组筛免费面 id 与目录。命名规则即免费面契约。"""
    free_ids, catalog = [], {}
    for m in models:
        if not isinstance(m, dict):
            continue
        mid = m.get("id") or ""
        if mid and (mid.startswith("cline-free/") or mid.endswith(":free")):
            free_ids.append(mid)
            catalog[mid] = m
    return free_ids, catalog


def _load_free_cache() -> None:
    try:
        with open(_FREE_CACHE, encoding="utf-8") as f:
            data = json.load(f)
        ids, catalog = _free_ids_from_body(data.get("models") or [])
    except Exception:
        return  # 无缓存或缓存损坏：首次启动本就如此，不是错误
    if not ids:
        return
    _free_models["ids"], _free_models["models"] = ids, catalog
    logger.info("[models] 用上次落盘清单 %d 个兜底", len(ids))


def _save_free_cache(models) -> None:
    try:
        tmp = _FREE_CACHE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"at": int(time.time()), "models": models}, f)
        os.replace(tmp, _FREE_CACHE)
    except Exception as e:
        logger.warning("[models] 清单落盘失败（不影响转发）: %s", e)


def _fetch_free_models(quick: bool = False) -> None:
    """拉 recommended-models：免费面 = body.free 数组（cline-free/* 前缀，无订阅可用）。
    clinePass/* 是订阅内含面（无 ClinePass 订阅一律 403 ENTITLEMENT），不放行。

    `quick=True` 给请求路径用：只试一次、超时短、失败进退避窗口。实测这条 GET 的失败
    是**成串来的**（坏窗口里连撞六次全挂，curl 走 SChannel 也挂），一次失败要 5~6s 才
    返回；重试三轮会挂在用户请求上十几秒，比临时按命名兜底糟得多。

    失败时**保留旧清单**不清空：清单空了会把合法免费模型一并拒掉（OpenCode2api 2026-10-13 00:25 的实况）。
    """
    if quick and time.time() < _free_models["fail_until"]:
        return
    attempts = 1 if quick else 3
    timeout = 4 if quick else 20
    last_err = None
    for attempt in range(attempts):
        try:
            r = requests.get(API_BASE + MODELS_PATH,
                             headers={"User-Agent": UA}, timeout=timeout)
            body = r.json() if r.content else {}
            free_ids, catalog = _free_ids_from_body(body.get("free") or [])
            if free_ids:
                _free_models["ids"], _free_models["models"] = free_ids, catalog
                _free_models["fail_until"] = 0.0
                _save_free_cache(list(catalog.values()))
                logger.info("[models] 免费模型 %d 个: %s", len(free_ids), ", ".join(free_ids))
            else:
                # 联系上上游了但免费面是空的（全转付费 / 命名规则变了筛空）：这同样是
                # 可信答案，按空清单认，并给退避 —— 否则每条请求都会因为 `not ids`
                # 再打一次上游，坏窗口外也会把日志刷成一条模样的空列表告警。
                _free_models["fail_until"] = time.time() + 30
                logger.warning("[models] free 列表为空（HTTP %s，顶层键=%s）；保留旧清单 %d 个",
                               r.status_code, list(body.keys())[:6], len(_free_models["ids"]))
            return
        except Exception as e:
            last_err = e
            if attempt < attempts - 1:
                time.sleep(0.5 * (attempt + 1))
    _free_models["fail_until"] = time.time() + 30
    # 带异常类型：把网络错误和代码错误（清单结构变了、r.json() 的解析错）吞在同一句
    # 「拉取失败」里，只打消息下次排查会认错方向。
    logger.warning("[models] 拉取失败（试 %d 次）: %s: %s；保留旧清单 %d 个",
                   attempts, type(last_err).__name__, last_err, len(_free_models["ids"]))


def _model_refresh_loop() -> None:
    """后台刷新：每 60s 醒一次。

    原来是固定睡 600 再拉一次，一次失败要等满 10 分钟才有下一次机会，坏窗口能连撞
    两三分钟。短周期轮询让失败后 60s 就能补上（这条链路没有 TTL 概念，60s 一次的
    上游清单请求成本可以忽略）。
    """
    while True:
        time.sleep(60)
        _fetch_free_models()


def _token_refresh_loop() -> None:
    """后台 token 续期：每 30 分钟查一次，**临期才刷**。

    原先固定每 30 分钟无脑 refresh()，而 accessToken 实测只活 60 分钟
    （JWT iat/exp 10:40:59 → 11:40:59），于是每轮都可能白刷一次；而磁盘 token 一旦
    临期，真正触发刷新的反而是 401 自愈那条路径，刷新被砸在用户第一个请求上
    （表现成"每次启动必刷新"）。现改为 ensure_fresh()：token 还新鲜就不碰上游。
    刷新失败只记日志，绝不打断服务（下次循环再试）。
    """
    while True:
        time.sleep(30 * 60)
        try:
            credential.ensure_fresh()
        except Exception as e:
            logger.warning("[token] 后台刷新失败（下次循环再试）: %s", e)


@app.before_request
def _auth_and_rid():
    g.rid = _next_req_id()
    g.t0 = time.time()
    if API_KEY and request.path != "/health":
        if request.headers.get("Authorization") != "Bearer " + API_KEY:
            return jsonify(error="unauthorized"), 401
    ua = (request.headers.get("User-Agent") or "-").replace("\r", " ").replace("\n", " ")
    body_len = request.content_length or 0
    try:
        payload = request.get_json(force=True, silent=True) or {}
    except Exception:
        payload = {}
    g.payload = payload
    logger.info("[%s] in: %s %s | model=%s msgs=%s tools=%s stream=%s | ua=%s | body=%dB",
                g.rid, request.method, request.path,
                payload.get("model"), len(payload.get("messages") or []),
                len(payload.get("tools") or []), payload.get("stream"), ua, body_len)
    # 只对 chat 请求落盘 req 原文（可直接重放的纯 body，绝不落 header）
    if request.path == "/v1/chat/completions":
        _dump_save(g.rid, "req", request.get_data(cache=True, as_text=True))


def _upstream_headers() -> dict:
    """身份头全套：上游 2025-09 起 Firebase→WorkOS 迁移后，Authorization 必须带
    `workos:` 前缀（社区实锤，OmniRoute #3321 同款问题），否则 401 要求重登录。"""
    return {"Authorization": "Bearer workos:" + credential.token(),
            "Content-Type": "application/json",
            "HTTP-Referer": "https://cline.bot", "X-Title": "Cline",
            "X-CLIENT-TYPE": "cline-sdk", "User-Agent": "Cline/2.6.9",
            "X-CLIENT-VERSION": "2.6.9", "X-PLATFORM": "cline-sdk",
            "X-PLATFORM-VERSION": "2.6.9", "X-IS-MULTIROOT": "false"}


@app.route("/health")
def health():
    return jsonify(ok=True, service="Cline2api")


@app.route("/v1/models")
def models():
    if not _free_models["ids"]:
        _fetch_free_models(quick=True)   # 客户端列模型不该被上游抖动拖住
    now = int(time.time())
    return jsonify(data=[{"id": mid, "object": "model", "created": now, "owned_by": "cline-free"}
                         for mid in _free_models["ids"]])


def _model_free(model_id: str) -> bool:
    """免费面判定：仅 cline-free/*（或 *:free）。cline-pass/* 是订阅面，一律拒。"""
    if not _free_models["ids"]:
        _fetch_free_models(quick=True)
    if _free_models["ids"]:
        return (model_id in _free_models["ids"]
                or model_id.startswith("cline-free/")
                or model_id.endswith(":free"))
    # 清单拉不到且无缓存：按命名规则兜底放行 —— 这条**不**违背「宁拒不放」。
    # 订阅面 id（cline-pass/*、cline-cloud/*）匹配不上 cline-free/ 前缀也不带 :free
    # 后缀，仍然被拒；真正被误杀的只有名字里就写着 free 的合法免费模型。
    return (model_id.startswith("cline-free/") or model_id.endswith(":free"))


PASSTHROUGH = chat.PASSTHROUGH   # 兼容旧引用（实际使用在 routes/chat.py）


# ── 协议路由层挂载（routes/）：每协议一个文件，本文件只做调度 ──────────────────
# register(app, core) 里 core 传本模块：路由经模块属性动态查找（credential、
# _upstream_headers、_model_free、_dump_save、DUMP_*、logger），测试对
# server.requests / server.credential 等的打桩对路由层同样生效。
import sys as _sys

_core = _sys.modules[__name__]
chat.register(app, _core)
routes_responses.register(app, _core)
messages.register(app, _core)


if __name__ == "__main__":
    try:
        info = credential.load()
        print("Cline2api 已启动: http://%s:%s (provider=%s)" % (LISTEN_HOST, LISTEN_PORT, info["provider"]))
    except Exception as error:
        print("凭证不可用: %s" % error)
        print("请先登录：python oauth.py")
        sys.exit(1)
    # 启动预检：磁盘 token 已临期就先续上，别等用户第一个请求吃 401 才刷
    # （失败不阻断启动，交给 401 自愈路径兜底）
    try:
        if credential.ensure_fresh():
            print("启动预检：token 临期，已提前刷新")
        else:
            print("启动预检：token 仍新鲜，跳过刷新")
    except Exception as error:
        print("启动预检刷新失败（不阻断，将由 401 自愈兜底）: %s" % error)
    _load_free_cache()        # 先用上次落盘的清单兜底，冷启动不从空清单开始
    _fetch_free_models(quick=True)   # 启动只试一次 4s；补拉交给下面的后台线程
    threading.Thread(target=_model_refresh_loop, daemon=True).start()
    threading.Thread(target=_token_refresh_loop, daemon=True).start()
    print("OpenAI Base URL: http://%s:%s/v1（仅免费模型）" % (LISTEN_HOST, LISTEN_PORT))
    app.run(host=LISTEN_HOST, port=LISTEN_PORT, threaded=True)
