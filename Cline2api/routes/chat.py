# -*- coding: utf-8 -*-
"""routes/chat.py — OpenAI Chat Completions 协议路由（/v1/chat/completions）。

上游 api.cline.bot 原生说 Chat（LiteLLM 形态），本层近乎直通：免费清单门 +
401 自愈（credential 刷新一次重发）+ 流式断流保护（GeneratorExit 分支、断流
落 resp_partial、补错误帧+[DONE]）。core = server 模块动态查找，测试打桩
server.requests / server._dump_save 等对路由层同样生效。
"""
import json
import time

import requests
from flask import Response, g, jsonify

# 直通上游的请求字段白名单
PASSTHROUGH = ("model", "messages", "stream", "temperature", "top_p", "max_tokens",
               "tools", "tool_choice", "stop", "user", "stream_options")


def _post_chat(core, payload, want_stream):
    """上游 chat POST + 401 自愈（刷新一次重发）。返回 upstream 或抛 RequestException。"""
    try:
        upstream = requests.post(core.API_BASE + core.CHAT_PATH, json=payload,
                                 headers=core._upstream_headers(),
                                 stream=want_stream, timeout=180)
    except requests.RequestException as e:
        raise
    if upstream.status_code == 401:
        # 401 自愈：刷新一次重发
        try:
            core.credential.refresh()
            upstream = requests.post(core.API_BASE + core.CHAT_PATH, json=payload,
                                     headers=core._upstream_headers(),
                                     stream=want_stream, timeout=180)
        except Exception as e:
            core.logger.error("[%s] 401 刷新后仍失败: %s", g.rid, e)
    return upstream


def register(app, core):
    """挂载 chat 路由。core = server 模块（动态查找，测试打桩对路由层生效）。"""
    logger = core.logger

    @app.route("/v1/chat/completions", methods=["POST"])
    def chat():
        p = g.payload
        model = p.get("model") or ""
        if model and not core._model_free(model):
            logger.warning("[%s] 拒绝非免费模型: %s", g.rid, model)
            return jsonify(error={"message": "model %s 不在免费清单（Cline2api 只转发免费模型）" % model,
                                  "type": "invalid_request_error"}), 400
        payload = {k: v for k, v in p.items() if k in PASSTHROUGH}
        payload["model"] = model  # 模型 id 原样转（cline-pass/ 前缀上游自认）

        want_stream = bool(p.get("stream"))
        try:
            upstream = _post_chat(core, payload, want_stream)
        except requests.RequestException as e:
            logger.error("[%s] 上游连接失败: %s", g.rid, e)
            return jsonify(error={"message": str(e), "type": "upstream_error"}), 502

        if upstream.status_code == 401:
            return jsonify(error={"message": "upstream 401 after refresh",
                                  "type": "upstream_error"}), 502
        if upstream.status_code >= 400:
            body = upstream.text[:400]
            logger.warning("[%s] 上游错误 HTTP %s: %s", g.rid, upstream.status_code, body)
            return Response(upstream.text, status=upstream.status_code,
                            content_type=upstream.headers.get("Content-Type", "application/json"))

        if not want_stream:
            body = upstream.content
            logger.info("[%s] done: status=200 %dB %.1fs", g.rid, len(body), time.time() - g.t0)
            core._dump_save(g.rid, "resp", body)
            return Response(body, status=200, content_type="application/json")

        # 流式生成器在请求上下文销毁后才迭代，期间禁止访问 g.*，否则抛
        # Working outside of application context（异常再被下面的 except 吞掉），
        # 表现为日志只有 in: 没有 done:、客户端收到空内容。
        rid, t0 = g.rid, g.t0
        dump_on = core.DUMP_ON  # 流式攒帧于内存，流末一次性落盘（DUMP=0 时不攒）

        def generate():
            total = 0
            buf = []
            try:
                for chunk in upstream.iter_content(chunk_size=None):
                    if chunk:
                        total += len(chunk)
                        if dump_on and total <= core.DUMP_MAX_BYTES:
                            buf.append(chunk)
                        yield chunk
                logger.info("[%s] done: stream %dB %.1fs", rid, total, time.time() - t0)
                if dump_on and buf:
                    core._dump_save(rid, "resp", b"".join(buf))
            except GeneratorExit:
                # 客户端主动断开（长流常见）。GeneratorExit 继承 BaseException，
                # 不加这分支会被下面的 except 漏掉、也不留日志，排查时看不出是谁先走的。
                logger.warning("[%s] client 断开: stream %dB %.1fs", rid, total, time.time() - t0)
                raise
            except Exception as e:
                logger.error("[%s] stream 中断: %s (%dB %.1fs)", rid, e, total, time.time() - t0)
                # 断流时把已收到的部分落盘：正常路径只在流末 _dump_save，中途断了
                # 就只剩 .req，2.8MB 证据全丢，下一次同样故障无从比对。
                if dump_on and buf:
                    core._dump_save(rid, "resp_partial", b"".join(buf))
                # 裸掐断会让客户端把半截回答当完整结果。显式补错误帧 + [DONE]
                # （与 CodeArts2api 的流内错误帧转 OpenAI 错误帧同一套约定）。
                err = json.dumps({"error": {"message": "upstream stream ended prematurely: %s" % e,
                                            "type": "upstream_error"}},
                                 ensure_ascii=False, separators=(",", ":")).encode("utf-8")
                try:
                    yield b"data: " + err + b"\n\n"
                    yield b"data: [DONE]\n\n"
                except GeneratorExit:
                    raise

        resp = Response(generate(), status=200, mimetype="text/event-stream")
        resp.headers["Cache-Control"] = "no-cache, no-transform"
        resp.headers["X-Accel-Buffering"] = "no"
        return resp
