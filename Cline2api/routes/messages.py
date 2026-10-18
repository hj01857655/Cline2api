# -*- coding: utf-8 -*-
"""routes/messages.py — Anthropic Messages 协议路由（/v1/messages）。

Claude Code 等原生 Anthropic 客户端。adapters/anthropic.py 把 Anthropic ⇄ Chat
双向翻掉，复用上游直连 + 401 自愈；响应侧包成 Anthropic SSE。
"""
import json
import time

import requests
from flask import Response, g, jsonify

from adapters import anthropic as ab
from routes.chat import PASSTHROUGH, _post_chat


def register(app, core):
    """挂载 messages 路由。core = server 模块（动态查找，见 routes/chat.py）。"""
    logger = core.logger

    @app.route("/v1/messages", methods=["POST"])
    def messages_api():
        p = g.payload
        if not isinstance(p, dict) or not p:
            return jsonify(ab.error_body("invalid_request_error",
                "invalid request: JSON object body required")), 400
        model = p.get("model") or ""
        if model and not core._model_free(model):
            logger.warning("[%s] 拒绝非免费模型: %s", g.rid, model)
            return jsonify(ab.error_body("invalid_request_error",
                "model %s 不在免费清单（Cline2api 只转发免费模型）" % model)), 400

        chat_body = ab.anthropic_to_chat(p)
        if not chat_body.get("messages"):
            return jsonify(ab.error_body("invalid_request_error",
                "invalid request: 'messages' is required")), 400

        payload = {k: v for k, v in chat_body.items() if k in PASSTHROUGH}
        payload["model"] = model

        want_stream = bool(p.get("stream"))
        try:
            upstream = _post_chat(core, payload, want_stream)
        except requests.RequestException as e:
            logger.error("[%s] 上游连接失败: %s", g.rid, e)
            return jsonify(ab.error_body("api_error", str(e))), 502

        if upstream.status_code == 401:
            return jsonify(ab.error_body("api_error", "upstream 401 after refresh")), 502
        if upstream.status_code >= 400:
            logger.warning("[%s] 上游错误 HTTP %s: %s", g.rid, upstream.status_code,
                           upstream.text[:400])
            core._dump_save(g.rid, "resp", upstream.text)
            err = ab.error_body(ab.err_type_for_http(upstream.status_code), upstream.text[:400])
            return Response(json.dumps(err, ensure_ascii=False), status=upstream.status_code,
                            content_type="application/json")

        if want_stream:
            rid, t0 = g.rid, g.t0

            def generate():
                total = 0
                try:
                    for frame in ab.chat_sse_to_anthropic(
                            upstream.iter_lines(decode_unicode=False), model):
                        total += len(frame)
                        yield frame
                    logger.info("[%s] done: messages stream %dB %.1fs",
                                rid, total, time.time() - t0)
                except GeneratorExit:
                    logger.warning("[%s] client 断开: messages stream %dB %.1fs",
                                   rid, total, time.time() - t0)
                    raise
                except Exception as e:
                    logger.error("[%s] messages stream 中断: %s (%dB %.1fs)",
                                 rid, e, total, time.time() - t0)

            resp = Response(generate(), status=200, mimetype="text/event-stream")
            resp.headers["Cache-Control"] = "no-cache, no-transform"
            resp.headers["X-Accel-Buffering"] = "no"
            return resp

        body = upstream.content
        result = json.loads(body.decode("utf-8", "ignore"))
        out = ab.chat_json_to_anthropic(result, model)
        body = json.dumps(out, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        logger.info("[%s] done: messages %dB %.1fs", g.rid, len(body), time.time() - g.t0)
        core._dump_save(g.rid, "resp", body)
        return Response(body, status=200, content_type="application/json")
