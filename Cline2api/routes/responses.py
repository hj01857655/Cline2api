# -*- coding: utf-8 -*-
"""routes/responses.py — OpenAI Responses 协议路由（/v1/responses）。

grok-build / Codex 等原生 Responses 客户端。上游仍是 Chat 协议：请求体经
adapters/responses.py 转成 Chat，响应侧把 Chat SSE 包成 Responses SSE（每帧
带 sequence_number）。免费清单门与 401 自愈同 chat 路由。
"""
import json
import time

import requests
from flask import Response, g, jsonify

from adapters import responses as rb
from routes.chat import PASSTHROUGH, _post_chat


def register(app, core):
    """挂载 responses 路由。core = server 模块（动态查找，见 routes/chat.py）。"""
    logger = core.logger

    @app.route("/v1/responses", methods=["POST"])
    def responses_api():
        p = g.payload
        if not isinstance(p, dict) or not p:
            return jsonify(error={"message": "invalid request: JSON object body required",
                                  "type": "invalid_request_error"}), 400
        model = p.get("model") or ""
        if model and not core._model_free(model):
            logger.warning("[%s] 拒绝非免费模型: %s", g.rid, model)
            return jsonify(error={"message": "model %s 不在免费清单（Cline2api 只转发免费模型）" % model,
                                  "type": "invalid_request_error"}), 400

        chat_body = rb.responses_to_chat(p)
        if not chat_body.get("messages"):
            return jsonify(error={"message": "invalid request: 'input' is required",
                                  "type": "invalid_request_error"}), 400

        payload = {k: v for k, v in chat_body.items() if k in PASSTHROUGH}
        payload["model"] = model

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
            logger.warning("[%s] 上游错误 HTTP %s: %s", g.rid, upstream.status_code,
                           upstream.text[:400])
            core._dump_save(g.rid, "resp", upstream.text)
            return Response(upstream.text, status=upstream.status_code,
                            content_type=upstream.headers.get("Content-Type", "application/json"))

        if want_stream:
            # 生成器在请求上下文销毁后才迭代，期间禁止访问 g.*（与 chat 同款）
            rid, t0 = g.rid, g.t0

            def generate():
                total = 0
                try:
                    for frame in rb.chat_sse_to_responses(
                            upstream.iter_lines(decode_unicode=False), model):
                        total += len(frame)
                        yield frame
                    logger.info("[%s] done: responses stream %dB %.1fs",
                                rid, total, time.time() - t0)
                except GeneratorExit:
                    logger.warning("[%s] client 断开: responses stream %dB %.1fs",
                                   rid, total, time.time() - t0)
                    raise
                except Exception as e:
                    logger.error("[%s] responses stream 中断: %s (%dB %.1fs)",
                                 rid, e, total, time.time() - t0)

            resp = Response(generate(), status=200, mimetype="text/event-stream")
            resp.headers["Cache-Control"] = "no-cache, no-transform"
            resp.headers["X-Accel-Buffering"] = "no"
            return resp

        body = upstream.content
        result = json.loads(body.decode("utf-8", "ignore"))
        out = rb.chat_json_to_responses(result, model)
        body = json.dumps(out, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        logger.info("[%s] done: responses %dB %.1fs", g.rid, len(body), time.time() - g.t0)
        core._dump_save(g.rid, "resp", body)
        return Response(body, status=200, content_type="application/json")
