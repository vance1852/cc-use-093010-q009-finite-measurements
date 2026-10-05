"""用于离线验收的无依赖 JSON HTTP API。

写入类接口对请求体使用严格 JSON 解析（拒绝 Infinity/-Infinity/NaN 与重复
键），契约失败时返回 422 和逐字段、逐规则的拒绝原因，而不是让污染数据入库
后在分析阶段才暴露。另提供与服务层等价的 :class:`JsonApplication` 供测试。
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Mapping
from urllib.parse import urlparse

from .contracts import StrictJsonError, loads_strict_json
from .errors import Conflict, Forbidden, InvalidState, NotFound, ServiceError, ValidationFailed
from .service import MetricQualityService


@dataclass(frozen=True, slots=True)
class Response:
    status: int
    body: dict[str, Any]


class JsonApplication:
    """把方法/路径映射到领域服务，不依赖网络，便于单元测试。"""

    def __init__(self, service: MetricQualityService) -> None:
        self.service = service

    @staticmethod
    def _strict_json(body: bytes) -> dict[str, Any]:
        if not body:
            raise ValidationFailed("请求体必须是 JSON 对象")
        value = loads_or_422(body)
        if not isinstance(value, dict):
            raise ValidationFailed("请求体必须是 JSON 对象")
        return value

    def handle(
        self,
        method: str,
        target: str,
        headers: Mapping[str, str] | None = None,
        body: bytes = b"",
    ) -> Response:
        normalized = {key.lower(): value for key, value in (headers or {}).items()}
        token = normalized.get("authorization", "").removeprefix("Bearer ").strip()
        path = urlparse(target).path.rstrip("/") or "/"
        parts = [part for part in path.split("/") if part]
        try:
            if method == "GET" and path == "/health":
                return Response(200, {"status": "ok", "service": "metric-quality"})

            payload = self._strict_json(body) if method in {"POST", "PUT", "PATCH"} else {}

            if method == "POST" and path == "/login":
                return Response(200, {"token": self.service.auth.login(payload["user_id"], payload["password"])})

            if method == "POST" and path == "/lots":
                result = self.service.create_lot(
                    token, payload["lot_id"], payload["product"], payload["process_rev"], payload["sample_count"]
                )
                return Response(201, result)

            if method == "GET" and len(parts) == 2 and parts[0] == "lots":
                return Response(200, self.service.get_lot(token, parts[1]))

            if method == "POST" and len(parts) == 3 and parts[0] == "lots" and parts[2] == "measurements":
                result = self.service.add_measurement(
                    token, parts[1], payload["observation_key"], payload["test_frequency_hz"],
                    payload["response"], payload.get("noise", 0.0), payload["instrument"],
                )
                return Response(201, result)

            if method == "POST" and len(parts) == 4 and parts[0] == "lots" and parts[2:] == ["measurements", "batch"]:
                result = self.service.add_measurements(token, parts[1], payload.get("measurements", []))
                return Response(201, result)

            if method == "POST" and len(parts) == 3 and parts[0] == "lots" and parts[2] == "analysis":
                return Response(200, self.service.analyze(token, parts[1]))

            if method == "GET" and len(parts) == 3 and parts[0] == "lots" and parts[2] == "quarantine":
                return Response(200, {"items": self.service.list_quarantine(token, parts[1])})

            if method == "POST" and path == "/quarantine/scan":
                result = self.service.quarantine_invalid_measurements(token, payload.get("lot_id"))
                return Response(200, result)

            if method == "POST" and len(parts) == 3 and parts[0] == "quarantine" and parts[2] == "review":
                result = self.service.review_quarantine(
                    token, int(parts[1]), payload["decision"], payload.get("note", "")
                )
                return Response(200, result)

            return Response(404, {"error": {"code": "route_not_found", "message": "接口不存在"}})
        except ValidationFailed as exc:
            error: dict[str, Any] = {"code": exc.code, "message": str(exc)}
            if exc.rejections:
                error["rejections"] = exc.rejections
            return Response(exc.status, {"error": error})
        except (NotFound, Conflict, Forbidden, InvalidState) as exc:
            return Response(exc.status, {"error": {"code": exc.code, "message": str(exc)}})
        except ServiceError as exc:
            return Response(exc.status, {"error": {"code": exc.code, "message": str(exc)}})
        except (KeyError, TypeError, ValueError) as exc:
            return Response(422, {"error": {"code": "invalid_request", "message": str(exc)}})


def loads_or_422(body: bytes) -> Any:
    """解析严格 JSON，把非有限常量等解析错误转成契约失败。"""

    try:
        return loads_strict_json(body)
    except StrictJsonError as exc:
        raise ValidationFailed(exc.message, [{"field": "$", "rule": exc.rule, "message": exc.message}]) from exc


def make_handler(application: JsonApplication):
    class Handler(BaseHTTPRequestHandler):
        server_version = "MetricQuality/1"

        def do_GET(self) -> None:  # noqa: N802
            self._dispatch()

        def do_POST(self) -> None:  # noqa: N802
            self._dispatch()

        def _dispatch(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length) if length else b""
            response = application.handle(self.command, self.path, dict(self.headers.items()), body)
            encoded = json.dumps(response.body, ensure_ascii=False, allow_nan=False).encode("utf-8")
            self.send_response(response.status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format: str, *args: object) -> None:
            return

    return Handler


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", default=":memory:")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8082)
    args = parser.parse_args()
    service = MetricQualityService(args.database)
    service.bootstrap_admin()
    application = JsonApplication(service)
    ThreadingHTTPServer((args.host, args.port), make_handler(application)).serve_forever()


if __name__ == "__main__":
    main()
