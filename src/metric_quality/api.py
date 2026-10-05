"""用于离线验收的无依赖 JSON HTTP API。"""

from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .contracts import CONTRACT_VERSION, MeasurementConflict, MeasurementRejected, NonFiniteLiteral
from .service import MetricQualityService


class Handler(BaseHTTPRequestHandler):
    service = MetricQualityService()

    def _json(self, status: int, body: dict) -> None:
        data = json.dumps(body, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _token(self) -> str:
        return self.headers.get("Authorization", "").removeprefix("Bearer ")

    def do_GET(self):
        with self.service.lock:
            self._get()

    def do_POST(self):
        with self.service.lock:
            self._post()

    def _get(self):
        if self.path == "/health":
            return self._json(200, {"status": "ok", "service": "metric-quality"})
        try:
            if self.path.startswith("/lots/") and self.path.endswith("/measurements"):
                return self._json(200, self.service.list_measurements(self._token(), self.path.split("/")[2]))
            if self.path.startswith("/lots/"):
                return self._json(200, self.service.get_lot(self._token(), self.path.split("/", 2)[2]))
        except PermissionError as exc:
            return self._json(403, {"error": str(exc)})
        except Exception as exc:
            return self._json(400, {"error": str(exc)})
        return self._json(404, {"error": "not found"})

    def _post(self):
        try:
            # 非有限常量先解析为占位符，契约校验再按字段报告拒绝原因。
            raw = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            body = json.loads(raw, parse_constant=NonFiniteLiteral) if raw.strip() else {}
            if self.path == "/login":
                return self._json(200, {"token": self.service.auth.login(body["user_id"], body["password"])})
            token = self._token()
            if self.path == "/lots":
                return self._json(201, self.service.create_lot(token, body["lot_id"], body["product"], body["process_rev"], body["sample_count"]))
            if self.path.startswith("/lots/") and self.path.endswith("/measurements/batch"):
                lot_id = self.path.split("/")[2]
                return self._json(201, self.service.add_measurements(token, lot_id, body.get("measurements")))
            if self.path.startswith("/lots/") and self.path.endswith("/measurements"):
                lot_id = self.path.split("/")[2]
                return self._json(201, self.service.add_measurement(token, lot_id, body.get("test_frequency_hz"), body.get("response"), body.get("noise", 0.0), body.get("instrument")))
            if self.path.startswith("/lots/") and self.path.endswith("/analysis"):
                return self._json(200, self.service.analyze(token, self.path.split("/")[2]))
            return self._json(404, {"error": "not found"})
        except MeasurementRejected as exc:
            return self._json(422, {"error": {
                "code": "measurement_rejected",
                "rule_version": CONTRACT_VERSION,
                "message": str(exc),
                "violations": [item.as_dict() for item in exc.violations],
            }})
        except MeasurementConflict as exc:
            return self._json(409, {"error": {
                "code": "measurement_conflict",
                "message": str(exc),
                "identity": exc.identity,
                "differing_fields": exc.differing_fields,
                "existing_measurement_id": exc.existing_measurement_id,
            }})
        except PermissionError as exc:
            return self._json(403, {"error": str(exc)})
        except Exception as exc:
            return self._json(400, {"error": str(exc)})


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", default=":memory:")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    Handler.service = MetricQualityService(args.database)
    Handler.service.bootstrap_admin()
    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
