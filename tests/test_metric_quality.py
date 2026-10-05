from __future__ import annotations

import http.client
import json
import threading
import unittest
from http.server import ThreadingHTTPServer

from metric_quality.acceptance import run as acceptance_run
from metric_quality.api import Handler
from metric_quality.contracts import (
    CONTRACT_VERSION,
    MeasurementConflict,
    MeasurementRejected,
)
from metric_quality.service import MetricQualityService


class ServiceContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = MetricQualityService()
        self.service.bootstrap_admin()
        self.token = self.service.auth.login("admin", "metric-admin")
        self.service.create_lot(self.token, "LOT-1", "cross-border-service-index", "POLICY-3.2", 10)

    def tearDown(self) -> None:
        self.service.db.close()

    def _counts(self) -> tuple[int, int]:
        measurements = self.service.db.execute("SELECT count(*) FROM measurements").fetchone()[0]
        events = self.service.db.execute("SELECT count(*) FROM lot_events").fetchone()[0]
        return measurements, events

    def _add_valid(self, test_frequency_hz: float = 450, response: float = 0.71, noise: float = 0.01, instrument: str = "reporting-gateway-1") -> dict:
        return self.service.add_measurement(self.token, "LOT-1", test_frequency_hz, response, noise, instrument)

    def test_non_finite_values_rejected_at_write_boundary(self) -> None:
        for bad in (float("inf"), float("-inf"), float("nan")):
            with self.assertRaises(MeasurementRejected) as caught:
                self._add_valid(response=bad)
            violation = caught.exception.violations[0]
            self.assertEqual(violation.field, "response")
            self.assertEqual(violation.rule, "must_be_finite")
        self.assertEqual(self._counts(), (0, 1))  # 仅剩批次创建事件，业务表与审计链未被污染

    def test_string_disguise_rejected(self) -> None:
        for bad in ("Infinity", "0.93", "1e999", "nan"):
            with self.assertRaises(MeasurementRejected) as caught:
                self._add_valid(response=bad)
            self.assertEqual(caught.exception.violations[0].rule, "must_be_json_number")
        with self.assertRaises(MeasurementRejected) as caught:
            self._add_valid(noise="0.01")
        self.assertEqual(caught.exception.violations[0].field, "noise")
        self.assertEqual(self._counts(), (0, 1))

    def test_bool_and_missing_fields_rejected(self) -> None:
        with self.assertRaises(MeasurementRejected) as caught:
            self._add_valid(response=True)
        self.assertEqual(caught.exception.violations[0].rule, "must_be_json_number")
        with self.assertRaises(MeasurementRejected) as caught:
            self._add_valid(response=None, instrument=None)
        rules = {(item.field, item.rule) for item in caught.exception.violations}
        self.assertIn(("response", "required"), rules)
        self.assertIn(("instrument", "required"), rules)
        self.assertEqual(self._counts(), (0, 1))

    def test_applicable_range_enforced(self) -> None:
        cases = [
            ({"response": 1e300}, "response"),      # 有限但超出指标量程
            ({"response": -(10**400)}, "response"),  # 巨型整数也不得溢出量程
            ({"test_frequency_hz": 0}, "test_frequency_hz"),
            ({"test_frequency_hz": -5}, "test_frequency_hz"),
            ({"noise": -0.1}, "noise"),
        ]
        for overrides, field in cases:
            with self.assertRaises(MeasurementRejected) as caught:
                self._add_valid(**overrides)
            violation = caught.exception.violations[0]
            self.assertEqual((violation.field, violation.rule), (field, "out_of_range"))
            self.assertIn("量程", violation.message)
        self.assertEqual(self._counts(), (0, 1))

    def test_source_identity_required(self) -> None:
        with self.assertRaises(MeasurementRejected) as caught:
            self._add_valid(instrument="   ")
        self.assertEqual(caught.exception.violations[0].rule, "must_be_non_empty")

    def test_identical_replay_is_idempotent(self) -> None:
        first = self._add_valid()
        second = self._add_valid()
        self.assertFalse(first["replayed"])
        self.assertTrue(second["replayed"])
        self.assertEqual(first["measurement_id"], second["measurement_id"])
        self.assertEqual(self._counts(), (1, 2))  # 重放不产生新业务行或审计事件

    def test_conflicting_replay_rejected(self) -> None:
        self._add_valid()
        with self.assertRaises(MeasurementConflict) as caught:
            self._add_valid(response=0.99)
        self.assertEqual(caught.exception.differing_fields, ["response"])
        self.assertEqual(caught.exception.identity["test_frequency_hz"], 450.0)
        self.assertEqual(self._counts(), (1, 2))  # 冲突重放未进入业务表或审计链

    def test_batch_write_is_atomic_on_violation(self) -> None:
        items = [
            {"test_frequency_hz": 450, "response": 0.71, "noise": 0.01, "instrument": "gw"},
            {"test_frequency_hz": 520, "response": "Infinity", "instrument": "gw"},
            {"test_frequency_hz": 650, "response": 0.84, "noise": 0.01, "instrument": "gw"},
        ]
        with self.assertRaises(MeasurementRejected) as caught:
            self.service.add_measurements(self.token, "LOT-1", items)
        violation = caught.exception.violations[0]
        self.assertEqual((violation.index, violation.field, violation.rule), (1, "response", "must_be_json_number"))
        self.assertEqual(self._counts(), (0, 1))  # 整批回滚

    def test_batch_write_is_atomic_on_conflict(self) -> None:
        self._add_valid()
        items = [
            {"test_frequency_hz": 450, "response": 0.99, "instrument": "reporting-gateway-1"},
            {"test_frequency_hz": 520, "response": 0.93, "instrument": "reporting-gateway-1"},
        ]
        with self.assertRaises(MeasurementConflict):
            self.service.add_measurements(self.token, "LOT-1", items)
        self.assertEqual(self._counts(), (1, 2))  # 已有行不变，新行未写入

    def test_batch_success_and_replay(self) -> None:
        items = [
            {"test_frequency_hz": 450, "response": 0.71, "instrument": "gw"},
            {"test_frequency_hz": 520, "response": 0.93, "noise": 0.02, "instrument": "gw"},
            {"test_frequency_hz": 650, "response": 0.84, "noise": 0.01, "instrument": "gw"},
        ]
        first = self.service.add_measurements(self.token, "LOT-1", items)
        self.assertEqual((first["inserted"], first["replayed"]), (3, 0))
        second = self.service.add_measurements(self.token, "LOT-1", items)
        self.assertEqual((second["inserted"], second["replayed"]), (0, 3))
        self.assertEqual(first["measurement_ids"], second["measurement_ids"])
        self.assertEqual(self._counts(), (3, 4))

    def test_batch_internal_conflict_rejected(self) -> None:
        items = [
            {"test_frequency_hz": 450, "response": 0.71, "instrument": "gw"},
            {"test_frequency_hz": 450.0, "response": 0.72, "instrument": "gw"},
        ]
        with self.assertRaises(MeasurementConflict):
            self.service.add_measurements(self.token, "LOT-1", items)
        self.assertEqual(self._counts(), (0, 1))

    def _insert_legacy_row(self, response: float, frequency: float = 999.0) -> None:
        self.service.db.execute(
            "INSERT INTO measurements VALUES(?,?,?,?,?,?,?,?)",
            ("legacy-1", "LOT-1", frequency, response, 0.01, "reporting-gateway-legacy", "admin", "2026-09-21T00:00:00+00:00"),
        )
        self.service.db.commit()

    def test_legacy_illegal_row_is_quarantined_not_silently_ignored(self) -> None:
        for frequency, response in ((450, 0.71), (520, 0.93), (650, 0.84)):
            self._add_valid(frequency, response)
        self._insert_legacy_row(float("inf"))
        result = self.service.analyze(self.token, "LOT-1")
        self.assertEqual(result["valid_count"], 3)  # 有效观测仍然形成结论
        self.assertEqual(len(result["quarantined"]), 1)
        record = result["quarantined"][0]
        self.assertEqual(record["measurement_id"], "legacy-1")
        self.assertEqual(record["handled_by"], "admin")  # 处置人已记录
        self.assertEqual(record["rule_version"], CONTRACT_VERSION)  # 规则版本已记录
        violations = json.loads(record["violations"])
        self.assertEqual(violations[0]["rule"], "must_be_finite")
        events = [row["event_type"] for row in self.service.audit(self.token, "LOT-1")]
        self.assertIn("measurement.quarantined", events)  # 隔离处置进入审计链
        again = self.service.analyze(self.token, "LOT-1")  # 重复读取不重复隔离
        self.assertEqual(len(again["quarantined"]), 1)
        count = self.service.db.execute("SELECT count(*) FROM measurement_quarantine").fetchone()[0]
        self.assertEqual(count, 1)

    def test_analyze_reports_shortage_after_quarantine(self) -> None:
        self._add_valid(450, 0.71)
        self._add_valid(520, 0.93)
        self._insert_legacy_row(float("inf"))
        with self.assertRaisesRegex(ValueError, "有效测量不足"):
            self.service.analyze(self.token, "LOT-1")
        count = self.service.db.execute("SELECT count(*) FROM measurement_quarantine").fetchone()[0]
        self.assertEqual(count, 1)  # 即使分析失败，隔离处置也已留痕

    def test_list_measurements_marks_quarantined_rows(self) -> None:
        self._add_valid()
        self._insert_legacy_row(float("inf"))
        listing = self.service.list_measurements(self.token, "LOT-1")
        self.assertEqual(listing["quarantined_count"], 1)
        by_status = {item["status"] for item in listing["measurements"]}
        self.assertEqual(by_status, {"valid", "quarantined"})
        bad = next(item for item in listing["measurements"] if item["status"] == "quarantined")
        self.assertEqual(bad["response"], "Infinity")  # 非有限值以文本形式呈现，不再产出非法 JSON
        self.assertEqual(bad["quarantine"]["rule_version"], CONTRACT_VERSION)
        self.assertEqual(bad["quarantine"]["violations"][0]["field"], "response")


class ApiBoundaryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = MetricQualityService()
        self.service.bootstrap_admin()
        self.token = self.service.auth.login("admin", "metric-admin")
        self.service.create_lot(self.token, "LOT-API", "cross-border-service-index", "POLICY-3.2", 10)
        handler_class = type("BoundHandler", (Handler,), {"service": self.service})
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), handler_class)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.port = self.server.server_address[1]

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.service.db.close()

    def _request(self, method: str, path: str, raw_body: str | None = None) -> tuple[int, dict]:
        connection = http.client.HTTPConnection("127.0.0.1", self.port)
        headers = {"Authorization": f"Bearer {self.token}"}
        connection.request(method, path, None if raw_body is None else raw_body.encode(), headers)
        response = connection.getresponse()
        payload = json.loads(response.read())
        connection.close()
        return response.status, payload

    def test_json_infinity_rejected_with_field_and_rule(self) -> None:
        status, payload = self._request(
            "POST",
            "/lots/LOT-API/measurements",
            '{"test_frequency_hz": 450, "response": Infinity, "noise": 0.01, "instrument": "reporting-gateway-1"}',
        )
        self.assertEqual(status, 422)
        error = payload["error"]
        self.assertEqual(error["code"], "measurement_rejected")
        self.assertEqual(error["rule_version"], CONTRACT_VERSION)
        violation = error["violations"][0]
        self.assertEqual((violation["field"], violation["rule"]), ("response", "must_be_finite"))
        self.assertIn("Infinity", violation["message"])
        status, listing = self._request("GET", "/lots/LOT-API/measurements")
        self.assertEqual(listing["measurements"], [])  # 未进入业务表
        events = self.service.audit(self.token, "LOT-API")
        self.assertEqual([row["event_type"] for row in events], ["created"])  # 未进入审计链

    def test_batch_endpoint_atomic_rejection(self) -> None:
        status, payload = self._request(
            "POST",
            "/lots/LOT-API/measurements/batch",
            json.dumps({"measurements": [
                {"test_frequency_hz": 450, "response": 0.71, "instrument": "gw"},
                {"test_frequency_hz": 520, "response": 1e999, "instrument": "gw"},
            ]}),
        )
        self.assertEqual(status, 422)
        violation = payload["error"]["violations"][0]
        self.assertEqual((violation["index"], violation["field"], violation["rule"]), (1, "response", "must_be_finite"))
        _, listing = self._request("GET", "/lots/LOT-API/measurements")
        self.assertEqual(listing["measurements"], [])

    def test_conflicting_replay_returns_409(self) -> None:
        body = json.dumps({"test_frequency_hz": 450, "response": 0.71, "instrument": "gw"})
        status, _ = self._request("POST", "/lots/LOT-API/measurements", body)
        self.assertEqual(status, 201)
        status, payload = self._request(
            "POST", "/lots/LOT-API/measurements",
            json.dumps({"test_frequency_hz": 450, "response": 0.72, "instrument": "gw"}),
        )
        self.assertEqual(status, 409)
        self.assertEqual(payload["error"]["code"], "measurement_conflict")
        self.assertEqual(payload["error"]["differing_fields"], ["response"])

    def test_valid_single_and_batch_flow(self) -> None:
        status, payload = self._request(
            "POST", "/lots/LOT-API/measurements",
            json.dumps({"test_frequency_hz": 450, "response": 0.71, "instrument": "gw"}),
        )
        self.assertEqual(status, 201)
        self.assertFalse(payload["replayed"])
        status, payload = self._request(
            "POST", "/lots/LOT-API/measurements/batch",
            json.dumps({"measurements": [
                {"test_frequency_hz": 520, "response": 0.93, "instrument": "gw"},
                {"test_frequency_hz": 650, "response": 0.84, "instrument": "gw"},
            ]}),
        )
        self.assertEqual(status, 201)
        self.assertEqual(payload["inserted"], 2)
        status, analysis = self._request("POST", "/lots/LOT-API/analysis")
        self.assertEqual(status, 200)
        self.assertEqual(analysis["valid_count"], 3)
        self.assertEqual(analysis["quarantined"], [])


class AcceptanceTests(unittest.TestCase):
    def test_offline_acceptance_still_passes(self) -> None:
        result = acceptance_run()
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["peak_period"], 520.0)


if __name__ == "__main__":
    unittest.main()
