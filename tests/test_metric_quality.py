from __future__ import annotations

import json
import math
import sqlite3
import tempfile
import unittest
from pathlib import Path

from metric_quality.api import JsonApplication
from metric_quality.contracts import CONTRACT_VERSION, StrictJsonError, loads_strict_json
from metric_quality.errors import Conflict, Forbidden, InvalidState, ValidationFailed
from metric_quality.service import MetricQualityService


def measurement(observation_key: str = "obs-1", **overrides: object) -> dict:
    row = {
        "observation_key": observation_key,
        "test_frequency_hz": 450.0,
        "response": 0.9,
        "noise": 0.01,
        "instrument": "reporting-gateway-1",
    }
    row.update(overrides)
    return row


class MetricQualityServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = MetricQualityService()
        self.service.bootstrap_admin()
        self.service.auth.create_user("op", "operator-pw", "operator")
        self.service.auth.create_user("qa", "quality-pw", "quality")
        self.admin = self.service.auth.login("admin", "metric-admin")
        self.operator = self.service.auth.login("op", "operator-pw")
        self.quality = self.service.auth.login("qa", "quality-pw")
        self.service.create_lot(self.admin, "LOT-1", "product", "REV-1", 10)

    def _count(self) -> int:
        return self.service.db.execute("SELECT count(*) FROM measurements").fetchone()[0]

    def _measurement_events(self) -> int:
        return self.service.db.execute(
            "SELECT count(*) FROM lot_events WHERE event_type='measurement'"
        ).fetchone()[0]

    # -- 单条写入边界 -------------------------------------------------------

    def test_single_infinity_is_rejected_before_storage(self) -> None:
        for value in (float("inf"), float("-inf"), float("nan")):
            with self.subTest(value=value):
                with self.assertRaises(ValidationFailed) as caught:
                    self.service.add_measurement(
                        self.operator, "LOT-1", "obs-x", 650, value, 0.01, "gw"
                    )
                reject = caught.exception.rejections[0]
                self.assertEqual(reject["field"], "response")
                self.assertEqual(reject["rule"], "finite_required")
        self.assertEqual(self._count(), 0)
        self.assertEqual(self._measurement_events(), 0)

    def test_string_disguise_and_wrong_types_are_rejected(self) -> None:
        cases = [
            (("obs", 450, "0.93", 0.01, "gw"), "response", "string_not_accepted_as_number"),
            (("obs", "450", 0.9, 0.01, "gw"), "test_frequency_hz", "string_not_accepted_as_number"),
            (("obs", 450, 0.9, "0.01", "gw"), "noise", "string_not_accepted_as_number"),
            (("obs", True, 0.9, 0.01, "gw"), "test_frequency_hz", "number_required"),
            (("obs", 450, 0.9, 0.01, 5), "instrument", "string_required"),
            (("", 450, 0.9, 0.01, "gw"), "observation_key", "string_required"),
        ]
        for args, field, rule in cases:
            with self.subTest(field=field):
                with self.assertRaises(ValidationFailed) as caught:
                    self.service.add_measurement(self.operator, "LOT-1", *args)
                self.assertEqual(caught.exception.rejections[0]["field"], field)
                self.assertEqual(caught.exception.rejections[0]["rule"], rule)
        self.assertEqual(self._count(), 0)

    def test_applicable_ranges_are_enforced(self) -> None:
        cases = [
            (0.0, 0.9, 0.01, "test_frequency_hz"),
            (-1.0, 0.9, 0.01, "test_frequency_hz"),
            (450.0, 1.01, 0.01, "response"),
            (450.0, -0.01, 0.01, "response"),
            (450.0, 0.9, 1.5, "noise"),
        ]
        for freq, response, noise, field in cases:
            with self.subTest(field=field):
                with self.assertRaises(ValidationFailed) as caught:
                    self.service.add_measurement(self.operator, "LOT-1", "obs", freq, response, noise, "gw")
                self.assertEqual(caught.exception.rejections[0]["rule"], "out_of_range")
                self.assertEqual(caught.exception.rejections[0]["field"], field)

    def test_valid_single_write_is_attested(self) -> None:
        result = self.service.add_measurement(self.operator, "LOT-1", "obs-1", 450, 0.9, 0.01, "gw")
        self.assertEqual(result["contract_version"], CONTRACT_VERSION)
        row = self.service.db.execute(
            "SELECT validity,contract_version FROM measurements WHERE measurement_id=?",
            (result["measurement_id"],),
        ).fetchone()
        self.assertEqual(row["validity"], "valid")
        self.assertEqual(row["contract_version"], CONTRACT_VERSION)

    # -- 批量原子性与冲突重放 ----------------------------------------------

    def test_batch_is_atomic_when_any_record_invalid(self) -> None:
        batch = [
            measurement("b1", response=0.71),
            measurement("b2", response=0.93),
            measurement("b3", response=float("inf")),
        ]
        with self.assertRaises(ValidationFailed) as caught:
            self.service.add_measurements(self.operator, "LOT-1", batch)
        self.assertEqual(caught.exception.rejections[0]["field"], "measurements[2].response")
        self.assertEqual(self._count(), 0)
        self.assertEqual(self._measurement_events(), 0)

    def test_batch_empty_or_non_array_rejected(self) -> None:
        with self.assertRaises(ValidationFailed):
            self.service.add_measurements(self.operator, "LOT-1", [])
        with self.assertRaises(ValidationFailed):
            self.service.add_measurements(self.operator, "LOT-1", "not-an-array")  # type: ignore[arg-type]

    def test_batch_collects_multiple_field_rejections(self) -> None:
        batch = [
            measurement("b1", response="0.5"),
            measurement("b2", response=2.0),
        ]
        with self.assertRaises(ValidationFailed) as caught:
            self.service.add_measurements(self.operator, "LOT-1", batch)
        fields = {item["field"] for item in caught.exception.rejections}
        self.assertEqual(fields, {"measurements[0].response", "measurements[1].response"})

    def test_conflicting_replay_within_and_across_requests_rolls_back(self) -> None:
        good = [measurement("b1", response=0.71), measurement("b2", response=0.93)]
        self.service.add_measurements(self.operator, "LOT-1", good)
        # 同一批次内同一观测身份给出冲突内容
        conflicting = good + [measurement("b2", response=0.99)]
        with self.assertRaises(ValidationFailed) as caught:
            self.service.add_measurements(self.operator, "LOT-1", conflicting)
        self.assertEqual(caught.exception.rejections[0]["rule"], "observation_conflict")
        # 跨请求冲突
        with self.assertRaises(Conflict):
            self.service.add_measurement(self.operator, "LOT-1", "b2", 450, 0.99, 0.01, "gw")
        # 冲突没有污染表，原响应仍是 0.93
        value = self.service.db.execute(
            "SELECT response FROM measurements WHERE observation_key='b2'"
        ).fetchone()[0]
        self.assertEqual(value, 0.93)

    def test_identical_replay_is_idempotent(self) -> None:
        rows = [measurement("b1", response=0.71), measurement("b2", response=0.93)]
        first = self.service.add_measurements(self.operator, "LOT-1", rows)
        second = self.service.add_measurements(self.operator, "LOT-1", rows)
        self.assertEqual((first["inserted"], first["replayed"]), (2, 0))
        self.assertEqual((second["inserted"], second["replayed"]), (0, 2))
        self.assertEqual(self._count(), 2)

    # -- 存量识别、隔离与处置 ----------------------------------------------

    def _seed_legacy_rows(self) -> None:
        # 契约建立前的历史数据：干净但缺版本、inf、字符串伪装。
        self.service.db.execute(
            "INSERT INTO measurements VALUES('legacy-ok','LOT-1',NULL,700,0.8,0.01,'gw','op','t','valid',NULL)"
        )
        self.service.db.execute(
            "INSERT INTO measurements(measurement_id,lot_id,observation_key,test_frequency_hz,response,noise,"
            "instrument,operator,measured_at,validity,contract_version) "
            "VALUES('legacy-inf','LOT-1',NULL,710,?,0.01,'gw','op','t','valid',NULL)",
            (float("inf"),),
        )
        self.service.db.execute(
            "INSERT INTO measurements VALUES('legacy-str','LOT-1',NULL,720,0.85,'NaN','gw','op','t','valid',NULL)"
        )

    def test_scan_identifies_quarantines_and_records_handler_and_version(self) -> None:
        self._seed_legacy_rows()
        report = self.service.quarantine_invalid_measurements(self.quality, "LOT-1")
        self.assertEqual(report["quarantined"], 3)
        self.assertEqual(report["unattested"], 1)
        rules = {
            item["measurement_id"]: [r["rule"] for r in item["rejections"]]
            for item in report["items"]
        }
        self.assertEqual(rules["legacy-inf"], ["finite_required"])
        self.assertEqual(rules["legacy-str"], ["stored_type_invalid"])
        self.assertEqual(rules["legacy-ok"], ["contract_unattested"])
        ledger = {row["measurement_id"]: row for row in self.service.list_quarantine(self.quality, "LOT-1")}
        for row in ledger.values():
            self.assertEqual(row["detected_by"], "qa")
            self.assertEqual(row["disposed_by"], "qa")
            self.assertEqual(row["rule_version"], CONTRACT_VERSION)
            self.assertEqual(row["disposition"], "quarantined")

    def test_polluted_record_cannot_be_released_but_can_be_purged(self) -> None:
        self._seed_legacy_rows()
        self.service.quarantine_invalid_measurements(self.quality, "LOT-1")
        ledger = {row["measurement_id"]: row for row in self.service.list_quarantine(self.quality, "LOT-1")}
        with self.assertRaises(InvalidState):
            self.service.review_quarantine(self.quality, ledger["legacy-inf"]["quarantine_id"], "release", "尝试放行")
        purged = self.service.review_quarantine(self.quality, ledger["legacy-inf"]["quarantine_id"], "purge", "量程溢出剔除")
        self.assertEqual(purged["disposition"], "purged")
        released = self.service.review_quarantine(
            self.quality, ledger["legacy-ok"]["quarantine_id"], "release", "人工复核数值合规"
        )
        self.assertEqual(released["disposition"], "released")

    def test_operator_cannot_dispose_quarantine(self) -> None:
        self._seed_legacy_rows()
        self.service.quarantine_invalid_measurements(self.quality, "LOT-1")
        with self.assertRaises(Forbidden):
            self.service.quarantine_invalid_measurements(self.operator, "LOT-1")

    # -- 分析只消费可追溯有效观测 ------------------------------------------

    def test_analysis_consumes_only_valid_attested_observations(self) -> None:
        for key, response in (("b1", 0.71), ("b2", 0.93), ("b3", 0.84)):
            self.service.add_measurement(self.operator, "LOT-1", key, 450, response, 0.01, "gw")
        self._seed_legacy_rows()
        self.service.quarantine_invalid_measurements(self.quality, "LOT-1")
        result = self.service.analyze(self.admin, "LOT-1")
        # 只有 3 条新契约记录可消费；3 条旧记录被隔离，不静默参与。
        self.assertEqual(result["valid_observations"], 3)
        self.assertEqual(result["quarantined_observations"], 3)

    def test_analysis_fails_loudly_when_valid_set_insufficient(self) -> None:
        self.service.add_measurement(self.operator, "LOT-1", "b1", 450, 0.71, 0.01, "gw")
        with self.assertRaises(InvalidState):
            self.service.analyze(self.admin, "LOT-1")


class StrictJsonTests(unittest.TestCase):
    def test_constants_and_duplicate_keys_are_rejected(self) -> None:
        for body, rule in (
            (b'{"response": Infinity}', "non_finite_json"),
            (b'{"response": -Infinity}', "non_finite_json"),
            (b'{"response": NaN}', "non_finite_json"),
            (b'{"a": 1, "a": 2}', "duplicate_key"),
        ):
            with self.subTest(body=body):
                with self.assertRaises(StrictJsonError) as caught:
                    loads_strict_json(body)
                self.assertEqual(caught.exception.rule, rule)

    def test_finite_json_is_parsed(self) -> None:
        self.assertEqual(loads_strict_json(b'{"response": 0.9}'), {"response": 0.9})


class ApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = MetricQualityService()
        self.service.bootstrap_admin()
        self.token = self.service.auth.login("admin", "metric-admin")
        self.service.create_lot(self.token, "LOT-1", "product", "REV-1", 10)
        self.app = JsonApplication(self.service)
        self.headers = {"Authorization": f"Bearer {self.token}"}

    def test_raw_infinity_constant_rejected_at_boundary(self) -> None:
        response = self.app.handle(
            "POST", "/lots/LOT-1/measurements", self.headers,
            b'{"observation_key":"h1","test_frequency_hz":450,"response":Infinity,"noise":0.01,"instrument":"gw"}',
        )
        self.assertEqual(response.status, 422)
        self.assertEqual(response.body["error"]["rejections"][0]["rule"], "non_finite_json")

    def test_field_rule_rejection_is_returned_to_submitter(self) -> None:
        response = self.app.handle(
            "POST", "/lots/LOT-1/measurements/batch", self.headers,
            json.dumps({"measurements": [measurement("z1", response="0.5")]}).encode(),
        )
        self.assertEqual(response.status, 422)
        reject = response.body["error"]["rejections"][0]
        self.assertEqual(reject["field"], "measurements[0].response")
        self.assertEqual(reject["rule"], "string_not_accepted_as_number")

    def test_valid_batch_round_trip(self) -> None:
        body = json.dumps({"measurements": [measurement("z1")]}).encode()
        response = self.app.handle("POST", "/lots/LOT-1/measurements/batch", self.headers, body)
        self.assertEqual(response.status, 201)
        self.assertEqual(response.body["inserted"], 1)


class LegacyDatabaseMigrationTests(unittest.TestCase):
    def test_pre_contract_database_rows_are_quarantined_not_silently_used(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "legacy.sqlite3"
            # 用旧版表结构（无 observation_key/validity/contract_version）建库并写入污染数据。
            db = sqlite3.connect(path)
            db.execute(
                "CREATE TABLE metric_batches(lot_id TEXT PRIMARY KEY,product TEXT,process_rev TEXT,"
                "sample_count INTEGER,status TEXT,owner TEXT,created_at TEXT,updated_at TEXT)"
            )
            db.execute(
                "CREATE TABLE measurements(measurement_id TEXT PRIMARY KEY,lot_id TEXT,"
                "test_frequency_hz REAL,response REAL,noise REAL,instrument TEXT,operator TEXT,measured_at TEXT)"
            )
            db.execute(
                "CREATE TABLE lot_events(event_id INTEGER PRIMARY KEY AUTOINCREMENT,lot_id TEXT,event_type TEXT,"
                "actor TEXT,payload TEXT,created_at TEXT)"
            )
            db.execute(
                "CREATE TABLE approvals(lot_id TEXT,reviewer TEXT,decision TEXT,reason TEXT,created_at TEXT)"
            )
            db.execute("INSERT INTO metric_batches VALUES('LOT-1','p','r',10,'engineering','admin','t','t')")
            db.execute("INSERT INTO measurements VALUES('m1','LOT-1',450,0.9,0.01,'gw','op','t')")
            db.commit()
            db.close()

            service = MetricQualityService(str(path))
            service.bootstrap_admin()
            token = service.auth.login("admin", "metric-admin")
            # 旧行默认隔离待甄别，分析不能静默使用。
            with self.assertRaises(InvalidState):
                service.analyze(token, "LOT-1")
            report = service.quarantine_invalid_measurements(token, "LOT-1")
            self.assertEqual(report["quarantined"], 1)
            self.assertEqual(report["unattested"], 1)


if __name__ == "__main__":
    unittest.main()
