"""协调认证、批次、测试和放行门禁的应用服务。"""

from __future__ import annotations

import json
import math
import threading
import uuid
from typing import Any, Mapping, Sequence

from .analytics import confidence_interval, summarize_response_profile, yield_rate
from .auth import Auth
from .contracts import (
    CONTRACT_VERSION,
    CleanMeasurement,
    MeasurementConflict,
    check_stored_measurement,
    validate_batch,
    validate_measurement,
)
from .storage import connect, event, transaction, utcnow


class MetricQualityService:
    def __init__(self, database: str = ":memory:"):
        self.db = connect(database)
        self.auth = Auth(self.db)
        # HTTP 入口在多线程中共享同一连接，所有公共操作经此锁串行化。
        self.lock = threading.RLock()

    def bootstrap_admin(self, user_id: str = "admin", password: str = "metric-admin") -> None:
        try:
            self.auth.create_user(user_id, password, "admin")
        except Exception:
            pass

    def create_lot(self, token: str, lot_id: str, product: str, process_rev: str, sample_count: int) -> dict:
        actor = self.auth.require(token, "submit")
        if sample_count <= 0 or not lot_id.strip() or not process_rev.strip():
            raise ValueError("lot fields are invalid")
        now = utcnow()
        with transaction(self.db):
            self.db.execute("INSERT INTO metric_batches VALUES(?,?,?,?,?,?,?,?)", (lot_id, product, process_rev, sample_count, "engineering", actor.user_id, now, now))
            event(self.db, lot_id, "created", actor.user_id, {"product": product, "process_rev": process_rev})
        return self.get_lot(token, lot_id)

    def get_lot(self, token: str, lot_id: str) -> dict:
        self.auth.require(token, "read")
        row = self.db.execute("SELECT * FROM metric_batches WHERE lot_id=?", (lot_id,)).fetchone()
        if not row:
            raise KeyError(lot_id)
        return dict(row)

    def _require_lot(self, lot_id: str) -> None:
        if not self.db.execute("SELECT 1 FROM metric_batches WHERE lot_id=?", (lot_id,)).fetchone():
            raise KeyError(lot_id)

    def _find_by_identity(self, lot_id: str, clean: CleanMeasurement):
        return self.db.execute(
            "SELECT * FROM measurements WHERE lot_id=? AND instrument=? AND test_frequency_hz=?",
            (lot_id, clean.instrument, clean.test_frequency_hz),
        ).fetchone()

    @staticmethod
    def _assert_same_content(existing, clean: CleanMeasurement) -> None:
        differing = [
            field
            for field, current in (("response", clean.response), ("noise", clean.noise))
            if existing[field] != current
        ]
        if differing:
            raise MeasurementConflict(
                {"instrument": clean.instrument, "test_frequency_hz": clean.test_frequency_hz},
                differing,
                existing["measurement_id"],
            )

    def _insert_measurement(self, lot_id: str, clean: CleanMeasurement, operator: str) -> str:
        measurement_id = uuid.uuid4().hex
        self.db.execute(
            "INSERT INTO measurements VALUES(?,?,?,?,?,?,?,?)",
            (measurement_id, lot_id, clean.test_frequency_hz, clean.response, clean.noise, clean.instrument, operator, utcnow()),
        )
        event(self.db, lot_id, "measurement", operator, {"measurement_id": measurement_id, "test_frequency_hz": clean.test_frequency_hz})
        return measurement_id

    def add_measurement(self, token: str, lot_id: str, test_frequency_hz: float, response: float, noise: float, instrument: str) -> dict:
        actor = self.auth.require(token, "measure")
        clean = validate_measurement(
            {"test_frequency_hz": test_frequency_hz, "response": response, "noise": noise, "instrument": instrument}
        )
        with transaction(self.db):
            self._require_lot(lot_id)
            existing = self._find_by_identity(lot_id, clean)
            if existing is not None:
                self._assert_same_content(existing, clean)
                return {"measurement_id": existing["measurement_id"], "lot_id": lot_id, "replayed": True, "rule_version": CONTRACT_VERSION}
            measurement_id = self._insert_measurement(lot_id, clean, actor.user_id)
        return {"measurement_id": measurement_id, "lot_id": lot_id, "replayed": False, "rule_version": CONTRACT_VERSION}

    def add_measurements(self, token: str, lot_id: str, items: Sequence[Mapping[str, Any]]) -> dict:
        """批量写入测量：全部通过契约校验才落库，任一失败则整批回滚。"""

        actor = self.auth.require(token, "measure")
        if not isinstance(items, Sequence) or isinstance(items, (str, bytes)) or not items:
            raise ValueError("measurements 必须是非空数组")
        cleaned = validate_batch(list(items))
        merged: dict[tuple[str, float], CleanMeasurement] = {}
        for clean in cleaned:
            previous = merged.get(clean.identity)
            if previous is not None:
                differing = [
                    field
                    for field, current in (("response", clean.response), ("noise", clean.noise))
                    if getattr(previous, field) != current
                ]
                if differing:
                    raise MeasurementConflict(
                        {"instrument": clean.instrument, "test_frequency_hz": clean.test_frequency_hz},
                        differing,
                        None,
                    )
                continue
            merged[clean.identity] = clean
        results: list[dict] = []
        with transaction(self.db):
            self._require_lot(lot_id)
            for clean in merged.values():
                existing = self._find_by_identity(lot_id, clean)
                if existing is not None:
                    self._assert_same_content(existing, clean)
                    results.append({"measurement_id": existing["measurement_id"], "replayed": True})
                    continue
                results.append({"measurement_id": self._insert_measurement(lot_id, clean, actor.user_id), "replayed": False})
        return {
            "lot_id": lot_id,
            "inserted": sum(1 for item in results if not item["replayed"]),
            "replayed": sum(1 for item in results if item["replayed"]),
            "measurement_ids": [item["measurement_id"] for item in results],
            "rule_version": CONTRACT_VERSION,
        }

    def _quarantine_invalid(self, lot_id: str, handled_by: str) -> list[dict]:
        """识别存量非法记录并隔离，记录处置人和规则版本，不悄悄忽略。"""

        rows = self.db.execute(
            "SELECT m.* FROM measurements m LEFT JOIN measurement_quarantine q ON q.measurement_id=m.measurement_id "
            "WHERE m.lot_id=? AND q.measurement_id IS NULL",
            (lot_id,),
        ).fetchall()
        offenders = [
            (row, check_stored_measurement(row["test_frequency_hz"], row["response"], row["noise"], row["instrument"]))
            for row in rows
        ]
        offenders = [(row, violations) for row, violations in offenders if violations]
        if offenders:
            with transaction(self.db):
                for row, violations in offenders:
                    cursor = self.db.execute(
                        "INSERT OR IGNORE INTO measurement_quarantine(measurement_id,lot_id,rule_version,violations,handled_by,handled_at) "
                        "VALUES(?,?,?,?,?,?)",
                        (
                            row["measurement_id"],
                            lot_id,
                            CONTRACT_VERSION,
                            json.dumps([item.as_dict() for item in violations], ensure_ascii=False, sort_keys=True),
                            handled_by,
                            utcnow(),
                        ),
                    )
                    if cursor.rowcount == 1:
                        event(
                            self.db,
                            lot_id,
                            "measurement.quarantined",
                            handled_by,
                            {
                                "measurement_id": row["measurement_id"],
                                "rule_version": CONTRACT_VERSION,
                                "violations": [item.as_dict() for item in violations],
                            },
                        )
        return [dict(row) for row in self.db.execute(
            "SELECT * FROM measurement_quarantine WHERE lot_id=? ORDER BY quarantine_id", (lot_id,)
        ).fetchall()]

    @staticmethod
    def _json_safe(value: Any) -> Any:
        if isinstance(value, float) and not math.isfinite(value):
            return "Infinity" if value > 0 else ("-Infinity" if value < 0 else "NaN")
        return value

    def list_measurements(self, token: str, lot_id: str) -> dict:
        """读取测量；存量非法记录在读取时被识别、隔离并标记，不悄悄忽略。"""

        actor = self.auth.require(token, "read")
        quarantined = {row["measurement_id"]: row for row in self._quarantine_invalid(lot_id, actor.user_id)}
        rows = self.db.execute(
            "SELECT * FROM measurements WHERE lot_id=? ORDER BY measured_at, measurement_id", (lot_id,)
        ).fetchall()
        items = []
        for row in rows:
            item = {
                "measurement_id": row["measurement_id"],
                "test_frequency_hz": self._json_safe(row["test_frequency_hz"]),
                "response": self._json_safe(row["response"]),
                "noise": self._json_safe(row["noise"]),
                "instrument": row["instrument"],
                "operator": row["operator"],
                "measured_at": row["measured_at"],
                "status": "quarantined" if row["measurement_id"] in quarantined else "valid",
            }
            record = quarantined.get(row["measurement_id"])
            if record is not None:
                item["quarantine"] = {
                    "rule_version": record["rule_version"],
                    "handled_by": record["handled_by"],
                    "handled_at": record["handled_at"],
                    "violations": json.loads(record["violations"]),
                }
            items.append(item)
        return {"lot_id": lot_id, "measurements": items, "quarantined_count": len(quarantined)}

    def analyze(self, token: str, lot_id: str) -> dict:
        actor = self.auth.require(token, "analyze")
        quarantined = self._quarantine_invalid(lot_id, actor.user_id)
        rows = self.db.execute(
            "SELECT m.test_frequency_hz,m.response FROM measurements m "
            "LEFT JOIN measurement_quarantine q ON q.measurement_id=m.measurement_id "
            "WHERE m.lot_id=? AND q.measurement_id IS NULL ORDER BY m.test_frequency_hz",
            (lot_id,),
        ).fetchall()
        if len(rows) < 3:
            raise ValueError(f"有效测量不足三条：{len(rows)} 条有效，{len(quarantined)} 条已隔离")
        summary = summarize_response_profile([r[0] for r in rows], [r[1] for r in rows])
        rates = yield_rate(self.get_lot(token, lot_id)["sample_count"], sum(1 for r in rows if r[1] >= 0.8), 0)
        ci = confidence_interval([r[1] for r in rows])
        return {
            "lot_id": lot_id,
            "response_profile": summary.__dict__,
            "yield": rates,
            "response_ci": ci,
            "valid_count": len(rows),
            "quarantined": quarantined,
        }

    def approve(self, token: str, lot_id: str, decision: str, reason: str) -> dict:
        actor = self.auth.require(token, "approve")
        if decision not in {"release", "hold", "reject"} or not reason.strip():
            raise ValueError("decision and reason are required")
        with transaction(self.db):
            self.db.execute("INSERT OR REPLACE INTO approvals VALUES(?,?,?,?,?)", (lot_id, actor.user_id, decision, reason, utcnow()))
            status = {"release": "released", "hold": "hold", "reject": "rejected"}[decision]
            self.db.execute("UPDATE metric_batches SET status=?,updated_at=? WHERE lot_id=?", (status, utcnow(), lot_id))
            event(self.db, lot_id, "approval", actor.user_id, {"decision": decision, "reason": reason})
        return self.get_lot(token, lot_id)

    def audit(self, token: str, lot_id: str) -> list[dict]:
        self.auth.require(token, "read")
        return [dict(r) for r in self.db.execute("SELECT * FROM lot_events WHERE lot_id=? ORDER BY event_id", (lot_id,)).fetchall()]
