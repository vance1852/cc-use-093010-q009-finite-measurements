"""协调认证、批次、测试和放行门禁的应用服务。

所有测量记录在进入 ``measurements`` 业务表和 ``lot_events`` 审计链之前，
必须先通过 :mod:`metric_quality.contracts` 的数值契约。单条与批量写入都在
单个事务内完成：任一条记录非法或发生同一观测的冲突重放，整批回滚。
"""

from __future__ import annotations

import json
import math
import uuid
from typing import Mapping, Sequence

from .analytics import confidence_interval, summarize_response_profile, yield_rate
from .auth import Auth
from .contracts import (
    CONTRACT_VERSION,
    FIELD_LABELS,
    MeasurementRecord,
    evaluate_batch,
    evaluate_measurement,
    rejection,
)
from .errors import Conflict, InvalidState, NotFound, ValidationFailed
from .storage import connect, event, transaction, utcnow


class MetricQualityService:
    def __init__(self, database: str = ":memory:"):
        self.db = connect(database)
        self.auth = Auth(self.db)

    def bootstrap_admin(self, user_id: str = "admin", password: str = "metric-admin") -> None:
        try:
            self.auth.create_user(user_id, password, "admin")
        except Exception:
            pass

    def create_lot(self, token: str, lot_id: str, product: str, process_rev: str, sample_count: int) -> dict:
        actor = self.auth.require(token, "submit")
        if (
            not isinstance(lot_id, str) or not lot_id.strip()
            or not isinstance(product, str) or not product.strip()
            or not isinstance(process_rev, str) or not process_rev.strip()
            or isinstance(sample_count, bool) or not isinstance(sample_count, int) or sample_count <= 0
        ):
            raise ValidationFailed("lot fields are invalid")
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

    def _insert_record(
        self, db, lot_id: str, item: MeasurementRecord, actor_id: str
    ) -> str:
        """在已开启的事务内插入一条已校验记录，处理同一观测的重放。

        返回测量编号；内容一致的重放幂等返回既有编号，内容冲突则拒绝。
        """

        existing = db.execute(
            "SELECT measurement_id,test_frequency_hz,response,noise,instrument "
            "FROM measurements WHERE lot_id=? AND observation_key=?",
            (lot_id, item.observation_key),
        ).fetchone()
        if existing is not None:
            current = (
                existing["test_frequency_hz"],
                existing["response"],
                existing["noise"],
                existing["instrument"],
            )
            if current != item.value_identity():
                raise Conflict(
                    f"观测身份 {item.observation_key} 重放内容与既有记录冲突"
                )
            return existing["measurement_id"]
        measurement_id = uuid.uuid4().hex
        db.execute(
            "INSERT INTO measurements(measurement_id,lot_id,observation_key,test_frequency_hz,"
            "response,noise,instrument,operator,measured_at,validity,contract_version) "
            "VALUES(?,?,?,?,?,?,?,?,?,'valid',?)",
            (
                measurement_id, lot_id, item.observation_key,
                item.test_frequency_hz, item.response, item.noise, item.instrument,
                actor_id, utcnow(), CONTRACT_VERSION,
            ),
        )
        event(db, lot_id, "measurement", actor_id, {
            "measurement_id": measurement_id,
            "observation_key": item.observation_key,
            "test_frequency_hz": item.test_frequency_hz,
            "contract_version": CONTRACT_VERSION,
        })
        return measurement_id

    def add_measurement(
        self,
        token: str,
        lot_id: str,
        observation_key: str,
        test_frequency_hz: float,
        response: float,
        noise: float,
        instrument: str,
    ) -> dict:
        actor = self.auth.require(token, "measure")
        raw = {
            "observation_key": observation_key,
            "test_frequency_hz": test_frequency_hz,
            "response": response,
            "noise": noise,
            "instrument": instrument,
        }
        record, rejections = evaluate_measurement(raw)
        if rejections:
            raise ValidationFailed("测量记录未通过写入契约", rejections)
        self._require_lot(lot_id)
        assert record is not None
        with transaction(self.db):
            measurement_id = self._insert_record(self.db, lot_id, record, actor.user_id)
        return {
            "measurement_id": measurement_id,
            "lot_id": lot_id,
            "observation_key": record.observation_key,
            "contract_version": CONTRACT_VERSION,
        }

    def add_measurements(
        self, token: str, lot_id: str, raw_items: Sequence[Mapping]
    ) -> dict:
        """批量写入：先整体校验，再单事务插入；任一非法则全部不写入。"""

        actor = self.auth.require(token, "measure")
        if not isinstance(raw_items, Sequence) or isinstance(raw_items, (str, bytes)):
            raise ValidationFailed("测量记录批量写入必须提供数组", [
                rejection("measurements", "array_required", "measurements 必须是数组")
            ])
        if len(raw_items) == 0:
            raise ValidationFailed("测量记录批量写入不能为空", [
                rejection("measurements", "required", "measurements 至少包含一条记录")
            ])
        records, rejections = evaluate_batch(raw_items)
        self._require_lot(lot_id)
        if rejections:
            raise ValidationFailed(
                f"{len(rejections)} 条字段规则未通过，整批拒绝写入", rejections
            )
        inserted = 0
        replayed = 0
        measurement_ids = []
        with transaction(self.db):
            for item in records:
                before = self.db.execute(
                    "SELECT 1 FROM measurements WHERE lot_id=? AND observation_key=?",
                    (lot_id, item.observation_key),
                ).fetchone()
                measurement_id = self._insert_record(self.db, lot_id, item, actor.user_id)
                measurement_ids.append(measurement_id)
                if before is not None:
                    replayed += 1
                else:
                    inserted += 1
        return {
            "lot_id": lot_id,
            "received": len(raw_items),
            "inserted": inserted,
            "replayed": replayed,
            "measurement_ids": measurement_ids,
            "contract_version": CONTRACT_VERSION,
        }

    # -- 存量非法记录的识别、隔离与处置 -------------------------------------

    def _stored_row_rejections(self, row) -> list[dict]:
        """对存量行重新套用写入契约，返回命中的规则（含伪装与溢出）。"""

        problems: list[dict] = []
        type_map = {
            "test_frequency_hz": row["freq_type"],
            "response": row["response_type"],
            "noise": row["noise_type"],
        }
        ranges = {
            "test_frequency_hz": (0.0, 1e12, False),
            "response": (0.0, 1.0, True),
            "noise": (0.0, 1.0, True),
        }
        for field, stored_type in type_map.items():
            value = row[field]
            label = FIELD_LABELS[field]
            if stored_type not in ("real", "integer"):
                # 字符串伪装成数值混进了 REAL 列。
                problems.append(rejection(field, "stored_type_invalid", f"{label}在库中不是数值类型({stored_type})", value))
                continue
            number = float(value)
            if not math.isfinite(number):
                problems.append(rejection(field, "finite_required", f"{label}在库中为非有限数值", repr(number)))
                continue
            minimum, maximum, inclusive = ranges[field]
            below = number <= minimum if not inclusive else number < minimum
            if below or number > maximum:
                problems.append(rejection(field, "out_of_range", f"{label}超出适用量程", number))
        instrument = row["instrument"]
        if not isinstance(instrument, str) or not instrument.strip():
            problems.append(rejection("instrument", "string_required", "来源身份缺失或不是字符串", instrument))
        return problems

    def quarantine_invalid_measurements(self, token: str, lot_id: str | None = None) -> dict:
        """识别并隔离存量问题记录，整次扫描在一个事务内完成。

        两类记录进入隔离队列，都不会被分析报告静默忽略：
        - 命中写入契约规则的记录（非有限数值、字符串伪装、超量程等）；
        - 契约建立前写入、缺少契约版本的旧记录，需人工重新确认后放行。
        每条隔离记录都登记命中规则、规则版本和处置人。
        """

        actor = self.auth.require(token, "quarantine")
        if lot_id is not None:
            self._require_lot(lot_id)
        query = (
            "SELECT m.*, typeof(test_frequency_hz) AS freq_type,"
            "typeof(response) AS response_type,typeof(noise) AS noise_type "
            "FROM measurements m WHERE NOT EXISTS("
            "SELECT 1 FROM measurement_quarantine q WHERE q.measurement_id=m.measurement_id)"
        )
        params: tuple = ()
        if lot_id is not None:
            query += " AND m.lot_id=?"
            params = (lot_id,)
        rows = self.db.execute(query, params).fetchall()
        quarantined: list[dict] = []
        unattested = 0
        with transaction(self.db):
            for row in rows:
                problems = self._stored_row_rejections(row)
                if not problems and row["contract_version"] is not None:
                    # 契约建立后的干净记录，无需处置。
                    continue
                if not problems:
                    # 数值本身合规，但缺少契约版本，不可追溯，需人工确认。
                    problems = [rejection(
                        "contract_version", "contract_unattested",
                        "该记录在写入契约建立前入库，缺少规则版本，需人工重新确认",
                        row["contract_version"],
                    )]
                    unattested += 1
                self.db.execute(
                    "UPDATE measurements SET validity='quarantined' WHERE measurement_id=?",
                    (row["measurement_id"],),
                )
                self.db.execute(
                    "INSERT INTO measurement_quarantine(measurement_id,lot_id,test_frequency_hz,response,noise,"
                    "instrument,reason_rules,rule_version,detected_at,detected_by,disposition,disposed_by,"
                    "disposed_at,disposition_note) VALUES(?,?,?,?,?,?,?,?,?,?,'quarantined',?,?,?)",
                    (
                        row["measurement_id"], row["lot_id"], row["test_frequency_hz"], row["response"],
                        row["noise"], row["instrument"], json.dumps(problems, ensure_ascii=False),
                        CONTRACT_VERSION, utcnow(), actor.user_id, actor.user_id, utcnow(),
                        "存量扫描识别后隔离，待质量负责人处置",
                    ),
                )
                event(self.db, row["lot_id"], "measurement.quarantined", actor.user_id, {
                    "measurement_id": row["measurement_id"],
                    "rules": [p["rule"] for p in problems],
                    "rule_version": CONTRACT_VERSION,
                })
                quarantined.append({"measurement_id": row["measurement_id"], "rejections": problems})
        return {
            "scanned": len(rows),
            "quarantined": len(quarantined),
            "unattested": unattested,
            "rule_version": CONTRACT_VERSION,
            "items": quarantined,
        }

    def list_quarantine(self, token: str, lot_id: str | None = None) -> list[dict]:
        self.auth.require(token, "read")
        query = "SELECT * FROM measurement_quarantine"
        params: tuple = ()
        if lot_id is not None:
            query += " WHERE lot_id=?"
            params = (lot_id,)
        query += " ORDER BY quarantine_id"
        rows = [dict(row) for row in self.db.execute(query, params).fetchall()]
        # 隔离台账保留了可能为 Infinity/NaN 的原始值；对外返回时用安全字符串表示，
        # 保证 JSON 响应本身始终合法。
        for row in rows:
            for key in ("test_frequency_hz", "response", "noise"):
                value = row.get(key)
                if isinstance(value, float) and not math.isfinite(value):
                    row[key] = repr(value)
        return rows

    def review_quarantine(self, token: str, quarantine_id: int, decision: str, note: str) -> dict:
        """质量负责人对隔离记录做最终处置：release 放行或 purge 剔除。

        只有数值本身合规、仅因缺少契约版本被隔离的旧记录，才能在人工重新
        确认后放行；命中数值/来源规则的污染记录不允许放行，只能剔除。
        """

        actor = self.auth.require(token, "quarantine")
        if decision not in {"release", "purge"} or not isinstance(note, str) or not note.strip():
            raise ValidationFailed("decision 必须是 release 或 purge，且需要处置说明")
        row = self.db.execute(
            "SELECT * FROM measurement_quarantine WHERE quarantine_id=?", (quarantine_id,)
        ).fetchone()
        if row is None:
            raise NotFound("隔离记录不存在")
        reasons = json.loads(row["reason_rules"])
        hard_violations = [r for r in reasons if r.get("rule") != "contract_unattested"]
        if decision == "release" and hard_violations:
            raise InvalidState(
                "命中数值或来源契约规则的记录不能放行，只能 purge 剔除"
            )
        disposition = "released" if decision == "release" else "purged"
        with transaction(self.db):
            if decision == "release":
                self.db.execute(
                    "UPDATE measurements SET validity='valid',contract_version=? WHERE measurement_id=?",
                    (CONTRACT_VERSION, row["measurement_id"]),
                )
            self.db.execute(
                "UPDATE measurement_quarantine SET disposition=?,disposed_by=?,disposed_at=?,"
                "disposition_note=? WHERE quarantine_id=?",
                (disposition, actor.user_id, utcnow(), note.strip(), quarantine_id),
            )
            event(self.db, row["lot_id"], "measurement.disposition", actor.user_id, {
                "measurement_id": row["measurement_id"],
                "quarantine_id": quarantine_id,
                "disposition": disposition,
                "rule_version": CONTRACT_VERSION,
            })
        return {"quarantine_id": quarantine_id, "disposition": disposition}

    # -- 分析与报告：只消费可追溯的有效观测 -------------------------------

    def analyze(self, token: str, lot_id: str) -> dict:
        self.auth.require(token, "analyze")
        rows = self.db.execute(
            "SELECT test_frequency_hz,response FROM measurements "
            "WHERE lot_id=? AND validity='valid' AND contract_version IS NOT NULL "
            "ORDER BY test_frequency_hz",
            (lot_id,),
        ).fetchall()
        quarantined = self.db.execute(
            "SELECT count(*) FROM measurements WHERE lot_id=? AND validity='quarantined'",
            (lot_id,),
        ).fetchone()[0]
        if len(rows) < 3:
            raise InvalidState("可追溯的有效观测不足三条，无法形成分析结论")
        summary = summarize_response_profile([r[0] for r in rows], [r[1] for r in rows])
        rates = yield_rate(self.get_lot(token, lot_id)["sample_count"], sum(1 for r in rows if r[1] >= 0.8), 0)
        ci = confidence_interval([r[1] for r in rows])
        return {
            "lot_id": lot_id,
            "response_profile": summary.__dict__,
            "yield": rates,
            "response_ci": ci,
            "valid_observations": len(rows),
            "quarantined_observations": quarantined,
            "contract_version": CONTRACT_VERSION,
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
