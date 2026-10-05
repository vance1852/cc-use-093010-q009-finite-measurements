"""统计样本批次和测量记录的 SQLite 结构、迁移及事务辅助函数。

写入契约见 ``metric_quality.contracts``。测量表带有效性标记：只有
``validity='valid'`` 且带契约版本的记录才能被分析报告消费；契约建立之前
写入的存量记录默认 ``quarantined``，必须经甄别后才能放行或隔离。
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator


SCHEMA_VERSION = "2"

SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_meta(
 key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS metric_batches(
 lot_id TEXT PRIMARY KEY, product TEXT NOT NULL, process_rev TEXT NOT NULL,
 sample_count INTEGER NOT NULL, status TEXT NOT NULL, owner TEXT NOT NULL,
 created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS measurements(
 measurement_id TEXT PRIMARY KEY, lot_id TEXT NOT NULL REFERENCES metric_batches(lot_id),
 observation_key TEXT,
 test_frequency_hz REAL NOT NULL, response REAL NOT NULL, noise REAL NOT NULL,
 instrument TEXT NOT NULL, operator TEXT NOT NULL, measured_at TEXT NOT NULL,
 validity TEXT NOT NULL DEFAULT 'valid' CHECK (validity IN ('valid','quarantined')),
 contract_version TEXT);
CREATE TABLE IF NOT EXISTS measurement_quarantine(
 quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT,
 measurement_id TEXT NOT NULL UNIQUE,
 lot_id TEXT NOT NULL,
 test_frequency_hz REAL, response REAL, noise REAL, instrument TEXT,
 reason_rules TEXT NOT NULL, rule_version TEXT NOT NULL,
 detected_at TEXT NOT NULL, detected_by TEXT NOT NULL,
 disposition TEXT NOT NULL CHECK (disposition IN ('quarantined','released','purged')),
 disposed_by TEXT, disposed_at TEXT, disposition_note TEXT);
CREATE TABLE IF NOT EXISTS lot_events(
 event_id INTEGER PRIMARY KEY AUTOINCREMENT, lot_id TEXT NOT NULL,
 event_type TEXT NOT NULL, actor TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS approvals(
 lot_id TEXT NOT NULL, reviewer TEXT NOT NULL, decision TEXT NOT NULL,
 reason TEXT NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY(lot_id,reviewer));
"""

# 旧库 measurements 表需要补齐的列：列名 -> 列定义。
_MEASUREMENT_ADDED_COLUMNS = {
    "observation_key": "TEXT",
    # 存量行先标记为隔离待甄别，避免旧的非法数值在读取时被静默放行。
    "validity": "TEXT NOT NULL DEFAULT 'quarantined'",
    "contract_version": "TEXT",
}


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _migrate(db: sqlite3.Connection) -> None:
    db.executescript(SCHEMA)
    existing = {row[1] for row in db.execute("PRAGMA table_info(measurements)").fetchall()}
    for column, declaration in _MEASUREMENT_ADDED_COLUMNS.items():
        if column not in existing:
            db.execute(f"ALTER TABLE measurements ADD COLUMN {column} {declaration}")
    # 依赖 observation_key 的索引必须在补列之后创建；旧行该列为 NULL，不参与唯一约束。
    db.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ix_measurements_lot_observation "
        "ON measurements(lot_id, observation_key)"
    )
    db.execute(
        "INSERT INTO schema_meta(key,value) VALUES('schema_version',?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (SCHEMA_VERSION,),
    )


def connect(path: str = ":memory:") -> sqlite3.Connection:
    db = sqlite3.connect(path, isolation_level=None)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys=ON")
    _migrate(db)
    return db


@contextmanager
def transaction(db: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    try:
        db.execute("BEGIN IMMEDIATE")
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise


def event(db: sqlite3.Connection, lot_id: str, event_type: str, actor: str, payload: dict) -> None:
    db.execute(
        "INSERT INTO lot_events(lot_id,event_type,actor,payload,created_at) VALUES(?,?,?,?,?)",
        (lot_id, event_type, actor, json.dumps(payload, ensure_ascii=False, sort_keys=True), utcnow()),
    )
