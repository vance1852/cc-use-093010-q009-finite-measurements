"""测量记录在写入边界的严格数值契约、规则版本与严格 JSON 解析。

报送机构把量程溢出编码成 Infinity/-Infinity/NaN，或用字符串伪装数值时，
必须在进入业务表和审计链之前被拒绝。所有规则集中在本模块，并带稳定的
规则代码与规则版本，供拒绝回执和存量隔离记录共同引用。
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

# 隔离记录与审计事件引用的契约规则版本。规则语义变化时必须提升版本。
CONTRACT_VERSION = "metric-measurement-contract-1.0.0"

# 观测期（测试频率）适用量程：必须为正的有限赫兹值。
FREQUENCY_MIN_HZ_EXCLUSIVE = 0.0
FREQUENCY_MAX_HZ = 1e12
# 指标值（归一化响应）适用量程。
RESPONSE_MIN = 0.0
RESPONSE_MAX = 1.0
# 偏差（噪声 RMS）适用量程。
NOISE_MIN = 0.0
NOISE_MAX = 1.0

IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}$")

FIELD_LABELS = {
    "observation_key": "观测身份",
    "test_frequency_hz": "观测期",
    "response": "指标值",
    "noise": "偏差",
    "instrument": "来源身份",
}


@dataclass(frozen=True, slots=True)
class MeasurementRecord:
    """已通过契约、可写入业务表的一条测量记录。"""

    observation_key: str
    test_frequency_hz: float
    response: float
    noise: float
    instrument: str

    def value_identity(self) -> tuple[float, float, float, str]:
        """同一观测身份重放时用于判断内容是否冲突的规范化内容。"""

        return (
            self.test_frequency_hz,
            self.response,
            self.noise,
            self.instrument,
        )


@dataclass(frozen=True, slots=True)
class StrictJsonError(ValueError):
    """HTTP 请求体不是严格 JSON（非有限常量、重复键、语法错误）。"""

    rule: str
    message: str

    def __str__(self) -> str:
        return self.message


def rejection(field: str, rule: str, message: str, observed: object = None) -> dict[str, Any]:
    """构造一条字段级拒绝原因，observed 永远使用可安全序列化的表示。"""

    result: dict[str, Any] = {"field": field, "rule": rule, "message": message}
    if observed is not None:
        result["observed"] = _safe_observed(observed)
    return result


def _safe_observed(value: object) -> object:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else repr(value)
    if isinstance(value, str):
        return value if len(value) <= 128 else value[:128] + "…"
    return repr(value)


def _check_number(
    value: object,
    field: str,
    *,
    minimum: float,
    maximum: float,
    minimum_inclusive: bool,
) -> dict[str, Any] | None:
    label = FIELD_LABELS[field]
    if isinstance(value, str):
        # 字符串伪装数值（"0.93"、"Infinity"、"NaN"）一律拒绝，不做隐式转换。
        return rejection(field, "string_not_accepted_as_number", f"{label}必须是 JSON 数值，不能是字符串", value)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return rejection(field, "number_required", f"{label}必须是 JSON 数值", value)
    number = float(value)
    if not math.isfinite(number):
        # Infinity、-Infinity、NaN：含报送机构的量程溢出编码。
        return rejection(field, "finite_required", f"{label}必须是有限数值，不允许 Infinity 或 NaN", value)
    below = number <= minimum if not minimum_inclusive else number < minimum
    if below or number > maximum:
        bracket = f"({minimum}, {maximum}]" if not minimum_inclusive else f"[{minimum}, {maximum}]"
        return rejection(
            field,
            "out_of_range",
            f"{label}超出适用量程，必须落在 {bracket} 之内",
            value,
        )
    return None


def _check_identifier(value: object, field: str) -> dict[str, Any] | None:
    label = FIELD_LABELS[field]
    if not isinstance(value, str):
        return rejection(field, "string_required", f"{label}必须是非空字符串", value)
    result = value.strip()
    if not result:
        return rejection(field, "string_required", f"{label}必须是非空字符串", value)
    if field == "observation_key" and not IDENTIFIER.fullmatch(result):
        return rejection(
            field,
            "identifier_invalid",
            "观测身份只能包含字母、数字、下划线、点、冒号、连字符，长度 1-64",
            value,
        )
    if len(result) > 128:
        return rejection(field, "string_too_long", f"{label}不能超过 128 个字符", value)
    return None


def evaluate_measurement(raw: object, index: int | None = None) -> tuple[MeasurementRecord | None, list[dict[str, Any]]]:
    """校验一条报送记录。

    返回（规范化记录，拒绝原因列表）；拒绝原因非空时记录为 None。
    index 用于批量回执定位第几条（从 0 开始）。
    """

    prefix = "measurement" if index is None else f"measurements[{index}]"

    def qualified(field: str) -> str:
        return field if index is None else f"{prefix}.{field}"

    rejections: list[dict[str, Any]] = []
    if not isinstance(raw, Mapping):
        return None, [rejection(prefix, "object_required", "每条测量记录必须是 JSON 对象", raw)]

    for field in ("observation_key", "test_frequency_hz", "response", "noise", "instrument"):
        if field not in raw:
            rejections.append(rejection(qualified(field), "required", f"{FIELD_LABELS[field]}缺失"))

    if rejections:
        # 缺字段时继续做其余字段类型检查意义不大，直接返回缺失原因。
        return None, rejections

    identity_reject = _check_identifier(raw["observation_key"], "observation_key")
    if identity_reject is not None:
        identity_reject["field"] = qualified("observation_key")
        rejections.append(identity_reject)

    instrument_reject = _check_identifier(raw["instrument"], "instrument")
    if instrument_reject is not None:
        instrument_reject["field"] = qualified("instrument")
        rejections.append(instrument_reject)

    numeric_specs = (
        ("test_frequency_hz", FREQUENCY_MIN_HZ_EXCLUSIVE, FREQUENCY_MAX_HZ, False),
        ("response", RESPONSE_MIN, RESPONSE_MAX, True),
        ("noise", NOISE_MIN, NOISE_MAX, True),
    )
    for field, minimum, maximum, inclusive in numeric_specs:
        problem = _check_number(raw[field], field, minimum=minimum, maximum=maximum, minimum_inclusive=inclusive)
        if problem is not None:
            problem["field"] = qualified(field)
            rejections.append(problem)

    if rejections:
        return None, rejections

    record = MeasurementRecord(
        observation_key=raw["observation_key"].strip(),
        test_frequency_hz=float(raw["test_frequency_hz"]),
        response=float(raw["response"]),
        noise=float(raw["noise"]),
        instrument=raw["instrument"].strip(),
    )
    return record, []


def evaluate_batch(raw_items: Sequence[object]) -> tuple[list[MeasurementRecord], list[dict[str, Any]]]:
    """校验一批报送记录，收集全部字段级拒绝原因，不在首条错误处停止。"""

    records: list[MeasurementRecord] = []
    rejections: list[dict[str, Any]] = []
    seen: dict[str, MeasurementRecord] = {}
    for index, item in enumerate(raw_items):
        record, item_rejections = evaluate_measurement(item, index)
        rejections.extend(item_rejections)
        if record is not None:
            # 批内同一观测身份重复：内容一致视为重放去重，不一致即冲突重放。
            prior = seen.get(record.observation_key)
            if prior is None:
                seen[record.observation_key] = record
                records.append(record)
            elif prior.value_identity() != record.value_identity():
                rejections.append(
                    rejection(
                        f"measurements[{index}].observation_key",
                        "observation_conflict",
                        f"观测身份 {record.observation_key} 在同一批次中出现了冲突内容",
                        record.observation_key,
                    )
                )
    return records, rejections


def loads_strict_json(body: bytes | str) -> Any:
    """解析严格 JSON：拒绝 Infinity/-Infinity/NaN 常量与重复键。"""

    def reject_constant(value: str) -> Any:
        raise StrictJsonError("non_finite_json", f"JSON 不允许非有限数值常量 {value}")

    def pairs_hook(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise StrictJsonError("duplicate_key", f"JSON 对象含重复键 {key}")
            result[key] = value
        return result

    try:
        text = body.decode("utf-8") if isinstance(body, bytes) else body
    except UnicodeDecodeError as exc:
        raise StrictJsonError("encoding_invalid", "请求体必须是 UTF-8 JSON") from exc
    try:
        return json.loads(text, parse_constant=reject_constant, object_pairs_hook=pairs_hook)
    except StrictJsonError:
        raise
    except json.JSONDecodeError as exc:
        raise StrictJsonError("invalid_json", f"请求体不是有效 JSON: {exc.msg}") from exc
