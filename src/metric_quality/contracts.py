"""报送网关测量数据的数值契约与写入边界校验。

所有进入业务表和审计链的测量必须先通过本模块的契约校验：
JSON 非有限数值、字符串伪装、超出适用量程的数值、缺失的来源身份
以及同一观测的冲突重放都会在写入边界被拒绝，拒绝信息携带具体字段、
规则和契约版本返回给报送方。读取路径对存量记录复用同一套规则，
保证写入边界与读取识别执行一致校验。
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping


CONTRACT_VERSION = "metric-quality-contract/1"

# 适用量程：观测期、指标值与偏差允许的取值范围。
TEST_FREQUENCY_HZ_MAX = 1_000_000.0
RESPONSE_ABS_MAX = 1_000_000.0
NOISE_MAX = 1_000_000.0
INSTRUMENT_MAX_LENGTH = 120

# 字段 -> (下界, 上界, 下界闭合, 上界闭合)
_RANGES: dict[str, tuple[float, float, bool, bool]] = {
    "test_frequency_hz": (0.0, TEST_FREQUENCY_HZ_MAX, False, True),
    "response": (-RESPONSE_ABS_MAX, RESPONSE_ABS_MAX, True, True),
    "noise": (0.0, NOISE_MAX, True, True),
}

_FIELD_LABELS = {
    "test_frequency_hz": "观测期",
    "response": "指标值",
    "noise": "偏差",
    "instrument": "来源身份",
}


class NonFiniteLiteral:
    """JSON 文本中 Infinity/-Infinity/NaN 字面量的占位符。

    API 解析请求体时用它替代非有限常量，使契约校验能把违规定位到
    具体字段，而不是在 JSON 解析阶段只给出笼统错误。
    """

    def __init__(self, token: str):
        self.token = token

    def __repr__(self) -> str:
        return f"NonFiniteLiteral({self.token!r})"


@dataclass(frozen=True)
class Violation:
    """单条契约违规：字段、规则和人类可读说明。"""

    field: str
    rule: str
    message: str
    index: int | None = None

    def as_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"field": self.field, "rule": self.rule, "message": self.message}
        if self.index is not None:
            result["index"] = self.index
        return result


class MeasurementRejected(ValueError):
    """测量负载违反数值契约；整批拒绝，不写入任何业务表或审计链。"""

    def __init__(self, violations: list[Violation]):
        self.violations = list(violations)
        fields = "、".join(sorted({item.field for item in self.violations}))
        super().__init__(f"测量违反数值契约（规则版本 {CONTRACT_VERSION}），涉及字段: {fields}")


class MeasurementConflict(RuntimeError):
    """同一观测身份的重放与已存记录内容冲突。"""

    def __init__(
        self,
        identity: Mapping[str, Any],
        differing_fields: list[str],
        existing_measurement_id: str | None,
    ):
        self.identity = dict(identity)
        self.differing_fields = list(differing_fields)
        self.existing_measurement_id = existing_measurement_id
        super().__init__(
            f"同一观测身份 {self.identity} 的重放与已存记录在字段 {self.differing_fields} 上冲突"
        )


@dataclass(frozen=True)
class CleanMeasurement:
    """通过契约校验、可以安全写入的测量。"""

    test_frequency_hz: float
    response: float
    noise: float
    instrument: str

    @property
    def identity(self) -> tuple[str, float]:
        """观测身份：来源身份 + 观测期。"""

        return self.instrument, self.test_frequency_hz


def _label(field: str) -> str:
    return f"{_FIELD_LABELS.get(field, field)}({field})"


def _range_text(field: str) -> str:
    low, high, low_closed, high_closed = _RANGES[field]
    return f"{'[' if low_closed else '('}{low:g}, {high:g}{']' if high_closed else ')'}"


def _in_range(field: str, value: float | int) -> bool:
    low, high, low_closed, high_closed = _RANGES[field]
    if value < low or (not low_closed and value == low):
        return False
    if value > high or (not high_closed and value == high):
        return False
    return True


def _check_number(field: str, value: Any, index: int | None) -> tuple[float | None, list[Violation]]:
    label = _label(field)
    if value is None:
        return None, [Violation(field, "required", f"{label} 为必填字段", index)]
    if isinstance(value, NonFiniteLiteral):
        return None, [
            Violation(field, "must_be_finite", f"{label} 不允许 JSON 非有限数值 {value.token}", index)
        ]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None, [
            Violation(field, "must_be_json_number", f"{label} 必须是 JSON 数值，不接受字符串等伪装类型", index)
        ]
    if isinstance(value, float) and not math.isfinite(value):
        return None, [
            Violation(field, "must_be_finite", f"{label} 必须是有限数值，不允许 Infinity 或 NaN", index)
        ]
    if not _in_range(field, value):
        return None, [
            Violation(field, "out_of_range", f"{label} 超出适用量程 {_range_text(field)}", index)
        ]
    return float(value), []


def _check_instrument(value: Any, index: int | None) -> tuple[str | None, list[Violation]]:
    label = _label("instrument")
    if value is None:
        return None, [Violation("instrument", "required", f"{label} 为必填字段", index)]
    if not isinstance(value, str):
        return None, [Violation("instrument", "must_be_text", f"{label} 必须是字符串", index)]
    text = value.strip()
    if not text:
        return None, [Violation("instrument", "must_be_non_empty", f"{label} 不能为空", index)]
    if len(text) > INSTRUMENT_MAX_LENGTH:
        return None, [
            Violation("instrument", "too_long", f"{label} 长度不能超过 {INSTRUMENT_MAX_LENGTH} 字符", index)
        ]
    return text, []


def validate_measurement(raw: Any, index: int | None = None) -> CleanMeasurement:
    """校验单条报送测量；失败时抛出携带全部违规的 MeasurementRejected。"""

    if not isinstance(raw, Mapping):
        raise MeasurementRejected([Violation("measurement", "must_be_object", "每条测量必须是 JSON 对象", index)])
    violations: list[Violation] = []
    frequency, errors = _check_number("test_frequency_hz", raw.get("test_frequency_hz"), index)
    violations += errors
    response, errors = _check_number("response", raw.get("response"), index)
    violations += errors
    # 偏差缺省为 0.0，与既有单条写入行为保持一致；显式 null 视为缺失。
    noise, errors = _check_number("noise", raw["noise"] if "noise" in raw else 0.0, index)
    violations += errors
    instrument, errors = _check_instrument(raw.get("instrument"), index)
    violations += errors
    if violations:
        raise MeasurementRejected(violations)
    return CleanMeasurement(frequency, response, noise, instrument)


def validate_batch(items: list[Any]) -> list[CleanMeasurement]:
    """校验一批报送测量；任一违规则整批拒绝，违规携带条目序号。"""

    violations: list[Violation] = []
    cleaned: list[CleanMeasurement] = []
    for index, item in enumerate(items):
        try:
            cleaned.append(validate_measurement(item, index))
        except MeasurementRejected as exc:
            violations.extend(exc.violations)
    if violations:
        raise MeasurementRejected(violations)
    return cleaned


def check_stored_measurement(
    test_frequency_hz: Any, response: Any, noise: Any, instrument: Any
) -> list[Violation]:
    """读取路径上的存量记录校验，规则与写入边界完全一致。"""

    violations: list[Violation] = []
    for field, value in (
        ("test_frequency_hz", test_frequency_hz),
        ("response", response),
        ("noise", noise),
    ):
        label = _label(field)
        if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value):
            violations.append(Violation(field, "must_be_finite", f"{label} 存量记录不是有限数值"))
            continue
        if not _in_range(field, value):
            violations.append(
                Violation(field, "out_of_range", f"{label} 存量记录超出适用量程 {_range_text(field)}")
            )
    if not isinstance(instrument, str) or not instrument.strip():
        violations.append(
            Violation("instrument", "must_be_non_empty", f"{_label('instrument')} 存量记录为空")
        )
    return violations
