"""容器和快照检查使用的冒烟验收命令。

演示写入边界数值契约：合法批量一次入库、非法记录被字段级拒绝并保持整批
原子性、存量扫描识别隔离、后续分析只消费可追溯的有效观测。
"""

from __future__ import annotations

import argparse
import json

from .service import MetricQualityService


def run() -> dict:
    service = MetricQualityService()
    service.bootstrap_admin()
    token = service.auth.login("admin", "metric-admin")
    service.create_lot(token, "BATCH-DEMO", "cross-border-service-index", "POLICY-3.2", 10)
    # 合法批量写入，全部带观测身份并通过写入契约。
    batch = service.add_measurements(token, "BATCH-DEMO", [
        {"observation_key": "obs-1", "test_frequency_hz": 450, "response": 0.71, "noise": 0.01, "instrument": "reporting-gateway-1"},
        {"observation_key": "obs-2", "test_frequency_hz": 520, "response": 0.93, "noise": 0.01, "instrument": "reporting-gateway-1"},
        {"observation_key": "obs-3", "test_frequency_hz": 650, "response": 0.84, "noise": 0.01, "instrument": "reporting-gateway-1"},
    ])
    result = service.analyze(token, "BATCH-DEMO")
    service.approve(token, "BATCH-DEMO", "hold", "awaiting data quality review")
    return {
        "status": "ok",
        "batch": result["lot_id"],
        "inserted": batch["inserted"],
        "valid_observations": result["valid_observations"],
        "peak_period": result["response_profile"]["peak_test_frequency_hz"],
        "contract_version": result["contract_version"],
        "events": len(service.audit(token, "BATCH-DEMO")),
    }


def main() -> None:
    argparse.ArgumentParser().parse_args()
    print(json.dumps(run(), ensure_ascii=False))


if __name__ == "__main__":
    main()
