"""验收脚本：重放取数链路修复后的端到端业务场景。

场景（对应任务最后一段）：
1. 指定一个历史批次进行重放；
2. 重放期间送入另一来源的一批正常日志并通过校验；
3. 对历史批次原文下发更正，核对重放实际读取的原文版本；
4. 提交不存在的批号，确认报错且无错误来源残留；
5. 检查实际重解析字段、校验状态和批次归属；
6. 中断重放后接续，确认剩余正文被新规则真实解析。
"""
from __future__ import annotations

import json
import sys

sys.path.insert(0, "/workspace/backend")

from app.seed import SEED_ROWS  # noqa: E402
from app.store import store  # noqa: E402

# 从种子状态开始，避免受其他用例污染
store._tables = {name: [dict(r) for r in rows] for name, rows in SEED_ROWS.items()}

from app.services.pipeline import pipeline  # noqa: E402


def add_parser(name: str, fields: str) -> None:
    rows = store.rows("field_parser")
    rows.append({
        "id": max(int(r["id"]) for r in rows) + 1,
        "规则名称": name,
        "适用数据源": "全部",
        "输入格式": "pipe",
        "输出字段列表": fields,
        "解析引擎": "split",
        "status": "已发布",
        "pending": False,
        "abnormal": False,
    })


def entities(batch_no):
    return [e for e in store.rows("parsed_entity") if e.get("来源批次") == batch_no]


def line(title: str) -> None:
    print(f"\n=== {title} ===")


# ---- 1. 历史批次 A：采集 → 旧规则解析 → 校验通过 ----
line("1. 历史批次 A 采集并通过旧规则解析/校验")
pipeline.register_source("hist-source", "http", "cn-north")
pipeline.validate_source("hist-source")
hist_logs = [
    {"body": "2026-03-01T09:00:00|INFO|old body one"},
    {"body": "2026-03-01T09:05:00|WARN|old body two"},
    {"body": "2026-03-01T09:10:00|ERROR|old body three"},
]
batch_a = pipeline.start_ingest_batch("hist-source", "2026-03-01", hist_logs)["batch"]["批次编号"]
pipeline.parse_batch(batch_a)
pipeline.validate_entities(batch_a)
add_parser("parser-v2", "ts,level,msg")
old_fields = json.loads(entities(batch_a)[0]["标准化字段集"])
print("批次 A:", batch_a, "| 旧字段:", old_fields)
assert set(old_fields) == {"time", "level", "message"}
assert all(e["status"] == "校验通过" for e in entities(batch_a))

# ---- 2. 另一来源的正常采集批次 B，与重放并发 ----
line("2. 另一来源正常批次 B 完成采集并通过校验")
pipeline.register_source("normal-source", "http", "cn-east")
pipeline.validate_source("normal-source")
batch_b = pipeline.start_ingest_batch(
    "normal-source", "2026-03-02",
    [{"body": "2026-03-02T10:00:00|INFO|normal flow"}],
)["batch"]["批次编号"]
pipeline.parse_batch(batch_b)
vb = pipeline.validate_entities(batch_b)
print("批次 B:", batch_b, "| 校验结果:", vb["passed"], "条通过")
assert vb["passed"] == 1

# ---- 3. 历史批次 A 的一条原文收到更正 ----
line("3. 历史批次 A 原文更正")
raw_a = next(r for r in store.rows("raw_records") if r["批次编号"] == batch_a)
correction = pipeline.correct_raw_record(
    batch_a, raw_a["id"], "2026-03-01T09:00:00|INFO|corrected body one"
)
print("原文版本:", correction["raw_version"], "| 新指纹:", correction["body_digest"])
assert correction["ok"] and correction["raw_version"] == 2

# ---- 4. 先提交不存在的批号 ----
line("4. 提交不存在的批号 INGE-9999")
ghost = pipeline.replay_batch("INGE-9999", "parser-v2")
replay_count_before = len(store.rows("replay_engine"))
print("返回:", ghost["ok"], "-", ghost["message"])
assert ghost["ok"] is False and ghost["missing_batches"] == ["INGE-9999"]
assert len(store.rows("replay_engine")) == replay_count_before
# B 批次校验结论未被任何错误来源污染
assert all(e["status"] == "校验通过" for e in entities(batch_b))

# ---- 5. 中断式重放 A（limit=2，范围共 3 条） ----
line("5. 重放批次 A（先处理 2 条，制造中断）")
first = pipeline.replay_batch(batch_a, "parser-v2", limit=2)
replay_id = first["replay"]["id"]
print("中断:", first["interrupted"], "| 已读:", [s["raw_record_id"] for s in first["read_sources"]])
assert first["interrupted"] and len(first["read_sources"]) == 2
# 已处理的 2 条已是新规则字段且待校验，未处理的 1 条状态不被提前改动
processed_ids = {s["raw_record_id"] for s in first["read_sources"]}
for e in entities(batch_a):
    fields = json.loads(e["标准化字段集"])
    if int(e["原始记录ID"]) in processed_ids:
        assert set(fields) == {"ts", "level", "msg"}
        assert e["status"] == "待校验"
    else:
        assert e["status"] == "校验通过"

# ---- 6. 接续重放剩余范围 ----
line("6. 接续重放并完成")
second = pipeline.replay_batch(batch_a, "parser-v2", resume_replay_id=replay_id)
print("接续处理:", second["replayed_count"], "条 | 状态:", second["replay"]["status"])
assert not second["interrupted"] and second["replayed_count"] == 1
assert second["replay"]["status"] == "已完成"

# ---- 7. 核对重解析字段、读取的原文版本、校验状态、批次归属 ----
line("7. 核对最终结果")
final_a = entities(batch_a)
assert len(final_a) == 3  # 同键 upsert，无重复明细
for e in final_a:
    fields = json.loads(e["标准化字段集"])
    assert set(fields) == {"ts", "level", "msg"}
    assert e["解析规则版本"] == "parser-v2"
    assert e["来源批次"] == batch_a
    assert str(e["replay_id"]) == str(replay_id)
    assert e["status"] == "校验通过"
corrected_entity = next(e for e in final_a if e["原始记录ID"] == str(raw_a["id"]))
assert json.loads(corrected_entity["标准化字段集"])["msg"] == "corrected body one"
assert corrected_entity["source_digest"] == correction["body_digest"]
print("更正记录读取指纹一致:", corrected_entity["source_digest"] == correction["body_digest"])

# 并发的 B 批次：归属、校验结论、无 replay 标记
final_b = entities(batch_b)
assert len(final_b) == 1 and final_b[0]["来源批次"] == batch_b
assert final_b[0]["status"] == "校验通过" and "replay_id" not in final_b[0]
print("批次 B 不受影响:", final_b[0]["来源批次"], final_b[0]["status"])

# 血缘清单回写
lineage = [x for x in store.rows("data_lineage") if x.get("replay_id") == str(replay_id)]
assert len(lineage) == 1 and lineage[0]["来源批次"] == batch_a
assert int(lineage[0]["record_count"]) == 3
print("血缘回写:", lineage[0]["标准化版本"], "| 记录数:", lineage[0]["record_count"])

# 范围表达式 + 不存在批号混合仍报错
line("8. 混合范围（真实批号 + 幽灵批号）")
mixed = pipeline.replay_batch(f"{batch_a},INGE-7777", "parser-v2")
assert mixed["ok"] is False and "INGE-7777" in mixed["message"]
print("返回:", mixed["ok"], "-", mixed["message"])

print("\n全部验收断言通过 ✅")
