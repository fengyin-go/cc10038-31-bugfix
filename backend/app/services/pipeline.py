"""跨模块数据采集流水线：编排 source_registry → ingest_batch → raw_records
→ field_parser → parsed_entity → dedup_engine → aggregation_view → data_export
的完整数据链路，包含版本追踪、失败恢复、批次去重和水位管理。
"""
from __future__ import annotations

import copy
import hashlib
import json
import re
import time
from typing import Any

from app.store import store


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def _stable_digest(data: dict[str, Any]) -> str:
    serialized = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()[:16]


def _next_id(module: str) -> int:
    return max((int(r.get("id", 0)) for r in store.rows(module)), default=0) + 1


class IngestPipeline:
    """采集流水线统一编排，管理从数据源到导出的完整数据链路。"""

    # ---------- 阶段0：数据源登记与版本 ----------
    def register_source(self, source_id: str, protocol: str, region: str) -> dict[str, Any]:
        sources = store.rows("source_registry")
        for s in sources:
            if s.get("来源标识") == source_id:
                return {"ok": False, "message": f"数据源 {source_id} 已登记", "entry": s}
        entry = {
            "id": _next_id("source_registry"),
            "来源标识": source_id,
            "接入协议": protocol,
            "连接凭证": f"token-{_stable_digest({'source': source_id})}",
            "采集区域": region,
            "状态": source_id,
            "status": "待验证",
            "pending": True,
            "abnormal": False,
            "source_version": 1,
            "registered_at": _now_iso(),
        }
        sources.append(entry)
        return {"ok": True, "message": "数据源已登记", "entry": entry}

    def validate_source(self, source_id: str) -> dict[str, Any]:
        sources = store.rows("source_registry")
        for s in sources:
            if s.get("来源标识") == source_id:
                s["status"] = "已验证"
                s["pending"] = False
                s["abnormal"] = False
                s["source_version"] = s.get("source_version", 1) + 1
                s["validated_at"] = _now_iso()
                return {"ok": True, "message": f"数据源 {source_id} 已验证", "entry": s}
        return {"ok": False, "message": f"数据源 {source_id} 不存在"}

    # ---------- 阶段1：采集批次启动 ----------
    def start_ingest_batch(
        self,
        source_id: str,
        time_window: str,
        raw_logs: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """启动采集批次：登记批次并为其生成原始日志记录。"""
        sources = store.rows("source_registry")
        source_entry = None
        for s in sources:
            if s.get("来源标识") == source_id:
                source_entry = s
                break
        if source_entry is None:
            return {"ok": False, "message": f"数据源 {source_id} 未登记"}
        if source_entry.get("status") != "已验证":
            return {"ok": False, "message": f"数据源 {source_id} 未通过验证"}

        # 检查是否有未完成的同源批次
        batches = store.rows("ingest_batch")
        for b in batches:
            if b.get("所属数据源") == source_id and b.get("status") in ("待采集", "采集中"):
                return {"ok": False, "message": f"数据源 {source_id} 已有进行中的批次 {b.get('批次编号')}", "batch": b}

        batch_no = f"INGE-{_next_id('ingest_batch'):04d}"
        batch_entry = {
            "id": _next_id("ingest_batch"),
            "批次编号": batch_no,
            "所属数据源": source_id,
            "采集时间窗口": time_window,
            "记录总数": str(len(raw_logs) if raw_logs else 0),
            "批次状态": time_window,
            "status": "采集中",
            "pending": True,
            "abnormal": False,
            "batch_version": 1,
            "source_version": source_entry.get("source_version", 1),
            "started_at": _now_iso(),
        }
        batches.append(batch_entry)

        # 生成原始日志记录
        raw_records_list = store.rows("raw_records")
        created_ids: list[int] = []
        if raw_logs:
            for idx, log in enumerate(raw_logs):
                raw_id = _next_id("raw_records")
                record = {
                    "id": raw_id,
                    "批次编号": batch_no,
                    "原始行号": str(idx + 1),
                    "采集时间戳": _now_iso(),
                    "原始正文": log.get("body", str(log)),
                    "解析状态": "未解析",
                    "status": "待解析",
                    "pending": True,
                    "abnormal": False,
                    "batch_version": 1,
                    "source_id": source_id,
                    "ingested_at": _now_iso(),
                }
                raw_records_list.append(record)
                created_ids.append(raw_id)

        batch_entry["记录总数"] = str(len(created_ids))
        batch_entry["raw_record_ids"] = json.dumps(created_ids)
        return {
            "ok": True,
            "message": f"采集批次 {batch_no} 已启动",
            "batch": batch_entry,
            "record_count": len(created_ids),
        }

    # ---------- 阶段2：字段解析 ----------
    def parse_batch(
        self,
        batch_no: str,
        parser_rule_name: str | None = None,
    ) -> dict[str, Any]:
        """解析批次中的原始日志为标准化明细。"""
        raw_records_list = store.rows("raw_records")
        batch_raw = [r for r in raw_records_list if r.get("批次编号") == batch_no]
        if not batch_raw:
            return {"ok": False, "message": f"批次 {batch_no} 无原始日志记录"}

        # 查找匹配的解析规则
        parsers = store.rows("field_parser")
        rule = None
        if parser_rule_name:
            for p in parsers:
                if p.get("规则名称") == parser_rule_name:
                    rule = p
                    break
        if rule is None:
            for p in parsers:
                if p.get("status") == "已发布":
                    rule = p
                    break
        if rule is None:
            return {"ok": False, "message": "无可用解析规则，请先发布一条字段解析规则"}

        # 获取当前 schema_version
        schema_versions = store.rows("schema_version")
        schema_ver = None
        for sv in schema_versions:
            if sv.get("status") == "已发布":
                schema_ver = sv
                break

        parsed_entities = store.rows("parsed_entity")
        success_count = 0
        failure_count = 0
        parsed_ids: list[int] = []
        failed_ids: list[int] = []

        for raw in batch_raw:
            raw_id = raw["id"]
            try:
                # 模拟字段解析：将原始正文按规则打散
                body = raw.get("原始正文", "")
                fields = self._parse_body(body, rule)
                ent_id = _next_id("parsed_entity")
                entity = {
                    "id": ent_id,
                    "来源批次": batch_no,
                    "原始记录ID": str(raw_id),
                    "解析规则版本": rule.get("规则名称", "unknown"),
                    "标准化字段集": json.dumps(fields, ensure_ascii=False),
                    "校验状态": "",
                    "status": "待校验",
                    "pending": True,
                    "abnormal": False,
                    "parser_rule": rule.get("规则名称"),
                    "schema_version": schema_ver.get("版本号") if schema_ver else "v1.0",
                    "parsed_at": _now_iso(),
                    "content_digest": _stable_digest(fields),
                }
                parsed_entities.append(entity)
                raw["status"] = "已解析"
                raw["pending"] = False
                raw["解析状态"] = "已解析"
                success_count += 1
                parsed_ids.append(ent_id)
            except Exception:
                raw["status"] = "解析失败"
                raw["解析状态"] = "解析失败"
                raw["abnormal"] = True
                failure_count += 1
                failed_ids.append(raw_id)

        # 更新批次状态
        batches = store.rows("ingest_batch")
        for b in batches:
            if b.get("批次编号") == batch_no:
                b["status"] = "已完成" if failure_count == 0 else "部分失败"
                b["pending"] = failure_count > 0
                b["abnormal"] = failure_count > 0
                b["parsed_ids"] = json.dumps(parsed_ids)
                b["batch_version"] = b.get("batch_version", 1) + 1
                break

        # 记录解析日志
        journal = store.rows("parse_journal")
        journal_entry = {
            "id": _next_id("parse_journal"),
            "操作类型": "批量解析",
            "批次编号": batch_no,
            "执行时间": _now_iso(),
            "影响的记录数": str(success_count),
            "执行结果": "成功" if failure_count == 0 else "部分成功",
            "status": "成功" if failure_count == 0 else "部分成功",
            "pending": failure_count > 0,
            "abnormal": failure_count > 0,
            "success_count": str(success_count),
            "failure_count": str(failure_count),
        }
        journal.append(journal_entry)

        # 失败记录进入重试队列
        if failure_count > 0:
            retry_q = store.rows("retry_queue")
            for fid in failed_ids:
                retry_entry = {
                    "id": _next_id("retry_queue"),
                    "失败记录ID": str(fid),
                    "失败原因": "字段解析失败",
                    "重试次数": "0",
                    "最大重试": "3",
                    "下次重试时间": _now_iso(),
                    "status": "待重试",
                    "pending": True,
                    "abnormal": False,
                    "batch_no": batch_no,
                    "parser_rule": rule.get("规则名称"),
                }
                retry_q.append(retry_entry)

        return {
            "ok": True,
            "message": f"批次 {batch_no} 解析完成",
            "success": success_count,
            "failure": failure_count,
            "parsed_ids": parsed_ids,
            "failed_ids": failed_ids,
        }

    def _parse_body(self, body: str, rule: dict[str, Any]) -> dict[str, Any]:
        """模拟字段解析逻辑。种子数据中的规则可能只有占位文本，此时使用默认字段名。"""
        parts = body.split("|") if "|" in body else [body]
        raw_field_list = (rule.get("输出字段列表", "") or "").strip()
        # 识别真实字段列表：含逗号分隔且每段不超过20字
        if raw_field_list and "," in raw_field_list:
            field_names = [f.strip() for f in raw_field_list.split(",")]
        else:
            field_names = ["time", "level", "message"]
        fields: dict[str, Any] = {}
        for i, fn in enumerate(field_names):
            fn = fn.strip()
            if fn:
                fields[fn] = parts[i].strip() if i < len(parts) else ""
        return fields

    # ---------- 阶段3：去重 ----------
    def run_dedup(
        self,
        dedup_key_fields: list[str],
        window: str,
        retain_strategy: str = "keep_first",
    ) -> dict[str, Any]:
        """对标准化明细执行去重。"""
        dedup_strategies = store.rows("dedup_engine")
        parsed = store.rows("parsed_entity")

        # 构建去重策略
        strategy_entry = {
            "id": _next_id("dedup_engine"),
            "策略名称": f"dedup-{_now_iso()}",
            "去重键字段": ",".join(dedup_key_fields),
            "去重窗口": window,
            "保留策略": retain_strategy,
            "生效数据源": "全部",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        }
        dedup_strategies.append(strategy_entry)

        # 执行去重
        seen: dict[str, int] = {}
        duplicates = 0
        for rec in parsed:
            if rec.get("status") != "校验通过":
                continue
            fields_json = rec.get("标准化字段集", "{}")
            try:
                fields_data = json.loads(fields_json)
            except (json.JSONDecodeError, TypeError):
                fields_data = {}
            key_parts = [str(fields_data.get(k, "")) for k in dedup_key_fields]
            key = "||".join(key_parts)
            if key in seen:
                if retain_strategy == "keep_first":
                    rec["status"] = "校验失败"
                    rec["abnormal"] = True
                    rec["dedup_id"] = str(seen[key])
                    duplicates += 1
                else:
                    # keep_last：把旧记录标记为重复
                    old = store.find("parsed_entity", seen[key])
                    if old:
                        old["status"] = "校验失败"
                        old["abnormal"] = True
                        old["dedup_id"] = str(rec["id"])
                    rec["status"] = "校验通过"
                    rec["dedup_id"] = ""
                    seen[key] = rec["id"]
            else:
                seen[key] = rec["id"]

        return {
            "ok": True,
            "message": f"去重完成",
            "strategy": strategy_entry["策略名称"],
            "dedup_key_fields": dedup_key_fields,
            "duplicates_found": duplicates,
            "unique_keys": len(seen),
        }

    # ---------- 阶段4：校验 ----------
    def validate_entities(
        self,
        batch_no: str | None = None,
        *,
        scope_batches: list[str] | None = None,
        replay_id: int | None = None,
    ) -> dict[str, Any]:
        """对标准化明细执行质量校验。

        普通调用只校验待校验明细；重放调用通过 scope_batches/replay_id
        把读取范围严格限制在本次重放确认范围内的标准化明细上，范围外
        已通过校验的记录不会被读取或改写，仍照常进入统计。
        """
        parsed = store.rows("parsed_entity")
        quality_rules = store.rows("quality_check")

        active_rule = None
        for r in quality_rules:
            if r.get("status") == "已生效":
                active_rule = r
                break

        target = [p for p in parsed if p.get("status") == "待校验"]
        if batch_no:
            target = [p for p in target if p.get("来源批次") == batch_no]
        if scope_batches is not None:
            scope_set = set(scope_batches)
            target = [p for p in target if p.get("来源批次") in scope_set]
        if replay_id is not None:
            target = [p for p in target if str(p.get("replay_id")) == str(replay_id)]

        passed = 0
        failed = 0
        for entity in target:
            try:
                fields_data = json.loads(entity.get("标准化字段集", "{}"))
            except (json.JSONDecodeError, TypeError):
                fields_data = {}

            is_valid = bool(fields_data) and all(
                str(v).strip() for v in fields_data.values()
            )
            if is_valid:
                entity["status"] = "校验通过"
                entity["校验状态"] = "校验通过"
                entity["pending"] = False
                entity["validated_at"] = _now_iso()
                if active_rule:
                    entity["quality_rule"] = active_rule.get("规则名称")
                passed += 1
            else:
                entity["status"] = "校验失败"
                entity["校验状态"] = "校验失败"
                entity["abnormal"] = True
                failed += 1

        return {
            "ok": True,
            "message": f"校验完成",
            "passed": passed,
            "failed": failed,
            "quality_rule": active_rule.get("规则名称") if active_rule else None,
        }

    # ---------- 阶段5：聚合视图刷新 ----------
    def refresh_aggregation(
        self,
        view_name: str,
        dim_field: str,
        metric_field: str,
        agg_func: str = "count",
    ) -> dict[str, Any]:
        """基于标准化明细刷新聚合视图。"""
        parsed = store.rows("parsed_entity")
        results: dict[str, dict[str, Any]] = {}

        for entity in parsed:
            if entity.get("status") != "校验通过":
                continue
            try:
                fields_data = json.loads(entity.get("标准化字段集", "{}"))
            except (json.JSONDecodeError, TypeError):
                continue

            dim_val = str(fields_data.get(dim_field, "unknown"))
            metric_val = fields_data.get(metric_field, "0")

            if dim_val not in results:
                results[dim_val] = {"count": 0, "sum": 0.0, "values": []}

            results[dim_val]["count"] += 1
            try:
                results[dim_val]["sum"] += float(metric_val) if metric_val else 0.0
            except (ValueError, TypeError):
                pass
            results[dim_val]["values"].append(metric_val)

        # 创建/更新聚合视图
        aggs = store.rows("aggregation_view")
        agg_entry = {
            "id": _next_id("aggregation_view"),
            "视图名称": view_name,
            "聚合维度": dim_field,
            "指标定义": f"{agg_func}({metric_field})",
            "数据源范围": "标准化明细",
            "刷新间隔": "manual",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
            "results": results,
            "refreshed_at": _now_iso(),
            "source_records": len(parsed),
        }
        aggs.append(agg_entry)

        return {
            "ok": True,
            "message": f"聚合视图 {view_name} 已刷新",
            "view": agg_entry,
            "dimensions": len(results),
        }

    # ---------- 阶段6：数据导出 ----------
    def build_export(
        self,
        view_name: str,
        export_format: str = "csv",
        filters: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """从聚合视图或标准化明细生成导出数据。"""
        exports = store.rows("data_export")
        lineages = store.rows("data_lineage")

        # 查找目标视图
        aggs = store.rows("aggregation_view")
        view = None
        for a in aggs:
            if a.get("视图名称") == view_name:
                view = a
                break

        results_data = view.get("results", {}) if view else {}

        export_id = _next_id("data_export")
        export_entry = {
            "id": export_id,
            "导出范围": view_name,
            "目标格式": export_format,
            "过滤条件": json.dumps(filters or {}, ensure_ascii=False),
            "导出状态": "",
            "文件路径": f"/exports/{view_name}.{export_format}",
            "status": "已完成",
            "pending": False,
            "abnormal": False,
            "results": results_data,
            "record_count": len(results_data),
            "exported_at": _now_iso(),
        }
        exports.append(export_entry)

        # 记录血缘
        lineage_entry = {
            "id": _next_id("data_lineage"),
            "来源批次": view_name,
            "解析规则版本": view.get("视图名称", "unknown") if view else "unknown",
            "标准化版本": view_name,
            "下游消费标识": f"export-{export_id}",
            "记录时间": _now_iso(),
            "status": "已记录",
            "pending": False,
            "abnormal": False,
            "export_id": str(export_id),
            "export_format": export_format,
        }
        lineages.append(lineage_entry)

        return {
            "ok": True,
            "message": f"数据导出 {view_name}.{export_format} 完成",
            "export": export_entry,
            "lineage": lineage_entry,
        }

    # ---------- 水位管理 ----------
    def advance_watermark(
        self,
        source_id: str,
        batch_no: str,
        watermark_value: str,
    ) -> dict[str, Any]:
        """推进指定数据源的采集水位，并防止倒退。"""
        watermarks = store.rows("watermark_mgr")
        existing = None
        for w in watermarks:
            if w.get("数据源") == source_id:
                existing = w
                break

        if existing is None:
            entry = {
                "id": _next_id("watermark_mgr"),
                "数据源": source_id,
                "最后成功批次": batch_no,
                "当前水位值": watermark_value,
                "水位类型": "增量时间戳",
                "更新时间": _now_iso(),
                "status": "正常",
                "pending": False,
                "abnormal": False,
                "previous_value": "",
                "is_rollback": False,
            }
            watermarks.append(entry)
            return {"ok": True, "message": f"数据源 {source_id} 水位已初始化", "watermark": entry}

        prev_value = existing.get("当前水位值", "")
        # 防止水位倒退
        if watermark_value < prev_value:
            existing["status"] = "异常回退"
            existing["abnormal"] = True
            existing["previous_value"] = prev_value
            existing["is_rollback"] = True
            existing["更新时间"] = _now_iso()
            return {"ok": False, "message": f"水位倒退被阻止：{watermark_value} < {prev_value}"}

        existing["最后成功批次"] = batch_no
        existing["当前水位值"] = watermark_value
        existing["更新时间"] = _now_iso()
        existing["status"] = "正常" if existing.get("status") == "异常回退" else existing.get("status", "正常")
        existing["previous_value"] = prev_value
        existing["is_rollback"] = False
        existing["abnormal"] = False

        return {"ok": True, "message": f"数据源 {source_id} 水位已推进到 {watermark_value}"}

    # ---------- 回溯重放 ----------
    def _resolve_batch_scope(self, history_batch_range: str) -> list[str]:
        """解析重放批次范围表达式，返回去重且保持顺序的批次号列表。

        支持三种写法并可混用：
        - 单批次：``INGE-0001``
        - 逗号/中文逗号/分号/空白分隔的多批次：``INGE-0001,INGE-0002``
        - 闭区间（``..`` 或 ``~`` 分隔）：``INGE-0001..INGE-0003``
          仅在两端同前缀同数字位数时展开，否则原样保留。
        """
        text = (history_batch_range or "").strip()
        if not text:
            return []

        tokens = [t for t in re.split(r"[,，;；\s]+", text) if t.strip()]
        resolved: list[str] = []

        def remember(batch_no: str) -> None:
            batch_no = batch_no.strip()
            if batch_no and batch_no not in resolved:
                resolved.append(batch_no)

        for token in tokens:
            range_match = re.match(
                r"^(?P<left>.+?)(?:\.\.|~)(?P<right>.+)$", token
            )
            if range_match is None:
                remember(token)
                continue
            left = range_match.group("left").strip()
            right = range_match.group("right").strip()
            left_match = re.match(r"^(?P<prefix>.*?)(?P<num>\d+)$", left)
            right_match = re.match(r"^(?P<prefix>.*?)(?P<num>\d+)$", right)
            if (
                left_match is None
                or right_match is None
                or left_match.group("prefix") != right_match.group("prefix")
                or len(left_match.group("num")) != len(right_match.group("num"))
            ):
                # 无法识别为同构数字区间时按两个独立批次处理，避免吞掉批号
                remember(left)
                remember(right)
                continue
            prefix = left_match.group("prefix")
            width = len(left_match.group("num"))
            start = int(left_match.group("num"))
            end = int(right_match.group("num"))
            if start > end:
                start, end = end, start
            for num in range(start, end + 1):
                remember(f"{prefix}{num:0{width}d}")
        return resolved

    def _find_parser_rule(self, rule_name: str | None) -> dict[str, Any] | None:
        """按名称精确查找解析规则；未指定名称时回退到任意已发布规则。"""
        parsers = store.rows("field_parser")
        if rule_name:
            for p in parsers:
                if p.get("规则名称") == rule_name:
                    return p
        for p in parsers:
            if p.get("status") == "已发布":
                return p
        return None

    def _current_schema_version(self) -> str:
        schema_versions = store.rows("schema_version")
        for sv in schema_versions:
            if sv.get("status") == "已发布":
                return str(sv.get("版本号", "v1.0"))
        return "v1.0"

    def _reparse_raw_record(
        self,
        raw: dict[str, Any],
        rule: dict[str, Any],
        rule_label: str,
        replay_id: int,
        schema_ver: str,
    ) -> dict[str, Any]:
        """用指定规则对一条原始日志真正重新解析，并在标准化明细中就地替换。

        每条原始记录在 parsed_entity 中只保留一条当前标准化明细（按
        来源批次 + 原始记录ID 匹配 upsert），重放不新增同键明细。
        """
        raw_id = int(raw["id"])
        batch_no = str(raw.get("批次编号", ""))
        body = raw.get("原始正文", "")
        fields = self._parse_body(body, rule)
        # source_digest 标识本次实际读取到的原文版本，与原文更正指纹同口径，
        # 便于核对重放究竟读取了哪一份原文；content_digest 标识解析结果。
        body_digest = raw.get("body_digest") or _stable_digest({"body": body})
        content_digest = _stable_digest({"body": body, "fields": fields})

        parsed_entities = store.rows("parsed_entity")
        target: dict[str, Any] | None = None
        for entity in parsed_entities:
            if (
                entity.get("来源批次") == batch_no
                and str(entity.get("原始记录ID")) == str(raw_id)
            ):
                target = entity
                break

        common = {
            "来源批次": batch_no,
            "原始记录ID": str(raw_id),
            "解析规则版本": rule_label,
            "标准化字段集": json.dumps(fields, ensure_ascii=False),
            "校验状态": "",
            "status": "待校验",
            "pending": True,
            "abnormal": False,
            "parser_rule": rule.get("规则名称"),
            "schema_version": schema_ver,
            "parsed_at": _now_iso(),
            "content_digest": content_digest,
            "replay_id": str(replay_id),
            "replayed_at": _now_iso(),
            "source_batch": batch_no,
            "source_digest": body_digest,
        }
        if target is None:
            target = {"id": _next_id("parsed_entity")}
            target.update(common)
            parsed_entities.append(target)
        else:
            target.update(common)
        return target

    def _write_replay_lineage(
        self,
        replay_entry: dict[str, Any],
        scope: list[str],
        rule_label: str,
    ) -> list[dict[str, Any]]:
        """本轮重放结果回写采集血缘清单。"""
        lineages = store.rows("data_lineage")
        parsed_entities = store.rows("parsed_entity")
        replay_id = replay_entry["id"]
        created: list[dict[str, Any]] = []
        for batch_no in scope:
            # 统计该批次下归属本次重放的全部标准化明细（含中断接续各段），
            # 而不是只统计最后一个接续段处理的明细。
            batch_entities = [
                e
                for e in parsed_entities
                if e.get("来源批次") == batch_no
                and str(e.get("replay_id")) == str(replay_id)
            ]
            lineage_entry = {
                "id": _next_id("data_lineage"),
                "来源批次": batch_no,
                "解析规则版本": rule_label,
                "标准化版本": f"replay-{replay_id}",
                "下游消费标识": f"replay-{replay_id}:{batch_no}",
                "记录时间": _now_iso(),
                "status": "已记录",
                "pending": False,
                "abnormal": False,
                "replay_id": str(replay_id),
                "record_count": len(batch_entities),
                "source_digests": json.dumps(
                    sorted({str(e.get("source_digest", "")) for e in batch_entities}),
                    ensure_ascii=False,
                ),
            }
            lineages.append(lineage_entry)
            created.append(lineage_entry)
        return created

    def correct_raw_record(
        self,
        batch_no: str,
        raw_record_id: int,
        new_body: str,
    ) -> dict[str, Any]:
        """对历史批次中的一条原文下发更正（重放取数链路的一部分）。

        更正会提升原文版本号并刷新正文指纹，重放时按指纹即可核对本次
        实际读取的是哪一份原文；不存在的批号/记录直接报错且不产生残留。
        """
        batches = store.rows("ingest_batch")
        if not any(b.get("批次编号") == batch_no for b in batches):
            return {"ok": False, "message": f"批次 {batch_no} 不存在，无法更正原文"}

        raw_records_list = store.rows("raw_records")
        target: dict[str, Any] | None = None
        for raw in raw_records_list:
            if raw.get("批次编号") == batch_no and int(raw.get("id", -1)) == int(raw_record_id):
                target = raw
                break
        if target is None:
            return {
                "ok": False,
                "message": f"批次 {batch_no} 中不存在原始记录 {raw_record_id}",
            }

        previous_body = target.get("原始正文", "")
        target["原始正文"] = new_body
        target["raw_version"] = int(target.get("raw_version", 1)) + 1
        target["body_digest"] = _stable_digest({"body": new_body})
        target["corrected_at"] = _now_iso()
        return {
            "ok": True,
            "message": f"批次 {batch_no} 原始记录 {raw_record_id} 原文已更正",
            "raw_record": target,
            "raw_version": target["raw_version"],
            "previous_digest": _stable_digest({"body": previous_body}),
            "body_digest": target["body_digest"],
        }

    def replay_batch(
        self,
        history_batch_range: str,
        new_parser_rule: str,
        *,
        limit: int | None = None,
        resume_replay_id: int | None = None,
    ) -> dict[str, Any]:
        """用新解析规则版本回溯重放指定历史批次范围内的原始日志。

        关键约束：
        - 先解析范围并校验每个批号真实存在；不存在的批号直接报错，
          且在校验通过前不创建任何重放任务，杜绝错误来源残留。
        - 只处理确认范围里的原始记录/标准化明细，范围外记录（含其他
          批次已校验通过的记录）完全不触碰，照常进入统计。
        - 对范围内每条原文真用新规则重新解析一遍（而非替换版本标签），
          并记录读取到的原文指纹，可核对本次究竟读取了哪份原文。
        - 支持中断接续：limit 截断后保留「重放中」任务，再次调用时
          传 resume_replay_id 接续实际尚未读取的范围。
        """
        replayers = store.rows("replay_engine")

        # ---- 接续模式：找回未完成的重放任务，沿用原范围与规则 ----
        if resume_replay_id is not None:
            replay_entry = next(
                (r for r in replayers if int(r.get("id", -1)) == int(resume_replay_id)),
                None,
            )
            if replay_entry is None:
                return {
                    "ok": False,
                    "message": f"重放任务 {resume_replay_id} 不存在，无法接续",
                }
            if replay_entry.get("status") != "重放中":
                return {
                    "ok": False,
                    "message": (
                        f"重放任务 {resume_replay_id} 已处于"
                        f"{replay_entry.get('status')} 状态，不可接续"
                    ),
                }
            scope = list(replay_entry.get("scope_batches", []))
            rule_label = str(replay_entry.get("新解析规则版本", ""))
        else:
            scope = self._resolve_batch_scope(history_batch_range)
            if not scope:
                return {"ok": False, "message": "重放批次范围为空，未执行重放"}

            # ---- 批号必须真实存在，错误在任何写入之前返回 ----
            batches = store.rows("ingest_batch")
            existing = {b.get("批次编号") for b in batches}
            missing = [b for b in scope if b not in existing]
            if missing:
                return {
                    "ok": False,
                    "message": f"重放批次不存在：{'、'.join(missing)}，未创建重放任务",
                    "missing_batches": missing,
                }

            replay_id = _next_id("replay_engine")
            replay_entry = {
                "id": replay_id,
                "历史批次范围": history_batch_range,
                "新解析规则版本": new_parser_rule,
                "重放状态": "重放中",
                "产出记录数": "0",
                "执行时间": _now_iso(),
                "status": "重放中",
                "pending": True,
                "abnormal": False,
                "scope_batches": scope,
                "processed_raw_ids": [],
                "processed_digests": [],
                "total_in_scope": 0,
            }
            replayers.append(replay_entry)
            rule_label = new_parser_rule

        # ---- 规则必须可定位，规则错误同样不允许留下半截任务数据 ----
        rule = self._find_parser_rule(rule_label)
        if rule is None:
            if resume_replay_id is None:
                replayers.remove(replay_entry)
            return {
                "ok": False,
                "message": f"解析规则 {rule_label} 不存在且无已发布规则可用，未执行重放",
            }
        # 规则名存在则以该规则解析；标签始终沿用任务声明的新版本号
        rule_label = str(replay_entry.get("新解析规则版本", rule_label))
        schema_ver = self._current_schema_version()

        # ---- 取数链路：只读取确认范围内的原始日志，按稳定顺序处理 ----
        raw_records_list = store.rows("raw_records")
        scope_set = set(scope)
        scope_raw = sorted(
            (r for r in raw_records_list if r.get("批次编号") in scope_set),
            key=lambda r: (str(r.get("批次编号", "")), int(r.get("id", 0))),
        )
        replay_entry["total_in_scope"] = len(scope_raw)

        processed_ids = {int(x) for x in replay_entry.get("processed_raw_ids", [])}
        pending_raw = [r for r in scope_raw if int(r["id"]) not in processed_ids]
        if limit is not None and limit >= 0:
            chunk = pending_raw[:limit]
        else:
            chunk = pending_raw
        interrupted = len(chunk) < len(pending_raw)

        reprocessed: list[dict[str, Any]] = []
        read_sources: list[dict[str, Any]] = []
        for raw in chunk:
            raw_id = int(raw["id"])
            body_digest = raw.get("body_digest") or _stable_digest(
                {"body": raw.get("原始正文", "")}
            )
            entity = self._reparse_raw_record(
                raw, rule, rule_label, int(replay_entry["id"]), schema_ver
            )
            reprocessed.append(entity)
            processed_ids.add(raw_id)
            replay_entry.setdefault("processed_digests", []).append(body_digest)
            read_sources.append(
                {
                    "raw_record_id": raw_id,
                    "批次编号": raw.get("批次编号"),
                    "raw_version": raw.get("raw_version", 1),
                    "body_digest": body_digest,
                }
            )

        replay_entry["processed_raw_ids"] = sorted(processed_ids)
        replay_entry["产出记录数"] = str(len(processed_ids))

        # ---- 中断：保留实际读取进度，等待接续 ----
        if interrupted:
            replay_entry["status"] = "重放中"
            replay_entry["重放状态"] = "重放中"
            replay_entry["pending"] = True
            replay_entry["resumed_at"] = _now_iso()
            return {
                "ok": True,
                "message": (
                    f"重放中断：已处理 {len(processed_ids)}/"
                    f"{replay_entry['total_in_scope']}，可凭 replay_id 接续"
                ),
                "replay": replay_entry,
                "interrupted": True,
                "replayed_count": len(reprocessed),
                "scope": scope,
                "read_sources": read_sources,
            }

        # ---- 完成：标准化明细的校验只读取范围内本轮重解析的明细 ----
        validation = self.validate_entities(
            batch_no=None,
            scope_batches=scope,
            replay_id=int(replay_entry["id"]),
        )

        # ---- 本轮结果回写血缘清单 ----
        lineage_entries = self._write_replay_lineage(
            replay_entry, scope, rule_label
        )

        failed = int(validation.get("failed", 0))
        final_status = "部分失败" if failed else "已完成"
        replay_entry["status"] = final_status
        replay_entry["重放状态"] = final_status
        replay_entry["pending"] = False
        replay_entry["abnormal"] = failed > 0
        replay_entry["completed_at"] = _now_iso()
        replay_entry["validation"] = validation
        replay_entry["lineage_ids"] = [str(x["id"]) for x in lineage_entries]

        return {
            "ok": True,
            "message": f"历史批次 {history_batch_range} 用规则 {rule_label} 重放完成",
            "replay": replay_entry,
            "replayed_count": len(reprocessed),
            "scope": scope,
            "read_sources": read_sources,
            "lineage": lineage_entries,
            "validation": validation,
            "interrupted": False,
        }

    # ---------- Schema 版本演进 ----------
    def evolve_schema(
        self,
        version: str,
        data_source: str,
        field_defs: list[str],
        backward_compatible: bool = True,
    ) -> dict[str, Any]:
        """发布新 Schema 版本并检查向后兼容性。"""
        schemas = store.rows("schema_version")

        # 如果已有同名版本，不能重复发布
        for s in schemas:
            if s.get("版本号") == version:
                return {"ok": False, "message": f"Schema 版本 {version} 已存在"}

        entry = {
            "id": _next_id("schema_version"),
            "版本号": version,
            "适用数据源": data_source,
            "字段定义": json.dumps(field_defs, ensure_ascii=False),
            "向后兼容标记": "是" if backward_compatible else "否",
            "发布说明": f"版本 {version} 发布，兼容={backward_compatible}",
            "status": "已发布",
            "pending": False,
            "abnormal": False,
            "published_at": _now_iso(),
            "field_count": len(field_defs),
        }
        schemas.append(entry)

        return {"ok": True, "message": f"Schema 版本 {version} 已发布", "schema": entry}

    # ---------- 全链路流水线 ----------
    def run_full_pipeline(
        self,
        source_id: str,
        time_window: str,
        raw_logs: list[dict[str, Any]],
        parser_rule: str | None = None,
    ) -> dict[str, Any]:
        """执行完整采集 → 解析 → 去重 → 校验 → 聚合 → 导出的全链路。"""
        steps: dict[str, Any] = {}

        # S0: 登记并验证数据源
        reg = self.register_source(source_id, "http", "default")
        if not reg["ok"]:
            return {"ok": False, "message": reg["message"]}
        self.validate_source(source_id)
        steps["source_registry"] = reg

        # S1: 启动采集批次
        batch_result = self.start_ingest_batch(source_id, time_window, raw_logs)
        if not batch_result["ok"]:
            return {"ok": False, "message": batch_result["message"]}
        steps["ingest_batch"] = batch_result
        batch_no = batch_result["batch"]["批次编号"]

        # S2: 字段解析
        parse_result = self.parse_batch(batch_no, parser_rule)
        steps["field_parser"] = parse_result

        # S3: 校验
        valid_result = self.validate_entities(batch_no)
        steps["validate"] = valid_result

        # S4: 去重
        dedup_result = self.run_dedup(["time", "level"], "batch", "keep_first")
        steps["dedup"] = dedup_result

        # S5: 聚合
        agg_result = self.refresh_aggregation(f"view-{batch_no}", "level", "time", "count")
        steps["aggregation"] = agg_result

        # S6: 导出
        export_result = self.build_export(f"view-{batch_no}", "csv")
        steps["export"] = export_result

        # S7: 水位推进
        wm_result = self.advance_watermark(source_id, batch_no, time_window)
        steps["watermark"] = wm_result

        return {
            "ok": True,
            "message": "全链路流水线执行完成",
            "batch_no": batch_no,
            "steps": steps,
        }

    # ---------- 失败恢复 ----------
    def recover_batch(self, batch_no: str) -> dict[str, Any]:
        """恢复失败的采集批次：重试解析失败记录。"""
        retry_q = store.rows("retry_queue")
        raw_records_list = store.rows("raw_records")

        failed_raw = [r for r in raw_records_list
                      if r.get("批次编号") == batch_no and r.get("status") == "解析失败"]

        recovered = 0
        still_failed = 0
        for raw in failed_raw:
            # 查找对应重试记录
            retry_entry = None
            for r in retry_q:
                if r.get("失败记录ID") == str(raw["id"]):
                    retry_entry = r
                    break
            if retry_entry:
                retry_count = int(retry_entry.get("重试次数", "0"))
                max_retry = int(retry_entry.get("最大重试", "3"))
                if retry_count >= max_retry:
                    retry_entry["status"] = "超过上限"
                    still_failed += 1
                    continue
                retry_entry["重试次数"] = str(retry_count + 1)
                retry_entry["status"] = "重试中"
                retry_entry["下次重试时间"] = _now_iso()

            # 重试解析
            body = raw.get("原始正文", "")
            parts = body.split("|") if "|" in body else [body]
            if len(parts) >= 2:
                raw["status"] = "待解析" if retry_entry else raw["status"]
                raw["解析状态"] = "待解析"
                raw["pending"] = True
                raw["abnormal"] = False
                recovered += 1
            else:
                raw["status"] = "解析失败"
                raw["abnormal"] = True
                still_failed += 1

        return {
            "ok": True,
            "message": f"批次 {batch_no} 恢复完成",
            "recovered": recovered,
            "still_failed": still_failed,
        }


pipeline = IngestPipeline()