"""回溯重放取数链路修复的业务行为测试。

覆盖修复要求：
1. 范围解析：单批号 / 逗号多批 / 闭区间，兼容已有批次号；不存在的批号报错且无残留。
2. 重放真用新规则重新解析范围内原文，形成范围内标准化明细，而非替换版本标签。
3. 重放校验只读取范围内的标准化明细；范围外已通过校验的记录不被改写，照常进入统计。
4. 本轮结果回写采集血缘清单，并可核对本次实际读取了哪份原文（正文指纹/版本）。
5. 历史原文在重放期间收到更正、与另一来源正常采集的校验并发时，只影响确认范围。
6. 重放中断后按实际读取范围接续，剩余正文仍被新规则真正解析一遍。
"""
from __future__ import annotations

import json

from app.store import store


def _register_parser(rule_name: str, fields: str, status: str = "已发布") -> None:
    parsers = store.rows("field_parser")
    parsers.append({
        "id": max((int(r.get("id", 0)) for r in parsers), default=0) + 1,
        "规则名称": rule_name,
        "适用数据源": "全部",
        "输入格式": "pipe",
        "输出字段列表": fields,
        "解析引擎": "split",
        "status": status,
        "pending": False,
        "abnormal": False,
    })


def _prepare_batch(pipeline, source: str, logs):
    pipeline.register_source(source, "http", "default")
    pipeline.validate_source(source)
    result = pipeline.start_ingest_batch(source, "2026Q1", logs)
    batch_no = result["batch"]["批次编号"]
    pipeline.parse_batch(batch_no)
    pipeline.validate_entities(batch_no)
    return batch_no


def _entities_of(batch_no: str):
    return [p for p in store.rows("parsed_entity") if p.get("来源批次") == batch_no]


class TestBatchScopeResolution:
    """重放范围解析与批号校验。"""

    def test_nonexistent_batch_reports_error(self, pipeline, sample_raw_logs):
        _prepare_batch(pipeline, "scope-src-a", sample_raw_logs[:2])
        before_replays = len(store.rows("replay_engine"))
        before_entities = len(store.rows("parsed_entity"))

        result = pipeline.replay_batch("INGE-9999", "new-rule-v2")

        assert result["ok"] is False
        assert "不存在" in result["message"]
        assert result["missing_batches"] == ["INGE-9999"]
        # 错误批号不允许产生重放任务或任何来源残留
        assert len(store.rows("replay_engine")) == before_replays
        assert len(store.rows("parsed_entity")) == before_entities

    def test_mixed_existing_and_missing_batch_is_rejected(self, pipeline, sample_raw_logs):
        batch_no = _prepare_batch(pipeline, "scope-src-b", sample_raw_logs[:1])
        before_replays = len(store.rows("replay_engine"))

        result = pipeline.replay_batch(f"{batch_no},INGE-8888", "new-rule-v2")

        assert result["ok"] is False
        assert "INGE-8888" in result["message"]
        assert len(store.rows("replay_engine")) == before_replays
        # 同批的已校验明细也不能被错误任务波及
        entity = _entities_of(batch_no)[0]
        assert entity["status"] == "校验通过"

    def test_range_expression_expands_to_real_batches(self, pipeline, sample_raw_logs):
        b1 = _prepare_batch(pipeline, "range-src-1", sample_raw_logs[:1])
        # 第二个批次需先把第一个批次标记完成，避免同源进行中冲突
        for b in store.rows("ingest_batch"):
            if b.get("批次编号") == b1:
                b["status"] = "已完成"
                b["pending"] = False
        b2 = _prepare_batch(pipeline, "range-src-2", sample_raw_logs[1:2])

        _register_parser("range-rule", "ts,level,msg")
        # INGE-000x 编号取实际批次号构造闭区间
        n1, n2 = b1.split("-")[1], b2.split("-")[1]
        lo, hi = sorted([n1, n2])
        expr = f"INGE-{lo}..INGE-{hi}"

        result = pipeline.replay_batch(expr, "range-rule")

        assert result["ok"] is True
        assert set(result["scope"]) == {b1, b2}
        assert result["replay"]["scope_batches"] == [b1, b2]

    def test_compatible_with_existing_batch_number_single(self, pipeline, sample_raw_logs):
        batch_no = _prepare_batch(pipeline, "compat-src", sample_raw_logs[:1])
        _register_parser("compat-rule", "ts,level,msg")

        result = pipeline.replay_batch(batch_no, "compat-rule")

        assert result["ok"] is True
        assert result["scope"] == [batch_no]


class TestRealReparsing:
    """重放必须真用新规则解析原文，而不是只替换版本标签。"""

    def test_replay_reparses_fields_with_new_rule(self, pipeline, sample_raw_logs):
        batch_no = _prepare_batch(pipeline, "reparse-src", sample_raw_logs[:2])
        old_entities = _entities_of(batch_no)
        old_fields = [json.loads(e["标准化字段集"]) for e in old_entities]
        assert set(old_fields[0].keys()) == {"time", "level", "message"}

        _register_parser("reparse-rule-v2", "ts,level,msg,host")
        result = pipeline.replay_batch(batch_no, "reparse-rule-v2")

        assert result["ok"] is True
        entities = _entities_of(batch_no)
        # 同键明细就地替换，不产生重复明细
        assert len(entities) == 2
        new_fields = json.loads(entities[0]["标准化字段集"])
        # 字段集真的按新规则重新解析了
        assert set(new_fields.keys()) == {"ts", "level", "msg", "host"}
        assert new_fields["ts"] == old_fields[0]["time"]
        assert new_fields["level"] == old_fields[0]["level"]
        assert all(e["解析规则版本"] == "reparse-rule-v2" for e in entities)
        assert all(str(e["replay_id"]) == str(result["replay"]["id"]) for e in entities)
        # 原文指纹随明细落盘，可核对读取来源
        assert all(e.get("source_digest") for e in entities)

    def test_replay_does_not_touch_out_of_scope_records(self, pipeline, sample_raw_logs):
        target_batch = _prepare_batch(pipeline, "target-src", sample_raw_logs[:2])
        other_batch = _prepare_batch(pipeline, "other-src", sample_raw_logs[2:4])

        other_before = [dict(e) for e in _entities_of(other_batch)]
        _register_parser("scoped-rule", "ts,level,msg")

        result = pipeline.replay_batch(target_batch, "scoped-rule")

        assert result["ok"] is True
        other_after = _entities_of(other_batch)
        for before, after in zip(other_before, other_after):
            # 范围外其他批次的校验结论与字段一个字节都不能变
            assert after["status"] == before["status"] == "校验通过"
            assert after["标准化字段集"] == before["标准化字段集"]
            assert after["解析规则版本"] == before["解析规则版本"]
            assert "replay_id" not in after

    def test_out_of_scope_passed_records_still_counted(self, pipeline, sample_raw_logs):
        target_batch = _prepare_batch(pipeline, "count-src-1", sample_raw_logs[:2])
        other_batch = _prepare_batch(pipeline, "count-src-2", sample_raw_logs[2:])
        _register_parser("count-rule", "ts,level,msg")

        passed_before = sum(
            1 for e in store.rows("parsed_entity") if e.get("status") == "校验通过"
        )
        pipeline.replay_batch(target_batch, "count-rule")
        passed_after = sum(
            1 for e in store.rows("parsed_entity") if e.get("status") == "校验通过"
        )

        # 范围外那条已通过校验的记录仍计入通过统计
        other_entities = _entities_of(other_batch)
        assert all(e["status"] == "校验通过" for e in other_entities)
        assert passed_after >= passed_before - 2  # 目标批2条回到待校验后重新通过
        target_entities = _entities_of(target_batch)
        assert all(e["status"] == "校验通过" for e in target_entities)

    def test_replay_validation_only_reads_in_scope_entities(self, pipeline, sample_raw_logs):
        target_batch = _prepare_batch(pipeline, "valid-scope-src", sample_raw_logs[:1])
        other_batch = _prepare_batch(pipeline, "valid-other-src", sample_raw_logs[1:2])
        _register_parser("valid-scope-rule", "ts,level,msg")

        result = pipeline.replay_batch(target_batch, "valid-scope-rule")

        # 校验只处理范围内 1 条，不会把范围外记录拉进来
        assert result["validation"]["passed"] == 1
        assert result["validation"]["failed"] == 0
        assert _entities_of(other_batch)[0]["status"] == "校验通过"


class TestLineageWriteback:
    """重放结果回写血缘清单。"""

    def test_lineage_entries_written_for_each_scope_batch(self, pipeline, sample_raw_logs):
        batch_no = _prepare_batch(pipeline, "lineage-src", sample_raw_logs[:2])
        _register_parser("lineage-rule", "ts,level,msg")
        before = len(store.rows("data_lineage"))

        result = pipeline.replay_batch(batch_no, "lineage-rule")

        lineages = store.rows("data_lineage")
        assert len(lineages) == before + 1
        entry = lineages[-1]
        assert entry["来源批次"] == batch_no
        assert entry["解析规则版本"] == "lineage-rule"
        assert entry["标准化版本"] == f"replay-{result['replay']['id']}"
        assert entry["status"] == "已记录"
        assert int(entry["record_count"]) == 2
        assert json.loads(entry["source_digests"])


class TestRawCorrectionAndConcurrency:
    """原文更正与并发正常采集只影响确认范围，并能核对读取的原文版本。"""

    def test_correction_before_replay_is_read_and_digest_tracked(self, pipeline, sample_raw_logs):
        batch_no = _prepare_batch(pipeline, "correct-src", sample_raw_logs[:1])
        raw = next(r for r in store.rows("raw_records") if r.get("批次编号") == batch_no)
        raw_id = raw["id"]

        correction = pipeline.correct_raw_record(
            batch_no, raw_id, "2026-01-15T11:00:00|INFO|corrected body"
        )
        assert correction["ok"] is True
        assert correction["raw_version"] == 2
        assert correction["body_digest"] != correction["previous_digest"]

        _register_parser("correct-rule", "ts,level,msg")
        result = pipeline.replay_batch(batch_no, "correct-rule")

        entity = _entities_of(batch_no)[0]
        fields = json.loads(entity["标准化字段集"])
        # 重放读到的是更正后的原文
        assert fields["ts"] == "2026-01-15T11:00:00"
        assert fields["msg"] == "corrected body"
        read = next(s for s in result["read_sources"] if s["raw_record_id"] == raw_id)
        assert read["body_digest"] == correction["body_digest"]
        assert read["raw_version"] == 2
        assert entity["source_digest"] == correction["body_digest"]

    def test_correct_nonexistent_batch_errors_without_residue(self, pipeline):
        result = pipeline.correct_raw_record("INGE-7777", 1, "x|y|z")
        assert result["ok"] is False
        assert "不存在" in result["message"]

    def test_concurrent_normal_ingestion_validation_isolated_from_replay(
        self, pipeline, sample_raw_logs
    ):
        # 历史批次 A（重放目标，原文随后被更正）
        batch_a = _prepare_batch(pipeline, "conc-hist-src", sample_raw_logs[:2])
        # 另一来源的正常采集批次 B 同步在跑校验
        batch_b = _prepare_batch(pipeline, "conc-normal-src", sample_raw_logs[2:4])

        # 历史批次 A 的原文收到更正
        raw_a = next(r for r in store.rows("raw_records") if r.get("批次编号") == batch_a)
        pipeline.correct_raw_record(
            batch_a, raw_a["id"], "2026-02-01T00:00:00|WARN|revised history"
        )

        _register_parser("conc-rule", "ts,level,msg")
        # 重放只确认 A 的范围
        replay_result = pipeline.replay_batch(batch_a, "conc-rule")
        # 与此同时批次 B 再做一次正常校验（无待校验记录时为空操作）
        normal_validation = pipeline.validate_entities(batch_b)

        assert replay_result["ok"] is True
        assert set(replay_result["scope"]) == {batch_a}
        # B 批次归属与校验结论完全不受重放影响
        b_entities = _entities_of(batch_b)
        assert all(e["来源批次"] == batch_b for e in b_entities)
        assert all(e["status"] == "校验通过" for e in b_entities)
        assert all("replay_id" not in e for e in b_entities)
        assert normal_validation["passed"] == 0
        # A 批次明细全部归属 A 且携带本次重放指纹
        a_entities = _entities_of(batch_a)
        assert all(e["来源批次"] == batch_a for e in a_entities)
        revised = next(
            e for e in a_entities if e["原始记录ID"] == str(raw_a["id"])
        )
        assert json.loads(revised["标准化字段集"])["msg"] == "revised history"

    def test_concurrent_replay_and_validation_keep_batch_ownership(
        self, pipeline, sample_raw_logs
    ):
        batch_a = _prepare_batch(pipeline, "own-src-a", sample_raw_logs[:2])
        batch_b = _prepare_batch(pipeline, "own-src-b", sample_raw_logs[2:])
        _register_parser("own-rule", "ts,level,msg")

        pipeline.replay_batch(batch_a, "own-rule")

        # 重放后两个批次的明细仍严格归属各自批号，无错位来源残留
        assert all(e["来源批次"] == batch_a for e in _entities_of(batch_a))
        assert all(e["来源批次"] == batch_b for e in _entities_of(batch_b))
        assert not any(
            e.get("replay_id") and e["来源批次"] != batch_a
            for e in store.rows("parsed_entity")
        )


class TestReplayInterruptionResume:
    """重放中断后接续实际读取范围，剩余正文真用新规则解析。"""

    def test_interrupted_replay_can_resume_remaining_scope(self, pipeline, sample_raw_logs):
        batch_no = _prepare_batch(pipeline, "resume-src", sample_raw_logs[:3])
        _register_parser("resume-rule", "ts,level,msg")

        first = pipeline.replay_batch(batch_no, "resume-rule", limit=1)
        assert first["ok"] is True
        assert first["interrupted"] is True
        replay_id = first["replay"]["id"]
        assert first["replay"]["status"] == "重放中"
        assert len(first["read_sources"]) == 1

        # 接续：处理剩余 2 条
        second = pipeline.replay_batch(
            batch_no, "resume-rule", resume_replay_id=replay_id
        )
        assert second["ok"] is True
        assert second["interrupted"] is False
        assert second["replayed_count"] == 2
        assert second["replay"]["status"] == "已完成"
        assert len(second["read_sources"]) == 2

        entities = _entities_of(batch_no)
        assert len(entities) == 3
        # 范围内正文全部经过新规则的真实解析
        for e in entities:
            fields = json.loads(e["标准化字段集"])
            assert set(fields.keys()) == {"ts", "level", "msg"}
            assert e["解析规则版本"] == "resume-rule"
            assert str(e["replay_id"]) == str(replay_id)
            assert e["status"] == "校验通过"
        # 实际读取范围 = 3 条原文，每段接续都记录了各自读取的原文指纹
        all_reads = first["read_sources"] + second["read_sources"]
        assert len(all_reads) == 3
        assert len(second["replay"]["processed_digests"]) == 3
        assert len({s["body_digest"] for s in all_reads}) == 3

    def test_resume_unknown_replay_id_errors(self, pipeline, sample_raw_logs):
        _prepare_batch(pipeline, "resume-ghost-src", sample_raw_logs[:1])
        result = pipeline.replay_batch(
            "INGE-0001", "any-rule", resume_replay_id=99999
        )
        assert result["ok"] is False
        assert "不存在" in result["message"]

    def test_resume_completed_replay_is_rejected(self, pipeline, sample_raw_logs):
        batch_no = _prepare_batch(pipeline, "resume-done-src", sample_raw_logs[:2])
        _register_parser("resume-done-rule", "ts,level,msg")
        done = pipeline.replay_batch(batch_no, "resume-done-rule")
        assert done["replay"]["status"] == "已完成"
        lineage_before = len(store.rows("data_lineage"))

        again = pipeline.replay_batch(
            batch_no, "resume-done-rule",
            resume_replay_id=done["replay"]["id"],
        )
        assert again["ok"] is False
        assert "不可接续" in again["message"]
        # 不会重复回写血缘
        assert len(store.rows("data_lineage")) == lineage_before

    def test_limit_zero_keeps_replay_interrupted(self, pipeline, sample_raw_logs):
        batch_no = _prepare_batch(pipeline, "limit-zero-src", sample_raw_logs[:2])
        _register_parser("limit-zero-rule", "ts,level,msg")

        first = pipeline.replay_batch(batch_no, "limit-zero-rule", limit=0)
        assert first["interrupted"] is True
        assert first["replayed_count"] == 0
        assert first["replay"]["status"] == "重放中"
        # 未读取任何原文，范围内明细保持原校验结论
        assert all(e["status"] == "校验通过" for e in _entities_of(batch_no))

        second = pipeline.replay_batch(
            batch_no, "limit-zero-rule",
            resume_replay_id=first["replay"]["id"],
        )
        assert second["interrupted"] is False
        assert second["replayed_count"] == 2
        assert all(e["status"] == "校验通过" for e in _entities_of(batch_no))

    def test_partial_failure_marks_replay_when_reparsed_entity_invalid(
        self, pipeline
    ):
        # 单字段正文在三字段规则下会产出空值，校验失败
        logs = [{"body": "only-one-field"}]
        batch_no = _prepare_batch(pipeline, "partial-src", logs)
        _register_parser("partial-rule", "ts,level,msg")

        result = pipeline.replay_batch(batch_no, "partial-rule")

        assert result["ok"] is True
        assert result["validation"]["failed"] == 1
        assert result["replay"]["status"] == "部分失败"
        assert result["replay"]["abnormal"] is True


class TestUnknownParserRule:
    """规则不存在时同样不能留下错误来源。"""

    def test_unknown_rule_without_published_fallback_rejected(
        self, pipeline, sample_raw_logs
    ):
        batch_no = _prepare_batch(pipeline, "rule-src", sample_raw_logs[:1])
        # 种子里存在已发布规则，先全部撤掉，制造无可用规则环境
        for p in store.rows("field_parser"):
            p["status"] = "草稿"
        before = len(store.rows("replay_engine"))

        result = pipeline.replay_batch(batch_no, "ghost-rule-v9")

        assert result["ok"] is False
        assert "解析规则" in result["message"]
        # 任务已回滚，无残留
        assert len(store.rows("replay_engine")) == before
        assert _entities_of(batch_no)[0]["status"] == "校验通过"
