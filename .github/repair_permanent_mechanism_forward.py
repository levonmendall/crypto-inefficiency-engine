from __future__ import annotations

from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    file = Path(path)
    text = file.read_text()
    if text.count(old) != 1:
        raise RuntimeError(f"expected exactly one match in {path}: {old[:80]!r}; found {text.count(old)}")
    file.write_text(text.replace(old, new, 1))


# 1) Keep the AllLaneEvidenceFactoryService public call contract unchanged. The
# production disposable factory toggles an internal ownership flag while the new
# permanent mechanism plane owns mutation and durable mechanism heartbeats.
path = Path("src/inefficiency_engine/all_lane_alpha_factory.py")
text = path.read_text()
method_start = text.index("    async def run_evidence_cycle(self, *, total_capital_usd: float | None = None):")
mechanism_start = text.index(
    "        try:\n            mechanism = await self.mechanism_execution.run_evidence_cycle(",
    method_start,
)
return_start = text.index("        return alpha", mechanism_start)
mechanism_block = text[mechanism_start:return_start]
indented = "".join(("    " + line) if line.strip() else line for line in mechanism_block.splitlines(True))
text = (
    text[:mechanism_start]
    + '        if getattr(self, "_mechanism_evidence_enabled", True):\n'
    + indented
    + text[return_start:]
)
path.write_text(text)

# 2) Production disposable alpha research keeps discovery/funnel publication and its
# bounded L2 snapshot, but temporarily disables the mechanism mutation section of its
# parent cycle. No caller-visible method signature changes.
replace_once(
    "src/inefficiency_engine/disposable_alpha_factory.py",
    "        try:\n            return await super().run_evidence_cycle(\n                total_capital_usd=total_capital_usd\n            )\n        finally:\n            self.core.collect_live_evidence = original_evidence\n",
    "        mechanism_evidence_enabled = getattr(self, \"_mechanism_evidence_enabled\", True)\n        self._mechanism_evidence_enabled = False\n        try:\n            return await super().run_evidence_cycle(\n                total_capital_usd=total_capital_usd\n            )\n        finally:\n            self._mechanism_evidence_enabled = mechanism_evidence_enabled\n            self.core.collect_live_evidence = original_evidence\n",
)
replace_once(
    "src/inefficiency_engine/disposable_alpha_factory.py",
    "    disposable research process. It does, however, run the executable alpha\n    refinements and all five native mechanism forward loops. Mechanism outcomes are\n    also fed into the Release D subtractive lane-success calibration plane.\n",
    "    disposable research process. It runs executable alpha refinements and the\n    bounded L2 sampler, while native mechanism-forward mutation is owned by the\n    permanent mechanism worker. Durable mechanism outcomes remain available to the\n    Release D subtractive lane-success calibration plane.\n",
)
replace_once(
    "src/inefficiency_engine/disposable_alpha_factory.py",
    "        \"\"\"Run alpha + native mechanisms against one independent bounded L2 snapshot.\n",
    "        \"\"\"Run disposable alpha research against one independent bounded L2 snapshot.\n",
)

# 3) Add the permanent mechanism plane to the combined runtime and isolate its restart
# semantics just like the permanent source plane.
replace_once(
    "src/inefficiency_engine/render_combined_runtime.py",
    '        "source": [sys.executable, "-m", "inefficiency_engine.permanent_source_worker"],\n        "api": [',
    '        "source": [sys.executable, "-m", "inefficiency_engine.permanent_source_worker"],\n        "mechanism": [sys.executable, "-m", "inefficiency_engine.permanent_mechanism_worker"],\n        "api": [',
)
replace_once(
    "src/inefficiency_engine/render_combined_runtime.py",
    '        for name in ("portfolio", "source", "api"):\n',
    '        for name in ("portfolio", "source", "mechanism", "api"):\n',
)
replace_once(
    "src/inefficiency_engine/render_combined_runtime.py",
    '                if name == "source":\n                    print(\n                        f"isolated source child exited code={return_code}; restarting source only",\n                        flush=True,\n                    )\n                    _start_permanent("source")\n                    continue\n',
    '                if name in {"source", "mechanism"}:\n                    print(\n                        f"isolated {name} child exited code={return_code}; restarting {name} only",\n                        flush=True,\n                    )\n                    _start_permanent(name)\n                    continue\n',
)

# 4) Recovery ownership: a stale mechanism heartbeat must no longer force a disposable
# alpha cycle that cannot repair it. Preserve the aggregate diagnostic while exposing
# mechanism recovery separately for the permanent plane.
replace_once(
    "src/inefficiency_engine/critical_evidence_recovery.py",
    '    alpha_required = bool(\n        workers["alpha_l2_sampling"].get("recovery_required")\n        or workers["mechanism_forward"].get("recovery_required")\n    )\n',
    '    alpha_required = bool(workers["alpha_l2_sampling"].get("recovery_required"))\n    mechanism_required = bool(workers["mechanism_forward"].get("recovery_required"))\n',
)
replace_once(
    "src/inefficiency_engine/critical_evidence_recovery.py",
    '        "source_refresh_required": source_required,\n        "alpha_forward_required": alpha_required,\n        "any_required": source_required or alpha_required,\n',
    '        "source_refresh_required": source_required,\n        "alpha_forward_required": alpha_required,\n        "mechanism_forward_required": mechanism_required,\n        "any_required": source_required or alpha_required or mechanism_required,\n',
)

# 5) Permanent mechanism-forward worker. It consumes the permanent source plane,
# performs the existing bounded L2 sampling path, then runs the same governed
# yield-shadow mechanism execution service independent of disposable research life.
Path("src/inefficiency_engine/permanent_mechanism_worker.py").write_text('''from __future__ import annotations\n\nimport asyncio\nimport gc\nimport os\nimport time\n\nfrom inefficiency_engine.config import Settings\nfrom inefficiency_engine.critical_evidence_recovery import MECHANISM_FORWARD_WORKER_ID\nfrom inefficiency_engine.disposable_alpha_factory import DisposableExpandedAlphaFactoryService\nfrom inefficiency_engine.evidence import build_evidence_store\nfrom inefficiency_engine.service import OpportunityService\nfrom inefficiency_engine.source_runtime_safety import (\n    install_bulk_provider_catalog_runtime,\n    install_research_source_delegation,\n    install_source_coverage_reconciliation_runtime,\n)\n\n\nDEFAULT_MECHANISM_FORWARD_INTERVAL_SECONDS = 30.0\n\n\ndef _interval_seconds() -> float:\n    try:\n        value = float(\n            os.getenv(\n                "CIE_MECHANISM_FORWARD_INTERVAL_SECONDS",\n                str(DEFAULT_MECHANISM_FORWARD_INTERVAL_SECONDS),\n            )\n        )\n    except ValueError:\n        value = DEFAULT_MECHANISM_FORWARD_INTERVAL_SECONDS\n    return max(5.0, value)\n\n\ndef mechanism_forward_funnel(execution, cycle) -> dict[str, object]:\n    readiness = execution.readiness_summary()\n    rows = [row for row in readiness.values() if isinstance(row, dict)]\n    return {\n        "mechanism_count": len(rows),\n        "current_spec_count": int(cycle.current_specs),\n        "trials_recorded": int(cycle.trials_recorded),\n        "outcomes_matured": int(cycle.outcomes_matured),\n        "forward_outcome_count": sum(int(row.get("forward_outcome_count") or 0) for row in rows),\n        "incremental_qualified_cohort_count": sum(\n            int(row.get("incremental_qualified_cohort_count") or 0) for row in rows\n        ),\n        "full_qualified_cohort_count": sum(\n            int(row.get("full_qualified_cohort_count") or 0) for row in rows\n        ),\n        "currently_qualified_mechanism_count": sum(\n            1 for row in rows if bool(row.get("currently_qualified"))\n        ),\n        "current_promoted_candidate_count": sum(\n            int(row.get("current_promoted_candidate_count") or 0) for row in rows\n        ),\n        "cycle_promoted_candidate_count": int(cycle.promoted_candidates),\n        "by_mechanism": readiness,\n    }\n\n\nasync def _run() -> None:\n    settings = Settings.from_env()\n    store = build_evidence_store(settings.evidence_db_path)\n    if store is None:\n        raise RuntimeError("permanent mechanism-forward worker requires durable evidence persistence")\n\n    install_bulk_provider_catalog_runtime()\n    install_source_coverage_reconciliation_runtime()\n    install_research_source_delegation()\n    service = OpportunityService(settings=settings, evidence_store=store)\n    factory = DisposableExpandedAlphaFactoryService(service, store)\n    execution = factory.mechanism_execution\n    interval = _interval_seconds()\n    sequence = 0\n\n    while True:\n        sequence += 1\n        started = time.monotonic()\n        try:\n            store.record_worker_heartbeat(\n                worker_id=MECHANISM_FORWARD_WORKER_ID,\n                state="running",\n                detail={\n                    "sequence": sequence,\n                    "permanent_process": True,\n                    "runtime_plane": "mechanism-forward",\n                    "allocation_authority": False,\n                    "paper_only": True,\n                },\n            )\n\n            original_evidence = service.collect_live_evidence\n            original_executability = getattr(service, "collect_live_executability", None)\n            snapshot = await factory.refresh_l2_source_snapshot(original_evidence)\n\n            async def cached_snapshot():\n                return snapshot\n\n            service.collect_live_evidence = cached_snapshot\n            if original_executability is not None:\n                service.collect_live_executability = cached_snapshot\n            try:\n                cycle = await execution.run_evidence_cycle()\n            finally:\n                service.collect_live_evidence = original_evidence\n                if original_executability is not None:\n                    service.collect_live_executability = original_executability\n\n            funnel = mechanism_forward_funnel(execution, cycle)\n            store.record_worker_heartbeat(\n                worker_id=MECHANISM_FORWARD_WORKER_ID,\n                state="success",\n                detail={\n                    "sequence": sequence,\n                    **funnel,\n                    "funnel_telemetry": True,\n                    "permanent_process": True,\n                    "runtime_plane": "mechanism-forward",\n                    "disposable_research_dependency": False,\n                    "qualification_thresholds_unchanged": True,\n                    "allocation_authority": False,\n                    "paper_only": True,\n                    "live_execution_authority": False,\n                },\n            )\n            sleep_seconds = max(0.25, interval - (time.monotonic() - started))\n        except Exception as exc:\n            try:\n                store.record_worker_heartbeat(\n                    worker_id=MECHANISM_FORWARD_WORKER_ID,\n                    state="error",\n                    error_type=type(exc).__name__,\n                    detail={\n                        "sequence": sequence,\n                        "message": str(exc)[:500],\n                        "permanent_process": True,\n                        "runtime_plane": "mechanism-forward",\n                        "disposable_research_dependency": False,\n                        "qualification_thresholds_unchanged": True,\n                        "allocation_authority": False,\n                        "paper_only": True,\n                        "live_execution_authority": False,\n                    },\n                )\n            except Exception:\n                pass\n            sleep_seconds = max(1.0, float(settings.worker_error_backoff_seconds))\n        gc.collect()\n        await asyncio.sleep(sleep_seconds)\n\n\ndef main() -> int:\n    asyncio.run(_run())\n    return 0\n\n\nif __name__ == "__main__":\n    raise SystemExit(main())\n''')

# 6) Regression coverage for permanent runtime ownership, recovery routing, telemetry,
# and backward-compatible disposable research behavior.
path = Path("tests/test_render_combined.py")
text = path.read_text()
text = text.replace(
    'def test_combined_runtime_keeps_portfolio_source_and_api_permanent():\n',
    'def test_combined_runtime_keeps_portfolio_source_mechanism_and_api_permanent():\n',
    1,
)
text = text.replace(
    '    assert set(commands) == {"portfolio", "source", "api"}\n',
    '    assert set(commands) == {"portfolio", "source", "mechanism", "api"}\n',
    1,
)
needle = '''    assert commands["source"] == [\n        sys.executable,\n        "-m",\n        "inefficiency_engine.permanent_source_worker",\n    ]\n'''
if text.count(needle) != 1:
    raise RuntimeError("render combined source command assertion changed unexpectedly")
text = text.replace(
    needle,
    needle + '''    assert commands["mechanism"] == [\n        sys.executable,\n        "-m",\n        "inefficiency_engine.permanent_mechanism_worker",\n    ]\n''',
    1,
)
path.write_text(text)

replace_once(
    "tests/test_disposable_runtime_contract.py",
    '    assert permanent == {"portfolio", "source", "api"}\n',
    '    assert permanent == {"portfolio", "source", "mechanism", "api"}\n',
)

path = Path("tests/test_critical_evidence_recovery.py")
text = path.read_text()
text = text.replace(
    '    assert status["alpha_forward_required"] is True\n    assert status["any_required"] is True\n',
    '    assert status["alpha_forward_required"] is True\n    assert status["mechanism_forward_required"] is True\n    assert status["any_required"] is True\n',
    1,
)
insert_before = '\ndef test_fresh_degraded_heartbeat_suppresses_immediate_retry():\n'
new_test = '''\ndef test_stale_mechanism_worker_does_not_force_disposable_alpha_recovery():\n    store = FakeStore(\n        {\n            SOURCE_REFRESH_WORKER_ID: _heartbeat(age_seconds=60),\n            ALPHA_L2_WORKER_ID: _heartbeat(age_seconds=60),\n            MECHANISM_FORWARD_WORKER_ID: _heartbeat(age_seconds=181),\n        }\n    )\n\n    status = critical_evidence_recovery_status(store, now=NOW)\n\n    assert status["source_refresh_required"] is False\n    assert status["alpha_forward_required"] is False\n    assert status["mechanism_forward_required"] is True\n    assert status["any_required"] is True\n\n'''
if text.count(insert_before) != 1:
    raise RuntimeError("critical recovery insertion point changed unexpectedly")
text = text.replace(insert_before, new_test + insert_before, 1)
path.write_text(text)

path = Path("tests/test_disposable_alpha_factory.py")
text = path.read_text()
text += '''\n\ndef test_disposable_alpha_factory_delegates_mechanism_mutation_to_permanent_worker():\n    source = inspect.getsource(DisposableExpandedAlphaFactoryService.run_evidence_cycle)\n\n    assert "_mechanism_evidence_enabled = False" in source\n'''
path.write_text(text)

Path("tests/test_permanent_mechanism_worker.py").write_text('''from __future__ import annotations\n\nfrom types import SimpleNamespace\n\nfrom inefficiency_engine.permanent_mechanism_worker import mechanism_forward_funnel\n\n\nclass FakeExecution:\n    def readiness_summary(self):\n        return {\n            "maker_rebate": {\n                "forward_outcome_count": 11,\n                "incremental_qualified_cohort_count": 1,\n                "full_qualified_cohort_count": 0,\n                "currently_qualified": True,\n                "current_promoted_candidate_count": 1,\n            },\n            "liquidation": {\n                "forward_outcome_count": 7,\n                "incremental_qualified_cohort_count": 0,\n                "full_qualified_cohort_count": 1,\n                "currently_qualified": True,\n                "current_promoted_candidate_count": 2,\n            },\n        }\n\n\ndef test_mechanism_forward_funnel_reports_durable_qualification_progress():\n    cycle = SimpleNamespace(\n        current_specs=5,\n        trials_recorded=3,\n        outcomes_matured=2,\n        promoted_candidates=3,\n    )\n\n    funnel = mechanism_forward_funnel(FakeExecution(), cycle)\n\n    assert funnel["mechanism_count"] == 2\n    assert funnel["forward_outcome_count"] == 18\n    assert funnel["incremental_qualified_cohort_count"] == 1\n    assert funnel["full_qualified_cohort_count"] == 1\n    assert funnel["currently_qualified_mechanism_count"] == 2\n    assert funnel["current_promoted_candidate_count"] == 3\n    assert funnel["cycle_promoted_candidate_count"] == 3\n''')

# Remove one-shot repair machinery from the final branch diff after validation inputs
# have been materialized. The running workflow already has its definition loaded.
Path(".github/repair_permanent_mechanism_forward.py").unlink()
Path(".github/workflows/repair-permanent-mechanism.yml").unlink()
