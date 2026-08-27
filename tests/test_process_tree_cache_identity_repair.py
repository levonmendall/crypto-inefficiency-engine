from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from pydantic import BaseModel
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine

from inefficiency_engine import bounded_control_evidence_runtime as bounded_control
from inefficiency_engine.control_cycle_runtime import ControlExecutorSupervisor
from inefficiency_engine.portfolio_stage_isolation import (
    PortfolioStageTimeout,
    run_stage_subprocess,
)


class _OutcomeA(BaseModel):
    value: int


class _OutcomeB(BaseModel):
    value: int


def _history_table(metadata: MetaData, *, extra_column: bool = False) -> Table:
    columns = [
        Column("id", Integer, primary_key=True),
        Column("payload_json", String, nullable=False),
    ]
    if extra_column:
        columns.append(Column("source", String, nullable=True))
    return Table("unit_outcome_history", metadata, *columns)


def _pid_still_running(pid: int) -> bool:
    proc_stat = Path(f"/proc/{pid}/stat")
    try:
        fields = proc_stat.read_text(encoding="utf-8").split()
    except FileNotFoundError:
        return False
    except OSError:
        fields = []
    if len(fields) >= 3:
        return fields[2] != "Z"
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _wait_for_process_exit(pid: int, *, timeout_seconds: float = 2.0) -> bool:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if not _pid_still_running(pid):
            return True
        time.sleep(0.01)
    return not _pid_still_running(pid)


def test_bounded_control_cache_identity_is_stable_and_structural():
    first = _history_table(MetaData())
    equivalent = _history_table(MetaData())
    changed = _history_table(MetaData(), extra_column=True)

    first_identity = bounded_control._structural_cache_identity(first, _OutcomeA)

    assert first_identity == bounded_control._structural_cache_identity(
        equivalent,
        _OutcomeA,
    )
    assert first_identity != bounded_control._structural_cache_identity(
        first,
        _OutcomeB,
    )
    assert first_identity != bounded_control._structural_cache_identity(
        changed,
        _OutcomeA,
    )


def test_legacy_checkpoint_is_migrated_without_restarting_history(monkeypatch):
    bounded_control._CACHE.clear()
    engine = create_engine("sqlite+pysqlite:///:memory:")
    table = _history_table(MetaData())
    table.create(engine)
    with engine.begin() as db:
        db.execute(table.insert().values(id=1, payload_json='{"value":7}'))

    loaded_keys: list[str] = []
    saved: list[tuple[str, dict[str, object], bool]] = []
    legacy_checkpoint = {
        "tail": 1,
        "target_tail": 1,
        "rows": ['{"value":7}'],
        "bootstrap_complete": True,
    }

    def load_checkpoint(_store, *, cache_key: str):
        loaded_keys.append(cache_key)
        if cache_key == "outcome-history:unit_outcome_history":
            return legacy_checkpoint
        return None

    def save_checkpoint(_store, *, cache_key: str, payload, complete: bool):
        saved.append((cache_key, dict(payload), bool(complete)))
        return True

    monkeypatch.setattr(
        bounded_control,
        "load_control_cache_checkpoint",
        load_checkpoint,
    )
    monkeypatch.setattr(
        bounded_control,
        "save_control_cache_checkpoint",
        save_checkpoint,
    )

    ledger = SimpleNamespace(store=SimpleNamespace(engine=engine))
    rows = bounded_control._refresh_rows(ledger, table, _OutcomeA)

    assert [row.value for row in rows] == [7]
    assert loaded_keys[0].startswith("outcome-history:v2:unit_outcome_history:")
    assert loaded_keys[1] == "outcome-history:unit_outcome_history"
    assert len(saved) == 1
    assert saved[0][0] == loaded_keys[0]
    assert saved[0][1]["tail"] == 1
    assert saved[0][2] is True
    diagnostics = bounded_control.bounded_control_outcome_cache_diagnostics()
    assert diagnostics["tables"]["unit_outcome_history"][
        "durable_checkpoint_migrated"
    ] is True


@pytest.mark.skipif(os.name != "posix", reason="process-group regression requires POSIX")
def test_control_deadline_terminates_executor_descendants(tmp_path: Path):
    descendant_pid_path = tmp_path / "control-descendant.pid"
    child_source = (
        "import os,pathlib,subprocess,sys,time; "
        "child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(3600)']); "
        "pathlib.Path(os.environ['DESCENDANT_PID_PATH']).write_text(str(child.pid)); "
        "time.sleep(3600)"
    )
    supervisor = ControlExecutorSupervisor(
        deadline_seconds=0.2,
        heartbeat_interval_seconds=0.02,
        terminate_grace_seconds=0.05,
        workspace=tmp_path,
    )

    result = supervisor.run_cycle(
        sequence=1,
        command=[sys.executable, "-c", child_source],
        environment={"DESCENDANT_PID_PATH": str(descendant_pid_path)},
    )

    descendant_pid = int(descendant_pid_path.read_text(encoding="utf-8"))
    assert result.error_type == "ControlExecutorDeadlineExceeded"
    assert result.executor_terminated is True
    assert _wait_for_process_exit(descendant_pid)


@pytest.mark.asyncio
@pytest.mark.skipif(os.name != "posix", reason="process-group regression requires POSIX")
async def test_portfolio_stage_timeout_terminates_descendants(tmp_path: Path):
    descendant_pid_path = tmp_path / "stage-descendant.pid"
    child_source = (
        "import os,pathlib,subprocess,sys,time; "
        "child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(3600)']); "
        "pathlib.Path(os.environ['DESCENDANT_PID_PATH']).write_text(str(child.pid)); "
        "time.sleep(3600)"
    )

    with pytest.raises(PortfolioStageTimeout):
        await run_stage_subprocess(
            [sys.executable, "-c", child_source],
            stage_name="process-tree-regression",
            timeout_seconds=0.2,
            env={"DESCENDANT_PID_PATH": str(descendant_pid_path)},
        )

    descendant_pid = int(descendant_pid_path.read_text(encoding="utf-8"))
    assert _wait_for_process_exit(descendant_pid)
