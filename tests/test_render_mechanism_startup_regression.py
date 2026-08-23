from __future__ import annotations

import inspect
import sys

from inefficiency_engine import mechanism_supervision_slot, render_combined, render_combined_runtime


def test_supervised_runtime_preserves_every_hardcoded_inner_startup_slot():
    commands = render_combined.supervised_runtime_child_commands("12345")
    required = ("portfolio", "source", "mechanism", "api")

    # The legacy inner runtime still starts these names explicitly. Removing the
    # mechanism key makes Render fail immediately with KeyError before the API binds.
    assert all(name in commands for name in required)
    assert commands["mechanism"] == [
        sys.executable,
        "-m",
        "inefficiency_engine.mechanism_supervision_slot",
    ]
    assert commands["mechanism"] != render_combined.mechanism_child_command()

    runtime_source = inspect.getsource(render_combined_runtime.main)
    assert 'for name in ("portfolio", "source", "mechanism", "api")' in runtime_source


def test_inner_mechanism_slot_is_inert_and_has_no_engine_authority():
    source = inspect.getsource(mechanism_supervision_slot)

    assert "permanent_mechanism_worker" not in source
    assert "OpportunityService" not in source
    assert "EvidenceStore" not in source
    assert "allocation" in source
    assert "execution" in source
