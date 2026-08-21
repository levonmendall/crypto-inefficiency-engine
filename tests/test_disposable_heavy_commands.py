import sys

from inefficiency_engine.render_combined import heavy_commands


def test_heavy_jobs_are_explicit_one_shot_subprocess_commands():
    commands = heavy_commands()
    assert commands == {
        "research": [sys.executable, "-m", "inefficiency_engine.disposable_heavy_job", "research"],
        "history": [sys.executable, "-m", "inefficiency_engine.disposable_heavy_job", "history"],
    }
