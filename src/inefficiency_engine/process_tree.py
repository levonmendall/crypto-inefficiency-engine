from __future__ import annotations

import os
import signal
from typing import Any


def subprocess_group_kwargs() -> dict[str, object]:
    """Return subprocess kwargs that isolate descendants behind one killable group."""

    return {"start_new_session": True} if os.name == "posix" else {}


def _direct_process_alive(process: Any) -> bool:
    poll = getattr(process, "poll", None)
    if callable(poll):
        return poll() is None
    return getattr(process, "returncode", None) is None


def process_tree_alive(process: Any) -> bool:
    """Return whether the direct child or any member of its isolated group remains."""

    direct_alive = _direct_process_alive(process)
    if os.name != "posix":
        return direct_alive
    try:
        os.killpg(int(process.pid), 0)
    except ProcessLookupError:
        return direct_alive
    except PermissionError:
        return True
    return True


def signal_process_tree(process: Any, sig: signal.Signals) -> bool:
    """Signal the entire isolated subprocess group, with a direct-child fallback."""

    if os.name == "posix":
        try:
            os.killpg(int(process.pid), sig)
            return True
        except ProcessLookupError:
            return False
        except OSError:
            pass

    if not _direct_process_alive(process):
        return False
    if sig == signal.SIGKILL:
        process.kill()
    else:
        process.terminate()
    return True
