from __future__ import annotations

import asyncio

from inefficiency_engine import disposable_heavy_job as base
from inefficiency_engine.research_observability_runtime_repair import (
    install_research_observability_runtime_repair,
)


def main() -> int:
    install_research_observability_runtime_repair()
    return asyncio.run(base._run("research"))


if __name__ == "__main__":
    raise SystemExit(main())
