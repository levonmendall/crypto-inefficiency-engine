from __future__ import annotations

from inefficiency_engine.bounded_heartbeat_runtime import install_bounded_heartbeat_runtime


# The canonical portfolio process owns both compact portfolio and lightweight research
# projection publication. Bound only its durable heartbeat reads; provider acquisition,
# qualification thresholds, allocation rules, and paper-only authority remain unchanged.
install_bounded_heartbeat_runtime()

from inefficiency_engine.lightweight_portfolio_worker import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
