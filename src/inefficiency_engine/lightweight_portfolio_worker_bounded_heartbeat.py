from __future__ import annotations

from inefficiency_engine.bounded_heartbeat_runtime import install_bounded_heartbeat_runtime


# The canonical portfolio process owns both compact portfolio and lightweight research
# projection publication. Bound only its durable heartbeat reads; provider acquisition,
# qualification thresholds, allocation rules, and paper-only authority remain unchanged.
install_bounded_heartbeat_runtime()

from inefficiency_engine import lightweight_portfolio_worker as base  # noqa: E402
from inefficiency_engine.portfolio_operational_recovery_runtime import (  # noqa: E402
    install_portfolio_operational_recovery_runtime,
)
from inefficiency_engine.research_projection_recovery_runtime import (  # noqa: E402
    install_research_projection_recovery_runtime,
)


# One transient DB/schema-inspection failure must not permanently kill the independent
# presentation publisher while the portfolio and research workers continue running.
install_research_projection_recovery_runtime()
# A canonical cycle that receives a SQLAlchemy OperationalError remains failed and is
# never replayed automatically. Recycle only the affected connection pool so the next
# normal cycle cannot inherit a dead PostgreSQL connection.
install_portfolio_operational_recovery_runtime(
    base.PersistedSourceCanonicalPaperPortfolioService
)
main = base.main


if __name__ == "__main__":
    raise SystemExit(main())
