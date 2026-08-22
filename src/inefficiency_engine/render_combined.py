from __future__ import annotations

# The canonical Render entrypoint owns the API application selection.  The large
# supervisor implementation lives in a private module so both the stable command
# and the former compatibility command execute exactly the same runtime.
from inefficiency_engine import render_combined_runtime as _runtime


CANONICAL_API_APP = "inefficiency_engine.read_api_card_history_deploy:app"
_runtime.API_APP = CANONICAL_API_APP

# Preserve the public helper surface used by tests and operational tooling while
# making the production API target impossible to depend on a Render command swap.
from inefficiency_engine.render_combined_runtime import *  # noqa: E402,F401,F403

API_APP = CANONICAL_API_APP


if __name__ == "__main__":
    raise SystemExit(main())
