from __future__ import annotations

# Backward-compatible command alias only.  API selection now lives in the stable
# render_combined entrypoint, so either historical Render command launches the
# same canonical dashboard contract.
from inefficiency_engine.render_combined import API_APP, main


if __name__ == "__main__":
    raise SystemExit(main())
