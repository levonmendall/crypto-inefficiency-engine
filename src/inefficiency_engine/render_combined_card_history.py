from __future__ import annotations

from inefficiency_engine import render_combined as _base


_base.API_APP = "inefficiency_engine.read_api_card_history_deploy:app"


def main() -> int:
    return _base.main()


if __name__ == "__main__":
    raise SystemExit(main())
