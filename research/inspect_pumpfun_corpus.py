#!/usr/bin/env python3
"""Inspect one independent Pump.fun corpus trade shard before multi-day replay."""
from __future__ import annotations

import json
from pathlib import Path

import duckdb
from huggingface_hub import hf_hub_download

REPO = "Slinky21/Pumpfun_Memecoin_Corpus"
SYSTEM_WALLET = "BwWK17cbHxwWBKZkUYvzxLcNQ1YVyaFezduWbtm2de6s"


def main() -> int:
    root = Path("/tmp/pumpfun-corpus-inspect")
    root.mkdir(parents=True, exist_ok=True)
    trade = Path(hf_hub_download(
        repo_id=REPO,
        repo_type="dataset",
        filename="trades/trades-00000.parquet",
        local_dir=root,
    ))
    token = Path(hf_hub_download(
        repo_id=REPO,
        repo_type="dataset",
        filename="tokens.parquet",
        local_dir=root,
    ))
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    schema = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{trade}')").fetchall()
    token_schema = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{token}')").fetchall()
    cols = [r[0] for r in schema]
    time_candidates = [c for c in cols if any(x in c.lower() for x in ("time", "date", "created", "timestamp"))]
    summary = {
        "trade_file": str(trade),
        "trade_size": trade.stat().st_size,
        "trade_rows": con.execute(f"SELECT count(*) FROM read_parquet('{trade}')").fetchone()[0],
        "trade_schema": schema,
        "token_schema": token_schema,
        "time_candidates": time_candidates,
        "sample": con.execute(f"SELECT * FROM read_parquet('{trade}') LIMIT 3").fetchdf().astype(str).to_dict(orient="records"),
    }
    for col in time_candidates:
        try:
            summary[f"range_{col}"] = con.execute(
                f"SELECT min({col}), max({col}) FROM read_parquet('{trade}')"
            ).fetchone()
        except Exception as exc:
            summary[f"range_{col}_error"] = str(exc)
    if "user_wallet" in cols:
        summary["system_rows"] = con.execute(
            f"SELECT count(*) FROM read_parquet('{trade}') WHERE user_wallet=?", [SYSTEM_WALLET]
        ).fetchone()[0]
    Path("pumpfun-corpus-schema.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
