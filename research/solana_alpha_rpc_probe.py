from __future__ import annotations

import datetime as dt
import json
import time
import urllib.error
import urllib.request

RPC_URL = "https://api.mainnet-beta.solana.com"
START = int(dt.datetime(2026, 8, 2, 19, 45, tzinfo=dt.timezone.utc).timestamp())
END = int(dt.datetime(2026, 9, 1, 19, 45, tzinfo=dt.timezone.utc).timestamp())
WALLETS = {
    # Fixed before the August backtest from the previously identified elite cohort.
    "cented": "CyaE1VxvBrahnPWkqm5VsdCvyS2QmNht2UFrKJHga54o",
    "cupsey": "2fg5QD1eD7rzNNCsvnhmXFm5hqNgwTTG8p7kQ6f3rx6f",
    "decu": "4vw54BmAogeRV3vPKWyFet5yf8DTLcREzdSzx4rw9Ud9",
    "kadenox": "B32QbbdDAyhvUQzjcaM5j6ZVKwjCxAwGH5Xgvb9SJqnC",
    "theo": "Bi4rd5FH5bYEN8scZ7wevxNZyNmKHdaBcvewdPFxYdLt",
    "jijo": "4BdKaxN8G6ka4GYtQQWk4G4dZRUTX2vQH9GcXdBREFUk",
    "kev": "BTf4A2exGK9BCVDNzy65b9dUzXgMqB4weVkvTMFQsadd",
}


def rpc(method: str, params: list[object], *, retries: int = 12) -> object:
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    for attempt in range(retries):
        req = urllib.request.Request(
            RPC_URL,
            data=payload,
            headers={"Content-Type": "application/json", "User-Agent": "cie-solana-alpha-certification/1"},
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                body = json.loads(response.read())
            if "error" in body:
                raise RuntimeError(f"rpc {method} error: {body['error']}")
            return body.get("result")
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, RuntimeError) as exc:
            if attempt + 1 >= retries:
                raise
            delay = min(20.0, 0.75 * (2 ** min(attempt, 5)))
            print(f"retry method={method} attempt={attempt + 1} delay={delay:.2f}s error={exc}", flush=True)
            time.sleep(delay)
    raise AssertionError("unreachable")


def scan_wallet(name: str, address: str) -> dict[str, object]:
    before: str | None = None
    in_window = 0
    pages = 0
    newest: int | None = None
    oldest: int | None = None
    reached_start = False
    null_block_time = 0
    failed = 0
    while pages < 250:
        config: dict[str, object] = {"limit": 1000, "commitment": "confirmed"}
        if before:
            config["before"] = before
        rows = rpc("getSignaturesForAddress", [address, config])
        if not isinstance(rows, list) or not rows:
            break
        pages += 1
        stop = False
        for row in rows:
            if not isinstance(row, dict):
                continue
            block_time = row.get("blockTime")
            if block_time is None:
                null_block_time += 1
                continue
            block_time = int(block_time)
            newest = block_time if newest is None else max(newest, block_time)
            oldest = block_time if oldest is None else min(oldest, block_time)
            if block_time < START:
                reached_start = True
                stop = True
                break
            if block_time <= END:
                in_window += 1
                if row.get("err") is not None:
                    failed += 1
        before = rows[-1].get("signature") if isinstance(rows[-1], dict) else None
        print(
            f"wallet={name} page={pages} rows_window={in_window} oldest={oldest} reached_start={reached_start}",
            flush=True,
        )
        if stop or len(rows) < 1000 or not before:
            break
        # The public endpoint is explicitly rate-limited; stay gentle and deterministic.
        time.sleep(0.25)
    return {
        "wallet": name,
        "address": address,
        "pages": pages,
        "signature_rows_in_window": in_window,
        "failed_signature_rows_in_window": failed,
        "newest_block_time": newest,
        "oldest_block_time": oldest,
        "reached_start": reached_start,
        "null_block_time": null_block_time,
    }


def main() -> None:
    print(json.dumps({"rpc": RPC_URL, "start": START, "end": END, "wallet_count": len(WALLETS)}), flush=True)
    results = []
    for name, address in WALLETS.items():
        try:
            results.append(scan_wallet(name, address))
        except Exception as exc:  # diagnostic must preserve failure rather than hide it
            results.append({"wallet": name, "address": address, "error": repr(exc)})
            print(f"wallet={name} terminal_error={exc!r}", flush=True)
    coverage = sum(bool(row.get("reached_start")) for row in results)
    total = sum(int(row.get("signature_rows_in_window", 0)) for row in results)
    print("SOLANA_ALPHA_RPC_PROBE_RESULT=" + json.dumps({
        "window_start_utc": "2026-08-02T19:45:00Z",
        "window_end_utc": "2026-09-01T19:45:00Z",
        "wallets_with_full_window_coverage": coverage,
        "wallet_count": len(WALLETS),
        "total_signature_rows_in_window": total,
        "wallets": results,
    }, sort_keys=True), flush=True)
    if coverage < len(WALLETS):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
