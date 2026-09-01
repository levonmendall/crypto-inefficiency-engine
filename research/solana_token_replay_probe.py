from __future__ import annotations

import json
import time
import urllib.request

RPC = "https://solana-rpc.publicnode.com"
WALLETS = {
    "kadenox": "B32QbbdDAyhvUQzjcaM5j6ZVKwjCxAwGH5Xgvb9SJqnC",
    "cented": "CyaE1VxvBrahnPWkqm5VsdCvyS2QmNht2UFrKJHga54o",
}
# Deterministically selected BEFORE replay from the locked chronological holdout:
# first holdout event, midpoint holdout event, final holdout event.
PROBES = [
    {"index": 4751, "mint": "64aNTxPrArrcHq2seN6EXYKfCmovtpv7zCTFB8Pfpump", "kol": "kadenox", "proxy": 1787848076},
    {"index": 5251, "mint": "DYhUVrUTCpw481ivCdaQ4uwF3BWvE7qNPq1Kv2nNpump", "kol": "kadenox", "proxy": 1788122171},
    {"index": 5750, "mint": "4xoDsc7Rt16zi5ohm2sraVoFFnNHmaG4xVNUbGqypump", "kol": "cented", "proxy": 1788291415},
]


def rpc(payload):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(RPC, data=data, headers={"Content-Type": "application/json", "User-Agent": "cie-alpha-replay/1"})
    for attempt in range(5):
        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                return json.loads(response.read())
        except Exception:
            if attempt == 4:
                raise
            time.sleep(0.4 * (attempt + 1))


def one(method, params):
    body = rpc({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    if "error" in body:
        raise RuntimeError(body["error"])
    return body.get("result")


def account_keys(tx):
    keys = tx["transaction"]["message"]["accountKeys"]
    out = []
    for key in keys:
        out.append(key.get("pubkey") if isinstance(key, dict) else key)
    return out


def ui_amount(balance):
    amount = balance.get("uiTokenAmount", {}) if balance else {}
    raw = amount.get("amount")
    decimals = amount.get("decimals", 0)
    if raw is None:
        return 0.0
    return int(raw) / (10 ** int(decimals))


def token_delta_for_owner(tx, mint, owner):
    meta = tx.get("meta") or {}
    pre = {}
    post = {}
    for row in meta.get("preTokenBalances") or []:
        if row.get("mint") == mint and row.get("owner") == owner:
            pre[row.get("accountIndex")] = ui_amount(row)
    for row in meta.get("postTokenBalances") or []:
        if row.get("mint") == mint and row.get("owner") == owner:
            post[row.get("accountIndex")] = ui_amount(row)
    return sum(post.values()) - sum(pre.values())


def wallet_native_delta(tx, owner):
    keys = account_keys(tx)
    if owner not in keys:
        return None
    idx = keys.index(owner)
    meta = tx.get("meta") or {}
    pre = meta.get("preBalances") or []
    post = meta.get("postBalances") or []
    if idx >= len(pre) or idx >= len(post):
        return None
    return (post[idx] - pre[idx]) / 1e9


def tx_summary(signature, tx, mint, wallet):
    meta = tx.get("meta") or {}
    token_delta = token_delta_for_owner(tx, mint, wallet)
    native_delta = wallet_native_delta(tx, wallet)
    fee_sol = float(meta.get("fee") or 0) / 1e9
    side = "buy" if token_delta > 0 else "sell" if token_delta < 0 else "other"
    price_sol = None
    if token_delta and native_delta is not None:
        # Remove the source wallet's transaction fee from native outflow when possible.
        economic_sol = native_delta + fee_sol if native_delta < 0 else native_delta
        price_sol = abs(economic_sol / token_delta)
    return {
        "signature": signature,
        "blockTime": tx.get("blockTime"),
        "slot": tx.get("slot"),
        "err": meta.get("err"),
        "fee_sol": fee_sol,
        "side": side,
        "token_delta": token_delta,
        "native_delta_sol": native_delta,
        "approx_execution_price_sol_per_token": price_sol,
    }


def replay_probe(probe):
    mint = probe["mint"]
    wallet = WALLETS[probe["kol"]]
    lower = probe["proxy"] - 7200
    upper = probe["proxy"] + 600
    before = None
    sig_rows = []
    pages = 0
    reached_lower = False
    while pages < 30:
        cfg = {"limit": 1000, "commitment": "confirmed"}
        if before:
            cfg["before"] = before
        rows = one("getSignaturesForAddress", [mint, cfg]) or []
        if not rows:
            break
        pages += 1
        for row in rows:
            bt = row.get("blockTime")
            if bt is None:
                continue
            if bt < lower:
                reached_lower = True
                break
            if bt <= upper:
                sig_rows.append(row)
        if reached_lower or len(rows) < 1000:
            break
        before = rows[-1].get("signature")
        if not before:
            break

    # Only fetch transaction bodies near the source wallet's known last-trade second.
    # This is a feasibility probe, not yet the full 5,751-event replay.
    nearby = [r for r in sig_rows if lower <= int(r.get("blockTime") or 0) <= upper]
    txs = []
    for row in nearby[:3000]:
        sig = row["signature"]
        tx = one("getTransaction", [sig, {"encoding": "jsonParsed", "commitment": "confirmed", "maxSupportedTransactionVersion": 0}])
        if tx is not None:
            txs.append((sig, tx))

    wallet_txs = []
    for sig, tx in txs:
        if wallet in account_keys(tx):
            summary = tx_summary(sig, tx, mint, wallet)
            if summary["side"] != "other" or summary["err"] is not None:
                wallet_txs.append(summary)
    wallet_txs.sort(key=lambda x: (x["blockTime"] or 0, x["slot"] or 0))
    buys = [x for x in wallet_txs if x["side"] == "buy" and x["err"] is None]
    first_buy = buys[0] if buys else None

    # Ledger flow around first buy: successful/failed mint-touching transactions per second.
    flow = {}
    delayed = {}
    if first_buy:
        t0 = int(first_buy["blockTime"])
        for row in sig_rows:
            bt = row.get("blockTime")
            if bt is None or not (t0 - 10 <= int(bt) <= t0 + 15):
                continue
            key = str(int(bt) - t0)
            bucket = flow.setdefault(key, {"success": 0, "failed": 0})
            bucket["failed" if row.get("err") is not None else "success"] += 1
        # Exact wall-clock 250ms cannot be reconstructed from integer-second blockTime.
        delayed["250ms"] = {"certified": False, "reason": "Solana historical blockTime is integer-second resolution"}
        for delay in (1, 3, 5, 10):
            target = t0 + delay
            candidates = [
                (sig, tx) for sig, tx in txs
                if tx.get("blockTime") is not None and int(tx["blockTime"]) >= target and int(tx["blockTime"]) <= target + 2
                and (tx.get("meta") or {}).get("err") is None
            ]
            candidates.sort(key=lambda pair: (pair[1].get("blockTime") or 0, pair[1].get("slot") or 0))
            observed = None
            for sig, tx in candidates:
                # Find a token owner with nonzero mint delta and a usable native delta.
                owners = []
                meta = tx.get("meta") or {}
                for bal in (meta.get("preTokenBalances") or []) + (meta.get("postTokenBalances") or []):
                    if bal.get("mint") == mint and bal.get("owner"):
                        owners.append(bal["owner"])
                for owner in dict.fromkeys(owners):
                    s = tx_summary(sig, tx, mint, owner)
                    if s["side"] in ("buy", "sell") and s["approx_execution_price_sol_per_token"]:
                        observed = s
                        break
                if observed:
                    break
            delayed[f"{delay}s"] = {"certified": observed is not None, "observed_trade": observed}

    return {
        **probe,
        "wallet": wallet,
        "signature_pages": pages,
        "signature_rows_window": len(sig_rows),
        "reached_two_hours_before_proxy": reached_lower,
        "transactions_fetched": len(txs),
        "wallet_transactions": wallet_txs,
        "first_buy": first_buy,
        "flow_relative_seconds": flow,
        "delayed_observed_prices": delayed,
    }


def main():
    results = []
    for probe in PROBES:
        try:
            result = replay_probe(probe)
        except Exception as exc:
            result = {**probe, "error": repr(exc)}
        results.append(result)
        print("probe=" + json.dumps(result, sort_keys=True), flush=True)
    passed_join = sum(bool(r.get("first_buy")) for r in results)
    summary = {"rpc": RPC, "probe_count": len(results), "first_buy_recovered": passed_join, "results": results}
    print("SOLANA_TOKEN_REPLAY_PROBE=" + json.dumps(summary, sort_keys=True), flush=True)
    with open("solana-token-replay-probe.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, sort_keys=True)
    if passed_join == 0:
        raise SystemExit(4)


if __name__ == "__main__":
    main()
