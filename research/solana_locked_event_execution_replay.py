from __future__ import annotations

import json
import math
import time
import urllib.request

RPC = "https://solana-rpc.publicnode.com"
EVENT = {
    "index": 5750,
    "mint": "4xoDsc7Rt16zi5ohm2sraVoFFnNHmaG4xVNUbGqypump",
    "wallet": "CyaE1VxvBrahnPWkqm5VsdCvyS2QmNht2UFrKJHga54o",
    "token_account": "58o8mJ8B5Pron41Rx5BTovC8H37HQ9Dvj4n62FPnb7xb",
    "last_trade_time": 1788291415,
}


def rpc(method, params):
    req = urllib.request.Request(
        RPC,
        data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
        headers={"Content-Type": "application/json", "User-Agent": "cie-oos-execution-replay/1"},
    )
    last = None
    for attempt in range(6):
        try:
            with urllib.request.urlopen(req, timeout=35) as response:
                body = json.loads(response.read())
            if "error" in body:
                raise RuntimeError(body["error"])
            return body.get("result")
        except Exception as exc:
            last = exc
            if attempt == 5:
                raise
            time.sleep(0.5 * (attempt + 1))
    raise last


def account_keys(transaction):
    message = transaction.get("message") or {}
    return [x.get("pubkey") if isinstance(x, dict) else x for x in (message.get("accountKeys") or [])]


def ui_amount(row):
    x = row.get("uiTokenAmount") or {}
    raw = x.get("amount")
    return 0.0 if raw is None else int(raw) / (10 ** int(x.get("decimals") or 0))


def token_delta(meta, mint, owner):
    pre = {}
    post = {}
    for row in meta.get("preTokenBalances") or []:
        if row.get("mint") == mint and row.get("owner") == owner:
            pre[row.get("accountIndex")] = ui_amount(row)
    for row in meta.get("postTokenBalances") or []:
        if row.get("mint") == mint and row.get("owner") == owner:
            post[row.get("accountIndex")] = ui_amount(row)
    return sum(post.values()) - sum(pre.values())


def native_delta(transaction, meta, owner):
    keys = account_keys(transaction)
    if owner not in keys:
        return None
    idx = keys.index(owner)
    pre = meta.get("preBalances") or []
    post = meta.get("postBalances") or []
    if idx >= len(pre) or idx >= len(post):
        return None
    return (int(post[idx]) - int(pre[idx])) / 1e9


def owners_for_mint(meta, mint):
    out = []
    for row in (meta.get("preTokenBalances") or []) + (meta.get("postTokenBalances") or []):
        if row.get("mint") == mint and row.get("owner") and row.get("owner") not in out:
            out.append(row.get("owner"))
    return out


def summarize(signature, tx, mint, owner):
    meta = tx.get("meta") or {}
    transaction = tx.get("transaction") or {}
    td = token_delta(meta, mint, owner)
    nd = native_delta(transaction, meta, owner)
    fee = float(meta.get("fee") or 0) / 1e9
    side = "buy" if td > 0 else "sell" if td < 0 else "other"
    # Net wallet-balance price is deliberately conservative: it includes the
    # source transaction's network fee, tips and any account-creation balance cost.
    net_price = abs(nd / td) if td and nd not in (None, 0) else None
    return {
        "signature": signature,
        "slot": int(tx.get("slot") or 0),
        "blockTime": int(tx.get("blockTime") or 0),
        "err": meta.get("err"),
        "owner": owner,
        "side": side,
        "token_delta": td,
        "native_delta_sol": nd,
        "fee_sol": fee,
        "net_sol_per_token": net_price,
    }


def get_transaction(signature):
    return rpc("getTransaction", [signature, {
        "encoding": "jsonParsed",
        "commitment": "confirmed",
        "maxSupportedTransactionVersion": 0,
    }])


def source_history():
    rows = rpc("getSignaturesForAddress", [EVENT["token_account"], {"limit": 1000, "commitment": "confirmed"}]) or []
    trades = []
    for row in rows:
        tx = get_transaction(row["signature"])
        if not tx:
            continue
        summary = summarize(row["signature"], tx, EVENT["mint"], EVENT["wallet"])
        if summary["side"] != "other" or summary["err"] is not None:
            trades.append(summary)
    trades.sort(key=lambda x: (x["blockTime"], x["slot"]))
    return rows, trades


def get_block(slot):
    return rpc("getBlock", [int(slot), {
        "encoding": "jsonParsed",
        "transactionDetails": "full",
        "rewards": False,
        "commitment": "confirmed",
        "maxSupportedTransactionVersion": 0,
    }])


def market_flow(first_buy, slots_per_second=3.15):
    start_slot = max(0, int(first_buy["slot"]) - 6)
    end_slot = int(first_buy["slot"]) + int(math.ceil(12 * slots_per_second)) + 8
    slots = rpc("getBlocks", [start_slot, end_slot, {"commitment": "confirmed"}]) or []
    events = []
    failed_mint_touches = 0
    blocks_scanned = 0
    t0 = int(first_buy["blockTime"])
    for slot in slots:
        block = get_block(slot)
        if not block:
            continue
        bt = block.get("blockTime")
        if bt is None or not (t0 - 2 <= int(bt) <= t0 + 12):
            continue
        blocks_scanned += 1
        for item in block.get("transactions") or []:
            meta = item.get("meta") or {}
            has_mint = any(
                row.get("mint") == EVENT["mint"]
                for row in (meta.get("preTokenBalances") or []) + (meta.get("postTokenBalances") or [])
            )
            if not has_mint:
                continue
            if meta.get("err") is not None:
                failed_mint_touches += 1
                continue
            transaction = item.get("transaction") or {}
            signatures = transaction.get("signatures") or []
            signature = signatures[0] if signatures else None
            for owner in owners_for_mint(meta, EVENT["mint"]):
                if owner == EVENT["wallet"]:
                    continue
                tx = {"transaction": transaction, "meta": meta, "slot": int(slot), "blockTime": int(bt)}
                summary = summarize(signature, tx, EVENT["mint"], owner)
                if summary["side"] in ("buy", "sell") and summary["net_sol_per_token"]:
                    events.append(summary)
                    break
    events.sort(key=lambda x: (x["blockTime"], x["slot"]))
    return blocks_scanned, failed_mint_touches, events


def delayed_entries(events, t0):
    result = {
        "250ms": {"certified": False, "reason": "historical Solana blockTime is integer-second resolution"}
    }
    for delay in (1, 3, 5, 10):
        target = int(t0) + delay
        buys = [e for e in events if e["side"] == "buy" and target <= e["blockTime"] <= target + 2]
        result[f"{delay}s"] = {
            "certified": bool(buys),
            "target_time": target,
            "observed_trade": buys[0] if buys else None,
        }
    return result


def copyability(source_trades, delayed):
    buys = [x for x in source_trades if x["side"] == "buy" and x["err"] is None]
    sells = [x for x in source_trades if x["side"] == "sell" and x["err"] is None]
    if not buys:
        return {}
    first = buys[0]
    last_sell = sells[-1] if sells else None
    horizon = (last_sell["blockTime"] - first["blockTime"]) if last_sell else None
    out = {
        "source_first_buy": first,
        "source_last_sell": last_sell,
        "source_trade_horizon_seconds": horizon,
    }
    if last_sell and last_sell.get("net_sol_per_token"):
        exit_price = last_sell["net_sol_per_token"]
        for key in ("1s", "3s", "5s", "10s"):
            entry = delayed.get(key, {}).get("observed_trade")
            entry_price = entry.get("net_sol_per_token") if entry else None
            target_delay = int(key[:-1])
            if not entry_price:
                out[key] = {"copyable_before_source_exit": False, "return_to_source_last_sell": None}
                continue
            before_exit = horizon is not None and target_delay < horizon
            out[key] = {
                "copyable_before_source_exit": before_exit,
                "entry_price_sol_per_token": entry_price,
                "source_last_sell_price_sol_per_token": exit_price,
                "return_to_source_last_sell": (exit_price / entry_price) - 1.0 if before_exit else None,
            }
    return out


def main():
    rows, source_trades = source_history()
    buys = [x for x in source_trades if x["side"] == "buy" and x["err"] is None]
    if not buys:
        result = {"event": EVENT, "source_signature_count": len(rows), "source_trades": source_trades, "error": "no source buy recovered"}
        print("SOLANA_LOCKED_EVENT_EXECUTION=" + json.dumps(result, sort_keys=True), flush=True)
        with open("solana-locked-event-execution.json", "w", encoding="utf-8") as f:
            json.dump(result, f, sort_keys=True)
        raise SystemExit(4)
    first_buy = buys[0]
    blocks_scanned, failed, events = market_flow(first_buy)
    delayed = delayed_entries(events, first_buy["blockTime"])
    result = {
        "event": EVENT,
        "source_signature_count": len(rows),
        "source_trades": source_trades,
        "blocks_scanned": blocks_scanned,
        "failed_mint_touch_transactions": failed,
        "market_trade_count": len(events),
        "flow_by_relative_second": {
            str(second): {
                "buys": sum(1 for e in events if e["side"] == "buy" and e["blockTime"] - first_buy["blockTime"] == second),
                "sells": sum(1 for e in events if e["side"] == "sell" and e["blockTime"] - first_buy["blockTime"] == second),
            }
            for second in range(-2, 13)
        },
        "delayed_entries": delayed,
        "copyability": copyability(source_trades, delayed),
    }
    print("SOLANA_LOCKED_EVENT_EXECUTION=" + json.dumps(result, sort_keys=True), flush=True)
    with open("solana-locked-event-execution.json", "w", encoding="utf-8") as f:
        json.dump(result, f, sort_keys=True)


if __name__ == "__main__":
    main()
