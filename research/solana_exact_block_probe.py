from __future__ import annotations

import json
import time
import urllib.request

RPC = "https://solana-rpc.publicnode.com"
PROBES = [
    {"index": 4751, "mint": "64aNTxPrArrcHq2seN6EXYKfCmovtpv7zCTFB8Pfpump", "wallet": "B32QbbdDAyhvUQzjcaM5j6ZVKwjCxAwGH5Xgvb9SJqnC", "target": 1787848076},
    {"index": 5251, "mint": "DYhUVrUTCpw481ivCdaQ4uwF3BWvE7qNPq1Kv2nNpump", "wallet": "B32QbbdDAyhvUQzjcaM5j6ZVKwjCxAwGH5Xgvb9SJqnC", "target": 1788122171},
    {"index": 5750, "mint": "4xoDsc7Rt16zi5ohm2sraVoFFnNHmaG4xVNUbGqypump", "wallet": "CyaE1VxvBrahnPWkqm5VsdCvyS2QmNht2UFrKJHga54o", "target": 1788291415},
]


def rpc(method, params):
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    req = urllib.request.Request(
        RPC,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", "User-Agent": "cie-exact-block-probe/1"},
    )
    last = None
    for attempt in range(6):
        try:
            with urllib.request.urlopen(req, timeout=45) as response:
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


def block_time(slot):
    try:
        return rpc("getBlockTime", [int(slot)])
    except Exception:
        return None


def valid_slot_time(slot, radius=40):
    for distance in range(radius + 1):
        for delta in ((0,) if distance == 0 else (distance, -distance)):
            candidate = int(slot) + delta
            if candidate < 0:
                continue
            value = block_time(candidate)
            if value is not None:
                return candidate, int(value)
    return None, None


def locate_slot(target):
    current = int(rpc("getSlot", [{"commitment": "confirmed"}]))
    current_slot, current_time = valid_slot_time(current)
    calibration_slot, calibration_time = valid_slot_time(current_slot - 100_000)
    if current_time is None or calibration_time is None or current_time <= calibration_time:
        raise RuntimeError("unable to establish recent slot/time calibration")
    slots_per_second = (current_slot - calibration_slot) / float(current_time - calibration_time)
    estimate = current_slot - int(round((current_time - target) * slots_per_second))
    trace = []
    for _ in range(10):
        actual_slot, actual_time = valid_slot_time(estimate)
        trace.append({"estimate": int(estimate), "actual_slot": actual_slot, "actual_time": actual_time})
        if actual_time is None:
            raise RuntimeError(f"no block time near estimated slot {estimate}")
        error_seconds = int(target) - int(actual_time)
        if abs(error_seconds) <= 1:
            return actual_slot, actual_time, slots_per_second, trace
        estimate = actual_slot + int(round(error_seconds * slots_per_second))
    return actual_slot, actual_time, slots_per_second, trace


def account_keys(tx):
    keys = tx["transaction"]["message"]["accountKeys"]
    return [k.get("pubkey") if isinstance(k, dict) else k for k in keys]


def ui_amount(row):
    value = row.get("uiTokenAmount", {})
    raw = value.get("amount")
    decimals = int(value.get("decimals") or 0)
    return 0.0 if raw is None else int(raw) / (10 ** decimals)


def token_delta(tx, mint, owner):
    meta = tx.get("meta") or {}
    pre, post = {}, {}
    for row in meta.get("preTokenBalances") or []:
        if row.get("mint") == mint and row.get("owner") == owner:
            pre[row.get("accountIndex")] = ui_amount(row)
    for row in meta.get("postTokenBalances") or []:
        if row.get("mint") == mint and row.get("owner") == owner:
            post[row.get("accountIndex")] = ui_amount(row)
    return sum(post.values()) - sum(pre.values())


def token_accounts(tx, mint, owner):
    keys = account_keys(tx)
    meta = tx.get("meta") or {}
    out = []
    seen = set()
    for row in (meta.get("preTokenBalances") or []) + (meta.get("postTokenBalances") or []):
        if row.get("mint") != mint or row.get("owner") != owner:
            continue
        idx = row.get("accountIndex")
        if isinstance(idx, int) and idx < len(keys) and keys[idx] not in seen:
            seen.add(keys[idx])
            out.append(keys[idx])
    return out


def scan(probe):
    anchor_slot, anchor_time, rate, trace = locate_slot(int(probe["target"]))
    slots = rpc("getBlocks", [max(0, anchor_slot - 30), anchor_slot + 30, {"commitment": "confirmed"}]) or []
    inspected = []
    matches = []
    for slot in slots:
        bt = block_time(slot)
        if bt is None or abs(int(bt) - int(probe["target"])) > 2:
            continue
        inspected.append({"slot": int(slot), "blockTime": int(bt)})
        block = rpc("getBlock", [int(slot), {
            "encoding": "jsonParsed",
            "transactionDetails": "full",
            "rewards": False,
            "commitment": "confirmed",
            "maxSupportedTransactionVersion": 0,
        }])
        if not block:
            continue
        for item in block.get("transactions") or []:
            transaction = item.get("transaction") or {}
            signatures = transaction.get("signatures") or []
            if not signatures:
                continue
            tx = {"transaction": transaction, "meta": item.get("meta") or {}}
            delta = token_delta(tx, probe["mint"], probe["wallet"])
            if delta == 0:
                continue
            matches.append({
                "signature": signatures[0],
                "slot": int(slot),
                "blockTime": int(bt),
                "delta": delta,
                "err": (item.get("meta") or {}).get("err"),
                "token_accounts": token_accounts(tx, probe["mint"], probe["wallet"]),
            })
    return {
        **probe,
        "anchor_slot": anchor_slot,
        "anchor_time": anchor_time,
        "slots_per_second": rate,
        "convergence_trace": trace,
        "inspected_blocks": inspected,
        "matches": matches,
    }


def main():
    results = []
    for probe in PROBES:
        try:
            result = scan(probe)
        except Exception as exc:
            result = {**probe, "error": repr(exc)}
        results.append(result)
        print("probe=" + json.dumps(result, sort_keys=True), flush=True)
    summary = {
        "matched_probes": sum(bool(r.get("matches")) for r in results),
        "exact_second_matches": sum(any(int(m["blockTime"]) == int(r["target"]) for m in r.get("matches", [])) for r in results),
        "results": results,
    }
    print("SOLANA_EXACT_BLOCK_PROBE=" + json.dumps(summary, sort_keys=True), flush=True)
    with open("solana-exact-block-probe.json", "w", encoding="utf-8") as handle:
        json.dump(summary, handle, sort_keys=True)
    if summary["matched_probes"] == 0:
        raise SystemExit(4)


if __name__ == "__main__":
    main()
