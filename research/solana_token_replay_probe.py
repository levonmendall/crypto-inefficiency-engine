from __future__ import annotations

import json
import time
import urllib.request

from solders.pubkey import Pubkey

RPC = "https://solana-rpc.publicnode.com"
TOKEN_PROGRAM = Pubkey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
ASSOCIATED_TOKEN_PROGRAM = Pubkey.from_string("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL")
WALLETS = {
    "kadenox": "B32QbbdDAyhvUQzjcaM5j6ZVKwjCxAwGH5Xgvb9SJqnC",
    "cented": "CyaE1VxvBrahnPWkqm5VsdCvyS2QmNht2UFrKJHga54o",
}
# Selected by position in the already-locked chronological OOS set before replay.
PROBES = [
    {"index": 4751, "mint": "64aNTxPrArrcHq2seN6EXYKfCmovtpv7zCTFB8Pfpump", "kol": "kadenox", "proxy": 1787848076},
    {"index": 5251, "mint": "DYhUVrUTCpw481ivCdaQ4uwF3BWvE7qNPq1Kv2nNpump", "kol": "kadenox", "proxy": 1788122171},
    {"index": 5750, "mint": "4xoDsc7Rt16zi5ohm2sraVoFFnNHmaG4xVNUbGqypump", "kol": "cented", "proxy": 1788291415},
]


def rpc(payload):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        RPC,
        data=data,
        headers={"Content-Type": "application/json", "User-Agent": "cie-alpha-replay/3"},
    )
    last = None
    for attempt in range(7):
        try:
            with urllib.request.urlopen(req, timeout=45) as response:
                return json.loads(response.read())
        except Exception as exc:
            last = exc
            if attempt == 6:
                raise
            time.sleep(0.6 * (attempt + 1))
    raise last


def one(method, params):
    body = rpc({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    if "error" in body:
        raise RuntimeError(body["error"])
    return body.get("result")


def batch_transactions(signatures, batch_size=20):
    out = []
    config = {"encoding": "jsonParsed", "commitment": "confirmed", "maxSupportedTransactionVersion": 0}
    for start in range(0, len(signatures), batch_size):
        chunk = signatures[start:start + batch_size]
        payload = [
            {"jsonrpc": "2.0", "id": i, "method": "getTransaction", "params": [sig, config]}
            for i, sig in enumerate(chunk)
        ]
        body = rpc(payload)
        if not isinstance(body, list):
            raise RuntimeError(f"unexpected batch response: {type(body)!r}")
        by_id = {int(item["id"]): item for item in body if isinstance(item, dict) and "id" in item}
        for i, sig in enumerate(chunk):
            item = by_id.get(i) or {}
            if item.get("error"):
                continue
            tx = item.get("result")
            if tx is not None:
                out.append((sig, tx))
    return out


def associated_token_account(owner: str, mint: str) -> str:
    owner_pk = Pubkey.from_string(owner)
    mint_pk = Pubkey.from_string(mint)
    ata, _ = Pubkey.find_program_address(
        [bytes(owner_pk), bytes(TOKEN_PROGRAM), bytes(mint_pk)],
        ASSOCIATED_TOKEN_PROGRAM,
    )
    return str(ata)


def signatures_for_address(address: str, max_pages=8):
    before = None
    rows = []
    for _ in range(max_pages):
        cfg = {"limit": 1000, "commitment": "confirmed"}
        if before:
            cfg["before"] = before
        page = one("getSignaturesForAddress", [address, cfg]) or []
        if not page:
            break
        rows.extend(page)
        if len(page) < 1000:
            break
        before = page[-1].get("signature")
        if not before:
            break
    return rows


def account_keys(tx):
    keys = tx["transaction"]["message"]["accountKeys"]
    return [key.get("pubkey") if isinstance(key, dict) else key for key in keys]


def ui_amount(balance):
    amount = balance.get("uiTokenAmount", {}) if balance else {}
    raw = amount.get("amount")
    decimals = amount.get("decimals", 0)
    return 0.0 if raw is None else int(raw) / (10 ** int(decimals))


def token_delta_for_owner(tx, mint, owner):
    meta = tx.get("meta") or {}
    pre, post = {}, {}
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
    pre, post = meta.get("preBalances") or [], meta.get("postBalances") or []
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
        "execution_price_sol_per_token": price_sol,
    }


def mint_token_accounts_in_tx(tx, mint):
    keys = account_keys(tx)
    meta = tx.get("meta") or {}
    result = []
    seen = set()
    for row in (meta.get("preTokenBalances") or []) + (meta.get("postTokenBalances") or []):
        if row.get("mint") != mint:
            continue
        idx = row.get("accountIndex")
        if not isinstance(idx, int) or idx >= len(keys):
            continue
        account = keys[idx]
        if account in seen:
            continue
        seen.add(account)
        result.append({"account": account, "owner": row.get("owner")})
    return result


def observed_trade_from_tx(signature, tx, mint, source_wallet=None):
    meta = tx.get("meta") or {}
    if meta.get("err") is not None:
        return None
    owners = []
    for bal in (meta.get("preTokenBalances") or []) + (meta.get("postTokenBalances") or []):
        if bal.get("mint") == mint and bal.get("owner"):
            owners.append(bal["owner"])
    for owner in dict.fromkeys(owners):
        if owner == source_wallet:
            continue
        summary = tx_summary(signature, tx, mint, owner)
        if summary["side"] in ("buy", "sell") and summary["execution_price_sol_per_token"]:
            return summary
    return None


def block_time(slot):
    try:
        return one("getBlockTime", [int(slot)])
    except Exception:
        return None


def valid_slot_time(slot, radius=12):
    for distance in range(radius + 1):
        deltas = (0,) if distance == 0 else (distance, -distance)
        for delta in deltas:
            candidate = int(slot) + delta
            if candidate < 0:
                continue
            value = block_time(candidate)
            if value is not None:
                return candidate, int(value)
    return None, None


def slot_for_timestamp(target):
    current = int(one("getSlot", [{"commitment": "confirmed"}]))
    # Ten million Solana slots comfortably spans this fixed 30-day research window.
    lo = max(0, current - 10_000_000)
    hi = current
    lo_slot, lo_time = valid_slot_time(lo)
    hi_slot, hi_time = valid_slot_time(hi)
    if lo_time is None or hi_time is None or not (lo_time <= target <= hi_time):
        raise RuntimeError(f"timestamp outside searchable slot bracket: target={target}, lo={lo_time}, hi={hi_time}")
    lo, hi = lo_slot, hi_slot
    best = (hi, abs(hi_time - target), hi_time)
    for _ in range(32):
        if lo > hi:
            break
        mid = (lo + hi) // 2
        actual_slot, actual_time = valid_slot_time(mid)
        if actual_time is None:
            lo = mid + 1
            continue
        distance = abs(actual_time - target)
        if distance < best[1]:
            best = (actual_slot, distance, actual_time)
        if actual_time < target:
            lo = actual_slot + 1
        elif actual_time > target:
            hi = mid - 1
        else:
            return actual_slot, actual_time
    return best[0], best[2]


def get_block(slot):
    config = {
        "encoding": "jsonParsed",
        "transactionDetails": "full",
        "rewards": False,
        "commitment": "confirmed",
        "maxSupportedTransactionVersion": 0,
    }
    return one("getBlock", [int(slot), config])


def source_trades_at_timestamp(target, mint, wallet, tolerance_seconds=2):
    anchor_slot, anchor_time = slot_for_timestamp(target)
    candidate_slots = one(
        "getBlocks",
        [max(0, anchor_slot - 24), anchor_slot + 24, {"commitment": "confirmed"}],
    ) or []
    matches = []
    inspected = []
    for slot in candidate_slots:
        bt = block_time(slot)
        if bt is None or abs(int(bt) - int(target)) > tolerance_seconds:
            continue
        inspected.append({"slot": int(slot), "blockTime": int(bt)})
        block = get_block(slot)
        if not block:
            continue
        for item in block.get("transactions") or []:
            transaction = item.get("transaction") or {}
            signatures = transaction.get("signatures") or []
            if not signatures:
                continue
            tx = {
                "transaction": transaction,
                "meta": item.get("meta") or {},
                "blockTime": int(bt),
                "slot": int(slot),
            }
            delta = token_delta_for_owner(tx, mint, wallet)
            if delta == 0:
                continue
            summary = tx_summary(signatures[0], tx, mint, wallet)
            matches.append((signatures[0], tx, summary))
    matches.sort(key=lambda x: (int(x[2].get("blockTime") or 0), int(x[2].get("slot") or 0)))
    return anchor_slot, anchor_time, inspected, matches


def replay_probe(probe):
    mint = probe["mint"]
    wallet = WALLETS[probe["kol"]]
    canonical_ata = associated_token_account(wallet, mint)

    # Primary join: KOL Explorer gives an exact Unix-second last-trade timestamp.
    # Find that second on Solana and inspect only adjacent blocks for this wallet's
    # mint balance change. No assumption about ATA layout is needed.
    anchor_slot, anchor_time, inspected_blocks, last_matches = source_trades_at_timestamp(
        int(probe["proxy"]), mint, wallet
    )
    exact_last = [x for x in last_matches if int(x[2].get("blockTime") or 0) == int(probe["proxy"])]
    last_pair = (exact_last or last_matches)[-1] if (exact_last or last_matches) else None
    last_trade = last_pair[2] if last_pair else None
    last_tx = last_pair[1] if last_pair else None

    actual_wallet_token_accounts = []
    if last_tx:
        actual_wallet_token_accounts = [
            x for x in mint_token_accounts_in_tx(last_tx, mint) if x.get("owner") == wallet
        ]

    # Recover the token-specific account's complete history. This should expose the
    # first buy even when the wallet's canonical ATA is not the account used.
    source_pairs_by_signature = {}
    account_signature_counts = {}
    accounts_to_query = [x["account"] for x in actual_wallet_token_accounts]
    if canonical_ata not in accounts_to_query:
        accounts_to_query.append(canonical_ata)
    for account in accounts_to_query:
        try:
            rows = signatures_for_address(account, max_pages=8)
        except Exception:
            rows = []
        account_signature_counts[account] = len(rows)
        for sig, tx in batch_transactions([r["signature"] for r in rows[:8000]]):
            source_pairs_by_signature[sig] = tx
    if last_pair:
        source_pairs_by_signature[last_pair[0]] = last_pair[1]

    source_txs = []
    for sig, tx in source_pairs_by_signature.items():
        summary = tx_summary(sig, tx, mint, wallet)
        if summary["side"] != "other" or summary["err"] is not None:
            source_txs.append(summary)
    source_txs.sort(key=lambda x: (x["blockTime"] or 0, x["slot"] or 0))
    buys = [x for x in source_txs if x["side"] == "buy" and x["err"] is None]
    first_buy = buys[0] if buys else None

    flow = {}
    delayed = {
        "250ms": {
            "certified": False,
            "reason": "historical Solana blockTime is integer-second resolution",
        }
    }
    pool_accounts = []
    pool_signature_rows = []
    pool_transactions = []

    if first_buy:
        first_tx = source_pairs_by_signature.get(first_buy["signature"])
        if first_tx:
            source_accounts = {x["account"] for x in mint_token_accounts_in_tx(first_tx, mint) if x.get("owner") == wallet}
            pool_accounts = [x for x in mint_token_accounts_in_tx(first_tx, mint) if x["account"] not in source_accounts]

        t0 = int(first_buy["blockTime"])
        lower, upper = t0 - 15, t0 + 20
        combined = {}
        for item in pool_accounts[:6]:
            try:
                rows = signatures_for_address(item["account"], max_pages=6)
            except Exception:
                continue
            for row in rows:
                bt = row.get("blockTime")
                if bt is None or not (lower <= int(bt) <= upper):
                    continue
                combined[row["signature"]] = row
        pool_signature_rows = sorted(
            combined.values(), key=lambda r: (int(r.get("blockTime") or 0), r.get("slot") or 0)
        )
        pool_transactions = batch_transactions([r["signature"] for r in pool_signature_rows])

        for row in pool_signature_rows:
            rel = str(int(row.get("blockTime") or 0) - t0)
            bucket = flow.setdefault(rel, {"success": 0, "failed": 0})
            bucket["failed" if row.get("err") is not None else "success"] += 1

        observed_market_trades = []
        for sig, tx in pool_transactions:
            observed = observed_trade_from_tx(sig, tx, mint, source_wallet=wallet)
            if observed:
                observed_market_trades.append(observed)
        observed_market_trades.sort(key=lambda x: (x["blockTime"] or 0, x["slot"] or 0))

        for delay in (1, 3, 5, 10):
            target = t0 + delay
            eligible = [x for x in observed_market_trades if x.get("blockTime") is not None and int(x["blockTime"]) >= target]
            observed = eligible[0] if eligible else None
            slippage_bps = None
            if observed and first_buy.get("execution_price_sol_per_token"):
                source_price = float(first_buy["execution_price_sol_per_token"])
                follower_price = float(observed["execution_price_sol_per_token"])
                if source_price > 0:
                    slippage_bps = (follower_price / source_price - 1.0) * 10000.0
            delayed[f"{delay}s"] = {
                "certified": observed is not None,
                "observed_trade": observed,
                "price_slippage_vs_source_bps": slippage_bps,
            }

    return {
        **probe,
        "wallet": wallet,
        "canonical_associated_token_account": canonical_ata,
        "anchor_slot": anchor_slot,
        "anchor_block_time": anchor_time,
        "inspected_blocks": inspected_blocks,
        "last_trade_candidates": [x[2] for x in last_matches],
        "last_trade": last_trade,
        "actual_wallet_token_accounts": actual_wallet_token_accounts,
        "account_signature_counts": account_signature_counts,
        "source_transactions": source_txs,
        "first_buy": first_buy,
        "pool_accounts": pool_accounts,
        "pool_signature_rows_window": len(pool_signature_rows),
        "pool_transactions_fetched": len(pool_transactions),
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
    last_recovered = sum(bool(r.get("last_trade")) for r in results)
    first_recovered = sum(bool(r.get("first_buy")) for r in results)
    delayed_counts = {
        f"{delay}s": sum(bool((r.get("delayed_observed_prices") or {}).get(f"{delay}s", {}).get("certified")) for r in results)
        for delay in (1, 3, 5, 10)
    }
    summary = {
        "rpc": RPC,
        "probe_count": len(results),
        "exact_last_trade_recovered": last_recovered,
        "first_buy_recovered": first_recovered,
        "delayed_price_recovered": delayed_counts,
        "results": results,
    }
    print("SOLANA_TOKEN_REPLAY_PROBE=" + json.dumps(summary, sort_keys=True), flush=True)
    with open("solana-token-replay-probe.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, sort_keys=True)
    if last_recovered == 0:
        raise SystemExit(4)


if __name__ == "__main__":
    main()
