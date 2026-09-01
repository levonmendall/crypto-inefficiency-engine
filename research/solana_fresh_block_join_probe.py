from __future__ import annotations

import json
import time
import urllib.request

RPC = "https://solana-rpc.publicnode.com"
PROBE = {
    "index": 5750,
    "mint": "4xoDsc7Rt16zi5ohm2sraVoFFnNHmaG4xVNUbGqypump",
    "wallet": "CyaE1VxvBrahnPWkqm5VsdCvyS2QmNht2UFrKJHga54o",
    "target": 1788291415,
}


def call(method, params):
    req = urllib.request.Request(
        RPC,
        data=json.dumps({"jsonrpc":"2.0","id":1,"method":method,"params":params}).encode(),
        headers={"Content-Type":"application/json","User-Agent":"cie-fresh-block-join/3"},
    )
    last = None
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=25) as response:
                body = json.loads(response.read())
            if "error" in body:
                raise RuntimeError(body["error"])
            return body.get("result")
        except Exception as exc:
            last = exc
            if attempt == 3:
                raise
            time.sleep(0.4 * (attempt + 1))
    raise last


def bt(slot):
    return call("getBlockTime", [int(slot)])


def locate(target):
    tip = int(call("getSlot", [{"commitment":"confirmed"}]))
    tip_time = int(bt(tip))
    cal_slot = tip - 20_000
    cal_time = int(bt(cal_slot))
    rate = (tip - cal_slot) / float(tip_time - cal_time)
    slot = tip - int(round((tip_time - target) * rate))
    trace = []
    for _ in range(6):
        t = bt(slot)
        trace.append({"slot":slot,"blockTime":t})
        if t is None:
            slot += 1
            continue
        err = target - int(t)
        if abs(err) <= 1:
            return slot, int(t), rate, trace
        slot += int(round(err * rate))
    return slot, int(bt(slot)), rate, trace


def keys(transaction):
    return [key.get("pubkey") if isinstance(key,dict) else key for key in (transaction.get("message") or {}).get("accountKeys") or []]


def amount(row):
    x=row.get("uiTokenAmount") or {}
    raw=x.get("amount")
    return 0.0 if raw is None else int(raw)/(10**int(x.get("decimals") or 0))


def delta(meta,mint,owner):
    pre={}; post={}
    for row in meta.get("preTokenBalances") or []:
        if row.get("mint")==mint and row.get("owner")==owner:
            pre[row.get("accountIndex")]=amount(row)
    for row in meta.get("postTokenBalances") or []:
        if row.get("mint")==mint and row.get("owner")==owner:
            post[row.get("accountIndex")]=amount(row)
    return sum(post.values())-sum(pre.values())


def main():
    anchor, anchor_time, rate, trace = locate(PROBE["target"])
    slots = call("getBlocks", [anchor-12, anchor+12, {"commitment":"confirmed"}]) or []
    matches=[]; scanned=[]
    cfg={"encoding":"jsonParsed","transactionDetails":"full","rewards":False,"commitment":"confirmed","maxSupportedTransactionVersion":0}
    found=False
    for slot in slots:
        block=call("getBlock",[int(slot),cfg])
        if not block:
            continue
        btime=block.get("blockTime")
        if btime is None or abs(int(btime)-PROBE["target"])>3:
            continue
        scanned.append({"slot":int(slot),"blockTime":int(btime),"tx_count":len(block.get("transactions") or [])})
        for item in block.get("transactions") or []:
            meta=item.get("meta") or {}
            d=delta(meta,PROBE["mint"],PROBE["wallet"])
            if d==0:
                continue
            tx=item.get("transaction") or {}
            sigs=tx.get("signatures") or []
            account_keys=keys(tx)
            token_accounts=[]
            for row in (meta.get("preTokenBalances") or [])+(meta.get("postTokenBalances") or []):
                if row.get("mint")==PROBE["mint"] and row.get("owner")==PROBE["wallet"]:
                    idx=row.get("accountIndex")
                    if isinstance(idx,int) and idx<len(account_keys) and account_keys[idx] not in token_accounts:
                        token_accounts.append(account_keys[idx])
            matches.append({"signature":sigs[0] if sigs else None,"slot":int(slot),"blockTime":int(btime),"token_delta":d,"err":meta.get("err"),"fee_lamports":meta.get("fee"),"token_accounts":token_accounts})
            found=True
            break
        if found:
            break
    result={**PROBE,"anchor_slot":anchor,"anchor_time":anchor_time,"slots_per_second":rate,"trace":trace,"scanned":scanned,"matches":matches}
    print("SOLANA_FRESH_BLOCK_JOIN="+json.dumps(result,sort_keys=True),flush=True)
    with open("solana-fresh-block-join.json","w",encoding="utf-8") as f: json.dump(result,f,sort_keys=True)
    if not matches: raise SystemExit(4)

if __name__=="__main__": main()
