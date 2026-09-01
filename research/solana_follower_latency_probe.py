from __future__ import annotations

import json
import time
import urllib.request

RPC = "https://solana-rpc.publicnode.com"
MINT = "4xoDsc7Rt16zi5ohm2sraVoFFnNHmaG4xVNUbGqypump"
SOURCE_WALLET = "CyaE1VxvBrahnPWkqm5VsdCvyS2QmNht2UFrKJHga54o"
FIRST_BUY_TIME = 1788291152
FIRST_BUY_SLOT = 443503484
SOURCE_ENTRY_PRICE = 7.058834137354345e-08
SOURCE_FIRST_SELL_TIME = 1788291414
SOURCE_FIRST_SELL_PRICE = 1.959978904029301e-07
SOURCE_FINAL_SELL_TIME = 1788291415
SOURCE_FINAL_SELL_PRICE = 1.6667982039707554e-07
DELAYS = (1, 3, 5, 10)


def call(method, params):
    payload={"jsonrpc":"2.0","id":1,"method":method,"params":params}
    req=urllib.request.Request(RPC,data=json.dumps(payload).encode(),headers={"Content-Type":"application/json","User-Agent":"cie-follower-latency/1"})
    last=None
    for attempt in range(5):
        try:
            with urllib.request.urlopen(req,timeout=45) as response:
                body=json.loads(response.read())
            if "error" in body: raise RuntimeError(body["error"])
            return body.get("result")
        except Exception as exc:
            last=exc
            if attempt==4: raise
            time.sleep(0.5*(attempt+1))
    raise last


def account_keys(transaction):
    return [x.get("pubkey") if isinstance(x,dict) else x for x in (transaction.get("message") or {}).get("accountKeys") or []]


def ui_amount(row):
    x=row.get("uiTokenAmount") or {}; raw=x.get("amount")
    return 0.0 if raw is None else int(raw)/(10**int(x.get("decimals") or 0))


def owner_token_delta(meta, owner):
    pre={}; post={}
    for row in meta.get("preTokenBalances") or []:
        if row.get("mint")==MINT and row.get("owner")==owner: pre[row.get("accountIndex")]=ui_amount(row)
    for row in meta.get("postTokenBalances") or []:
        if row.get("mint")==MINT and row.get("owner")==owner: post[row.get("accountIndex")]=ui_amount(row)
    return sum(post.values())-sum(pre.values())


def native_delta(meta, keys, owner):
    if owner not in keys: return None
    idx=keys.index(owner); pre=meta.get("preBalances") or []; post=meta.get("postBalances") or []
    if idx>=len(pre) or idx>=len(post): return None
    return (post[idx]-pre[idx])/1e9


def labels(meta):
    out=[]
    for line in meta.get("logMessages") or []:
        low=line.lower()
        if "instruction: buy" in low: out.append("buy")
        if "instruction: sell" in low: out.append("sell")
        if "instruction: swap" in low: out.append("swap")
    return sorted(set(out))


def mint_owners(meta):
    out=[]
    for row in (meta.get("preTokenBalances") or [])+(meta.get("postTokenBalances") or []):
        if row.get("mint")==MINT and row.get("owner") and row.get("owner") not in out: out.append(row["owner"])
    return out


def candidate_buys(block, slot):
    btime=int(block.get("blockTime") or 0); out=[]
    for tx_index,item in enumerate(block.get("transactions") or []):
        meta=item.get("meta") or {}
        if meta.get("err") is not None: continue
        trade_labels=labels(meta)
        if "buy" not in trade_labels and "swap" not in trade_labels: continue
        transaction=item.get("transaction") or {}; keys=account_keys(transaction); signatures=transaction.get("signatures") or []
        fee=float(meta.get("fee") or 0)/1e9
        fee_payer=keys[0] if keys else None
        for owner in mint_owners(meta):
            if owner==SOURCE_WALLET: continue
            td=owner_token_delta(meta,owner)
            if td<=0: continue
            nd=native_delta(meta,keys,owner)
            if nd is None or nd>=-0.000001: continue
            # Network fee is not DEX consideration; remove it if this owner paid it.
            economic=nd+fee if owner==fee_payer else nd
            if economic>=-0.000001: continue
            price=abs(economic/td)
            out.append({
                "signature":signatures[0] if signatures else None,
                "blockTime":btime,
                "slot":int(slot),
                "tx_index":tx_index,
                "owner":owner,
                "token_delta":td,
                "native_delta_sol":nd,
                "economic_native_delta_sol":economic,
                "network_fee_sol":fee,
                "labels":trade_labels,
                "price_sol_per_token":price,
            })
    return out


def main():
    # At ~3.15 slots/s, +55 slots covers comfortably more than 10 seconds.
    slots=call("getBlocks",[FIRST_BUY_SLOT, FIRST_BUY_SLOT+55, {"commitment":"confirmed"}]) or []
    cfg={"encoding":"jsonParsed","transactionDetails":"full","rewards":False,"commitment":"confirmed","maxSupportedTransactionVersion":0}
    observed=[]; flow={}; scanned=[]
    for slot in slots:
        block=call("getBlock",[int(slot),cfg])
        if not block: continue
        bt=block.get("blockTime")
        if bt is None or int(bt)<FIRST_BUY_TIME or int(bt)>FIRST_BUY_TIME+13: continue
        successful_touch=0; failed_touch=0
        for item in block.get("transactions") or []:
            meta=item.get("meta") or {}
            touches=any(row.get("mint")==MINT for row in (meta.get("preTokenBalances") or [])+(meta.get("postTokenBalances") or []))
            if not touches: continue
            if meta.get("err") is None: successful_touch+=1
            else: failed_touch+=1
        rel=int(bt)-FIRST_BUY_TIME
        bucket=flow.setdefault(str(rel),{"successful_mint_txs":0,"failed_mint_txs":0})
        bucket["successful_mint_txs"]+=successful_touch; bucket["failed_mint_txs"]+=failed_touch
        buys=candidate_buys(block,slot)
        observed.extend(buys)
        scanned.append({"slot":int(slot),"blockTime":int(bt),"successful_mint_txs":successful_touch,"failed_mint_txs":failed_touch,"candidate_buys":len(buys)})
    observed.sort(key=lambda x:(x["blockTime"],x["slot"],x["tx_index"]))

    latency={}
    for delay in DELAYS:
        target=FIRST_BUY_TIME+delay
        options=[x for x in observed if x["blockTime"]>=target]
        trade=options[0] if options else None
        if trade:
            price=float(trade["price_sol_per_token"])
            slippage_bps=(price/SOURCE_ENTRY_PRICE-1.0)*10000.0
            gross_to_first_sell=SOURCE_FIRST_SELL_PRICE/price-1.0
            gross_to_final_sell=SOURCE_FINAL_SELL_PRICE/price-1.0
            latency[f"{delay}s"]={
                "certified":True,
                "target_time":target,
                "observed_trade":trade,
                "entry_price_slippage_vs_source_bps":slippage_bps,
                "gross_return_if_exit_at_source_first_sell":gross_to_first_sell,
                "gross_return_if_exit_at_source_final_sell":gross_to_final_sell,
            }
        else:
            latency[f"{delay}s"]={"certified":False,"target_time":target,"reason":"no directly priced successful buy found in scanned window"}
    latency["250ms"]={"certified":False,"reason":"native historical blockTime has integer-second resolution"}

    result={
        "mint":MINT,
        "source_wallet":SOURCE_WALLET,
        "source_entry":{"time":FIRST_BUY_TIME,"slot":FIRST_BUY_SLOT,"price_sol_per_token":SOURCE_ENTRY_PRICE},
        "source_exits":{"first_sell":{"time":SOURCE_FIRST_SELL_TIME,"price_sol_per_token":SOURCE_FIRST_SELL_PRICE},"final_sell":{"time":SOURCE_FINAL_SELL_TIME,"price_sol_per_token":SOURCE_FINAL_SELL_PRICE}},
        "latency":latency,
        "flow_relative_seconds":flow,
        "observed_market_buys":observed,
        "scanned_blocks":scanned,
    }
    print("SOLANA_FOLLOWER_LATENCY="+json.dumps(result,sort_keys=True),flush=True)
    with open("solana-follower-latency.json","w",encoding="utf-8") as f: json.dump(result,f,sort_keys=True)
    if not any(latency[f"{d}s"]["certified"] for d in DELAYS): raise SystemExit(4)

if __name__=="__main__": main()
