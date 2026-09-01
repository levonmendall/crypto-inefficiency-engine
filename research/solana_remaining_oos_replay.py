from __future__ import annotations

import json
import time
import urllib.request

RPC = "https://solana-rpc.publicnode.com"
PROBES = [
    {
        "index": 4751,
        "mint": "64aNTxPrArrcHq2seN6EXYKfCmovtpv7zCTFB8Pfpump",
        "wallet": "B32QbbdDAyhvUQzjcaM5j6ZVKwjCxAwGH5Xgvb9SJqnC",
        "locked_last_time": 1787848076,
    },
    {
        "index": 5251,
        "mint": "DYhUVrUTCpw481ivCdaQ4uwF3BWvE7qNPq1Kv2nNpump",
        "wallet": "B32QbbdDAyhvUQzjcaM5j6ZVKwjCxAwGH5Xgvb9SJqnC",
        "locked_last_time": 1788122171,
    },
]
DELAYS = (1, 3, 5, 10)


def call(method, params):
    req = urllib.request.Request(
        RPC,
        data=json.dumps({"jsonrpc":"2.0","id":1,"method":method,"params":params}).encode(),
        headers={"Content-Type":"application/json","User-Agent":"cie-remaining-oos-replay/1"},
    )
    last=None
    for attempt in range(6):
        try:
            with urllib.request.urlopen(req,timeout=45) as response:
                body=json.loads(response.read())
            if "error" in body:
                raise RuntimeError(body["error"])
            return body.get("result")
        except Exception as exc:
            last=exc
            if attempt==5:
                raise
            time.sleep(0.5*(attempt+1))
    raise last


def block_time(slot):
    try:
        return call("getBlockTime", [int(slot)])
    except Exception:
        return None


def nearest_valid_time(slot, radius=24):
    for d in range(radius+1):
        offsets=(0,) if d==0 else (d,-d)
        for off in offsets:
            s=int(slot)+off
            if s<0: continue
            t=block_time(s)
            if t is not None: return s,int(t)
    return None,None


def locate_slot(target):
    tip=int(call("getSlot", [{"commitment":"confirmed"}]))
    tip_slot,tip_time=nearest_valid_time(tip)
    cal_slot,cal_time=nearest_valid_time(tip_slot-100_000)
    if tip_time is None or cal_time is None:
        raise RuntimeError("recent slot calibration unavailable")
    rate=(tip_slot-cal_slot)/float(tip_time-cal_time)
    estimate=tip_slot-int(round((tip_time-target)*rate))
    trace=[]
    for _ in range(10):
        s,t=nearest_valid_time(estimate)
        trace.append({"estimate":estimate,"slot":s,"blockTime":t})
        if t is None: raise RuntimeError("slot timestamp unavailable")
        err=int(target)-int(t)
        if abs(err)<=1: return s,t,rate,trace
        estimate=s+int(round(err*rate))
    return s,t,rate,trace


def account_keys(transaction):
    return [k.get("pubkey") if isinstance(k,dict) else k for k in (transaction.get("message") or {}).get("accountKeys") or []]


def ui_amount(row):
    x=row.get("uiTokenAmount") or {}; raw=x.get("amount")
    return 0.0 if raw is None else int(raw)/(10**int(x.get("decimals") or 0))


def owner_token_delta(meta,mint,owner):
    pre={};post={}
    for row in meta.get("preTokenBalances") or []:
        if row.get("mint")==mint and row.get("owner")==owner: pre[row.get("accountIndex")]=ui_amount(row)
    for row in meta.get("postTokenBalances") or []:
        if row.get("mint")==mint and row.get("owner")==owner: post[row.get("accountIndex")]=ui_amount(row)
    return sum(post.values())-sum(pre.values())


def native_delta(meta,keys,owner):
    if owner not in keys: return None
    i=keys.index(owner); pre=meta.get("preBalances") or []; post=meta.get("postBalances") or []
    if i>=len(pre) or i>=len(post): return None
    return (post[i]-pre[i])/1e9


def log_labels(meta):
    out=[]
    for line in meta.get("logMessages") or []:
        low=line.lower()
        if "instruction: buy" in low: out.append("buy")
        if "instruction: sell" in low: out.append("sell")
        if "instruction: swap" in low: out.append("swap")
    return sorted(set(out))


def mint_accounts(meta,keys,mint):
    out=[];seen=set()
    for row in (meta.get("preTokenBalances") or [])+(meta.get("postTokenBalances") or []):
        if row.get("mint")!=mint: continue
        i=row.get("accountIndex")
        if isinstance(i,int) and i<len(keys) and keys[i] not in seen:
            seen.add(keys[i]); out.append({"account":keys[i],"owner":row.get("owner")})
    return out


def transaction_summary(signature,tx,mint,wallet):
    meta=tx.get("meta") or {}; transaction=tx.get("transaction") or {}; keys=account_keys(transaction)
    td=owner_token_delta(meta,mint,wallet); nd=native_delta(meta,keys,wallet); fee=float(meta.get("fee") or 0)/1e9
    economic=nd+fee if nd is not None and nd<0 else nd
    price=abs(economic/td) if td and economic is not None and abs(economic)>1e-7 else None
    return {
        "signature":signature,"blockTime":tx.get("blockTime"),"slot":tx.get("slot"),"err":meta.get("err"),
        "token_delta":td,"native_delta_sol":nd,"economic_native_delta_sol":economic,"network_fee_sol":fee,
        "price_sol_per_token":price,"labels":log_labels(meta),"mint_accounts":mint_accounts(meta,keys,mint),
    }


def scan_last_event(probe):
    target=int(probe["locked_last_time"]); mint=probe["mint"]; wallet=probe["wallet"]
    anchor,anchor_time,rate,trace=locate_slot(target)
    slots=call("getBlocks",[max(0,anchor-18),anchor+18,{"commitment":"confirmed"}]) or []
    cfg={"encoding":"jsonParsed","transactionDetails":"full","rewards":False,"commitment":"confirmed","maxSupportedTransactionVersion":0}
    matches=[]
    for slot in slots:
        block=call("getBlock",[int(slot),cfg])
        if not block: continue
        bt=block.get("blockTime")
        if bt is None or abs(int(bt)-target)>3: continue
        for item in block.get("transactions") or []:
            meta=item.get("meta") or {}
            td=owner_token_delta(meta,mint,wallet)
            if td==0: continue
            transaction=item.get("transaction") or {}; sigs=transaction.get("signatures") or []
            tx={"transaction":transaction,"meta":meta,"blockTime":int(bt),"slot":int(slot)}
            matches.append(transaction_summary(sigs[0] if sigs else None,tx,mint,wallet))
    if not matches:
        raise RuntimeError("no wallet/mint transaction found near locked last timestamp")
    exact=[m for m in matches if int(m.get("blockTime") or 0)==target]
    last=(exact or matches)[-1]
    wallet_accounts=[x["account"] for x in last["mint_accounts"] if x.get("owner")==wallet]
    if not wallet_accounts:
        raise RuntimeError("matched transaction did not expose wallet token account")
    return {"anchor_slot":anchor,"anchor_time":anchor_time,"rate":rate,"trace":trace,"matches":matches,"last":last,"wallet_token_accounts":wallet_accounts}


def signatures_for_account(account,max_pages=12):
    out=[];before=None
    for _ in range(max_pages):
        cfg={"limit":1000,"commitment":"confirmed"}
        if before: cfg["before"]=before
        page=call("getSignaturesForAddress",[account,cfg]) or []
        if not page: break
        out.extend(page)
        if len(page)<1000: break
        before=page[-1].get("signature")
        if not before: break
    return out


def get_transaction(signature):
    try:
        return call("getTransaction",[signature,{"encoding":"jsonParsed","commitment":"confirmed","maxSupportedTransactionVersion":0}])
    except Exception:
        return None


def recover_source_history(probe,accounts):
    mint=probe["mint"];wallet=probe["wallet"]
    all_sig={}
    account_counts={}
    for account in accounts:
        rows=signatures_for_account(account)
        account_counts[account]=len(rows)
        for row in rows:
            # Successful signatures are enough to identify actual buys/sells. Keep failed count separately.
            all_sig[row["signature"]]=row
    successful=[r for r in all_sig.values() if r.get("err") is None]
    failed=sum(r.get("err") is not None for r in all_sig.values())
    history=[]
    for i,row in enumerate(sorted(successful,key=lambda r:int(r.get("blockTime") or 0))):
        tx=get_transaction(row["signature"])
        if tx is None: continue
        s=transaction_summary(row["signature"],tx,mint,wallet)
        if s["token_delta"]!=0: history.append(s)
    history.sort(key=lambda x:(int(x.get("blockTime") or 0),int(x.get("slot") or 0)))
    buys=[x for x in history if x["err"] is None and ("buy" in x["labels"] or (x["token_delta"]>0 and (x["economic_native_delta_sol"] or 0)<-0.0001))]
    sells=[x for x in history if x["err"] is None and ("sell" in x["labels"] or (x["token_delta"]<0 and (x["economic_native_delta_sol"] or 0)>0.0001))]
    if not buys: raise RuntimeError("no first buy recovered from discovered token account")
    return {"account_signature_counts":account_counts,"unique_signatures":len(all_sig),"failed_signature_count":failed,"history":history,"buys":buys,"sells":sells,"first_buy":buys[0]}


def candidate_buys(block,slot,mint,source_wallet):
    bt=int(block.get("blockTime") or 0);out=[]
    for tx_index,item in enumerate(block.get("transactions") or []):
        meta=item.get("meta") or {}
        if meta.get("err") is not None: continue
        labels=log_labels(meta)
        if "buy" not in labels and "swap" not in labels: continue
        transaction=item.get("transaction") or {}; keys=account_keys(transaction); sigs=transaction.get("signatures") or []
        fee=float(meta.get("fee") or 0)/1e9; payer=keys[0] if keys else None
        owners=[]
        for row in (meta.get("preTokenBalances") or [])+(meta.get("postTokenBalances") or []):
            if row.get("mint")==mint and row.get("owner") and row.get("owner") not in owners: owners.append(row["owner"])
        for owner in owners:
            if owner==source_wallet: continue
            td=owner_token_delta(meta,mint,owner)
            if td<=0: continue
            nd=native_delta(meta,keys,owner)
            if nd is None or nd>=-0.000001: continue
            economic=nd+fee if owner==payer else nd
            if economic>=-0.000001: continue
            price=abs(economic/td)
            # Reject dust/account-rent artifacts that imply absurd token prices relative to active meme-token trades.
            if td<1000: continue
            out.append({"signature":sigs[0] if sigs else None,"blockTime":bt,"slot":int(slot),"tx_index":tx_index,"owner":owner,"token_delta":td,"economic_native_delta_sol":economic,"network_fee_sol":fee,"price_sol_per_token":price})
    return out


def replay_latency(probe,first_buy,sells):
    mint=probe["mint"];source=probe["wallet"];t0=int(first_buy["blockTime"]);slot0=int(first_buy["slot"]);source_price=float(first_buy["price_sol_per_token"])
    slots=call("getBlocks",[slot0,slot0+60,{"commitment":"confirmed"}]) or []
    cfg={"encoding":"jsonParsed","transactionDetails":"full","rewards":False,"commitment":"confirmed","maxSupportedTransactionVersion":0}
    buys=[];flow={}
    for slot in slots:
        block=call("getBlock",[int(slot),cfg])
        if not block: continue
        bt=block.get("blockTime")
        if bt is None or int(bt)<t0 or int(bt)>t0+14: continue
        succ=fail=0
        for item in block.get("transactions") or []:
            meta=item.get("meta") or {}
            touches=any(r.get("mint")==mint for r in (meta.get("preTokenBalances") or [])+(meta.get("postTokenBalances") or []))
            if touches:
                if meta.get("err") is None: succ+=1
                else: fail+=1
        b=flow.setdefault(str(int(bt)-t0),{"successful_mint_txs":0,"failed_mint_txs":0})
        b["successful_mint_txs"]+=succ;b["failed_mint_txs"]+=fail
        buys.extend(candidate_buys(block,slot,mint,source))
    buys.sort(key=lambda x:(x["blockTime"],x["slot"],x["tx_index"]))
    first_sell=sells[0] if sells else None;final_sell=sells[-1] if sells else None
    latency={"250ms":{"certified":False,"reason":"historical blockTime is integer-second resolution"}}
    for delay in DELAYS:
        target=t0+delay;opts=[x for x in buys if x["blockTime"]>=target];obs=opts[0] if opts else None
        row={"certified":obs is not None,"target_time":target,"observed_trade":obs}
        if obs:
            p=float(obs["price_sol_per_token"]);row["entry_slippage_vs_source_bps"]=(p/source_price-1)*10000
            if first_sell and first_sell.get("price_sol_per_token"): row["gross_to_source_first_sell"]=(float(first_sell["price_sol_per_token"])/p)-1
            if final_sell and final_sell.get("price_sol_per_token"): row["gross_to_source_final_sell"]=(float(final_sell["price_sol_per_token"])/p)-1
        latency[f"{delay}s"]=row
    return {"market_buys":buys,"flow":flow,"latency":latency,"source_first_sell":first_sell,"source_final_sell":final_sell}


def run_probe(probe):
    joined=scan_last_event(probe)
    history=recover_source_history(probe,joined["wallet_token_accounts"])
    latency=replay_latency(probe,history["first_buy"],history["sells"])
    return {**probe,"join":joined,"source_history":history,"follower":latency}


def main():
    results=[]
    for p in PROBES:
        try: r=run_probe(p)
        except Exception as exc: r={**p,"error":repr(exc)}
        results.append(r);print("probe="+json.dumps(r,sort_keys=True),flush=True)
    summary={
        "probe_count":len(results),
        "complete_replays":sum("follower" in r for r in results),
        "latency_certified":{f"{d}s":sum(bool((r.get("follower") or {}).get("latency",{}).get(f"{d}s",{}).get("certified")) for r in results) for d in DELAYS},
        "results":results,
    }
    print("SOLANA_REMAINING_OOS_REPLAY="+json.dumps(summary,sort_keys=True),flush=True)
    with open("solana-remaining-oos-replay.json","w",encoding="utf-8") as f: json.dump(summary,f,sort_keys=True)
    if summary["complete_replays"]==0: raise SystemExit(4)

if __name__=="__main__": main()
