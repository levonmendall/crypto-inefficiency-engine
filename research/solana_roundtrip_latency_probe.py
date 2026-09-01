from __future__ import annotations

import json
import time
import urllib.request

RPC="https://solana-rpc.publicnode.com"
MINT="4xoDsc7Rt16zi5ohm2sraVoFFnNHmaG4xVNUbGqypump"
SOURCE="CyaE1VxvBrahnPWkqm5VsdCvyS2QmNht2UFrKJHga54o"
ENTRY_TIME=1788291152
ENTRY_SLOT=443503484
SOURCE_FIRST_SELL_TIME=1788291414
SOURCE_FIRST_SELL_SLOT=443504304
DELAYS=(1,3,5,10)
ENTRY_PRICES={1:9.690894174740576e-08,3:1.450789504598305e-07,5:1.7487837800396354e-07,10:2.494666738541888e-07}
ENTRY_PROXY_FEES={1:0.000205,3:0.000076,5:0.0001041,10:0.00007493}


def call(method,params):
    req=urllib.request.Request(RPC,data=json.dumps({"jsonrpc":"2.0","id":1,"method":method,"params":params}).encode(),headers={"Content-Type":"application/json","User-Agent":"cie-roundtrip-latency/1"})
    last=None
    for attempt in range(5):
        try:
            with urllib.request.urlopen(req,timeout=45) as r:body=json.loads(r.read())
            if "error" in body:raise RuntimeError(body["error"])
            return body.get("result")
        except Exception as exc:
            last=exc
            if attempt==4:raise
            time.sleep(.5*(attempt+1))
    raise last


def keys(transaction):
    return [k.get("pubkey") if isinstance(k,dict) else k for k in (transaction.get("message") or {}).get("accountKeys") or []]


def amount(row):
    u=row.get("uiTokenAmount") or {};raw=u.get("amount")
    return 0.0 if raw is None else int(raw)/(10**int(u.get("decimals") or 0))


def token_delta(meta,owner):
    pre={};post={}
    for row in meta.get("preTokenBalances") or []:
        if row.get("mint")==MINT and row.get("owner")==owner:pre[row.get("accountIndex")]=amount(row)
    for row in meta.get("postTokenBalances") or []:
        if row.get("mint")==MINT and row.get("owner")==owner:post[row.get("accountIndex")]=amount(row)
    return sum(post.values())-sum(pre.values())


def native_delta(meta,ks,owner):
    if owner not in ks:return None
    i=ks.index(owner);pre=meta.get("preBalances") or [];post=meta.get("postBalances") or []
    if i>=len(pre) or i>=len(post):return None
    return (post[i]-pre[i])/1e9


def labels(meta):
    out=[]
    for line in meta.get("logMessages") or []:
        low=line.lower()
        if "instruction: sell" in low:out.append("sell")
        if "instruction: swap" in low:out.append("swap")
    return sorted(set(out))


def owners(meta):
    out=[]
    for row in (meta.get("preTokenBalances") or [])+(meta.get("postTokenBalances") or []):
        if row.get("mint")==MINT and row.get("owner") and row.get("owner") not in out:out.append(row["owner"])
    return out


def candidate_sells(block,slot):
    bt=int(block.get("blockTime") or 0);out=[]
    for txi,item in enumerate(block.get("transactions") or []):
        meta=item.get("meta") or {}
        if meta.get("err") is not None:continue
        labs=labels(meta)
        if "sell" not in labs and "swap" not in labs:continue
        transaction=item.get("transaction") or {};ks=keys(transaction);sigs=transaction.get("signatures") or [];fee=float(meta.get("fee") or 0)/1e9;payer=ks[0] if ks else None
        for owner in owners(meta):
            if owner==SOURCE:continue
            td=token_delta(meta,owner)
            if td>=0 or abs(td)<1000:continue
            nd=native_delta(meta,ks,owner)
            if nd is None or nd<=0:continue
            # Seller proceeds are already positive; network fee is separate and remains a follower cost.
            price=nd/abs(td)
            out.append({"signature":sigs[0] if sigs else None,"blockTime":bt,"slot":int(slot),"tx_index":txi,"owner":owner,"token_delta":td,"native_delta_sol":nd,"network_fee_sol":fee,"price_sol_per_token":price,"labels":labs})
    return out


def main():
    slots=call("getBlocks",[SOURCE_FIRST_SELL_SLOT,SOURCE_FIRST_SELL_SLOT+70,{"commitment":"confirmed"}]) or []
    cfg={"encoding":"jsonParsed","transactionDetails":"full","rewards":False,"commitment":"confirmed","maxSupportedTransactionVersion":0}
    sells=[];flow={}
    for slot in slots:
        block=call("getBlock",[int(slot),cfg])
        if not block:continue
        bt=block.get("blockTime")
        if bt is None or int(bt)<SOURCE_FIRST_SELL_TIME or int(bt)>SOURCE_FIRST_SELL_TIME+15:continue
        succ=fail=0
        for item in block.get("transactions") or []:
            meta=item.get("meta") or {};touch=any(r.get("mint")==MINT for r in (meta.get("preTokenBalances") or [])+(meta.get("postTokenBalances") or []))
            if touch:
                if meta.get("err") is None:succ+=1
                else:fail+=1
        b=flow.setdefault(str(int(bt)-SOURCE_FIRST_SELL_TIME),{"successful_mint_txs":0,"failed_mint_txs":0});b["successful_mint_txs"]+=succ;b["failed_mint_txs"]+=fail
        sells.extend(candidate_sells(block,slot))
    sells.sort(key=lambda x:(x["blockTime"],x["slot"],x["tx_index"]))
    roundtrips={}
    for d in DELAYS:
        target=SOURCE_FIRST_SELL_TIME+d;opts=[x for x in sells if x["blockTime"]>=target];exit_trade=opts[0] if opts else None
        row={"entry_delay_seconds":d,"exit_delay_seconds":d,"entry_price":ENTRY_PRICES[d],"source_sell_trigger_time":SOURCE_FIRST_SELL_TIME,"exit_target_time":target,"exit_trade":exit_trade}
        if exit_trade:
            xp=float(exit_trade["price_sol_per_token"]);ep=float(ENTRY_PRICES[d]);gross=xp/ep-1
            row.update({"certified":True,"exit_price":xp,"roundtrip_gross_return":gross,"entry_proxy_network_fee_sol":ENTRY_PROXY_FEES[d],"exit_proxy_network_fee_sol":float(exit_trade["network_fee_sol"]),"proxy_total_network_fee_sol":ENTRY_PROXY_FEES[d]+float(exit_trade["network_fee_sol"])})
        else:row.update({"certified":False,"reason":"no successful market sell found after delayed exit target"})
        roundtrips[f"{d}s"] = row
    result={"mint":MINT,"source":SOURCE,"entry_signal_time":ENTRY_TIME,"source_first_sell_time":SOURCE_FIRST_SELL_TIME,"roundtrips":roundtrips,"observed_market_sells":sells,"exit_flow_relative_seconds":flow}
    print("SOLANA_ROUNDTRIP_LATENCY="+json.dumps(result,sort_keys=True),flush=True)
    with open("solana-roundtrip-latency.json","w",encoding="utf-8") as f:json.dump(result,f,sort_keys=True)
    if not any(v.get("certified") for v in roundtrips.values()):raise SystemExit(4)

if __name__=="__main__":main()
