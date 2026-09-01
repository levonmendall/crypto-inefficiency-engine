from __future__ import annotations

import json
import time
import urllib.request

RPC = "https://solana-rpc.publicnode.com"
TARGET = 1788122171
MINT = "DYhUVrUTCpw481ivCdaQ4uwF3BWvE7qNPq1Kv2nNpump"
LABELED_WALLET = "B32QbbdDAyhvUQzjcaM5j6ZVKwjCxAwGH5Xgvb9SJqnC"


def call(method, params):
    req=urllib.request.Request(RPC,data=json.dumps({"jsonrpc":"2.0","id":1,"method":method,"params":params}).encode(),headers={"Content-Type":"application/json","User-Agent":"cie-oos-5251-attribution/1"})
    last=None
    for attempt in range(6):
        try:
            with urllib.request.urlopen(req,timeout=45) as r: body=json.loads(r.read())
            if "error" in body: raise RuntimeError(body["error"])
            return body.get("result")
        except Exception as exc:
            last=exc
            if attempt==5: raise
            time.sleep(.5*(attempt+1))
    raise last


def bt(slot):
    try: return call("getBlockTime",[int(slot)])
    except Exception: return None


def nearest(slot):
    for d in range(25):
        for off in ((0,) if d==0 else (d,-d)):
            s=int(slot)+off;t=bt(s)
            if t is not None:return s,int(t)
    return None,None


def locate(target):
    tip=int(call("getSlot",[{"commitment":"confirmed"}]))
    ts,tt=nearest(tip);cs,ct=nearest(ts-100000);rate=(ts-cs)/float(tt-ct)
    estimate=ts-int(round((tt-target)*rate));trace=[]
    for _ in range(10):
        s,t=nearest(estimate);trace.append({"slot":s,"blockTime":t,"estimate":estimate})
        err=target-t
        if abs(err)<=1:return s,t,rate,trace
        estimate=s+int(round(err*rate))
    return s,t,rate,trace


def keys(transaction):
    return [x.get("pubkey") if isinstance(x,dict) else x for x in (transaction.get("message") or {}).get("accountKeys") or []]


def amount(row):
    u=row.get("uiTokenAmount") or {};raw=u.get("amount")
    return 0.0 if raw is None else int(raw)/(10**int(u.get("decimals") or 0))


def owner_deltas(meta):
    pre={};post={};owners={}
    for row in meta.get("preTokenBalances") or []:
        if row.get("mint")==MINT:
            k=(row.get("owner"),row.get("accountIndex"));pre[k]=amount(row);owners[k]=row.get("owner")
    for row in meta.get("postTokenBalances") or []:
        if row.get("mint")==MINT:
            k=(row.get("owner"),row.get("accountIndex"));post[k]=amount(row);owners[k]=row.get("owner")
    agg={}
    for k in set(pre)|set(post):
        owner=k[0] or "<unknown>";agg[owner]=agg.get(owner,0.0)+post.get(k,0.0)-pre.get(k,0.0)
    return {o:d for o,d in agg.items() if abs(d)>0}


def labels(meta):
    out=[]
    for line in meta.get("logMessages") or []:
        low=line.lower()
        for tag in ("buy","sell","swap","transfer"):
            if f"instruction: {tag}" in low and tag not in out:out.append(tag)
    return out


def main():
    anchor,anchor_time,rate,trace=locate(TARGET)
    slots=call("getBlocks",[anchor-35,anchor+35,{"commitment":"confirmed"}]) or []
    cfg={"encoding":"jsonParsed","transactionDetails":"full","rewards":False,"commitment":"confirmed","maxSupportedTransactionVersion":0}
    mint_txs=[];wallet_account_txs=[]
    for slot in slots:
        block=call("getBlock",[int(slot),cfg])
        if not block:continue
        btime=block.get("blockTime")
        if btime is None or abs(int(btime)-TARGET)>8:continue
        for idx,item in enumerate(block.get("transactions") or []):
            meta=item.get("meta") or {};transaction=item.get("transaction") or {};ks=keys(transaction);sigs=transaction.get("signatures") or []
            deltas=owner_deltas(meta)
            touches=bool(deltas) or any(r.get("mint")==MINT for r in (meta.get("preTokenBalances") or [])+(meta.get("postTokenBalances") or []))
            wallet_present=LABELED_WALLET in ks
            if touches:
                mint_txs.append({"signature":sigs[0] if sigs else None,"slot":int(slot),"blockTime":int(btime),"tx_index":idx,"err":meta.get("err"),"labels":labels(meta),"wallet_present":wallet_present,"owner_deltas":deltas,"fee_lamports":meta.get("fee")})
            if wallet_present:
                wallet_account_txs.append({"signature":sigs[0] if sigs else None,"slot":int(slot),"blockTime":int(btime),"tx_index":idx,"err":meta.get("err"),"labels":labels(meta),"touches_mint":touches,"owner_deltas":deltas})
    result={"target":TARGET,"mint":MINT,"labeled_wallet":LABELED_WALLET,"anchor_slot":anchor,"anchor_time":anchor_time,"slots_per_second":rate,"trace":trace,"mint_transactions":mint_txs,"labeled_wallet_account_transactions":wallet_account_txs,"mint_tx_count":len(mint_txs),"wallet_account_tx_count":len(wallet_account_txs)}
    print("SOLANA_OOS_5251_ATTRIBUTION="+json.dumps(result,sort_keys=True),flush=True)
    with open("solana-oos-5251-attribution.json","w",encoding="utf-8") as f:json.dump(result,f,sort_keys=True)

if __name__=="__main__":main()
