from __future__ import annotations

import json
import time
import urllib.request

RPC = "https://solana-rpc.publicnode.com"
WALLET = "CyaE1VxvBrahnPWkqm5VsdCvyS2QmNht2UFrKJHga54o"
MINT = "4xoDsc7Rt16zi5ohm2sraVoFFnNHmaG4xVNUbGqypump"
TOKEN_ACCOUNT = "58o8mJ8B5Pron41Rx5BTovC8H37HQ9Dvj4n62FPnb7xb"
LOCKED_LAST_TIME = 1788291415


def call(payload):
    req=urllib.request.Request(RPC,data=json.dumps(payload).encode(),headers={"Content-Type":"application/json","User-Agent":"cie-actual-account-history/2"})
    last=None
    for attempt in range(5):
        try:
            with urllib.request.urlopen(req,timeout=35) as response: return json.loads(response.read())
        except Exception as exc:
            last=exc
            if attempt==4: raise
            time.sleep(0.5*(attempt+1))
    raise last


def one(method,params):
    body=call({"jsonrpc":"2.0","id":1,"method":method,"params":params})
    if "error" in body: raise RuntimeError(body["error"])
    return body.get("result")


def signatures():
    out=[]; before=None
    for _ in range(4):
        cfg={"limit":1000,"commitment":"confirmed"}
        if before: cfg["before"]=before
        page=one("getSignaturesForAddress",[TOKEN_ACCOUNT,cfg]) or []
        if not page: break
        out.extend(page)
        if len(page)<1000: break
        before=page[-1].get("signature")
        if not before: break
    return out


def transactions(sigs):
    out=[]; cfg={"encoding":"jsonParsed","commitment":"confirmed","maxSupportedTransactionVersion":0}
    for sig in sigs[:250]:
        try: tx=one("getTransaction",[sig,cfg])
        except Exception: continue
        if tx is not None: out.append((sig,tx))
    return out


def amount(row):
    x=row.get("uiTokenAmount") or {}; raw=x.get("amount")
    return 0.0 if raw is None else int(raw)/(10**int(x.get("decimals") or 0))


def account_keys(tx):
    return [k.get("pubkey") if isinstance(k,dict) else k for k in (tx.get("transaction") or {}).get("message",{}).get("accountKeys") or []]


def token_delta(tx):
    meta=tx.get("meta") or {}; pre={}; post={}
    for row in meta.get("preTokenBalances") or []:
        if row.get("mint")==MINT and row.get("owner")==WALLET: pre[row.get("accountIndex")]=amount(row)
    for row in meta.get("postTokenBalances") or []:
        if row.get("mint")==MINT and row.get("owner")==WALLET: post[row.get("accountIndex")]=amount(row)
    return sum(post.values())-sum(pre.values())


def native_delta(tx):
    keys=account_keys(tx)
    if WALLET not in keys: return None
    idx=keys.index(WALLET); meta=tx.get("meta") or {}; pre=meta.get("preBalances") or []; post=meta.get("postBalances") or []
    return None if idx>=len(pre) or idx>=len(post) else (post[idx]-pre[idx])/1e9


def mint_accounts(tx):
    keys=account_keys(tx); meta=tx.get("meta") or {}; out=[]; seen=set()
    for row in (meta.get("preTokenBalances") or [])+(meta.get("postTokenBalances") or []):
        if row.get("mint")!=MINT: continue
        idx=row.get("accountIndex")
        if isinstance(idx,int) and idx<len(keys) and keys[idx] not in seen:
            seen.add(keys[idx]); out.append({"account":keys[idx],"owner":row.get("owner")})
    return out


def classify_logs(tx):
    labels=[]
    for line in (tx.get("meta") or {}).get("logMessages") or []:
        low=line.lower()
        if "instruction: buy" in low: labels.append("buy")
        if "instruction: sell" in low: labels.append("sell")
        if "instruction: swap" in low: labels.append("swap")
    return sorted(set(labels))


def summary(sig,tx):
    meta=tx.get("meta") or {}; td=token_delta(tx); nd=native_delta(tx); fee=float(meta.get("fee") or 0)/1e9
    economic=nd+fee if nd is not None and nd<0 else nd
    price=abs(economic/td) if td and economic is not None and abs(economic)>1e-6 else None
    return {"signature":sig,"blockTime":tx.get("blockTime"),"slot":tx.get("slot"),"err":meta.get("err"),"fee_sol":fee,"token_delta":td,"native_delta_sol":nd,"economic_native_delta_sol":economic,"execution_price_sol_per_token":price,"log_trade_labels":classify_logs(tx),"mint_accounts":mint_accounts(tx)}


def main():
    sig_rows=signatures()
    print("ACCOUNT_SIGNATURE_COUNT="+str(len(sig_rows)),flush=True)
    tx_pairs=transactions([r["signature"] for r in sig_rows])
    rows=[summary(sig,tx) for sig,tx in tx_pairs]
    rows=[r for r in rows if r["token_delta"]!=0 or r["err"] is not None]
    rows.sort(key=lambda x:(int(x.get("blockTime") or 0),int(x.get("slot") or 0)))
    buys=[r for r in rows if r["err"] is None and ("buy" in r["log_trade_labels"] or (r["token_delta"]>0 and (r["economic_native_delta_sol"] or 0)<-0.001))]
    sells=[r for r in rows if r["err"] is None and ("sell" in r["log_trade_labels"] or (r["token_delta"]<0 and (r["economic_native_delta_sol"] or 0)>0.001))]
    result={"wallet":WALLET,"mint":MINT,"token_account":TOKEN_ACCOUNT,"locked_last_time":LOCKED_LAST_TIME,"signature_count":len(sig_rows),"transaction_count":len(tx_pairs),"wallet_mint_transactions":rows,"first_buy":buys[0] if buys else None,"classified_buy_count":len(buys),"classified_sell_count":len(sells),"last_trade":rows[-1] if rows else None}
    print("SOLANA_ACTUAL_ACCOUNT_HISTORY="+json.dumps(result,sort_keys=True),flush=True)
    with open("solana-actual-account-history.json","w",encoding="utf-8") as f: json.dump(result,f,sort_keys=True)
    if not buys: raise SystemExit(4)

if __name__=="__main__": main()
