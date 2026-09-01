from __future__ import annotations

import json
import time
import urllib.error
import urllib.request

SLOTS = {
    "oos_5251": 442971450,
    "recent_5750": 443504308,
}
ENDPOINTS = [
    "https://api.mainnet-beta.solana.com",
    "https://solana-rpc.publicnode.com",
    "https://solana.drpc.org/",
    "https://rpc.ankr.com/solana",
]


def post(url, method, params):
    payload={"jsonrpc":"2.0","id":1,"method":method,"params":params}
    req=urllib.request.Request(url,data=json.dumps(payload).encode(),headers={"Content-Type":"application/json","User-Agent":"cie-archival-probe/1"})
    started=time.monotonic()
    try:
        with urllib.request.urlopen(req,timeout=30) as r:
            raw=r.read();status=getattr(r,"status",200)
        body=json.loads(raw)
        return {"http_status":status,"elapsed_s":round(time.monotonic()-started,3),"error":body.get("error"),"has_result":body.get("result") is not None,"result_summary":summarize(body.get("result"))}
    except urllib.error.HTTPError as exc:
        text=exc.read().decode("utf-8","replace")[:500]
        return {"http_status":exc.code,"elapsed_s":round(time.monotonic()-started,3),"error":"HTTPError: "+text,"has_result":False}
    except Exception as exc:
        return {"http_status":None,"elapsed_s":round(time.monotonic()-started,3),"error":repr(exc),"has_result":False}


def summarize(result):
    if result is None:return None
    if isinstance(result,int):return result
    if isinstance(result,dict):return {"blockTime":result.get("blockTime"),"transaction_count":len(result.get("transactions") or []),"signature_count":len(result.get("signatures") or [])}
    return str(type(result))


def main():
    results={}
    for url in ENDPOINTS:
        endpoint={}
        for label,slot in SLOTS.items():
            endpoint[label+"_block_time"]=post(url,"getBlockTime",[slot])
            endpoint[label+"_block_signatures"]=post(url,"getBlock",[slot,{"encoding":"json","transactionDetails":"signatures","rewards":False,"commitment":"confirmed","maxSupportedTransactionVersion":0}])
        results[url]=endpoint
        print("endpoint="+json.dumps({url:endpoint},sort_keys=True),flush=True)
    print("SOLANA_ARCHIVAL_ENDPOINT_PROBE="+json.dumps(results,sort_keys=True),flush=True)
    with open("solana-archival-endpoint-probe.json","w",encoding="utf-8") as f:json.dump(results,f,sort_keys=True)

if __name__=="__main__":main()
