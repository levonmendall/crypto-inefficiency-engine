from __future__ import annotations

import json
import re
import urllib.request

URLS = {
    "history": "https://kolexplorer.com/token/kol/kadenox?page=1&per=50",
    "token": "https://kolexplorer.com/token/G8CG3LdHVjBfFF2WRoTMJaYt9JozVBP2gfDerygQpump",
}

out = {}
for name, url in URLS.items():
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as response:
        raw = response.read().decode("utf-8", "replace")
    fields = sorted(set(re.findall(
        r"\b([a-zA-Z_][a-zA-Z0-9_]*(?:blocktime|timestamp|signature|tx_hash|first_trade|last_trade|buy_time|sell_time)[a-zA-Z0-9_]*)\b",
        raw, re.I,
    )))
    signatures = re.findall(r"(?<![1-9A-HJ-NP-Za-km-z])([1-9A-HJ-NP-Za-km-z]{80,90})(?![1-9A-HJ-NP-Za-km-z])", raw)
    attrs = sorted(set(re.findall(r"\b(data-[a-zA-Z0-9_-]+)=", raw)))
    ajax = sorted(set(re.findall(r"(?:ajax|action|endpoint)[=:]['\"]?([a-zA-Z0-9_./?=&-]+)", raw, re.I)))
    snippets = {}
    low = raw.lower()
    for term in ("first_trade", "last_trade", "signature", "tx_hash", "blocktime", "trades", "swap"):
        pos = low.find(term)
        if pos >= 0:
            snippets[term] = re.sub(r"\s+", " ", raw[max(0,pos-300):pos+800])
    out[name] = {
        "url": url,
        "bytes": len(raw),
        "fields": fields,
        "data_attributes": attrs,
        "base58_signature_candidates": signatures[:10],
        "base58_signature_candidate_count": len(signatures),
        "ajax_tokens": ajax[:30],
        "snippets": snippets,
    }
print("KOLEXPLORER_TOKEN_METADATA=" + json.dumps(out, sort_keys=True))
