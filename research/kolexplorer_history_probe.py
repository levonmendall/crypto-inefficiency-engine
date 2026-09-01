from __future__ import annotations

import json
import re
import urllib.request

URL = "https://kolexplorer.com/token/kol/kadenox?page=1&per=50"
req = urllib.request.Request(URL, headers={"User-Agent": "Mozilla/5.0"})
with urllib.request.urlopen(req, timeout=30) as response:
    raw = response.read().decode("utf-8", "replace")

patterns = {
    "unix_10digit": r"(?<!\d)(1[6-9]\d{8})(?!\d)",
    "iso_timestamp": r"20\d\d-\d\d-\d\d[T ][0-2]\d:[0-5]\d:[0-5]\d(?:\.\d+)?Z?",
    "solscan_tx": r"(?:solscan\.io/tx/|/tx/)([1-9A-HJ-NP-Za-km-z]{70,100})",
    "signature_json": r'(?i)["\'](?:signature|tx_hash|txHash|transactionSignature)["\']\s*[:=]\s*["\']([1-9A-HJ-NP-Za-km-z]{70,100})',
    "timestamp_key": r'(?i)["\'](?:timestamp|blockTime|block_time|last_trade_at|first_trade_at)["\']\s*[:=]\s*["\']?([^"\',}< ]+)',
    "api_paths": r'["\']([^"\']*(?:api|ajax)[^"\']*)["\']',
}
result = {"url": URL, "bytes": len(raw)}
for name, pattern in patterns.items():
    matches = re.findall(pattern, raw)
    result[name] = matches[:20]
    result[name + "_count"] = len(matches)

# Emit snippets around potentially useful metadata terms without dumping page contents.
terms = ["blockTime", "timestamp", "signature", "tx_hash", "first_trade", "last_trade", "data-ts", "data-time"]
snippets = {}
low = raw.lower()
for term in terms:
    idx = low.find(term.lower())
    if idx >= 0:
        snippets[term] = re.sub(r"\s+", " ", raw[max(0, idx-180):idx+400])
result["snippets"] = snippets
print("KOLEXPLORER_HISTORY_PROBE=" + json.dumps(result, sort_keys=True))
