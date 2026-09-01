from __future__ import annotations

import json
import re
import urllib.request

URL = "https://kolexplorer.com/token/kol/kadenox?page=1&per=50"
req = urllib.request.Request(URL, headers={"User-Agent": "Mozilla/5.0"})
with urllib.request.urlopen(req, timeout=30) as response:
    raw = response.read().decode("utf-8", "replace")

# Use values independently visible on the page to locate one server-rendered row.
needles = [
    "GgPBgn8Fm1whMxazJWXXa8xVeRSzaPqUoaXL4zHpUGGq",
    "1788282245",
    "last_trade_blocktime",
    "first_trade_blocktime",
]
snippets = {}
for needle in needles:
    idx = raw.find(needle)
    if idx >= 0:
        snippets[needle] = re.sub(r"\s+", " ", raw[max(0, idx - 900): idx + 1800])

# Inventory data-* attributes and field names that appear near the table body.
data_attrs = sorted(set(re.findall(r"\b(data-[a-zA-Z0-9_-]+)=", raw)))
field_names = sorted(set(re.findall(r"\b([a-zA-Z_][a-zA-Z0-9_]*(?:blocktime|timestamp|signature|tx_hash|first_trade|last_trade)[a-zA-Z0-9_]*)\b", raw, re.I)))
print("KOLEXPLORER_ROW_SCHEMA=" + json.dumps({
    "bytes": len(raw),
    "data_attributes": data_attrs,
    "field_names": field_names,
    "snippets": snippets,
}, sort_keys=True))
