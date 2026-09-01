from __future__ import annotations

import collections
import datetime as dt
import html
import json
import re
import statistics
import time
import urllib.request

START = int(dt.datetime(2026, 8, 2, 19, 45, tzinfo=dt.timezone.utc).timestamp())
END = int(dt.datetime(2026, 9, 1, 19, 45, tzinfo=dt.timezone.utc).timestamp())
PER_PAGE = 50
KOLS = ("cented", "cupsey", "decu", "kadenox", "theo", "jijo", "kev")
# Fixed before outcomes: one target every three days across the full 30-day window.
TARGET_AGES_DAYS = tuple(range(1, 30, 3))
ROW_RE = re.compile(
    r'<tr class="data-row clickable-row"\s+'
    r'data-ca="(?P<ca>[^"]+)"\s+'
    r'data-last="(?P<last>\d+)"\s+'
    r'data-token="(?P<token>[^"]*)"\s+'
    r'data-tx="(?P<tx>[-+0-9.eE]+)"\s+'
    r'data-vol="(?P<vol>[-+0-9.eE]+)"\s+'
    r'data-pnl="(?P<pnl>[-+0-9.eE]+)"\s+'
    r'data-roi="(?P<roi>[-+0-9.eE]+)"', re.I,
)
CACHE: dict[tuple[str, int], list[dict[str, object]]] = {}


def fetch_page(kol: str, page: int) -> list[dict[str, object]]:
    key = (kol, page)
    if key in CACHE:
        return CACHE[key]
    url = f"https://kolexplorer.com/token/kol/{kol}?page={page}&per={PER_PAGE}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=20) as response:
                raw = response.read().decode("utf-8", "replace")
            rows = []
            for match in ROW_RE.finditer(raw):
                g = match.groupdict()
                rows.append({
                    "kol": kol, "page": page, "ca": html.unescape(g["ca"]),
                    "last": int(g["last"]), "token": html.unescape(g["token"]),
                    "tx": float(g["tx"]), "vol": float(g["vol"]),
                    "pnl": float(g["pnl"]), "roi": float(g["roi"]),
                })
            CACHE[key] = rows
            return rows
        except Exception:
            if attempt == 3:
                raise
            time.sleep(0.35 * (attempt + 1))
    raise AssertionError("unreachable")


def med(rows: list[dict[str, object]]) -> int | None:
    vals = [int(row["last"]) for row in rows]
    return int(statistics.median(vals)) if vals else None


def estimated_page(kol: str, target: int) -> int:
    p1, p11 = fetch_page(kol, 1), fetch_page(kol, 11)
    t1, t11 = med(p1), med(p11)
    if t1 is None or t11 is None or t11 >= t1:
        return 1
    pages_per_second = 10.0 / float(t1 - t11)
    return max(1, int(round(1.0 + max(0, t1 - target) * pages_per_second)))


def main() -> None:
    sampled_rows: list[dict[str, object]] = []
    pages_used: dict[str, list[int]] = {}
    for kol in KOLS:
        bases = {estimated_page(kol, END - days * 86400) for days in TARGET_AGES_DAYS}
        # Two deterministic adjacent pages around each estimated time target.
        expanded = sorted({p for base in bases for p in (base, base + 1) if p >= 1})
        pages_used[kol] = expanded
        before = len(sampled_rows)
        for page in expanded:
            sampled_rows.extend(
                row for row in fetch_page(kol, page)
                if START <= int(row["last"]) <= END
            )
        print(f"kol={kol} pages={len(expanded)} rows_in_window={len(sampled_rows)-before}", flush=True)

    grouped: dict[str, list[dict[str, object]]] = collections.defaultdict(list)
    for row in sampled_rows:
        grouped[str(row["ca"])].append(row)

    episodes = []
    for ca, rows in grouped.items():
        rows = sorted(rows, key=lambda r: (int(r["last"]), str(r["kol"])))
        episodes.append({
            "ca": ca,
            "time_proxy": min(int(r["last"]) for r in rows),
            "kols": sorted({str(r["kol"]) for r in rows}),
            "wallet_count": len({str(r["kol"]) for r in rows}),
            "source_roi_mean": statistics.fmean(float(r["roi"]) for r in rows),
            "source_pnl_sum": sum(float(r["pnl"]) for r in rows),
            "source_volume_sum": sum(float(r["vol"]) for r in rows),
            "records": len(rows),
        })
    episodes.sort(key=lambda e: (int(e["time_proxy"]), str(e["ca"])))

    holdout_n = 1000 if len(episodes) >= 3000 else 0
    train = episodes[:-holdout_n] if holdout_n else episodes
    holdout = episodes[-holdout_n:] if holdout_n else []
    day_counts = collections.Counter(
        dt.datetime.fromtimestamp(int(e["time_proxy"]), dt.timezone.utc).strftime("%Y-%m-%d")
        for e in episodes
    )
    wallet_counts = collections.Counter()
    for e in episodes:
        wallet_counts.update(e["kols"])
    rois = [float(e["source_roi_mean"]) for e in episodes]
    result = {
        "window_start": "2026-08-02T19:45:00Z", "window_end": "2026-09-01T19:45:00Z",
        "fixed_wallets": list(KOLS), "target_age_days": list(TARGET_AGES_DAYS),
        "sampled_wallet_token_rows": len(sampled_rows),
        "independent_unique_token_episodes": len(episodes),
        "train_episode_count": len(train), "locked_holdout_episode_count": len(holdout),
        "meets_2000_train_plus_1000_holdout": len(train) >= 2000 and len(holdout) >= 1000,
        "distinct_calendar_days": len(day_counts),
        "calendar_day_counts": dict(sorted(day_counts.items())),
        "wallet_episode_counts": dict(sorted(wallet_counts.items())),
        "pages_used": pages_used,
        "source_roi_descriptive_only": {
            "mean": statistics.fmean(rois) if rois else None,
            "median": statistics.median(rois) if rois else None,
            "positive_fraction": sum(r > 0 for r in rois) / len(rois) if rois else None,
            "gte_100_fraction": sum(r >= 100 for r in rois) / len(rois) if rois else None,
        },
        "limitations": [
            "time_proxy is earliest observed wallet-token LAST-trade second, not first-entry time",
            "source ROI is descriptive only and is not follower P&L",
            "selection uses only fixed wallet identity and fixed target dates, never PnL/ROI outcomes",
        ],
    }
    with open("solana-alpha-episode-universe.json", "w", encoding="utf-8") as handle:
        json.dump({"summary": result, "episodes": episodes}, handle, sort_keys=True)
    print("SOLANA_ALPHA_EPISODE_UNIVERSE=" + json.dumps(result, sort_keys=True), flush=True)
    if not result["meets_2000_train_plus_1000_holdout"]:
        raise SystemExit(3)


if __name__ == "__main__":
    main()
