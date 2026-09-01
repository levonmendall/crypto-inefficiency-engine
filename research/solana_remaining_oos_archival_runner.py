from __future__ import annotations

import json
import os
import time

import research.solana_remaining_oos_replay as replay

ARCHIVAL_RPC = "https://api.mainnet-beta.solana.com"


def main() -> None:
    wanted = int(os.environ["PROBE_INDEX"])
    probe = next(p for p in replay.PROBES if int(p["index"]) == wanted)

    replay.RPC = ARCHIVAL_RPC
    base_call = replay.call

    # The official public RPC has the archival blocks that PublicNode prunes, but
    # it is rate-limited. Apply a small deterministic throttle while preserving
    # the existing retry/backoff behavior.
    def throttled_call(method, params):
        time.sleep(0.08)
        return base_call(method, params)

    replay.call = throttled_call
    try:
        result = replay.run_probe(probe)
    except Exception as exc:
        result = {**probe, "rpc": ARCHIVAL_RPC, "error": repr(exc)}

    result["rpc"] = ARCHIVAL_RPC
    path = f"solana-oos-archival-{wanted}.json"
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(result, handle, sort_keys=True)
    print("SOLANA_OOS_ARCHIVAL_PROBE=" + json.dumps(result, sort_keys=True), flush=True)
    if "follower" not in result:
        raise SystemExit(4)


if __name__ == "__main__":
    main()
