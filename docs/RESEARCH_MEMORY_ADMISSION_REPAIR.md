# Research memory admission repair

This repair closes a production deadlock where the combined supervisor could admit a disposable research process below the pre-spawn memory threshold, but the child could immediately re-apply the same start-block threshold after importing its own Python stack. Its bootstrap footprint could therefore push aggregate memory above the start threshold and cause exit code 75 before research ran. The supervisor would retry indefinitely while the dashboard remained readable but stale.

The supervisor is now the single pre-spawn start-block authority. Once the child exists, the child rejects only the harder aggregate terminate boundary; the supervisor continues monitoring aggregate memory while the job runs.

Research liveness now also requires both a recent research-worker heartbeat and a recent research dashboard projection. A fresh degraded research heartbeat cannot hide a failed projection publisher. Research jobs have a hard runtime bound, repeated unsuccessful recovery attempts remain accumulated instead of being cleared by every child exit, and prolonged inability to restore publication triggers a clean service restart.

No profitability, source, statistical, execution, settlement, risk, or live-money authority threshold is changed.
