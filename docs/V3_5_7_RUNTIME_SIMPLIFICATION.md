# V3.5.7 Runtime Simplification

Production evidence showed that v3.5.6 fixed market-evidence fanout but the Render Starter worker continued to restart its entire process tree before the first canonical portfolio cycle completed. The Starter instance has a constrained memory envelope, while the worker topology had grown to a supervisor process, research child, portfolio child, and disposable provider/allocation/certification subprocesses.

V3.5.7 keeps the important isolation boundary between broad research and canonical portfolio operation, but removes the now-redundant disposable portfolio stage processes. Individual provider surfaces and order-book requests remain independently bounded by the v3.5.6 timeout layer, and the operating loop plus parent supervisor retain their portfolio/certification deadlines and watchdog.

The `cie worker` entrypoint is also lazy-loaded so the lightweight supervisor no longer imports or constructs the full research, portfolio, or stage service graph before spawning its children.

Safety semantics are unchanged: paper only, fixed canonical $250,000 genesis, no threshold reductions, no live-money authority, and provider failures remain fail-closed.
