from __future__ import annotations

from inefficiency_engine.alpha_factory import AlphaStrategyRegistry
from inefficiency_engine.memory_bounded_alpha_factory import MemoryBoundedExpandedAlphaFactoryService
from inefficiency_engine.trade_flow import TradeFlowImbalanceStrategy, TradeFlowLedger


class ExecutableExpandedAlphaFactoryService(MemoryBoundedExpandedAlphaFactoryService):
    """Memory-bounded alpha factory with real public trade-flow alpha enabled.

    The new strategy shares the existing forward/statistical/health/execution gates.
    Recording public trades never grants allocation authority by itself.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.trade_flow_ledger = TradeFlowLedger(self.store)
        strategies = list(getattr(self.registry, "_strategies", []))
        if not any(item.manifest.strategy_id == "public_trade_flow_imbalance_v1" for item in strategies):
            strategies.append(TradeFlowImbalanceStrategy(self.trade_flow_ledger))
        self.registry = AlphaStrategyRegistry(strategies)
