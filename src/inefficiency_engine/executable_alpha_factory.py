from __future__ import annotations

from inefficiency_engine.alpha_factory import AlphaStrategyRegistry
from inefficiency_engine.alpha_refinements import (
    BtcRelativeResidualMeanReversionStrategy,
    CrossVenueResidualMeanReversionStrategy,
    LiquidityConditionedMeanReversionStrategy,
    MultiHorizonMeanReversionStrategy,
    OnChainFactorBreadthStrategy,
    TradeFlowLeadLagStrategy,
    VolatilityConditionedMeanReversionStrategy,
)
from inefficiency_engine.memory_bounded_alpha_factory import MemoryBoundedExpandedAlphaFactoryService
from inefficiency_engine.trade_flow import TradeFlowImbalanceStrategy, TradeFlowLedger


class ExecutableExpandedAlphaFactoryService(MemoryBoundedExpandedAlphaFactoryService):
    """Memory-bounded alpha factory with isolated refinement cohorts enabled.

    Existing strategy evidence is never rewritten. Every refinement has a new
    strategy ID and therefore must build its own independent forward record under
    the existing multiple-testing, statistical, health and execution gates.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.trade_flow_ledger = TradeFlowLedger(self.store)
        strategies = list(getattr(self.registry, "_strategies", []))
        additions = [
            TradeFlowImbalanceStrategy(self.trade_flow_ledger),
            TradeFlowLeadLagStrategy(self.trade_flow_ledger),
            CrossVenueResidualMeanReversionStrategy(),
            MultiHorizonMeanReversionStrategy(),
            VolatilityConditionedMeanReversionStrategy(),
            LiquidityConditionedMeanReversionStrategy(),
            BtcRelativeResidualMeanReversionStrategy(),
            OnChainFactorBreadthStrategy(self.fundamental_ledger),
        ]
        existing = {item.manifest.strategy_id for item in strategies}
        for strategy in additions:
            if strategy.manifest.strategy_id not in existing:
                strategies.append(strategy)
                existing.add(strategy.manifest.strategy_id)
        self.registry = AlphaStrategyRegistry(strategies)
