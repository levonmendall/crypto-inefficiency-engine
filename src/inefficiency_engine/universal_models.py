from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field, model_validator


def _now() -> datetime:
    return datetime.now(timezone.utc)


class UniversalFamily(str, Enum):
    STABLECOIN_DISLOCATION = "stablecoin_dislocation"
    CEX_DEX = "cex_dex"
    DEX_DEX = "dex_dex"
    CROSS_CHAIN = "cross_chain"
    LIQUIDATION_BACKSTOP = "liquidation_backstop"
    SOLVER = "solver"
    OPTION_RELATIVE_VALUE = "option_relative_value"


class StablecoinConversionObservation(BaseModel):
    venue: str
    base_currency: str
    quote_currency: str
    symbol: str
    bid: float = Field(gt=0)
    ask: float = Field(gt=0)
    mid: float = Field(gt=0)
    observed_at: datetime = Field(default_factory=_now)
    source: str

    @model_validator(mode="after")
    def validate_spread(self):
        if self.bid > self.ask:
            raise ValueError("bid cannot exceed ask")
        return self


class StablecoinConversionEdge(BaseModel):
    source_currency: str
    target_currency: str
    venue: str
    rate: float = Field(gt=0)
    spread_bps: float = Field(ge=0)
    depeg_bps: float = Field(ge=0)
    risk_haircut_bps: float = Field(ge=0)
    total_conversion_cost_bps: float = Field(ge=0)
    observed_at: datetime
    source: str
    usable: bool = True
    reason: str | None = None


class ChainToken(BaseModel):
    token_id: str
    chain_id: str
    address: str
    symbol: str
    name: str | None = None
    canonical_asset: str | None = None
    decimals: int | None = Field(default=None, ge=0, le=36)


class DexPoolSnapshot(BaseModel):
    chain_id: str
    dex_id: str
    pair_address: str
    base_token: ChainToken
    quote_token: ChainToken
    price_native: float | None = Field(default=None, gt=0)
    price_usd: float | None = Field(default=None, gt=0)
    liquidity_usd: float | None = Field(default=None, ge=0)
    reported_base_liquidity: float | None = Field(default=None, ge=0)
    reported_quote_liquidity: float | None = Field(default=None, ge=0)
    volume_24h_usd: float | None = Field(default=None, ge=0)
    observed_at: datetime = Field(default_factory=_now)
    source: str = "dexscreener"
    depth_model: Literal["reported_liquidity_proxy", "constant_product_exact"] = "reported_liquidity_proxy"
    executable_depth_supported: bool = False


class BridgeQuote(BaseModel):
    provider: str
    asset: str
    origin_chain_id: str
    destination_chain_id: str
    input_amount: float = Field(gt=0)
    output_amount: float = Field(gt=0)
    fee_bps: float = Field(ge=0)
    expected_fill_seconds: float = Field(ge=0)
    settlement_risk_haircut_bps: float = Field(ge=0)
    observed_at: datetime = Field(default_factory=_now)
    expires_at: datetime
    source: str
    executable_eligible: bool = False
    blocked_reason: str | None = None


class OptionQuote(BaseModel):
    venue: str = "Deribit"
    asset: str
    instrument_name: str
    option_type: Literal["call", "put"]
    strike: float = Field(gt=0)
    expires_at: datetime
    bid_price: float | None = Field(default=None, ge=0)
    ask_price: float | None = Field(default=None, ge=0)
    mark_price: float | None = Field(default=None, ge=0)
    mark_iv: float | None = Field(default=None, ge=0)
    underlying_price: float | None = Field(default=None, gt=0)
    open_interest: float | None = Field(default=None, ge=0)
    observed_at: datetime = Field(default_factory=_now)
    source: str


class ExternalOpportunitySignal(BaseModel):
    family: Literal["liquidation_backstop", "solver"]
    provider: str
    asset: str
    gross_edge_bps: float
    modeled_cost_bps: float = Field(ge=0)
    risk_haircut_bps: float = Field(ge=0)
    capacity_usd: float = Field(gt=0)
    observed_at: datetime = Field(default_factory=_now)
    expires_at: datetime
    source: str
    authoritative_capacity: bool = False
    executable_eligible: bool = False

    @property
    def net_edge_bps(self) -> float:
        return self.gross_edge_bps - self.modeled_cost_bps - self.risk_haircut_bps


class UniversalCandidate(BaseModel):
    candidate_id: str
    family: UniversalFamily
    asset: str
    gross_edge_bps: float
    modeled_cost_bps: float = Field(ge=0)
    risk_haircut_bps: float = Field(ge=0)
    net_edge_bps: float
    capacity_usd: float | None = Field(default=None, ge=0)
    observed_at: datetime
    expires_at: datetime
    executable_eligible: bool = False
    blocked_reason: str | None = None
    evidence: dict[str, object] = Field(default_factory=dict)
    paper_only: bool = True


class UniversalNode(BaseModel):
    node_id: str
    kind: str
    label: str
    metadata: dict[str, object] = Field(default_factory=dict)


class UniversalEdge(BaseModel):
    edge_id: str
    source_id: str
    target_id: str
    kind: str
    cost_bps: float | None = Field(default=None, ge=0)
    risk_haircut_bps: float | None = Field(default=None, ge=0)
    executable_eligible: bool = False
    metadata: dict[str, object] = Field(default_factory=dict)


class UniversalGraphSnapshot(BaseModel):
    graph_version: str = "v0.9.2"
    observed_at: datetime
    nodes: list[UniversalNode] = Field(default_factory=list)
    edges: list[UniversalEdge] = Field(default_factory=list)
    candidates: list[UniversalCandidate] = Field(default_factory=list)
    capability_status: dict[str, str] = Field(default_factory=dict)
    paper_only: bool = True

    def summary(self) -> dict[str, object]:
        return {
            "graph_version": self.graph_version,
            "node_count": len(self.nodes),
            "edge_count": len(self.edges),
            "candidate_count": len(self.candidates),
            "candidate_families": sorted({item.family.value for item in self.candidates}),
            "executable_candidate_count": sum(1 for item in self.candidates if item.executable_eligible),
            "capability_status": dict(self.capability_status),
            "paper_only": True,
        }
