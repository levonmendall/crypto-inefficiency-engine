from __future__ import annotations

from collections import defaultdict
from typing import Literal, Protocol

from pydantic import BaseModel, Field


ExposureKind = Literal["market_neutral", "directional_long", "directional_short"]


class RiskCandidate(Protocol):
    candidate_id: str
    family: str
    strategy: str
    capital_required_usd: float
    exposure_kind: ExposureKind


class PortfolioRiskBudget(BaseModel):
    total_capital_usd: float = Field(gt=0)
    alpha_capital_usd: float = Field(ge=0)
    market_neutral_capital_usd: float = Field(ge=0)
    directional_capital_usd: float = Field(ge=0)
    directional_long_capital_usd: float = Field(ge=0)
    directional_short_capital_usd: float = Field(ge=0)
    directional_net_capital_usd: float
    strategy_capital_usd: dict[str, float] = Field(default_factory=dict)
    paper_only: bool = True


class PortfolioRiskDecision(BaseModel):
    candidate_id: str
    accepted: bool
    reason: str | None = None
    paper_only: bool = True


class PortfolioRiskOverlay:
    """Cross-strategy paper risk budget applied after opportunity qualification.

    Existing venue/asset/conflict gates stop local concentration. This overlay
    protects the portfolio from a different failure mode: many individually good
    predictive strategies quietly stacking the same directional crypto beta.

    The overlay can only reject candidates. It cannot make an ineligible candidate
    allocatable and it never authorizes execution.
    """

    def __init__(self, settings, *, total_capital_usd: float):
        if total_capital_usd <= 0:
            raise ValueError("total_capital_usd must be positive")
        self.total = total_capital_usd
        self.max_alpha_fraction = float(getattr(settings, "allocator_max_alpha_fraction", 0.40))
        self.max_directional_fraction = float(getattr(settings, "allocator_max_directional_fraction", 0.35))
        self.max_same_direction_fraction = float(getattr(settings, "allocator_max_same_direction_fraction", 0.25))
        self.max_alpha_strategy_fraction = float(getattr(settings, "allocator_max_alpha_strategy_fraction", 0.20))
        for name, value in {
            "allocator_max_alpha_fraction": self.max_alpha_fraction,
            "allocator_max_directional_fraction": self.max_directional_fraction,
            "allocator_max_same_direction_fraction": self.max_same_direction_fraction,
            "allocator_max_alpha_strategy_fraction": self.max_alpha_strategy_fraction,
        }.items():
            if not 0 < value <= 1:
                raise ValueError(f"{name} must be in (0, 1]")
        self.alpha_used = 0.0
        self.market_neutral_used = 0.0
        self.directional_used = 0.0
        self.long_used = 0.0
        self.short_used = 0.0
        self.strategy_used: dict[str, float] = defaultdict(float)

    def decision(self, candidate: RiskCandidate) -> PortfolioRiskDecision:
        capital = candidate.capital_required_usd
        if capital <= 0:
            return PortfolioRiskDecision(
                candidate_id=candidate.candidate_id,
                accepted=False,
                reason="non-positive capital requirement",
            )
        if candidate.family == "alpha":
            if self.alpha_used + capital > self.total * self.max_alpha_fraction + 1e-9:
                return PortfolioRiskDecision(
                    candidate_id=candidate.candidate_id,
                    accepted=False,
                    reason="predictive alpha portfolio risk budget",
                )
            if self.strategy_used[candidate.strategy] + capital > self.total * self.max_alpha_strategy_fraction + 1e-9:
                return PortfolioRiskDecision(
                    candidate_id=candidate.candidate_id,
                    accepted=False,
                    reason="predictive alpha strategy concentration budget",
                )
        if candidate.exposure_kind != "market_neutral":
            if self.directional_used + capital > self.total * self.max_directional_fraction + 1e-9:
                return PortfolioRiskDecision(
                    candidate_id=candidate.candidate_id,
                    accepted=False,
                    reason="total directional risk budget",
                )
            if candidate.exposure_kind == "directional_long":
                if self.long_used + capital > self.total * self.max_same_direction_fraction + 1e-9:
                    return PortfolioRiskDecision(
                        candidate_id=candidate.candidate_id,
                        accepted=False,
                        reason="long directional risk budget",
                    )
            elif candidate.exposure_kind == "directional_short":
                if self.short_used + capital > self.total * self.max_same_direction_fraction + 1e-9:
                    return PortfolioRiskDecision(
                        candidate_id=candidate.candidate_id,
                        accepted=False,
                        reason="short directional risk budget",
                    )
        return PortfolioRiskDecision(candidate_id=candidate.candidate_id, accepted=True)

    def register(self, candidate: RiskCandidate) -> None:
        decision = self.decision(candidate)
        if not decision.accepted:
            raise ValueError(decision.reason or "candidate violates portfolio risk budget")
        capital = candidate.capital_required_usd
        self.strategy_used[candidate.strategy] += capital
        if candidate.family == "alpha":
            self.alpha_used += capital
        if candidate.exposure_kind == "market_neutral":
            self.market_neutral_used += capital
        else:
            self.directional_used += capital
            if candidate.exposure_kind == "directional_long":
                self.long_used += capital
            else:
                self.short_used += capital

    def snapshot(self) -> PortfolioRiskBudget:
        return PortfolioRiskBudget(
            total_capital_usd=self.total,
            alpha_capital_usd=self.alpha_used,
            market_neutral_capital_usd=self.market_neutral_used,
            directional_capital_usd=self.directional_used,
            directional_long_capital_usd=self.long_used,
            directional_short_capital_usd=self.short_used,
            directional_net_capital_usd=self.long_used - self.short_used,
            strategy_capital_usd=dict(sorted(self.strategy_used.items())),
            paper_only=True,
        )
