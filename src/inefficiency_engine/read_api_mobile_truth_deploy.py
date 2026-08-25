from __future__ import annotations

from inefficiency_engine import read_api_bounded_heartbeat_deploy as bounded
from inefficiency_engine import read_api_card_history_deploy as cards


_MOBILE_TRUTH_STYLE = r'''
<style id="mobile-truth-repair">
html,body{max-width:100%;overflow-x:hidden}
.wrap,.section,.hero-card,.card,.metric,.stat,.runtime-card,.item,.queue-item,.history-item{min-width:0;max-width:100%}
.item-title,.item-sub,.d,.reason,.next,.meta,.section-note{overflow-wrap:anywhere;word-break:break-word}
@media(max-width:650px){
  .cardmetrics,.strip{grid-template-columns:1fr}
  .item-top,.cardhead,.section-head{flex-wrap:wrap;min-width:0}
  .badge{white-space:normal;max-width:100%;overflow-wrap:anywhere;text-align:center}
  .status-row{align-items:flex-start}
  .status-val{max-width:58%;overflow-wrap:anywhere}
}
</style>
'''

_OLD_FAMILY_LABEL = '<span class="muted">Opportunity families</span>'
_NEW_FAMILY_LABEL = '<span class="muted">Allocation family gates</span>'
_OLD_FAMILY_STATE = "$('familyStatus').textContent=failures.length?`${failures.length} degraded`:'Healthy';"
_NEW_FAMILY_STATE = "$('familyStatus').textContent=failures.length?`${failures.length} degraded`:'No family-level failures';"
_FORWARD_STAT_HELPER_MARKER = "function researchTimeline(c){"
_FORWARD_STAT_HELPER = "function forwardStatPct(outcomes,value){return (+outcomes||0)>0?pct(value):'—'}\n"
_FORWARD_STAT_REPLACEMENTS = (
    (
        "mean ${pct(s.mean_forward_net_return)}",
        "mean ${forwardStatPct(s.forward_outcomes,s.mean_forward_net_return)}",
    ),
    (
        "CI ${pct(s.mean_forward_net_return_ci_lower)}",
        "CI ${forwardStatPct(s.forward_outcomes,s.mean_forward_net_return_ci_lower)}",
    ),
    (
        "hit CI ${pct(s.forward_hit_rate_ci_lower)}",
        "hit CI ${forwardStatPct(s.forward_outcomes,s.forward_hit_rate_ci_lower)}",
    ),
    (
        "${metric('Forward mean',pct(c.mean_forward_net_return),'net return')}",
        "${metric('Forward mean',forwardStatPct(c.forward_outcome_count,c.mean_forward_net_return),'net return')}",
    ),
    (
        "${metric('CI lower',pct(c.mean_forward_net_return_ci_lower),'forward mean lower bound')}",
        "${metric('CI lower',forwardStatPct(c.forward_outcome_count,c.mean_forward_net_return_ci_lower),'forward mean lower bound')}",
    ),
    (
        "${metric('Hit rate',pct(c.forward_hit_rate),'forward outcomes')}",
        "${metric('Hit rate',forwardStatPct(c.forward_outcome_count,c.forward_hit_rate),'forward outcomes')}",
    ),
)

_original_dashboard_html = cards._dashboard_html


def repaired_dashboard_html() -> str:
    """Keep dashboard truth explicit and make dense diagnostic cards fit mobile."""

    html = _original_dashboard_html()
    html = html.replace(_OLD_FAMILY_LABEL, _NEW_FAMILY_LABEL, 1)
    html = html.replace(_OLD_FAMILY_STATE, _NEW_FAMILY_STATE, 1)
    html = html.replace(
        _FORWARD_STAT_HELPER_MARKER,
        _FORWARD_STAT_HELPER + _FORWARD_STAT_HELPER_MARKER,
        1,
    )
    for old, new in _FORWARD_STAT_REPLACEMENTS:
        html = html.replace(old, new, 1)
    html = html.replace("</head>", _MOBILE_TRUTH_STYLE + "</head>", 1)
    return html


cards._dashboard_html = repaired_dashboard_html
app = bounded.app


__all__ = ["app", "repaired_dashboard_html"]
