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

_original_dashboard_html = cards._dashboard_html


def repaired_dashboard_html() -> str:
    """Keep dashboard truth explicit and make dense diagnostic cards fit mobile."""

    html = _original_dashboard_html()
    html = html.replace(_OLD_FAMILY_LABEL, _NEW_FAMILY_LABEL, 1)
    html = html.replace(_OLD_FAMILY_STATE, _NEW_FAMILY_STATE, 1)
    html = html.replace("</head>", _MOBILE_TRUTH_STYLE + "</head>", 1)
    return html


cards._dashboard_html = repaired_dashboard_html
app = bounded.app


__all__ = ["app", "repaired_dashboard_html"]
