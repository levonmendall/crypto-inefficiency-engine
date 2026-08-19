from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import HTMLResponse


DASHBOARD_HTML = r'''<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover" />
  <meta name="theme-color" content="#071018" />
  <title>Crypto Opportunity Engine — Portfolio Command Center</title>
  <style>
    :root {
      color-scheme: dark;
      --bg:#071018; --panel:#0d1822; --panel2:#101f2b; --line:#203341;
      --text:#edf7fb; --muted:#8ea7b5; --good:#4ade80; --bad:#fb7185;
      --warn:#facc15; --accent:#67e8f9; --accent2:#38bdf8; --paper:#a78bfa;
    }
    *{box-sizing:border-box} body{margin:0;background:radial-gradient(circle at 15% -10%,#123246 0,transparent 34%),var(--bg);color:var(--text);font:14px/1.45 ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
    a{color:inherit} button{font:inherit}.shell{max-width:1440px;margin:0 auto;padding:18px clamp(14px,3vw,34px) 48px}
    .top{display:flex;gap:16px;justify-content:space-between;align-items:flex-start;margin-bottom:18px}.eyebrow{color:var(--accent);font-size:11px;font-weight:800;letter-spacing:.14em;text-transform:uppercase}.title{font-size:clamp(24px,5vw,40px);font-weight:800;letter-spacing:-.035em;margin:4px 0 5px}.sub{color:var(--muted);max-width:720px}
    .top-actions{display:flex;gap:8px;align-items:center;flex-wrap:wrap;justify-content:flex-end}.pill,.btn{border:1px solid var(--line);border-radius:999px;padding:8px 11px;background:#0a151e;color:var(--muted);font-weight:700;font-size:12px}.pill.paper{color:#ddd6fe;border-color:#4c3b72;background:#171329}.pill.on{color:#bbf7d0;border-color:#215836;background:#0b2115}.pill.off{color:#fecdd3;border-color:#66303c;background:#251017}.btn{cursor:pointer;color:var(--text)}.btn:active{transform:translateY(1px)}
    .hero{display:grid;grid-template-columns:minmax(0,1.6fr) minmax(280px,.8fr);gap:14px;margin-bottom:14px}.card{background:linear-gradient(180deg,rgba(16,31,43,.96),rgba(11,24,34,.96));border:1px solid var(--line);border-radius:18px;box-shadow:0 18px 60px rgba(0,0,0,.18)}.hero-main{padding:22px}.label{color:var(--muted);font-size:12px;font-weight:700;text-transform:uppercase;letter-spacing:.08em}.nav{font-size:clamp(38px,8vw,70px);font-weight:850;letter-spacing:-.055em;line-height:1;margin:10px 0 8px}.return{font-size:18px;font-weight:800}.good{color:var(--good)}.bad{color:var(--bad)}.muted{color:var(--muted)}
    .hero-side{padding:18px;display:grid;gap:12px;align-content:center}.status-row{display:flex;justify-content:space-between;gap:16px;padding-bottom:10px;border-bottom:1px solid var(--line)}.status-row:last-child{border:0;padding-bottom:0}.status-val{font-weight:800;text-align:right}
    .metrics{display:grid;grid-template-columns:repeat(8,minmax(120px,1fr));gap:10px;margin-bottom:14px}.metric{padding:14px}.metric .v{font-size:19px;font-weight:800;margin-top:4px;white-space:nowrap}.metric .k{color:var(--muted);font-size:11px;text-transform:uppercase;letter-spacing:.07em;font-weight:750}
    .grid2{display:grid;grid-template-columns:minmax(0,1.5fr) minmax(300px,.8fr);gap:14px;margin-bottom:14px}.section{padding:18px}.section-head{display:flex;justify-content:space-between;gap:12px;align-items:baseline;margin-bottom:14px}.section-title{font-size:17px;font-weight:800}.section-note{color:var(--muted);font-size:12px}.chart-wrap{height:260px;position:relative}.chart{width:100%;height:100%;display:block}.chart-empty{position:absolute;inset:0;display:grid;place-items:center;color:var(--muted)}
    .attribution{display:grid;gap:9px}.bar-row{display:grid;grid-template-columns:minmax(110px,1fr) 2fr auto;gap:9px;align-items:center}.bar-name{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.bar-track{height:9px;border-radius:999px;background:#071019;overflow:hidden}.bar-fill{height:100%;border-radius:inherit;background:linear-gradient(90deg,var(--accent2),var(--accent))}.bar-fill.neg{background:linear-gradient(90deg,#fb7185,#f43f5e)}.bar-val{font-variant-numeric:tabular-nums;font-weight:750}
    .full{margin-bottom:14px}.table-wrap{overflow:auto;border-radius:12px;border:1px solid var(--line)}table{width:100%;border-collapse:collapse;min-width:720px}th,td{text-align:left;padding:11px 12px;border-bottom:1px solid var(--line);vertical-align:top}th{font-size:10px;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);background:#0a151e;position:sticky;top:0}tbody tr:last-child td{border-bottom:0}.num{text-align:right;font-variant-numeric:tabular-nums}.state{display:inline-flex;border-radius:999px;padding:4px 8px;font-size:10px;font-weight:850;letter-spacing:.04em;text-transform:uppercase;border:1px solid var(--line)}.state.certified{color:#bbf7d0;border-color:#215836}.state.certifying,.state.collecting{color:#bae6fd;border-color:#245270}.state.provider_gap,.state.settlement_blocked,.state.execution_blocked{color:#fde68a;border-color:#665c22}.state.poor_economics,.state.statistical_failure{color:#fecdd3;border-color:#66303c}
    .mobile-list{display:none}.item{padding:13px;border:1px solid var(--line);border-radius:13px;background:#0a151e;margin-bottom:8px}.item-top{display:flex;justify-content:space-between;gap:10px}.item-title{font-weight:800}.item-sub{color:var(--muted);font-size:12px;margin-top:4px}.item-pnl{font-weight:800;white-space:nowrap}
    .queue{display:grid;gap:8px}.queue-item{padding:12px;border:1px solid var(--line);border-radius:12px;background:#0a151e}.queue-title{display:flex;gap:8px;align-items:center;justify-content:space-between}.queue-reason{margin-top:6px;color:var(--muted)}.queue-action{margin-top:5px;color:#d7f4fb}.footer{display:flex;justify-content:space-between;gap:15px;color:var(--muted);font-size:11px;padding-top:4px}.error{display:none;background:#32121a;border:1px solid #743044;color:#fecdd3;border-radius:12px;padding:10px 12px;margin-bottom:14px}.error.show{display:block}
    @media(max-width:1050px){.metrics{grid-template-columns:repeat(4,1fr)}.hero,.grid2{grid-template-columns:1fr}}
    @media(max-width:650px){.shell{padding-left:12px;padding-right:12px}.top{display:block}.top-actions{justify-content:flex-start;margin-top:12px}.hero-main,.hero-side,.section{padding:15px}.metrics{grid-template-columns:repeat(2,1fr)}.metric .v{font-size:17px}.table-wrap{display:none}.mobile-list{display:block}.chart-wrap{height:210px}.footer{display:block}.footer>*{margin-top:4px}}
  </style>
</head>
<body>
<div class="shell">
  <header class="top">
    <div>
      <div class="eyebrow">Crypto Opportunity Engine</div>
      <h1 class="title">Portfolio Command Center</h1>
      <div class="sub">Canonical compounding paper account. Qualified opportunities can be opened automatically in paper form; unsupported settlement paths remain fail-closed.</div>
    </div>
    <div class="top-actions">
      <span class="pill paper">PAPER · $250K GENESIS</span>
      <span class="pill on">AUTO PAPER EXECUTION · ON</span>
      <span class="pill off">LIVE MONEY · DISABLED</span>
      <button class="btn" id="refreshBtn">Refresh</button>
    </div>
  </header>

  <div id="error" class="error"></div>

  <section class="hero">
    <div class="card hero-main">
      <div class="label">Current portfolio NAV</div>
      <div id="nav" class="nav">—</div>
      <div id="totalReturn" class="return muted">—</div>
      <div id="updated" class="muted" style="margin-top:9px">Awaiting portfolio data…</div>
    </div>
    <div class="card hero-side">
      <div class="status-row"><span class="muted">Portfolio</span><span class="status-val">Canonical / persistent</span></div>
      <div class="status-row"><span class="muted">Paper execution</span><span class="status-val good">Automatic</span></div>
      <div class="status-row"><span class="muted">Live execution</span><span class="status-val bad">No authority</span></div>
      <div class="status-row"><span class="muted">Mechanisms certified</span><span id="certifiedCount" class="status-val">—</span></div>
    </div>
  </section>

  <section class="metrics">
    <div class="card metric"><div class="k">Starting capital</div><div class="v">$250,000</div></div>
    <div class="card metric"><div class="k">Cash</div><div id="cash" class="v">—</div></div>
    <div class="card metric"><div class="k">Deployed</div><div id="deployed" class="v">—</div></div>
    <div class="card metric"><div class="k">Realized P&L</div><div id="realized" class="v">—</div></div>
    <div class="card metric"><div class="k">Unrealized P&L</div><div id="unrealized" class="v">—</div></div>
    <div class="card metric"><div class="k">Max drawdown</div><div id="maxdd" class="v">—</div></div>
    <div class="card metric"><div class="k">Open positions</div><div id="openCount" class="v">—</div></div>
    <div class="card metric"><div class="k">Closed trades</div><div id="tradeCount" class="v">—</div></div>
  </section>

  <section class="grid2">
    <div class="card section">
      <div class="section-head"><div class="section-title">Equity curve</div><div id="historyCount" class="section-note">NAV snapshots</div></div>
      <div class="chart-wrap"><canvas id="equityChart" class="chart"></canvas><div id="chartEmpty" class="chart-empty">Waiting for more NAV snapshots</div></div>
    </div>
    <div class="card section">
      <div class="section-head"><div class="section-title">P&L attribution</div><div class="section-note">Current + realized</div></div>
      <div id="attribution" class="attribution"><div class="muted">No attribution yet.</div></div>
    </div>
  </section>

  <section class="card section full">
    <div class="section-head"><div class="section-title">Open paper positions</div><div class="section-note">Marked automatically until modeled horizon</div></div>
    <div class="table-wrap"><table><thead><tr><th>Asset / strategy</th><th>Venue</th><th>Opened</th><th>Due</th><th class="num">Capital</th><th class="num">Entry</th><th class="num">Mark</th><th class="num">Unrealized P&L</th></tr></thead><tbody id="positionsBody"></tbody></table></div>
    <div id="positionsMobile" class="mobile-list"></div>
  </section>

  <section class="grid2">
    <div class="card section">
      <div class="section-head"><div class="section-title">Recent completed trades</div><div class="section-note">Realized paper outcomes</div></div>
      <div id="tradesList" class="mobile-list" style="display:block"></div>
    </div>
    <div class="card section">
      <div class="section-head"><div class="section-title">Skipped / rejected allocations</div><div class="section-note">Fail-closed reasons</div></div>
      <div id="skipsList" class="mobile-list" style="display:block"></div>
    </div>
  </section>

  <section class="card section full">
    <div class="section-head"><div class="section-title">Profit mechanism certification</div><div class="section-note">Provider → evidence → economics → execution → settlement → certification</div></div>
    <div class="table-wrap"><table><thead><tr><th>Mechanism</th><th>Status</th><th class="num">Forward outcomes</th><th class="num">Allocator settlements</th><th class="num">Forward mean</th><th>Current reason</th></tr></thead><tbody id="mechanismsBody"></tbody></table></div>
    <div id="mechanismsMobile" class="mobile-list"></div>
  </section>

  <section class="card section full">
    <div class="section-head"><div class="section-title">What needs attention next</div><div class="section-note">Generated from current operating blockers</div></div>
    <div id="actionQueue" class="queue"><div class="muted">No action queue yet.</div></div>
  </section>

  <footer class="footer"><div>Auto-refresh: 30 seconds · Source: canonical durable evidence database</div><div>Paper-only research system · No private keys, custody, signing, deposits, withdrawals, or live order submission</div></footer>
</div>
<script>
const $=id=>document.getElementById(id);
const money=n=>Number.isFinite(+n)?new Intl.NumberFormat('en-US',{style:'currency',currency:'USD',maximumFractionDigits:2}).format(+n):'—';
const pct=n=>Number.isFinite(+n)?`${(+n*100).toFixed(2)}%`:'—';
const num=n=>Number.isFinite(+n)?new Intl.NumberFormat('en-US').format(+n):'—';
const when=s=>s?new Date(s).toLocaleString([], {month:'short',day:'numeric',hour:'numeric',minute:'2-digit'}):'—';
const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function pnlClass(n){return +n>0?'good':(+n<0?'bad':'muted')}
async function getJSON(url){const r=await fetch(url,{cache:'no-store'});if(!r.ok)throw new Error(`${url}: HTTP ${r.status}`);return r.json()}
function itemEmpty(text){return `<div class="muted">${esc(text)}</div>`}
function renderChart(snapshots){
  const c=$('equityChart'), empty=$('chartEmpty'), rows=[...(snapshots||[])].reverse().filter(x=>Number.isFinite(+x.nav_usd));
  if(rows.length<2){empty.style.display='grid'; const ctx=c.getContext('2d');ctx.clearRect(0,0,c.width,c.height);return}
  empty.style.display='none'; const dpr=window.devicePixelRatio||1, rect=c.getBoundingClientRect();c.width=Math.max(1,rect.width*dpr);c.height=Math.max(1,rect.height*dpr);
  const ctx=c.getContext('2d');ctx.scale(dpr,dpr);const w=rect.width,h=rect.height,p=22;const vals=rows.map(x=>+x.nav_usd), lo=Math.min(...vals,250000), hi=Math.max(...vals,250000);const span=Math.max(1,hi-lo);
  ctx.clearRect(0,0,w,h);ctx.strokeStyle='#203341';ctx.lineWidth=1;for(let i=0;i<4;i++){const y=p+(h-2*p)*i/3;ctx.beginPath();ctx.moveTo(p,y);ctx.lineTo(w-p,y);ctx.stroke()}
  ctx.strokeStyle='#67e8f9';ctx.lineWidth=2.5;ctx.beginPath();rows.forEach((r,i)=>{const x=p+(w-2*p)*(i/(rows.length-1));const y=h-p-(h-2*p)*((+r.nav_usd-lo)/span);i?ctx.lineTo(x,y):ctx.moveTo(x,y)});ctx.stroke();
  ctx.setLineDash([5,5]);ctx.strokeStyle='#475569';const y0=h-p-(h-2*p)*((250000-lo)/span);ctx.beginPath();ctx.moveTo(p,y0);ctx.lineTo(w-p,y0);ctx.stroke();ctx.setLineDash([]);
}
function renderAttribution(data){const m=data?.pnl_by_mechanism_usd||{}, s=data?.pnl_by_strategy_usd||{};let rows=[...Object.entries(m).map(([k,v])=>[`Mechanism · ${k}`,v]),...Object.entries(s).map(([k,v])=>[`Strategy · ${k}`,v])];rows=rows.filter(([,v])=>Number.isFinite(+v)).sort((a,b)=>Math.abs(+b[1])-Math.abs(+a[1])).slice(0,8);if(!rows.length){$('attribution').innerHTML=itemEmpty('No realized or open-position attribution yet.');return}const max=Math.max(...rows.map(([,v])=>Math.abs(+v)),1);$('attribution').innerHTML=rows.map(([k,v])=>`<div class="bar-row"><div class="bar-name" title="${esc(k)}">${esc(k)}</div><div class="bar-track"><div class="bar-fill ${+v<0?'neg':''}" style="width:${Math.max(2,Math.abs(+v)/max*100)}%"></div></div><div class="bar-val ${pnlClass(v)}">${money(v)}</div></div>`).join('')}
function renderPositions(rows){rows=rows||[];$('positionsBody').innerHTML=rows.length?rows.map(p=>`<tr><td><strong>${esc(p.asset)}</strong><br><span class="muted">${esc(p.strategy)}</span></td><td>${esc(p.venue)}<br><span class="muted">${esc(p.symbol)}</span></td><td>${when(p.opened_at)}</td><td>${when(p.due_at)}</td><td class="num">${money(p.capital_reserved_usd)}</td><td class="num">${money(p.entry_reference_price)}</td><td class="num">${money(p.current_reference_price)}</td><td class="num ${pnlClass(p.unrealized_pnl_usd)}">${money(p.unrealized_pnl_usd)}</td></tr>`).join(''):`<tr><td colspan="8" class="muted">No open paper positions.</td></tr>`;$('positionsMobile').innerHTML=rows.length?rows.map(p=>`<div class="item"><div class="item-top"><div><div class="item-title">${esc(p.asset)} · ${esc(p.strategy)}</div><div class="item-sub">${esc(p.venue)} · due ${when(p.due_at)} · ${money(p.capital_reserved_usd)} reserved</div></div><div class="item-pnl ${pnlClass(p.unrealized_pnl_usd)}">${money(p.unrealized_pnl_usd)}</div></div></div>`).join(''):itemEmpty('No open paper positions.')}
function renderTrades(rows){rows=rows||[];$('tradesList').innerHTML=rows.length?rows.map(t=>`<div class="item"><div class="item-top"><div><div class="item-title">${esc(t.asset||'—')} · ${esc(t.strategy||'—')}</div><div class="item-sub">${esc(t.venue||'—')} · closed ${when(t.observed_at)}</div></div><div class="item-pnl ${pnlClass(t.realized_pnl_delta_usd)}">${money(t.realized_pnl_delta_usd)}</div></div></div>`).join(''):itemEmpty('No completed paper trades yet.')}
function renderSkips(rows){rows=rows||[];$('skipsList').innerHTML=rows.length?rows.map(t=>`<div class="item"><div class="item-title">${esc(t.asset||'—')} · ${esc(t.strategy||t.family||'allocation')}</div><div class="item-sub">${when(t.observed_at)} · ${esc(t.reason||'No reason recorded')}</div></div>`).join(''):itemEmpty('No skipped allocations recorded.')}
function renderMechanisms(rows){rows=rows||[];const state=r=>`<span class="state ${esc(r.state)}">${esc((r.state||'unknown').replaceAll('_',' '))}</span>`;$('mechanismsBody').innerHTML=rows.length?rows.map(r=>`<tr><td><strong>${esc(r.name)}</strong><br><span class="muted">${esc(r.mechanism_id)}</span></td><td>${state(r)}</td><td class="num">${num(r.independent_forward_outcome_count)}</td><td class="num">${num(r.settled_allocator_outcome_count)}</td><td class="num ${pnlClass(r.mean_forward_net_return)}">${pct(r.mean_forward_net_return)}</td><td>${esc(r.primary_reason)}</td></tr>`).join(''):`<tr><td colspan="6" class="muted">No certification snapshot yet.</td></tr>`;$('mechanismsMobile').innerHTML=rows.length?rows.map(r=>`<div class="item"><div class="item-top"><div class="item-title">${esc(r.name)}</div>${state(r)}</div><div class="item-sub">${esc(r.primary_reason)}</div></div>`).join(''):itemEmpty('No certification snapshot yet.')}
function renderQueue(rows){rows=rows||[];$('actionQueue').innerHTML=rows.length?rows.slice(0,12).map(r=>`<div class="queue-item"><div class="queue-title"><strong>${esc(r.name)}</strong><span class="state ${esc(r.state)}">${esc((r.state||'').replaceAll('_',' '))}</span></div><div class="queue-reason">${esc(r.primary_reason)}</div><div class="queue-action">Next: ${esc(r.next_action)}</div></div>`).join(''):itemEmpty('No unresolved mechanism actions.')}
async function refresh(){
  $('refreshBtn').disabled=true;$('error').classList.remove('show');
  try{
    const [portfolio,performance,positions,trades,history,skips,attribution,mechanisms,queue]=await Promise.all([
      getJSON('/v3/portfolio/canonical'),getJSON('/v3/portfolio/performance'),getJSON('/v3/portfolio/positions'),getJSON('/v3/portfolio/trades?limit=20'),getJSON('/v3/portfolio/history?limit=500'),getJSON('/v3/portfolio/skips?limit=20'),getJSON('/v3/portfolio/attribution'),getJSON('/v3/operations/mechanisms'),getJSON('/v3/operations/action-queue')
    ]);
    const nav=+performance.current_nav_usd, ret=+performance.total_return;$('nav').textContent=money(nav);$('totalReturn').textContent=`${ret>=0?'+':''}${pct(ret)} since $250,000 genesis`;$('totalReturn').className=`return ${pnlClass(ret)}`;$('updated').textContent=portfolio.observed_at?`Last portfolio snapshot ${new Date(portfolio.observed_at).toLocaleString()}`:'Portfolio awaiting first worker snapshot';
    $('cash').textContent=money(performance.cash_usd);$('deployed').textContent=money(performance.reserved_capital_usd);$('realized').textContent=money(performance.realized_pnl_usd);$('realized').className=`v ${pnlClass(performance.realized_pnl_usd)}`;$('unrealized').textContent=money(performance.unrealized_pnl_usd);$('unrealized').className=`v ${pnlClass(performance.unrealized_pnl_usd)}`;$('maxdd').textContent=pct(performance.max_drawdown_fraction);$('openCount').textContent=num(performance.open_position_count);$('tradeCount').textContent=num(performance.closed_trade_count);
    const mechRows=mechanisms.mechanisms||[];$('certifiedCount').textContent=`${mechRows.filter(x=>x.state==='certified').length} / ${mechRows.length}`;$('historyCount').textContent=`${history.count||0} NAV snapshots`;
    renderChart(history.snapshots);renderAttribution(attribution);renderPositions(positions.positions);renderTrades(trades.trades);renderSkips(skips.skips);renderMechanisms(mechRows);renderQueue(queue.actions);
  }catch(e){$('error').textContent=`Dashboard refresh failed: ${e.message}`;$('error').classList.add('show')}
  finally{$('refreshBtn').disabled=false}
}
$('refreshBtn').addEventListener('click',refresh);window.addEventListener('resize',()=>refresh());refresh();setInterval(refresh,30000);
</script>
</body>
</html>'''


def build_dashboard_router() -> APIRouter:
    router = APIRouter()

    @router.get("/", include_in_schema=False, response_class=HTMLResponse)
    def dashboard_root() -> HTMLResponse:
        return HTMLResponse(DASHBOARD_HTML, headers={"Cache-Control": "no-store"})

    @router.get("/dashboard", include_in_schema=False, response_class=HTMLResponse)
    def portfolio_dashboard() -> HTMLResponse:
        return HTMLResponse(DASHBOARD_HTML, headers={"Cache-Control": "no-store"})

    return router
