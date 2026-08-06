'use strict';

// ── State ──────────────────────────────────────────────────────────────────────
let ws = null, retryDelay = 1000, retryTimer = null;
let isPaused = false, startTime = null, uptimeTimer = null;
let allAlerts = [];          // full alert log
let patternCounts = {FAN_OUT:0, FAN_IN:0, SCATTER_GATHER:0, VELOCITY_ABUSE:0};
let txnFeedCount = 0;
const MAX_TXN_ROWS = 40;
const MAX_DETECT_ROWS = 50;

// ── Page Navigation ────────────────────────────────────────────────────────────
function switchPage(name, btn) {
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(b => b.classList.remove('active'));
  document.getElementById('page-' + name).classList.add('active');
  btn.classList.add('active');
  if (name === 'graph') GraphVis.resize();
}

// ── WebSocket ──────────────────────────────────────────────────────────────────
function connect() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  ws = new WebSocket(`${proto}://${location.host}/ws/stream`);

  ws.onopen = () => {
    retryDelay = 1000;
    setConn('connected', 'Connected');
    if (!startTime) { startTime = Date.now(); startUptime(); }
  };
  ws.onmessage = e => { try { route(JSON.parse(e.data)); } catch(_) {} };
  ws.onclose = () => { setConn('', 'Reconnecting…'); schedule(); };
  ws.onerror = () => { setConn('error', 'Error'); };
}

function schedule() {
  clearTimeout(retryTimer);
  retryTimer = setTimeout(() => { connect(); retryDelay = Math.min(retryDelay * 1.5, 30000); }, retryDelay);
}

function wsSend(obj) { if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj)); }

// ── Message Router ─────────────────────────────────────────────────────────────
function route(msg) {
  switch (msg.type) {
    case 'connected':
      updateMetrics(msg.metrics);
      if (msg.recent_alerts) msg.recent_alerts.slice().reverse().forEach(ingestAlert);
      break;
    case 'txn_tick':
      if (msg.graph) GraphVis.update(msg.graph);
      if (msg.metrics) updateMetrics(msg.metrics);
      appendTxn(msg);
      break;
    case 'fraud_alert':
      if (msg.alert) { ingestAlert(msg.alert); }
      if (msg.metrics) updateMetrics(msg.metrics);
      break;
    case 'metrics_tick':
      updateMetrics(msg.metrics);
      break;
  }
}

// ── Metrics ────────────────────────────────────────────────────────────────────
function updateMetrics(m) {
  if (!m) return;
  setText('m-tps',       fmt1(m.current_tps));
  setText('m-processed', fmtN(m.total_processed));
  setText('m-blocked',   fmtN(m.total_blocked));
  setText('m-flagged',   fmtN(m.total_flagged));
  setText('m-latency',   fmt1(m.avg_latency_ms));
  setText('m-sla',       m.sla_compliance != null ? (m.sla_compliance * 100).toFixed(1) + '%' : '—');

  // System page
  setText('sys-processed', fmtN(m.total_processed));
  setText('sys-alerts',    fmtN(m.total_alerts));
  setText('sys-blocked',   fmtN(m.total_blocked));
  setText('sys-flagged',   fmtN(m.total_flagged));
  setText('sys-nodes',     fmtN(m.graph_nodes));
  setText('sys-edges',     fmtN(m.graph_edges));
  setText('sys-tps',       fmt1(m.current_tps));
  setText('sys-uptime',    fmtUptime(m.uptime_s));

  // Graph page stats
  setText('gs-nodes',   fmtN(m.graph_nodes));
  setText('gs-edges',   fmtN(m.graph_edges));

  // Latency gauge + table
  drawGauge(m.avg_latency_ms || 0);
  setText('gauge-val', Math.round(m.avg_latency_ms || 0));
  const gv = document.getElementById('gauge-val');
  if (gv) gv.style.color = (m.avg_latency_ms >= 200) ? 'var(--red)' : (m.avg_latency_ms >= 150) ? 'var(--amber)' : 'var(--green)';

  setText('lt-p50',     m.p50_latency_ms  != null ? m.p50_latency_ms.toFixed(1) + 'ms' : '—');
  setText('lt-p95',     m.p95_latency_ms  != null ? m.p95_latency_ms.toFixed(1) + 'ms' : '—');
  setText('lt-p99',     m.p99_latency_ms  != null ? m.p99_latency_ms.toFixed(1) + 'ms' : '—');
  setText('lt-breaches', fmtN(m.sla_breaches));

  // Emitter
  if (m.emitter) {
    const em = m.emitter;
    setText('em-mode',    em.mode || '—');
    setText('em-total',   fmtN(em.total_emitted));
    setText('em-fraud',   fmtN(em.fraud_emitted));
    setText('em-rate',    em.fraud_rate != null ? (em.fraud_rate * 100).toFixed(2) + '%' : '—');
    setText('em-tps',     fmt1(em.current_tps));
    setText('em-running', em.paused ? 'Paused' : 'Running');
  }
}

// ── Transaction Feed (Monitor page) ────────────────────────────────────────────
function appendTxn(msg) {
  const feed = document.getElementById('txn-feed');
  const empty = feed.querySelector('.empty-msg');
  if (empty) empty.remove();

  const row = document.createElement('div');
  row.className = 'txn-row';
  const amount = msg.amount != null ? '₹' + msg.amount.toLocaleString('en-IN', {maximumFractionDigits: 0}) : '—';
  row.innerHTML = `
    <span class="txn-flow">${trunc(msg.sender,10)}→${trunc(msg.receiver,10)}</span>
    <span class="txn-amount">${amount}</span>
    <span class="txn-time">${new Date().toLocaleTimeString()}</span>`;
  feed.insertBefore(row, feed.firstChild);
  txnFeedCount++;
  if (txnFeedCount > MAX_TXN_ROWS && feed.lastChild) feed.removeChild(feed.lastChild);
}

// ── Alert Ingestion ────────────────────────────────────────────────────────────
function ingestAlert(a) {
  allAlerts.unshift(a);
  patternCounts[a.pattern] = (patternCounts[a.pattern] || 0) + 1;

  // Update nav badge
  const badge = document.getElementById('nav-alert-count');
  badge.textContent = allAlerts.length;
  badge.classList.add('show');

  // Update pattern chips
  setText('pc-fan-out',  patternCounts.FAN_OUT       || 0);
  setText('pc-fan-in',   patternCounts.FAN_IN         || 0);
  setText('pc-scatter',  patternCounts.SCATTER_GATHER || 0);
  setText('pc-velocity', patternCounts.VELOCITY_ABUSE || 0);
  setText('detection-count', allAlerts.length);

  // Detection feed (monitor page)
  addDetectionRow(a);

  // Alert table (alerts page)
  rebuildTable();

  // Graph stats
  const blocked = allAlerts.filter(x => x.verdict === 'BLOCK').length;
  const flagged  = allAlerts.filter(x => x.verdict === 'FLAG').length;
  setText('gs-blocked', blocked);
  setText('gs-flagged',  flagged);
}

function addDetectionRow(a) {
  const feed = document.getElementById('detection-feed');
  const empty = feed.querySelector('.empty-msg');
  if (empty) empty.remove();

  const row = document.createElement('div');
  row.className = 'detection-row';
  row.onclick = () => showAlert(a);
  row.innerHTML = `
    <div class="detection-header">
      <span class="detection-pattern ${patClass(a.pattern)}">${patLabel(a.pattern)}</span>
      <span class="badge badge--${a.verdict.toLowerCase()}">${a.verdict}</span>
    </div>
    <div class="detection-summary">${esc(a.summary || '')}</div>
    <div class="detection-meta">${fmt1(a.latency_ms)}ms · ${a.within_sla ? 'SLA ✓' : 'SLA ✗'} · ${new Date(a.timestamp).toLocaleTimeString()}</div>`;
  feed.insertBefore(row, feed.firstChild);
  while (feed.children.length > MAX_DETECT_ROWS) feed.removeChild(feed.lastChild);
}

// ── Alert Table ────────────────────────────────────────────────────────────────
function rebuildTable() {
  const pf = document.getElementById('alert-filter').value;
  const vf = document.getElementById('verdict-filter').value;
  const filtered = allAlerts.filter(a =>
    (pf === 'all' || a.pattern === pf) &&
    (vf === 'all' || a.verdict === vf)
  );

  const tbody = document.getElementById('alert-table-body');
  if (!filtered.length) {
    tbody.innerHTML = '<tr><td colspan="7" class="table-empty">No alerts match filter</td></tr>';
    return;
  }
  tbody.innerHTML = filtered.map(a => `
    <tr onclick="showAlert(${escJson(a)})">
      <td>${new Date(a.timestamp).toLocaleTimeString()}</td>
      <td class="${patClass(a.pattern)}">${patLabel(a.pattern)}</td>
      <td><span class="badge badge--${a.verdict.toLowerCase()}">${a.verdict}</span></td>
      <td><span class="mono">${(a.score||0).toFixed(4)}</span></td>
      <td><span class="mono">${fmt1(a.latency_ms)}ms</span></td>
      <td>${(a.accounts||[]).length}</td>
      <td><button class="detail-btn" onclick="event.stopPropagation();showAlert(${escJson(a)})">View →</button></td>
    </tr>`).join('');
}

function filterAlerts() { rebuildTable(); }
function setFilter(pattern) {
  document.getElementById('alert-filter').value = pattern;
  rebuildTable();
}

// ── Alert Detail Modal ─────────────────────────────────────────────────────────
function showAlert(a) {
  const overlay = document.getElementById('modal-overlay');
  const tag     = document.getElementById('modal-tag');
  const title   = document.getElementById('modal-title');
  const body    = document.getElementById('modal-body');

  tag.textContent  = a.verdict;
  tag.className    = 'modal-tag ' + a.verdict;
  title.textContent = patLabel(a.pattern);

  body.innerHTML = `
    <div class="modal-section">
      <div class="modal-section-title">Risk Score</div>
      <div class="modal-score ${a.verdict}">${(a.score||0).toFixed(4)}</div>
    </div>
    <div class="modal-section">
      <div class="modal-section-title">Detection</div>
      <div class="modal-section-body">${fmt1(a.latency_ms)}ms · ${a.within_sla ? '✓ Within SLA (200ms)' : '✗ SLA breach'} · ${new Date(a.timestamp).toLocaleString()}</div>
    </div>
    <div class="modal-section">
      <div class="modal-section-title">Causal Explanation</div>
      <div class="modal-section-body">${esc(a.summary || '')}</div>
    </div>
    <div class="modal-section">
      <div class="modal-section-title">Recommended Action</div>
      <div class="modal-action-text">${esc(a.action || 'No action specified')}</div>
    </div>
    <div class="modal-section">
      <div class="modal-section-title">Implicated Accounts (${(a.accounts||[]).length})</div>
      <div class="modal-accounts">${(a.accounts||[]).map(ac => `<span class="account-chip">${ac}</span>`).join('')}</div>
    </div>`;

  overlay.classList.add('open');
}

function closeAlert() { document.getElementById('modal-overlay').classList.remove('open'); }
function closeModal(e) { if (e.target === e.currentTarget) closeAlert(); }

// ── Inject Modal ───────────────────────────────────────────────────────────────
function openInjectModal()  { document.getElementById('inject-overlay').classList.add('open'); }
function closeInjectModal(e) {
  if (!e || e.target === e.currentTarget)
    document.getElementById('inject-overlay').classList.remove('open');
}

async function injectScenario(scenario) {
  closeInjectModal();
  document.getElementById('inject-overlay').classList.remove('open');
  try {
    const r = await fetch('/api/inject', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({scenario})
    });
    const d = await r.json();
    toast(`Injected: ${patLabel(scenario)} (${d.txn_count} txns)`);
  } catch(e) { toast('Injection failed', true); }
}

// ── Pause / Resume ─────────────────────────────────────────────────────────────
function togglePause() {
  isPaused = !isPaused;
  const btn = document.getElementById('btn-pause');
  if (isPaused) {
    wsSend({action:'pause'});
    btn.innerHTML = `<svg viewBox="0 0 16 16" fill="none"><path d="M5 3l8 5-8 5V3z" fill="currentColor"/></svg> Resume`;
    btn.classList.add('paused');
  } else {
    wsSend({action:'resume'});
    btn.innerHTML = `<svg viewBox="0 0 16 16" fill="none"><rect x="3" y="3" width="4" height="10" rx="1" fill="currentColor"/><rect x="9" y="3" width="4" height="10" rx="1" fill="currentColor"/></svg> Pause`;
    btn.classList.remove('paused');
  }
}

// ── Emitter TPS ────────────────────────────────────────────────────────────────
async function setTPS() {
  const tps = parseInt(document.getElementById('tps-input').value);
  if (!tps || tps < 1) return;
  await fetch('/api/emitter/tps', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({tps})});
  toast(`TPS set to ${tps}`);
}

async function apiPost(url) {
  await fetch(url, {method:'POST'});
  toast(url.split('/').pop() + ' applied');
}

// ── Latency Gauge ──────────────────────────────────────────────────────────────
function drawGauge(ms) {
  const canvas = document.getElementById('latency-gauge');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  const w = canvas.width, h = canvas.height;
  const cx = w/2, cy = h - 8, r = Math.min(w/2, h) - 14;
  ctx.clearRect(0, 0, w, h);

  // bg arc
  ctx.beginPath(); ctx.arc(cx, cy, r, Math.PI, 2*Math.PI);
  ctx.strokeStyle = 'rgba(255,255,255,0.06)'; ctx.lineWidth = 10; ctx.lineCap = 'round'; ctx.stroke();

  // value arc
  const frac = Math.min(ms / 400, 1);
  const endAngle = Math.PI + frac * Math.PI;
  const col = ms >= 200 ? '#ef4444' : ms >= 150 ? '#f59e0b' : '#22c55e';
  ctx.beginPath(); ctx.arc(cx, cy, r, Math.PI, endAngle);
  ctx.strokeStyle = col; ctx.lineWidth = 10; ctx.lineCap = 'round'; ctx.stroke();

  // SLA tick at 200ms (50%)
  const slaA = Math.PI * 1.5;
  ctx.beginPath();
  ctx.arc(cx + r * Math.cos(slaA), cy + r * Math.sin(slaA), 3, 0, 2*Math.PI);
  ctx.fillStyle = 'rgba(255,255,255,0.3)'; ctx.fill();
}

// ── Uptime ─────────────────────────────────────────────────────────────────────
function startUptime() {
  uptimeTimer = setInterval(() => {
    const s = Math.floor((Date.now() - startTime) / 1000);
    const h = String(Math.floor(s/3600)).padStart(2,'0');
    const m = String(Math.floor((s%3600)/60)).padStart(2,'0');
    const sec = String(s%60).padStart(2,'0');
    setText('uptime', `${h}:${m}:${sec}`);
  }, 1000);
}

// ── Connection status ──────────────────────────────────────────────────────────
function setConn(state, label) {
  const dot = document.getElementById('conn-dot');
  const lbl = document.getElementById('conn-label');
  if (dot) { dot.className = 'conn-dot'; if (state) dot.classList.add(state); }
  if (lbl) lbl.textContent = label;
}

// ── Toast ──────────────────────────────────────────────────────────────────────
function toast(msg, error=false) {
  const c = document.getElementById('toast-container');
  const t = document.createElement('div');
  t.className = 'toast';
  t.style.borderLeftColor = error ? 'var(--red)' : 'var(--green)';
  t.style.borderLeftWidth = '3px';
  t.style.borderLeftStyle = 'solid';
  t.textContent = msg;
  c.appendChild(t);
  setTimeout(() => t.remove(), 3000);
}

// ── Helpers ────────────────────────────────────────────────────────────────────
function setText(id, v) { const el = document.getElementById(id); if (el) el.textContent = v; }
function fmt1(n) { return n != null ? Number(n).toFixed(1) : '—'; }
function fmtN(n) { return n != null ? Math.round(n).toLocaleString('en-IN') : '—'; }
function fmtUptime(s) { if (!s) return '—'; const h=Math.floor(s/3600), m=Math.floor((s%3600)/60), sec=Math.floor(s%60); return `${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}:${String(sec).padStart(2,'0')}`; }
function trunc(s, n) { return s ? (s.length > n ? s.slice(0, n) + '…' : s) : '—'; }
function esc(s) { const d = document.createElement('div'); d.appendChild(document.createTextNode(s)); return d.innerHTML; }
function escJson(a) { return "'" + JSON.stringify(a).replace(/'/g, "\\'") + "'"; }

function patLabel(p) {
  return {FAN_OUT:'Fan-Out',FAN_IN:'Fan-In',SCATTER_GATHER:'Scatter-Gather',VELOCITY_ABUSE:'Velocity',fan_out:'Fan-Out',fan_in:'Fan-In',scatter_gather:'Scatter-Gather',velocity:'Velocity',random:'Random'}[p] || p;
}
function patClass(p) {
  return {FAN_OUT:'pat-fan-out',FAN_IN:'pat-fan-in',SCATTER_GATHER:'pat-scatter',VELOCITY_ABUSE:'pat-velocity'}[p] || '';
}

// Fix: showAlert called from inline onclick with JSON string
window.showAlert = function(a) {
  if (typeof a === 'string') { try { a = JSON.parse(a); } catch(_) { return; } }
  const overlay = document.getElementById('modal-overlay');
  document.getElementById('modal-tag').textContent  = a.verdict;
  document.getElementById('modal-tag').className    = 'modal-tag ' + a.verdict;
  document.getElementById('modal-title').textContent = patLabel(a.pattern);
  document.getElementById('modal-body').innerHTML = `
    <div class="modal-section">
      <div class="modal-section-title">Risk Score</div>
      <div class="modal-score ${a.verdict}">${(a.score||0).toFixed(4)}</div>
    </div>
    <div class="modal-section">
      <div class="modal-section-title">Detection Latency</div>
      <div class="modal-section-body">${fmt1(a.latency_ms)}ms · ${a.within_sla ? '✓ Within SLA (200ms)' : '✗ SLA breach'}</div>
    </div>
    <div class="modal-section">
      <div class="modal-section-title">Causal Explanation</div>
      <div class="modal-section-body">${esc(a.summary || '')}</div>
    </div>
    <div class="modal-section">
      <div class="modal-section-title">Recommended Action</div>
      <div class="modal-action-text">${esc(a.action || 'No action specified')}</div>
    </div>
    <div class="modal-section">
      <div class="modal-section-title">Implicated Accounts (${(a.accounts||[]).length})</div>
      <div class="modal-accounts">${(a.accounts||[]).map(ac => `<span class="account-chip">${ac}</span>`).join('')}</div>
    </div>`;
  overlay.classList.add('open');
};

function clearNodeDetail() { document.getElementById('node-detail-card').style.display = 'none'; }

window.addEventListener('DOMContentLoaded', () => connect());
