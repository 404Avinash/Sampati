'use strict';

const GraphVis = (() => {
  const CFG = {
    repulsion: 4000, spring: 90, springK: 0.04,
    damping: 0.88, boundary: 0.3, fps: 50,
    nodeR: 6, nodeRLarge: 9, maxNodes: 150,
  };
  const C = {
    normal: '#2d3a52', flagged: '#f59e0b', blocked: '#ef4444',
    edgeNormal: 'rgba(100,130,180,0.12)', edgeNew: 'rgba(59,130,246,0.7)',
  };

  let canvas, ctx, W, H, raf, lastT = 0, glowT = 0, initialized = false;
  let nodes = new Map();   // id → {x,y,vx,vy,risk,flagged,blocked}
  let edges = [];          // {source,target,count,isNew,newAge}

  function init() {
    canvas = document.getElementById('graph-canvas');
    if (!canvas) return;
    ctx = canvas.getContext('2d');
    resize();
    window.addEventListener('resize', resize);
    initialized = true;
    raf = requestAnimationFrame(loop);
  }

  function resize() {
    if (!canvas) return;
    const p = canvas.parentElement.getBoundingClientRect();
    canvas.width  = p.width  || canvas.offsetWidth;
    canvas.height = p.height || canvas.offsetHeight;
    W = canvas.width; H = canvas.height;
  }

  function update(data) {
    if (!initialized || !data) return;
    const { nodes: sn = [], edges: se = [] } = data;

    // Sync nodes
    const ids = new Set(sn.map(n => n.id));
    for (const id of nodes.keys()) if (!ids.has(id)) nodes.delete(id);
    for (const n of sn) {
      if (nodes.has(n.id)) Object.assign(nodes.get(n.id), {risk:n.risk, flagged:n.flagged, blocked:n.blocked});
      else nodes.set(n.id, {id:n.id, x:W/2+(Math.random()-.5)*200, y:H/2+(Math.random()-.5)*200, vx:0,vy:0, risk:n.risk, flagged:n.flagged, blocked:n.blocked});
    }

    // Sync edges
    const existing = new Set(edges.map(e=>`${e.source}:${e.target}`));
    edges = (se||[]).filter(e => nodes.has(e.source) && nodes.has(e.target))
                    .map(e => ({ ...e, isNew: !existing.has(`${e.source}:${e.target}`), newAge: 0 }));

    // Hide loading overlay
    const ov = document.getElementById('graph-loading');
    if (ov && nodes.size > 0) ov.classList.add('hidden');
  }

  function simulate() {
    const arr = Array.from(nodes.values());

    // Repulsion
    for (let i = 0; i < arr.length; i++) for (let j = i+1; j < arr.length; j++) {
      const a = arr[i], b = arr[j];
      const dx = b.x-a.x, dy = b.y-a.y;
      const d2 = dx*dx + dy*dy + 1, d = Math.sqrt(d2);
      const f = CFG.repulsion / d2;
      a.vx -= f*dx/d; a.vy -= f*dy/d;
      b.vx += f*dx/d; b.vy += f*dy/d;
    }

    // Springs
    for (const e of edges) {
      const s = nodes.get(e.source), t = nodes.get(e.target);
      if (!s||!t) continue;
      const dx = t.x-s.x, dy = t.y-s.y;
      const d = Math.sqrt(dx*dx+dy*dy) + .001;
      const f = (d - CFG.spring) * CFG.springK;
      s.vx += f*dx/d; s.vy += f*dy/d;
      t.vx -= f*dx/d; t.vy -= f*dy/d;
    }

    // Integrate
    const mg = 40;
    for (const n of arr) {
      if (n.x < mg) n.vx += CFG.boundary*(mg-n.x);
      if (n.x > W-mg) n.vx += CFG.boundary*(W-mg-n.x);
      if (n.y < mg) n.vy += CFG.boundary*(mg-n.y);
      if (n.y > H-mg) n.vy += CFG.boundary*(H-mg-n.y);
      n.vx *= CFG.damping; n.vy *= CFG.damping;
      n.x += n.vx; n.y += n.vy;
    }

    glowT = (glowT + .04) % (Math.PI*2);
    for (const e of edges) if (e.isNew) { e.newAge++; if (e.newAge > 40) e.isNew = false; }
  }

  function render() {
    ctx.clearRect(0, 0, W, H);

    // Edges
    for (const e of edges) {
      const s = nodes.get(e.source), t = nodes.get(e.target);
      if (!s||!t) continue;
      const alpha = e.isNew ? 0.7*(1 - e.newAge/40) : Math.min(0.08 + (e.count||1)*0.01, 0.4);
      ctx.beginPath();
      ctx.strokeStyle = e.isNew ? `rgba(59,130,246,${alpha+.3})` : `rgba(80,110,160,${alpha})`;
      ctx.lineWidth = e.isNew ? 1.5 : 1;
      ctx.moveTo(s.x, s.y); ctx.lineTo(t.x, t.y); ctx.stroke();
      // arrow
      const ang = Math.atan2(t.y-s.y, t.x-s.x), nr = CFG.nodeR+2;
      const ex = t.x - nr*Math.cos(ang), ey = t.y - nr*Math.sin(ang);
      ctx.beginPath();
      ctx.fillStyle = e.isNew ? `rgba(59,130,246,${alpha})` : `rgba(80,110,160,${alpha*.8})`;
      ctx.moveTo(ex, ey);
      ctx.lineTo(ex-7*Math.cos(ang-.4), ey-7*Math.sin(ang-.4));
      ctx.lineTo(ex-7*Math.cos(ang+.4), ey-7*Math.sin(ang+.4));
      ctx.closePath(); ctx.fill();
    }

    // Nodes
    for (const n of nodes.values()) {
      const r = (n.blocked||n.flagged) ? CFG.nodeRLarge : CFG.nodeR;
      const col = n.blocked ? C.blocked : n.flagged ? C.flagged : C.normal;

      // Glow halo for flagged/blocked
      if (n.blocked||n.flagged) {
        const ga = (n.blocked ? .15 : .08) + .06*Math.sin(glowT);
        ctx.beginPath(); ctx.arc(n.x, n.y, r+7, 0, Math.PI*2);
        ctx.fillStyle = n.blocked ? `rgba(239,68,68,${ga})` : `rgba(245,158,11,${ga})`;
        ctx.fill();
      }

      ctx.beginPath(); ctx.arc(n.x, n.y, r, 0, Math.PI*2);
      ctx.fillStyle = col; ctx.fill();

      ctx.beginPath(); ctx.arc(n.x, n.y, r, 0, Math.PI*2);
      ctx.strokeStyle = n.blocked ? 'rgba(239,68,68,.6)' : n.flagged ? 'rgba(245,158,11,.5)' : 'rgba(255,255,255,0.08)';
      ctx.lineWidth = n.blocked||n.flagged ? 1.5 : 1; ctx.stroke();
    }
  }

  function loop(ts) {
    if (ts - lastT >= 1000/CFG.fps) { simulate(); render(); lastT = ts; }
    raf = requestAnimationFrame(loop);
  }

  return { init, update, resize };
})();

window.addEventListener('DOMContentLoaded', () => GraphVis.init());
