'use strict';

const GraphVis = (() => {
  let svg, simulation, linkGroup, nodeGroup, g;
  let nodesMap = new Map();
  let width, height;

  function init() {
    const container = document.getElementById('d3-graph-container');
    if (!container) return;

    width = container.clientWidth;
    height = container.clientHeight;

    svg = d3.select('#d3-graph-svg')
      .attr('viewBox', [0, 0, width, height]);

    // Zoom capabilities
    const zoom = d3.zoom()
      .scaleExtent([0.1, 4])
      .on('zoom', (event) => {
        g.attr('transform', event.transform);
      });
    svg.call(zoom);

    g = svg.append('g');
    linkGroup = g.append('g').attr('class', 'links');
    nodeGroup = g.append('g').attr('class', 'nodes');

    // Physical forces
    simulation = d3.forceSimulation()
      .force('link', d3.forceLink().id(d => d.id).distance(50))
      .force('charge', d3.forceManyBody().strength(-150))
      .force('center', d3.forceCenter(width / 2, height / 2))
      .force('collide', d3.forceCollide().radius(12));

    simulation.on('tick', () => {
      linkGroup.selectAll('.d3-link')
        .attr('x1', d => d.source.x)
        .attr('y1', d => d.source.y)
        .attr('x2', d => d.target.x)
        .attr('y2', d => d.target.y);

      nodeGroup.selectAll('.d3-node')
        .attr('cx', d => d.x)
        .attr('cy', d => d.y);
    });

    window.addEventListener('resize', resize);
  }

  function resize() {
    const container = document.getElementById('d3-graph-container');
    if (!container) return;
    width = container.clientWidth;
    height = container.clientHeight;
    svg.attr('viewBox', [0, 0, width, height]);
    simulation.force('center', d3.forceCenter(width / 2, height / 2));
    simulation.alpha(0.3).restart();
  }

  function update(data) {
    if (!svg || !data) return;
    // Note: The backend `GraphStore.snapshot()` returns `nodes` and `links`.
    const { nodes: sn = [], links: se = [] } = data;

    const nodeArr = [];
    sn.forEach(n => {
      let existing = nodesMap.get(n.id);
      if (existing) {
        existing.group = n.group;
        existing.risk_score = n.risk_score;
        nodeArr.push(existing);
      } else {
        const newNode = { 
            ...n, 
            x: width/2 + (Math.random()-0.5)*100, 
            y: height/2 + (Math.random()-0.5)*100 
        };
        nodesMap.set(n.id, newNode);
        nodeArr.push(newNode);
      }
    });

    // Remove old nodes from Map
    const currentIds = new Set(sn.map(n => n.id));
    for (const id of nodesMap.keys()) {
      if (!currentIds.has(id)) nodesMap.delete(id);
    }

    const linkArr = se.map(e => ({
      source: e.source, target: e.target, value: e.value
    })).filter(e => nodesMap.has(e.source) && nodesMap.has(e.target));

    // Hide loading overlay
    const ov = document.getElementById('graph-loading');
    if (ov && nodeArr.length > 0) ov.classList.add('hidden');

    // D3 Update Pattern - Links
    const link = linkGroup.selectAll('.d3-link')
      .data(linkArr, d => `${d.source.id || d.source}-${d.target.id || d.target}`);
      
    link.exit().remove();
    link.enter().append('line')
      .attr('class', 'd3-link')
      .merge(link);

    // D3 Update Pattern - Nodes
    const node = nodeGroup.selectAll('.d3-node')
      .data(nodeArr, d => d.id);

    node.exit().remove();
    
    const nodeEnter = node.enter().append('circle')
      .attr('class', 'd3-node')
      .attr('r', d => d.group > 0 ? 10 : 6)
      .call(d3.drag()
        .on('start', dragstarted)
        .on('drag', dragged)
        .on('end', dragended));

    nodeEnter.append('title').text(d => `Account: ${d.id}\nRisk: ${d.risk_score}`);

    // Update classes based on block/flag state
    node.merge(nodeEnter)
      .attr('class', d => {
        let cls = 'd3-node ';
        if (d.group === 2) cls += 'd3-node--blocked';
        else if (d.group === 1) cls += 'd3-node--flagged';
        else cls += 'd3-node--normal';
        return cls;
      })
      .attr('r', d => d.group > 0 ? 10 : 6);

    simulation.nodes(nodeArr);
    simulation.force('link').links(linkArr);
    simulation.alpha(0.3).restart();
  }

  function dragstarted(event, d) {
    if (!event.active) simulation.alphaTarget(0.3).restart();
    d.fx = d.x;
    d.fy = d.y;
  }
  
  function dragged(event, d) {
    d.fx = event.x;
    d.fy = event.y;
  }
  
  function dragended(event, d) {
    if (!event.active) simulation.alphaTarget(0);
    d.fx = null;
    d.fy = null;
  }

  return { init, update, resize };
})();

window.addEventListener('DOMContentLoaded', () => {
  if (typeof d3 !== 'undefined') {
    GraphVis.init();
  } else {
    console.error("D3.js failed to load. Graph rendering aborted.");
  }
});
