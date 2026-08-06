'use strict';

window.GeoMap = (() => {
  let deckgl = null;
  let arcs = [];
  let blockedNodes = new Set();
  
  // Major Indian cities for realistic geospatial scatter
  const CITIES = [
    { name: 'Delhi',      coords: [77.2090, 28.6139] },
    { name: 'Mumbai',     coords: [72.8777, 19.0760] },
    { name: 'Bangalore',  coords: [77.5946, 12.9716] },
    { name: 'Chennai',    coords: [80.2707, 13.0827] },
    { name: 'Kolkata',    coords: [88.3639, 22.5726] },
    { name: 'Hyderabad',  coords: [78.4867, 17.3850] },
    { name: 'Pune',       coords: [73.8567, 18.5204] },
    { name: 'Ahmedabad',  coords: [72.5714, 23.0225] },
    { name: 'Jaipur',     coords: [75.7873, 26.9124] },
    { name: 'Lucknow',    coords: [80.9462, 26.8467] },
    { name: 'Surat',      coords: [72.8311, 21.1702] },
    { name: 'Kanpur',     coords: [80.3319, 26.4499] },
    { name: 'Nagpur',     coords: [79.0882, 21.1458] },
    { name: 'Indore',     coords: [75.8577, 22.7196] },
    { name: 'Thane',      coords: [72.9781, 19.1970] },
    { name: 'Bhopal',     coords: [77.4126, 23.2599] },
    { name: 'Visakhapatnam', coords: [83.2185, 17.6868] },
    { name: 'Patna',      coords: [85.1376, 25.5941] },
    { name: 'Vadodara',   coords: [73.1812, 22.3072] },
    { name: 'Ludhiana',   coords: [75.8573, 30.9010] }
  ];

  // Deterministic hash to map an account ID to a fixed geographic coordinate (with slight jitter)
  function getCoordsForAccount(accId) {
    if (!accId) return CITIES[0].coords;
    let hash = 0;
    for (let i = 0; i < accId.length; i++) {
      hash = ((hash << 5) - hash) + accId.charCodeAt(i);
      hash |= 0;
    }
    const absHash = Math.abs(hash);
    const city = CITIES[absHash % CITIES.length];
    
    // Add deterministic jitter so nodes in the same city don't perfectly overlap
    const jitterLng = ((absHash % 100) - 50) / 1000;
    const jitterLat = (((absHash >> 2) % 100) - 50) / 1000;
    
    return [city.coords[0] + jitterLng, city.coords[1] + jitterLat];
  }

  function init() {
    if (!document.getElementById('deck-canvas-container')) return;
    
    // Check if deck is loaded
    if (typeof deck === 'undefined') {
      setTimeout(init, 500);
      return;
    }

    deckgl = new deck.DeckGL({
      container: 'deck-canvas-container',
      mapStyle: 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
      initialViewState: {
        longitude: 78.9629,
        latitude: 20.5937,
        zoom: 4.2,
        pitch: 45,
        bearing: 10
      },
      controller: true,
      layers: []
    });

    // Cleanup old arcs loop
    setInterval(() => {
      const now = Date.now();
      let changed = false;
      const initialLength = arcs.length;
      arcs = arcs.filter(a => now - a.timestamp < 3000); // Remove arcs older than 3s
      if (arcs.length !== initialLength) render();
    }, 500);
  }

  function updateTxn(msg) {
    if (!deckgl || !msg || !msg.sender || !msg.receiver) return;
    
    const source = getCoordsForAccount(msg.sender);
    const target = getCoordsForAccount(msg.receiver);
    
    // If they map to the same coordinates exactly, jitter target
    if (source[0] === target[0] && source[1] === target[1]) {
      target[0] += 0.05;
      target[1] += 0.05;
    }

    const isFraud = blockedNodes.has(msg.sender) || blockedNodes.has(msg.receiver);
    
    arcs.push({
      source,
      target,
      timestamp: Date.now(),
      isFraud,
      amount: msg.amount || 100
    });
    
    // Keep max 200 arcs for performance
    if (arcs.length > 200) arcs.shift();
    
    render();
  }

  function flashAlert(accountIds) {
    if (!deckgl || !accountIds) return;
    for (const id of accountIds) blockedNodes.add(id);
    
    // Remove from blocked after 10 seconds so the map clears up
    setTimeout(() => {
      for (const id of accountIds) blockedNodes.delete(id);
      render();
    }, 10000);
    
    // Draw immediate alert lines for all implicated accounts to show the ring
    const now = Date.now();
    for (let i = 0; i < accountIds.length - 1; i++) {
      for (let j = i + 1; j < accountIds.length; j++) {
         arcs.push({
            source: getCoordsForAccount(accountIds[i]),
            target: getCoordsForAccount(accountIds[j]),
            timestamp: now,
            isFraud: true,
            amount: 1000
         });
      }
    }
    render();
  }

  function render() {
    if (!deckgl) return;
    
    const arcLayer = new deck.ArcLayer({
      id: 'transaction-arcs',
      data: arcs,
      getSourcePosition: d => d.source,
      getTargetPosition: d => d.target,
      getSourceColor: d => d.isFraud ? [239, 68, 68, 200] : [59, 130, 246, 120],
      getTargetColor: d => d.isFraud ? [239, 68, 68, 255] : [59, 130, 246, 200],
      getWidth: d => d.isFraud ? 4 : 2,
      getHeight: d => d.isFraud ? 1.5 : 1, // Make fraud arcs arc higher
      tilt: 15
    });
    
    deckgl.setProps({
      layers: [arcLayer]
    });
  }

  function resize() {
    if (deckgl) {
      deckgl.redraw(true);
    }
  }

  return { init, updateTxn, flashAlert, resize };
})();

window.addEventListener('DOMContentLoaded', () => GeoMap.init());
