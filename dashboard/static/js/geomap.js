'use strict';

window.GeoMap = (() => {
  let deckgl = null;
  let arcs = [];
  let blockedNodes = new Set();

  // Major Indian cities — source of truth for geographic scatter
  const CITIES = [
    { name: 'Delhi',         coords: [77.2090, 28.6139] },
    { name: 'Mumbai',        coords: [72.8777, 19.0760] },
    { name: 'Bangalore',     coords: [77.5946, 12.9716] },
    { name: 'Chennai',       coords: [80.2707, 13.0827] },
    { name: 'Kolkata',       coords: [88.3639, 22.5726] },
    { name: 'Hyderabad',     coords: [78.4867, 17.3850] },
    { name: 'Pune',          coords: [73.8567, 18.5204] },
    { name: 'Ahmedabad',     coords: [72.5714, 23.0225] },
    { name: 'Jaipur',        coords: [75.7873, 26.9124] },
    { name: 'Lucknow',       coords: [80.9462, 26.8467] },
    { name: 'Surat',         coords: [72.8311, 21.1702] },
    { name: 'Kanpur',        coords: [80.3319, 26.4499] },
    { name: 'Nagpur',        coords: [79.0882, 21.1458] },
    { name: 'Indore',        coords: [75.8577, 22.7196] },
    { name: 'Thane',         coords: [72.9781, 19.1970] },
    { name: 'Bhopal',        coords: [77.4126, 23.2599] },
    { name: 'Visakhapatnam', coords: [83.2185, 17.6868] },
    { name: 'Patna',         coords: [85.1376, 25.5941] },
    { name: 'Vadodara',      coords: [73.1812, 22.3072] },
    { name: 'Ludhiana',      coords: [75.8573, 30.9010] },
  ];

  /**
   * Deterministic hash: maps a full account ID to a fixed city + jitter.
   * MUST receive the full (non-truncated) account ID — the same ID the
   * fraud_alert.accounts array will contain — so fraud arcs land on the
   * same city as the prior blue arcs for those accounts.
   */
  function getCoordsForAccount(accId) {
    if (!accId) return CITIES[0].coords;
    let hash = 0;
    for (let i = 0; i < accId.length; i++) {
      hash = ((hash << 5) - hash) + accId.charCodeAt(i);
      hash |= 0;
    }
    const absHash = Math.abs(hash);
    const city = CITIES[absHash % CITIES.length];
    // Deterministic sub-city jitter so co-located accounts don't overlap exactly
    const jitterLng = ((absHash % 100) - 50) / 1200;
    const jitterLat = (((absHash >> 2) % 100) - 50) / 1200;
    return [city.coords[0] + jitterLng, city.coords[1] + jitterLat];
  }

  function init() {
    if (!document.getElementById('deck-canvas-container')) return;

    if (typeof deck === 'undefined') {
      setTimeout(init, 500);
      return;
    }

    deckgl = new deck.DeckGL({
      container: 'deck-canvas-container',
      mapStyle: 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json',
      initialViewState: {
        longitude: 79.5,      // centred on India
        latitude:  22.5,
        zoom:      4.5,       // tighter zoom — shows all of India without cutting off
        pitch:     35,        // moderate tilt — less extreme than 45°, cleaner look
        bearing:   0,
      },
      controller: true,
      layers: [],
    });

    // Render city dots immediately on init — map isn't empty at startup
    _renderCityDots();

    // Arc cleanup loop — remove arcs older than 4s
    setInterval(() => {
      const now = Date.now();
      const before = arcs.length;
      arcs = arcs.filter(a => now - a.timestamp < 4000);
      if (arcs.length !== before) render();
    }, 500);
  }

  function _renderCityDots() {
    if (!deckgl) return;
    const scatterLayer = new deck.ScatterplotLayer({
      id: 'city-nodes',
      data: CITIES,
      getPosition: d => d.coords,
      getRadius: 18000,         // ~18km radius dot
      getFillColor: [59, 130, 246, 40],  // dim blue fill
      getLineColor: [59, 130, 246, 120],
      lineWidthMinPixels: 1,
      stroked: true,
      pickable: false,
    });
    deckgl.setProps({ layers: [scatterLayer] });
  }

  function updateTxn(msg) {
    if (!deckgl || !msg || !msg.sender || !msg.receiver) return;

    // Use the full IDs — they match what fraud_alert.accounts contains
    const source = getCoordsForAccount(msg.sender);
    const target = getCoordsForAccount(msg.receiver);

    // Skip same-city transactions with tiny jitter — too visually noisy
    const dist = Math.abs(source[0] - target[0]) + Math.abs(source[1] - target[1]);
    if (dist < 0.01) return;

    const isFraud = blockedNodes.has(msg.sender) || blockedNodes.has(msg.receiver);
    arcs.push({
      source,
      target,
      timestamp: Date.now(),
      isFraud,
      amount: msg.amount || 100,
    });

    // Cap at 300 arcs for performance
    if (arcs.length > 300) arcs.shift();
    render();
  }

  function flashAlert(accountIds) {
    if (!deckgl || !accountIds || !accountIds.length) return;

    // Register as blocked so future txns for these accounts render as fraud
    for (const id of accountIds) blockedNodes.add(id);

    // Clear block status after 12s so the map self-cleans
    setTimeout(() => {
      for (const id of accountIds) blockedNodes.delete(id);
      render();
    }, 12000);

    // Draw immediate red ring arcs between all implicated accounts
    const now = Date.now();
    for (let i = 0; i < accountIds.length - 1; i++) {
      for (let j = i + 1; j < accountIds.length; j++) {
        const source = getCoordsForAccount(accountIds[i]);
        const target = getCoordsForAccount(accountIds[j]);
        const dist = Math.abs(source[0] - target[0]) + Math.abs(source[1] - target[1]);
        // Skip trivially close pairs — would be invisible
        if (dist < 0.005) continue;
        arcs.push({
          source,
          target,
          timestamp: now,
          isFraud: true,
          amount: 10000,
        });
      }
    }
    render();
  }

  function render() {
    if (!deckgl) return;

    const cityLayer = new deck.ScatterplotLayer({
      id: 'city-nodes',
      data: CITIES,
      getPosition: d => d.coords,
      getRadius: 18000,
      getFillColor: [59, 130, 246, 35],
      getLineColor: [59, 130, 246, 100],
      lineWidthMinPixels: 1,
      stroked: true,
      pickable: false,
    });

    const arcLayer = new deck.ArcLayer({
      id: 'transaction-arcs',
      data: arcs,
      getSourcePosition: d => d.source,
      getTargetPosition: d => d.target,
      // Fraud arcs: vivid red with glow. Normal arcs: clearly visible blue (was 120, now 180).
      getSourceColor: d => d.isFraud ? [239, 68, 68, 220]  : [59, 130, 246, 180],
      getTargetColor: d => d.isFraud ? [251, 113, 133, 255] : [99, 179, 255, 220],
      getWidth:  d => d.isFraud ? 5 : 1.5,
      getHeight: d => d.isFraud ? 2.0 : 0.8,  // fraud arcs arc higher = more dramatic
      tilt: 10,
    });

    deckgl.setProps({ layers: [cityLayer, arcLayer] });
  }

  function resize() {
    if (deckgl) deckgl.redraw(true);
  }

  return { init, updateTxn, flashAlert, resize };
})();

window.addEventListener('DOMContentLoaded', () => window.GeoMap.init());
