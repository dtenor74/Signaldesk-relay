/**
 * SignalDesk — Combined Databento + Finnhub Relay
 * ─────────────────────────────────────────────────
 * Databento  → Real NQ, ES, YM, RTY, CL, GC futures (true CME tick data)
 * Finnhub    → Forex, Crypto, Stocks, ETF proxies (free tier)
 *
 * Both feeds run simultaneously and broadcast to SignalDesk dashboard.
 *
 * DEPLOY: Push to GitHub → Railway auto-deploys
 * DASHBOARD URL: wss://sinaldesk.up.railway.app/ws
 */

const WebSocket = require('ws');
const http      = require('http');
const https     = require('https');

// ── API KEYS ─────────────────────────────────────
const DATABENTO_KEY = process.env.DATABENTO_KEY || 'AELXndmcsCuAJ5bARvJtPgpD8bG9c';
const FINNHUB_KEY   = process.env.FINNHUB_KEY   || 'd7mg8ahr01qngrvo3vm0d7mg8ahr01qngrvo3vmg';

// ── PORT ─────────────────────────────────────────
const PORT = process.env.PORT || 8000;

// ── DATABENTO CONFIG ──────────────────────────────
const DB_URL = 'wss://live.databento.com/v0/live';

// CME futures continuous front-month contracts
const DB_FUTURES = [
  { dbSym: 'NQ.c.0',  key: 'NQ'  },
  { dbSym: 'ES.c.0',  key: 'ES'  },
  { dbSym: 'YM.c.0',  key: 'YM'  },
  { dbSym: 'RTY.c.0', key: 'RTY' },
  { dbSym: 'CL.c.0',  key: 'CL'  },
  { dbSym: 'GC.c.0',  key: 'GC'  },
  { dbSym: 'SI.c.0',  key: 'SI'  },
];

// ── FINNHUB CONFIG ────────────────────────────────
const FH_URL = `wss://ws.finnhub.io?token=${FINNHUB_KEY}`;

// ETF proxies for cross-reference + spot price tracking
const FH_ETFS    = ['QQQ','SPY','DIA','IWM','GLD','USO','SLV','TLT','XLF','XLE'];
const FH_STOCKS  = ['NVDA','AAPL','TSLA','META','MSFT','AMZN','GOOGL','AMD'];
const FH_FOREX   = ['OANDA:EUR_USD','OANDA:GBP_USD','OANDA:USD_JPY','OANDA:USD_CAD','OANDA:AUD_USD','OANDA:USD_CHF'];
const FH_CRYPTO  = ['BINANCE:BTCUSDT','BINANCE:ETHUSDT','BINANCE:SOLUSDT','BINANCE:XRPUSDT'];

// ETF → Titan key (for spot price cross-reference with futures)
const ETF_TITAN = {
  'QQQ':'NQ','SPY':'ES','DIA':'YM','IWM':'RTY',
  'GLD':'GC','USO':'CL','SLV':'SI',
};

// Finnhub display symbol cleanup
const FH_DISPLAY = {
  'OANDA:EUR_USD':'EUR/USD','OANDA:GBP_USD':'GBP/USD',
  'OANDA:USD_JPY':'USD/JPY','OANDA:USD_CAD':'USD/CAD',
  'OANDA:AUD_USD':'AUD/USD','OANDA:USD_CHF':'USD/CHF',
  'BINANCE:BTCUSDT':'BTC','BINANCE:ETHUSDT':'ETH',
  'BINANCE:SOLUSDT':'SOL','BINANCE:XRPUSDT':'XRP',
};

// ── STATE ─────────────────────────────────────────
let clients     = new Set();
let lastPrices  = {};   // symbol → latest price data
let dbWs        = null;
let fhWs        = null;
let dbReconTimer= null;
let fhReconTimer= null;
let fhPingTimer = null;

// Feed status
const feedStatus = {
  databento: 'offline',
  finnhub:   'offline',
};

// ── HTTP SERVER ───────────────────────────────────
const httpServer = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status:      'ok',
      clients:     clients.size,
      feeds:       feedStatus,
      streaming:   Object.keys(lastPrices).length + ' symbols',
      prices:      lastPrices,
      uptime:      Math.round(process.uptime()) + 's',
    }));
  } else {
    res.writeHead(200);
    res.end('SignalDesk — Databento + Finnhub Relay');
  }
});

// ── DASHBOARD WS SERVER ───────────────────────────
const wss = new WebSocket.Server({ server: httpServer, path: '/ws' });

wss.on('connection', ws => {
  console.log(`[Client] Connected · Total: ${clients.size + 1}`);
  clients.add(ws);

  // Immediate snapshot of all current prices
  send(ws, { type: 'status', message: '✅ Connected to SignalDesk relay' });
  Object.entries(lastPrices).forEach(([sym, data]) => {
    send(ws, { type: 'quote', symbol: sym, ...data });
  });
  send(ws, {
    type: 'feeds',
    databento: feedStatus.databento,
    finnhub:   feedStatus.finnhub,
    message: `Databento: ${feedStatus.databento} | Finnhub: ${feedStatus.finnhub}`,
  });

  ws.on('close', () => { clients.delete(ws); });
  ws.on('error', ()  => clients.delete(ws));
  ws.on('message', raw => {
    try {
      const msg = JSON.parse(raw);
      if (msg.cmd === 'reset_baseline') {
        broadcastAll({ type: 'baseline_reset', ts: Date.now() });
      }
    } catch(e) {}
  });
});

// ══════════════════════════════════════════════════
// DATABENTO — CME Futures Feed
// ══════════════════════════════════════════════════
function connectDatabento() {
  if (dbReconTimer) { clearTimeout(dbReconTimer); dbReconTimer = null; }
  console.log('[Databento] Connecting...');

  try {
    dbWs = new WebSocket(DB_URL, {
      headers: { 'Authorization': `Bearer ${DATABENTO_KEY}` }
    });
  } catch(e) {
    console.error('[Databento] Failed to connect:', e.message);
    schedulDbRecon();
    return;
  }

  dbWs.on('open', () => {
    console.log('[Databento] Connected ✓ — authenticating');
    feedStatus.databento = 'connecting';

    // Authenticate
    dbWs.send(JSON.stringify({ action: 'auth', key: DATABENTO_KEY }));

    // Subscribe to CME futures trades
    dbWs.send(JSON.stringify({
      action:   'subscribe',
      schema:   'trades',
      dataset:  'GLBX.MDP3',
      symbols:  DB_FUTURES.map(f => f.dbSym),
      stype_in: 'continuous',
    }));

    console.log('[Databento] Subscribed to:', DB_FUTURES.map(f => f.dbSym).join(', '));
    broadcastAll({ type: 'status', message: '🟢 Databento CME futures feed active' });
    feedStatus.databento = 'live';
    broadcastFeeds();
  });

  dbWs.on('message', raw => {
    try {
      // Databento sends JSON messages
      const msg = JSON.parse(raw.toString());
      handleDatabentoMsg(msg);
    } catch(e) {
      // Some Databento messages are binary — skip gracefully
    }
  });

  dbWs.on('close', code => {
    console.log(`[Databento] Closed (${code}) — reconnecting in 8s`);
    feedStatus.databento = 'offline';
    broadcastAll({ type: 'status', message: '🔴 Databento disconnected — reconnecting...' });
    broadcastFeeds();
    schedulDbRecon();
  });

  dbWs.on('error', err => {
    console.error('[Databento] Error:', err.message);
    feedStatus.databento = 'error';
    broadcastAll({ type: 'status', message: `⚠️ Databento: ${err.message}` });
    broadcastFeeds();
  });
}

function handleDatabentoMsg(msg) {
  if (!msg) return;

  // Auth response
  if (msg.type === 'auth' || msg.action === 'auth') {
    if (msg.success || msg.status === 'ok') {
      console.log('[Databento] Authenticated ✓');
      feedStatus.databento = 'live';
      broadcastAll({ type: 'status', message: '🔑 Databento authorized — streaming CME futures' });
    }
    return;
  }

  // Trade record — Databento sends these as objects with hd (header) + price
  const symbol = msg.symbol || msg.instrument_id;
  const price  = msg.price  != null ? Number(msg.price) / 1e9 : // Fixed-point nano
                 msg.px     != null ? Number(msg.px)    / 1e9 : null;
  const size   = msg.size   || msg.qty || null;

  if (!symbol || !price) return;

  // Map to our key
  const entry = DB_FUTURES.find(f => f.dbSym === symbol || symbol.includes(f.key));
  if (!entry) return;

  const key = entry.key;

  // Update stored price
  if (!lastPrices[key]) lastPrices[key] = {};
  const prev = lastPrices[key].futures;
  lastPrices[key].futures   = price;
  lastPrices[key].size      = size;
  lastPrices[key].timestamp = Date.now();
  lastPrices[key].source    = 'databento';

  // Calculate real % change
  if (prev && prev > 0) {
    lastPrices[key].chg = ((price - prev) / prev) * 100;
  }

  // Broadcast to all dashboard clients
  broadcastAll({
    type:    'quote',
    symbol:  key,
    futures: price,
    spot:    lastPrices[key].spot || null,
    size,
    source:  'databento',
  });
}

function schedulDbRecon() {
  if (dbReconTimer) return;
  dbReconTimer = setTimeout(connectDatabento, 8000);
}

// ══════════════════════════════════════════════════
// FINNHUB — Forex, Crypto, Stocks, ETFs
// ══════════════════════════════════════════════════
function connectFinnhub() {
  if (fhReconTimer) { clearTimeout(fhReconTimer); fhReconTimer = null; }
  console.log('[Finnhub] Connecting...');

  fhWs = new WebSocket(FH_URL);

  fhWs.on('open', () => {
    console.log('[Finnhub] Connected ✓ — subscribing');
    feedStatus.finnhub = 'connecting';

    const allSymbols = [
      ...FH_ETFS,
      ...FH_STOCKS,
      ...FH_FOREX,
      ...FH_CRYPTO,
    ];

    // Stagger subscriptions to avoid rate limiting
    allSymbols.forEach((sym, i) => {
      setTimeout(() => {
        if (fhWs?.readyState === WebSocket.OPEN) {
          fhWs.send(JSON.stringify({ type: 'subscribe', symbol: sym }));
        }
      }, i * 60);
    });

    // Ping every 20s
    if (fhPingTimer) clearInterval(fhPingTimer);
    fhPingTimer = setInterval(() => {
      if (fhWs?.readyState === WebSocket.OPEN) {
        fhWs.send(JSON.stringify({ type: 'ping' }));
      }
    }, 20000);

    broadcastAll({ type: 'status', message: `📡 Finnhub subscribed — ${allSymbols.length} symbols` });
    feedStatus.finnhub = 'live';
    broadcastFeeds();
  });

  fhWs.on('message', raw => {
    try {
      const msg = JSON.parse(raw);
      if (msg.type === 'trade') handleFinnhubTrades(msg.data);
      if (msg.type === 'ping')  fhWs.send(JSON.stringify({ type: 'pong' }));
      if (msg.type === 'error') {
        console.error('[Finnhub] Error:', msg.msg);
        if (msg.msg?.includes('rate limit')) {
          console.log('[Finnhub] Rate limited — backing off');
        }
      }
    } catch(e) {}
  });

  fhWs.on('close', code => {
    console.log(`[Finnhub] Closed (${code}) — reconnecting in 5s`);
    if (fhPingTimer) clearInterval(fhPingTimer);
    feedStatus.finnhub = 'offline';
    broadcastAll({ type: 'status', message: '🔴 Finnhub disconnected — reconnecting...' });
    broadcastFeeds();
    scheduleFhRecon();
  });

  fhWs.on('error', err => {
    console.error('[Finnhub] Error:', err.message);
    feedStatus.finnhub = 'error';
    broadcastFeeds();
  });
}

function handleFinnhubTrades(trades) {
  if (!trades?.length) return;

  // Take latest price per symbol from the batch
  const latest = {};
  trades.forEach(t => {
    if (!latest[t.s] || t.t > latest[t.s].t) latest[t.s] = t;
  });

  Object.entries(latest).forEach(([fhSym, trade]) => {
    const price      = trade.p;
    const size       = trade.v;
    const displaySym = FH_DISPLAY[fhSym] || fhSym;

    if (!price) return;

    // Store raw price
    if (!lastPrices[displaySym]) lastPrices[displaySym] = {};
    const prev = lastPrices[displaySym].price;
    lastPrices[displaySym] = { price, size, timestamp: trade.t, source: 'finnhub' };
    if (prev && prev > 0) lastPrices[displaySym].chg = ((price - prev) / prev) * 100;

    // Broadcast raw quote
    broadcastAll({ type: 'quote', symbol: displaySym, price, size, source: 'finnhub' });

    // If ETF proxy → also update spot price for Titan spread calc
    const titanKey = ETF_TITAN[fhSym];
    if (titanKey) {
      if (!lastPrices[titanKey]) lastPrices[titanKey] = {};
      lastPrices[titanKey].spot      = price;
      lastPrices[titanKey].timestamp = trade.t;

      // Broadcast spot update for this Titan instrument
      broadcastAll({
        type:    'quote',
        symbol:  titanKey,
        futures: lastPrices[titanKey].futures || null,
        spot:    price,
        etfProxy: fhSym,
        source:  'finnhub_proxy',
      });
    }
  });
}

function scheduleFhRecon() {
  if (fhReconTimer) return;
  fhReconTimer = setTimeout(connectFinnhub, 5000);
}

// ── REST FALLBACK (poll stale symbols every 45s) ──
async function pollStaleSymbols() {
  const staleCutoff = Date.now() - 60000; // 60s
  const toRefresh = [...FH_ETFS, ...FH_STOCKS].filter(sym => {
    const p = lastPrices[sym];
    return !p || p.timestamp < staleCutoff;
  });

  for (const sym of toRefresh.slice(0, 10)) { // max 10 per cycle
    try {
      const price = await fetchFinnhubRest(sym);
      if (price) {
        lastPrices[sym] = { price, timestamp: Date.now(), source: 'rest' };
        broadcastAll({ type: 'quote', symbol: sym, price, source: 'finnhub_rest' });
        // Titan proxy
        const titanKey = ETF_TITAN[sym];
        if (titanKey) {
          if (!lastPrices[titanKey]) lastPrices[titanKey] = {};
          lastPrices[titanKey].spot = price;
          broadcastAll({ type: 'quote', symbol: titanKey, futures: lastPrices[titanKey].futures || null, spot: price });
        }
      }
    } catch(e) {}
    await sleep(300);
  }
}

function fetchFinnhubRest(sym) {
  return new Promise((resolve, reject) => {
    const url = `https://finnhub.io/api/v1/quote?symbol=${encodeURIComponent(sym)}&token=${FINNHUB_KEY}`;
    https.get(url, res => {
      let raw = '';
      res.on('data', d => raw += d);
      res.on('end', () => {
        try { resolve(JSON.parse(raw).c || null); }
        catch(e) { resolve(null); }
      });
    }).on('error', reject);
  });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }
setInterval(pollStaleSymbols, 45000);

// ── BROADCAST HELPERS ─────────────────────────────
function broadcastAll(msg) {
  const d = JSON.stringify(msg);
  clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(d); });
}
function send(ws, msg) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
}
function broadcastFeeds() {
  broadcastAll({
    type:      'feeds',
    databento: feedStatus.databento,
    finnhub:   feedStatus.finnhub,
    message:   `Databento: ${feedStatus.databento} | Finnhub: ${feedStatus.finnhub}`,
  });
}

// ── HEARTBEAT ─────────────────────────────────────
setInterval(() => {
  broadcastAll({
    type:      'heartbeat',
    ts:        Date.now(),
    clients:   clients.size,
    feeds:     feedStatus,
    streaming: Object.keys(lastPrices).length,
  });
}, 30000);

// ── BOOT ─────────────────────────────────────────
httpServer.listen(PORT, () => {
  console.log(`\n╔════════════════════════════════════════════════╗`);
  console.log(`║  SignalDesk — Databento + Finnhub Relay        ║`);
  console.log(`║  WS:       ws://localhost:${PORT}/ws              ║`);
  console.log(`║  Health:   http://localhost:${PORT}/health        ║`);
  console.log(`╠════════════════════════════════════════════════╣`);
  console.log(`║  Databento: ${DATABENTO_KEY.slice(0,12)}...              ║`);
  console.log(`║  Finnhub:   ${FINNHUB_KEY.slice(0,12)}...              ║`);
  console.log(`╚════════════════════════════════════════════════╝\n`);

  // Connect both feeds simultaneously
  connectDatabento();
  connectFinnhub();

  // Initial REST poll to populate prices right away
  setTimeout(pollStaleSymbols, 4000);
});
