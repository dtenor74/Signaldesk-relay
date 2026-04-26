/**
 * SignalDesk — Finnhub Live Data Relay
 * ─────────────────────────────────────────
 * Streams real-time prices for ETF proxies (NQ/ES/YM/RTY),
 * forex pairs, and crypto from Finnhub into SignalDesk.
 *
 * FREE tier covers everything needed:
 *   - US stocks/ETFs (QQQ→NQ, SPY→ES, DIA→YM, IWM→RTY)
 *   - Forex pairs (EUR/USD, GBP/USD, USD/JPY, etc.)
 *   - Crypto (BTC, ETH, SOL)
 *
 * DEPLOY on Railway.app (free):
 *   Push server.js + package.json to GitHub
 *   Connect Railway → auto-deploys
 *   SignalDesk Settings → wss://your-app.up.railway.app/ws
 *
 * LOCAL:
 *   npm install && node server.js
 *   SignalDesk Settings → ws://localhost:8000/ws
 */

const WebSocket = require('ws');
const http      = require('http');
const https     = require('https');

// ── API KEY ──────────────────────────────────────
const FINNHUB_KEY = process.env.FINNHUB_KEY || 'd7mg8ahr01qngrvo3vm0d7mg8ahr01qngrvo3vmg';

// ── SYMBOLS ──────────────────────────────────────
// ETF proxies for Titan futures instruments
const ETF_SYMBOLS = ['QQQ','SPY','DIA','IWM','GLD','USO','SLV','TLT','XLF','XLE'];
// Individual stocks for watchlist
const STOCK_SYMBOLS = ['NVDA','AAPL','TSLA','META','MSFT','AMZN','GOOGL'];
// Forex — Finnhub format
const FOREX_SYMBOLS = ['OANDA:EUR_USD','OANDA:GBP_USD','OANDA:USD_JPY','OANDA:USD_CAD','OANDA:AUD_USD','OANDA:USD_CHF'];
// Crypto — Finnhub format
const CRYPTO_SYMBOLS = ['BINANCE:BTCUSDT','BINANCE:ETHUSDT','BINANCE:SOLUSDT'];

// Map ETF → Titan instrument key
const ETF_TO_TITAN = {
  'QQQ':'NQ', 'SPY':'ES', 'DIA':'YM', 'IWM':'RTY',
  'GLD':'GC', 'USO':'CL', 'SLV':'SI',
};

// Map Finnhub symbol → clean display symbol
const SYMBOL_DISPLAY = {
  'OANDA:EUR_USD':'EUR/USD','OANDA:GBP_USD':'GBP/USD',
  'OANDA:USD_JPY':'USD/JPY','OANDA:USD_CAD':'USD/CAD',
  'OANDA:AUD_USD':'AUD/USD','OANDA:USD_CHF':'USD/CHF',
  'BINANCE:BTCUSDT':'BTC','BINANCE:ETHUSDT':'ETH','BINANCE:SOLUSDT':'SOL',
};

// ── STATE ────────────────────────────────────────
const PORT     = process.env.PORT || 8000;
let clients    = new Set();
let lastPrices = {};
let finnhubWs  = null;
let reconnTimer= null;
let pingTimer  = null;

// ── HTTP SERVER ──────────────────────────────────
const httpServer = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status:    'ok',
      clients:   clients.size,
      finnhub:   finnhubWs?.readyState === 1 ? 'live' : 'offline',
      streaming: Object.keys(lastPrices).length + ' symbols',
      uptime:    Math.round(process.uptime()) + 's',
      prices:    lastPrices,
    }));
  } else {
    res.writeHead(200);
    res.end('SignalDesk Finnhub Relay — running');
  }
});

// ── DASHBOARD WS SERVER ──────────────────────────
const wss = new WebSocket.Server({ server: httpServer, path: '/ws' });

wss.on('connection', ws => {
  console.log(`[Client] Connected · Total: ${clients.size + 1}`);
  clients.add(ws);

  // Send current snapshot immediately on connect
  send(ws, { type: 'status', message: '✅ Connected to SignalDesk Finnhub relay' });
  Object.entries(lastPrices).forEach(([sym, data]) => {
    send(ws, { type: 'quote', symbol: sym, ...data });
  });
  send(ws, {
    type: 'status',
    message: finnhubWs?.readyState === 1
      ? '🟢 Finnhub streaming live data'
      : '🔄 Connecting to Finnhub...'
  });

  ws.on('close', () => { clients.delete(ws); console.log(`[Client] Left · Remaining: ${clients.size}`); });
  ws.on('error', ()  => clients.delete(ws));
});

// ── CONNECT FINNHUB ──────────────────────────────
function connectFinnhub() {
  console.log('[Finnhub] Connecting...');
  finnhubWs = new WebSocket(`wss://ws.finnhub.io?token=${FINNHUB_KEY}`);

  finnhubWs.on('open', () => {
    console.log('[Finnhub] Connected ✓ — subscribing to symbols');
    broadcastAll({ type: 'status', message: '🔑 Finnhub connected — subscribing to feeds' });

    // Subscribe to all symbols
    const allSymbols = [
      ...ETF_SYMBOLS,
      ...STOCK_SYMBOLS,
      ...FOREX_SYMBOLS,
      ...CRYPTO_SYMBOLS,
    ];

    allSymbols.forEach((sym, i) => {
      // Stagger subscriptions slightly to avoid rate limiting
      setTimeout(() => {
        if (finnhubWs?.readyState === 1) {
          finnhubWs.send(JSON.stringify({ type: 'subscribe', symbol: sym }));
        }
      }, i * 50);
    });

    // Ping every 20s to keep connection alive
    if (pingTimer) clearInterval(pingTimer);
    pingTimer = setInterval(() => {
      if (finnhubWs?.readyState === 1) {
        finnhubWs.send(JSON.stringify({ type: 'ping' }));
      }
    }, 20000);

    broadcastAll({ type: 'status', message: `📡 Subscribed to ${allSymbols.length} symbols` });
  });

  finnhubWs.on('message', raw => {
    try {
      const msg = JSON.parse(raw);
      if (msg.type === 'trade') handleTrades(msg.data);
      if (msg.type === 'ping')  finnhubWs.send(JSON.stringify({ type: 'pong' }));
      if (msg.type === 'error') {
        console.error('[Finnhub] Error:', msg.msg);
        broadcastAll({ type: 'status', message: `⚠️ Finnhub: ${msg.msg}` });
      }
    } catch(e) {
      console.error('[Finnhub] Parse error:', e.message);
    }
  });

  finnhubWs.on('close', code => {
    console.log(`[Finnhub] Closed (${code}) — reconnecting in 5s`);
    if (pingTimer) clearInterval(pingTimer);
    broadcastAll({ type: 'status', message: '🔴 Finnhub disconnected — reconnecting...' });
    scheduleReconnect();
  });

  finnhubWs.on('error', err => {
    console.error('[Finnhub] Error:', err.message);
    broadcastAll({ type: 'status', message: `⚠️ ${err.message}` });
  });
}

// ── HANDLE TRADE DATA ────────────────────────────
function handleTrades(trades) {
  if (!trades || !trades.length) return;

  // Finnhub batches trades — take the latest price per symbol
  const latest = {};
  trades.forEach(t => {
    if (!latest[t.s] || t.t > latest[t.s].t) latest[t.s] = t;
  });

  Object.entries(latest).forEach(([fhSym, trade]) => {
    const price = trade.p;
    const size  = trade.v;
    if (!price) return;

    // Get clean display symbol
    const displaySym = SYMBOL_DISPLAY[fhSym] || fhSym;

    // Store price
    if (!lastPrices[displaySym]) lastPrices[displaySym] = {};
    lastPrices[displaySym] = { price, size, timestamp: trade.t };

    // Broadcast raw quote
    broadcastAll({ type: 'quote', symbol: displaySym, price, size });

    // If it's an ETF proxy → also broadcast as Titan instrument
    const titanKey = ETF_TO_TITAN[fhSym];
    if (titanKey) {
      if (!lastPrices[titanKey]) lastPrices[titanKey] = {};
      // Use previous futures price if we have it, otherwise use ETF as proxy
      const prevFutures = lastPrices[titanKey].futures;
      // Scale ETF price to approximate futures price
      const scale = FUTURES_SCALE[titanKey] || 1;
      const futuresProxy = price * scale;

      lastPrices[titanKey] = {
        futures:   prevFutures || futuresProxy,
        spot:      price,  // ETF = spot proxy
        timestamp: trade.t,
      };

      broadcastAll({
        type:    'quote',
        symbol:  titanKey,
        futures: lastPrices[titanKey].futures,
        spot:    price,
        etfProxy: fhSym,
      });
    }
  });
}

// Approximate multipliers to convert ETF price → futures price
// These are rough — the Titan BPS system tracks relative moves, so exact price matters less
const FUTURES_SCALE = {
  NQ:  40,   // QQQ ~485 × 40 ≈ NQ ~19400
  ES:  11.2, // SPY ~540 × 11.2 ≈ ES ~6000 (adjust as needed)
  YM:  83,   // DIA ~403 × 83 ≈ YM ~33449
  RTY: 4.1,  // IWM ~200 × 4.1 ≈ RTY ~820 (check current)
  GC:  1,    // GLD tracks gold closely enough
  CL:  1,    // USO tracks oil
  SI:  1,    // SLV tracks silver
};

// ── REST API FALLBACK ────────────────────────────
// Poll Finnhub REST API for any symbols that haven't ticked via WS
// This ensures we always have a price even in slow markets
async function pollQuotes() {
  const symbols = [...ETF_SYMBOLS, ...STOCK_SYMBOLS];
  for (const sym of symbols) {
    if (lastPrices[sym] && Date.now() - lastPrices[sym].timestamp < 60000) continue; // skip if fresh
    try {
      const price = await fetchQuote(sym);
      if (price) {
        lastPrices[sym] = { price, timestamp: Date.now(), source: 'rest' };
        broadcastAll({ type: 'quote', symbol: sym, price });
        // Titan proxy
        const titanKey = ETF_TO_TITAN[sym];
        if (titanKey) {
          const scale = FUTURES_SCALE[titanKey] || 1;
          if (!lastPrices[titanKey]) lastPrices[titanKey] = {};
          lastPrices[titanKey].spot = price;
          if (!lastPrices[titanKey].futures) lastPrices[titanKey].futures = price * scale;
          broadcastAll({ type: 'quote', symbol: titanKey, futures: lastPrices[titanKey].futures, spot: price });
        }
      }
    } catch(e) {}
    await sleep(200); // 200ms between requests to respect rate limits
  }
}

function fetchQuote(sym) {
  return new Promise((resolve, reject) => {
    https.get(`https://finnhub.io/api/v1/quote?symbol=${sym}&token=${FINNHUB_KEY}`, res => {
      let raw = '';
      res.on('data', d => raw += d);
      res.on('end', () => {
        try {
          const data = JSON.parse(raw);
          resolve(data.c || null); // c = current price
        } catch(e) { resolve(null); }
      });
    }).on('error', reject);
  });
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// Poll REST every 30s as fallback for stale symbols
setInterval(pollQuotes, 30000);

// ── RECONNECT ────────────────────────────────────
function scheduleReconnect() {
  if (reconnTimer) return;
  reconnTimer = setTimeout(() => { reconnTimer = null; connectFinnhub(); }, 5000);
}

// ── BROADCAST ────────────────────────────────────
function broadcastAll(msg) {
  const d = JSON.stringify(msg);
  clients.forEach(c => { if (c.readyState === WebSocket.OPEN) c.send(d); });
}
function send(ws, msg) {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
}

// ── HEARTBEAT ────────────────────────────────────
setInterval(() => {
  broadcastAll({
    type:      'heartbeat',
    ts:        Date.now(),
    clients:   clients.size,
    finnhub:   finnhubWs?.readyState === 1 ? 'live' : 'offline',
    streaming: Object.keys(lastPrices).length,
  });
}, 30000);

// ── BOOT ─────────────────────────────────────────
httpServer.listen(PORT, () => {
  console.log(`\n╔══════════════════════════════════════════╗`);
  console.log(`║  SignalDesk — Finnhub Live Relay          ║`);
  console.log(`║  WS:     ws://localhost:${PORT}/ws          ║`);
  console.log(`║  Health: http://localhost:${PORT}/health    ║`);
  console.log(`╚══════════════════════════════════════════╝\n`);
  console.log(`[Boot] Finnhub key: ${FINNHUB_KEY.slice(0,8)}...`);
  connectFinnhub();
  // Initial REST poll to get prices right away
  setTimeout(pollQuotes, 3000);
});
