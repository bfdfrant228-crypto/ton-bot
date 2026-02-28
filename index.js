/**
 * ton-bot MRKT + WebApp Panel (single-file)
 * version: 2026-02-28-webapp-dark-fragment-proxy-v6
 */

const TelegramBot = require('node-telegram-bot-api');
const express = require('express');
const crypto = require('crypto');
const { Readable } = require('stream');

// =====================
// ENV
// =====================
const token = process.env.TELEGRAM_TOKEN;
if (!token) {
  console.error('TELEGRAM_TOKEN not set');
  process.exit(1);
}

const MODE = process.env.MODE || 'real';

// WebApp
const WEBAPP_URL = process.env.WEBAPP_URL || null;
const WEBAPP_AUTH_MAX_AGE_SEC = Number(process.env.WEBAPP_AUTH_MAX_AGE_SEC || 86400);

// Admin
const ADMIN_USER_ID = process.env.ADMIN_USER_ID ? Number(process.env.ADMIN_USER_ID) : null;

// Redis (optional)
const REDIS_URL = process.env.REDIS_URL || process.env.REDIS_PRIVATE_URL || null;

// MRKT
const MRKT_API_URL = 'https://api.tgmrkt.io/api/v1';
const MRKT_CDN_BASE = (process.env.MRKT_CDN_BASE || 'https://cdn.tgmrkt.io/').trim();
const FRAGMENT_GIFT_IMG_BASE = (process.env.FRAGMENT_GIFT_IMG_BASE || 'https://nft.fragment.com/gift/').trim();

let MRKT_AUTH_RUNTIME = (process.env.MRKT_AUTH || '').trim() || null;

// monitor
const CHECK_INTERVAL_MS = Number(process.env.CHECK_INTERVAL_MS || 5000);
const MRKT_TIMEOUT_MS = Number(process.env.MRKT_TIMEOUT_MS || 8000);

const MRKT_COUNT = Number(process.env.MRKT_COUNT || 50);
const MRKT_PAGES = Number(process.env.MRKT_PAGES || 6);
const MRKT_PAGES_MONITOR = Number(process.env.MRKT_PAGES_MONITOR || 1);
const ONLY_CHEAPEST_PER_CHECK = String(process.env.ONLY_CHEAPEST_PER_CHECK || '1') !== '0';
const SEND_DELAY_MS = Number(process.env.SEND_DELAY_MS || 80);
const SENT_TTL_MS = Number(process.env.SENT_TTL_MS || 24 * 60 * 60 * 1000);

// WebApp lots/suggest
const WEBAPP_LOTS_LIMIT = Number(process.env.WEBAPP_LOTS_LIMIT || 25);
const WEBAPP_LOTS_PAGES = Number(process.env.WEBAPP_LOTS_PAGES || 1);
const WEBAPP_SUGGEST_LIMIT = Number(process.env.WEBAPP_SUGGEST_LIMIT || 18);

// collections for gift suggest
const MRKT_COLLECTIONS = String(process.env.MRKT_COLLECTIONS || '')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);

console.log('Bot version 2026-02-28-webapp-dark-fragment-proxy-v6');
console.log('MODE=', MODE);
console.log('WEBAPP_URL=', WEBAPP_URL || 'not set');
console.log('ADMIN_USER_ID=', ADMIN_USER_ID || 'not set');
console.log('REDIS_URL=', REDIS_URL ? 'set' : 'not set');
console.log('MRKT_AUTH_RUNTIME=', MRKT_AUTH_RUNTIME ? 'set' : 'not set');
console.log('MRKT_CDN_BASE=', MRKT_CDN_BASE);
console.log('FRAGMENT_GIFT_IMG_BASE=', FRAGMENT_GIFT_IMG_BASE);
console.log('MRKT_COLLECTIONS=', MRKT_COLLECTIONS.length ? MRKT_COLLECTIONS.length : 'not set');

const bot = new TelegramBot(token, { polling: true });

// =====================
// Telegram UI
// =====================
const MAIN_KEYBOARD = {
  keyboard: [
    [{ text: '🔍 Запустить поиск' }, { text: '⏹ Остановить поиск' }],
    [{ text: '💰 Макс. цена' }, { text: '💵 Мин. цена' }],
    [{ text: '📌 Статус' }]
  ],
  resize_keyboard: true,
};

// =====================
// State
// =====================
const users = new Map();
const sentDeals = new Map();

let isChecking = false;

const mrktAuthState = { ok: null, lastOkAt: 0, lastFailAt: 0, lastFailCode: null };

// caches
const CACHE_TTL_MS = 5 * 60_000;
const modelsCache = new Map();    // gift -> { time, items:[{name, rarityPerMille, thumbKey}] }
const backdropsCache = new Map(); // gift -> { time, items:[{name, rarityPerMille, centerHex}] }

// =====================
// Helpers
// =====================
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const nowMs = () => Date.now();

function n(x) {
  const v = Number(String(x).replace(',', '.'));
  return Number.isFinite(v) ? v : NaN;
}
function norm(s) {
  return String(s || '').toLowerCase().trim().replace(/\s+/g, ' ');
}
function normTraitName(s) {
  return norm(s).replace(/\s*\([^)]*%[^)]*\)\s*/g, ' ').replace(/\s+/g, ' ').trim();
}
function sameTrait(actual, expectedLower) {
  if (!expectedLower) return true;
  return normTraitName(actual) === normTraitName(expectedLower);
}
function inRange(price, minPrice, maxPrice) {
  if (!Number.isFinite(price)) return false;
  const min = minPrice != null ? Number(minPrice) : 0;
  const max = maxPrice != null ? Number(maxPrice) : null;
  if (Number.isFinite(min) && price < min) return false;
  if (max != null && Number.isFinite(max) && price > max) return false;
  return true;
}
function pruneSentDeals() {
  const now = nowMs();
  for (const [k, ts] of sentDeals.entries()) {
    if (now - ts > SENT_TTL_MS) sentDeals.delete(k);
  }
}
function mrktLotUrlFromId(id) {
  if (!id) return 'https://t.me/mrkt';
  const appId = String(id).replace(/-/g, '');
  return `https://t.me/mrkt/app?startapp=${appId}`;
}
function joinUrl(base, key) {
  const b = String(base || '').endsWith('/') ? String(base) : String(base) + '/';
  const k = String(key || '').startsWith('/') ? String(key).slice(1) : String(key);
  return b + k;
}
function intToHexColor(intVal) {
  const v = Number(intVal);
  if (!Number.isFinite(v)) return null;
  const hex = (v >>> 0).toString(16).padStart(6, '0');
  return '#' + hex.slice(-6);
}
function formatPctFromPermille(perMille) {
  const v = Number(perMille);
  if (!Number.isFinite(v)) return null;
  const pct = v / 10;
  const s = (pct % 1 === 0) ? pct.toFixed(0) : pct.toFixed(1);
  return `${s}%`;
}
function fragmentGiftRemoteUrl(giftName) {
  if (!giftName) return null;
  return joinUrl(FRAGMENT_GIFT_IMG_BASE, `${encodeURIComponent(String(giftName))}.medium.jpg`);
}
function giftNameFallbackFromCollectionAndNumber(collectionTitleOrName, number) {
  const base = String(collectionTitleOrName || '').replace(/\s+/g, '');
  if (!base || number == null) return null;
  return `${base}-${number}`;
}

async function sendMessageSafe(chatId, text, opts) {
  while (true) {
    try {
      return await bot.sendMessage(chatId, text, opts);
    } catch (e) {
      const retryAfter =
        e?.response?.body?.parameters?.retry_after ??
        e?.response?.parameters?.retry_after ??
        null;
      if (retryAfter) {
        await sleep((Number(retryAfter) + 1) * 1000);
        continue;
      }
      throw e;
    }
  }
}
async function fetchWithTimeout(url, options, timeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

function verifyTelegramWebAppInitData(initData) {
  if (!initData) return { ok: false, reason: 'NO_INIT_DATA' };

  const params = new URLSearchParams(initData);
  const hash = params.get('hash');
  if (!hash) return { ok: false, reason: 'NO_HASH' };
  params.delete('hash');

  const authDate = Number(params.get('auth_date') || 0);
  if (!authDate) return { ok: false, reason: 'NO_AUTH_DATE' };
  const ageSec = Math.floor(Date.now() / 1000) - authDate;
  if (ageSec > WEBAPP_AUTH_MAX_AGE_SEC) return { ok: false, reason: 'INITDATA_EXPIRED' };

  const pairs = [];
  for (const [k, v] of params.entries()) pairs.push([k, v]);
  pairs.sort((a, b) => a[0].localeCompare(b[0]));
  const dataCheckString = pairs.map(([k, v]) => `${k}=${v}`).join('\n');

  const secretKey = crypto.createHmac('sha256', 'WebAppData').update(token).digest();
  const calcHash = crypto.createHmac('sha256', secretKey).update(dataCheckString).digest('hex');

  if (calcHash !== hash) return { ok: false, reason: 'BAD_HASH' };

  let user = null;
  try { user = JSON.parse(params.get('user') || 'null'); } catch {}
  const userId = user?.id;
  if (!userId) return { ok: false, reason: 'NO_USER' };

  return { ok: true, userId };
}

// =====================
// Redis (store MRKT_AUTH in Redis)
// =====================
let redis = null;
const MRKT_AUTH_KEY = 'mrkt:auth:token';

async function initRedis() {
  if (!REDIS_URL) return;
  try {
    const { createClient } = require('redis');
    redis = createClient({ url: REDIS_URL });
    redis.on('error', (e) => console.error('Redis error:', e));
    await redis.connect();
    console.log('Redis connected');
  } catch (e) {
    console.error('Redis init failed:', e?.message || e);
    redis = null;
  }
}

async function loadMrktAuthFromRedis() {
  if (!redis) return;
  try {
    const t = await redis.get(MRKT_AUTH_KEY);
    if (t && String(t).trim()) {
      MRKT_AUTH_RUNTIME = String(t).trim();
      console.log('MRKT_AUTH loaded from Redis');
    }
  } catch (e) {
    console.error('loadMrktAuthFromRedis error:', e?.message || e);
  }
}

async function saveMrktAuthToRedis(newToken) {
  if (!redis) return;
  await redis.set(MRKT_AUTH_KEY, String(newToken || '').trim());
}

// =====================
// User state
// =====================
function getOrCreateUser(userId) {
  if (!users.has(userId)) {
    users.set(userId, {
      enabled: true,
      minPriceTon: 0,
      maxPriceTon: null,
      state: null,
      filters: { gift: '', model: '', backdrop: '' },
      logs: []
    });
  }
  return users.get(userId);
}

function pushUserLog(user, type, text) {
  const entry = { ts: Date.now(), tsIso: new Date().toISOString(), type, text };
  if (!Array.isArray(user.logs)) user.logs = [];
  user.logs.unshift(entry);
  user.logs = user.logs.slice(0, 120);
}

// =====================
// MRKT headers
// =====================
function mrktHeaders() {
  return {
    Authorization: MRKT_AUTH_RUNTIME || '',
    'Content-Type': 'application/json',
    Accept: 'application/json',
    Origin: 'https://cdn.tgmrkt.io',
    Referer: 'https://cdn.tgmrkt.io/',
    'User-Agent': 'Mozilla/5.0',
  };
}

async function markMrktFail(statusCode) {
  mrktAuthState.ok = false;
  mrktAuthState.lastFailAt = nowMs();
  mrktAuthState.lastFailCode = statusCode;
}
function markMrktOk() {
  mrktAuthState.ok = true;
  mrktAuthState.lastOkAt = nowMs();
}

// =====================
// MRKT endpoints
// =====================
async function mrktFetchSalingPage({ collectionName, modelName, backdropName, cursor }) {
  if (!MRKT_AUTH_RUNTIME) return { ok: false, reason: 'NO_AUTH', gifts: [], cursor: '' };

  const body = {
    collectionNames: [collectionName],
    modelNames: modelName ? [modelName] : [],
    backdropNames: backdropName ? [backdropName] : [],
    symbolNames: [],
    ordering: 'Price',
    lowToHigh: true,
    maxPrice: null,
    minPrice: null,
    mintable: null,
    number: null,
    count: MRKT_COUNT,
    cursor: cursor || '',
    query: null,
    promotedFirst: false
  };

  const res = await fetchWithTimeout(`${MRKT_API_URL}/gifts/saling`, {
    method: 'POST',
    headers: mrktHeaders(),
    body: JSON.stringify(body)
  }, MRKT_TIMEOUT_MS).catch(() => null);

  if (!res) return { ok: false, reason: 'FETCH_ERROR', gifts: [], cursor: '' };
  if (!res.ok) {
    await markMrktFail(res.status);
    return { ok: false, reason: `HTTP_${res.status}`, gifts: [], cursor: '' };
  }
  markMrktOk();

  const data = await res.json().catch(() => null);
  return {
    ok: true,
    reason: 'OK',
    gifts: Array.isArray(data?.gifts) ? data.gifts : [],
    cursor: data?.cursor || ''
  };
}

async function mrktSearchLots({ gift, model, backdrop }, minPriceTon, maxPriceTon, pagesLimit = MRKT_PAGES) {
  if (!gift) return { ok: false, reason: 'NO_GIFT', gifts: [] };

  let cursor = '';
  const out = [];

  for (let page = 0; page < pagesLimit; page++) {
    const r = await mrktFetchSalingPage({
      collectionName: gift,
      modelName: model || null,
      backdropName: backdrop || null,
      cursor
    });
    if (!r.ok) return { ok: false, reason: r.reason, gifts: [] };

    for (const g of r.gifts) {
      const nano = (g?.salePriceWithoutFee && Number(g.salePriceWithoutFee) > 0) ? g.salePriceWithoutFee : g?.salePrice;
      if (nano == null) continue;

      const priceTon = Number(nano) / 1e9;
      if (!Number.isFinite(priceTon) || priceTon <= 0) continue;
      if (!inRange(priceTon, minPriceTon, maxPriceTon)) continue;

      const baseName = (g.collectionTitle || g.collectionName || g.title || gift).trim();
      const number = g.number ?? null;
      const displayName = number ? `${baseName} #${number}` : baseName;

      const modelName = g.modelTitle || g.modelName || '';
      const backdropName = g.backdropName || '';
      const symbolName = g.symbolName || '';
      if (model && !sameTrait(modelName, norm(model))) continue;
      if (backdrop && !sameTrait(backdropName, norm(backdrop))) continue;

      // giftName for Fragment image:
      // 1) g.name (if exists)
      // 2) build from collection + number: VictoryMedal-29983, SnakeBox-151365
      const giftName =
        g.name ||
        giftNameFallbackFromCollectionAndNumber(g.collectionTitle || g.collectionName || g.title, g.number) ||
        null;

      const urlTelegram = giftName && String(giftName).includes('-') ? `https://t.me/nft/${giftName}` : 'https://t.me/mrkt';
      const urlMarket = g.id ? mrktLotUrlFromId(g.id) : 'https://t.me/mrkt';

      out.push({
        id: g.id,
        name: displayName,
        giftName,
        priceTon,
        urlTelegram,
        urlMarket,
        attrs: { model: modelName || null, backdrop: backdropName || null, symbol: symbolName || null },
        raw: g
      });

      if (out.length >= 300) break;
    }

    cursor = r.cursor || '';
    if (!cursor) break;
    if (out.length >= 300) break;
  }

  out.sort((a, b) => a.priceTon - b.priceTon);
  return { ok: true, reason: 'OK', gifts: out };
}

// filters dictionaries
async function mrktGetModelsForGift(gift) {
  if (!gift) return [];
  const cached = modelsCache.get(gift);
  if (cached && (nowMs() - cached.time < CACHE_TTL_MS)) return cached.items;

  const body = { collections: [gift] };

  const res = await fetchWithTimeout(`${MRKT_API_URL}/gifts/models`, {
    method: 'POST',
    headers: { ...mrktHeaders(), Authorization: MRKT_AUTH_RUNTIME || '' },
    body: JSON.stringify(body)
  }, MRKT_TIMEOUT_MS).catch(() => null);

  if (!res || !res.ok) {
    if (res) await markMrktFail(res.status);
    modelsCache.set(gift, { time: nowMs(), items: [] });
    return [];
  }

  const data = await res.json().catch(() => null);
  const arr = Array.isArray(data) ? data : [];

  const map = new Map();
  for (const it of arr) {
    const name = it.modelTitle || it.modelName;
    if (!name) continue;
    const key = norm(name);
    if (!map.has(key)) {
      map.set(key, {
        name: String(name),
        rarityPerMille: it.rarityPerMille ?? null,
        thumbKey: it.modelStickerThumbnailKey || null
      });
    }
  }

  const items = Array.from(map.values()).sort((a, b) => {
    const ra = a.rarityPerMille == null ? Infinity : Number(a.rarityPerMille);
    const rb = b.rarityPerMille == null ? Infinity : Number(b.rarityPerMille);
    if (ra !== rb) return ra - rb;
    return a.name.localeCompare(b.name);
  });

  modelsCache.set(gift, { time: nowMs(), items });
  return items;
}

async function mrktGetBackdropsForGift(gift) {
  if (!gift) return [];
  const cached = backdropsCache.get(gift);
  if (cached && (nowMs() - cached.time < CACHE_TTL_MS)) return cached.items;

  const body = { collections: [gift] };

  const res = await fetchWithTimeout(`${MRKT_API_URL}/gifts/backdrops`, {
    method: 'POST',
    headers: { ...mrktHeaders(), Authorization: MRKT_AUTH_RUNTIME || '' },
    body: JSON.stringify(body)
  }, MRKT_TIMEOUT_MS).catch(() => null);

  if (!res || !res.ok) {
    if (res) await markMrktFail(res.status);
    backdropsCache.set(gift, { time: nowMs(), items: [] });
    return [];
  }

  const data = await res.json().catch(() => null);
  const arr = Array.isArray(data) ? data : [];

  const map = new Map();
  for (const it of arr) {
    const name = it.backdropName;
    if (!name) continue;
    const key = norm(name);
    if (!map.has(key)) {
      map.set(key, {
        name: String(name),
        rarityPerMille: it.rarityPerMille ?? null,
        centerHex: intToHexColor(it.colorsCenterColor)
      });
    }
  }

  const items = Array.from(map.values()).sort((a, b) => {
    const ra = a.rarityPerMille == null ? Infinity : Number(a.rarityPerMille);
    const rb = b.rarityPerMille == null ? Infinity : Number(b.rarityPerMille);
    if (ra !== rb) return ra - rb;
    return a.name.localeCompare(b.name);
  });

  backdropsCache.set(gift, { time: nowMs(), items });
  return items;
}

// =====================
// Monitor
// =====================
async function checkMarketsForAllUsers() {
  if (MODE !== 'real') return;
  if (isChecking) return;

  isChecking = true;
  try {
    pruneSentDeals();

    for (const [userId, user] of users.entries()) {
      if (!user.enabled) continue;
      if (!user.maxPriceTon) continue;
      if (!user.filters.gift) continue;

      const minP = user.minPriceTon != null ? Number(user.minPriceTon) : 0;
      const maxP = Number(user.maxPriceTon);

      const lots = await mrktSearchLots(
        { gift: user.filters.gift, model: user.filters.model, backdrop: user.filters.backdrop },
        minP,
        maxP,
        MRKT_PAGES_MONITOR
      );
      if (!lots.ok || !lots.gifts.length) continue;

      const list = ONLY_CHEAPEST_PER_CHECK ? lots.gifts.slice(0, 1) : lots.gifts;

      for (const g of list) {
        const key = `${userId}:mrkt:${g.id}`;
        if (sentDeals.has(key)) continue;

        sentDeals.set(key, nowMs());
        await sendMessageSafe(userId, `Найден лот:\n${g.priceTon.toFixed(3)} TON\n${g.name}\n${g.urlTelegram}`, {
          disable_web_page_preview: false,
          reply_markup: { inline_keyboard: [[{ text: 'Открыть MRKT', url: g.urlMarket }]] }
        });

        if (SEND_DELAY_MS > 0) await sleep(SEND_DELAY_MS);
      }
    }
  } catch (e) {
    console.error('monitor error:', e);
  } finally {
    isChecking = false;
  }
}

// =====================
// Telegram start/menu button
// =====================
async function tgApi(method, body) {
  const res = await fetch(`https://api.telegram.org/bot${token}/${method}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  }).catch(() => null);

  const data = res ? await res.json().catch(() => null) : null;
  if (!data?.ok) throw new Error(`Telegram API ${method} failed: ${JSON.stringify(data)?.slice(0, 200)}`);
  return data.result;
}

bot.onText(/^\/start\b/, async (msg) => {
  getOrCreateUser(msg.from.id);

  if (WEBAPP_URL) {
    try {
      await tgApi('setChatMenuButton', {
        chat_id: msg.chat.id,
        menu_button: { type: 'web_app', text: 'Панель', web_app: { url: WEBAPP_URL } }
      });
    } catch (e) {
      console.error('setChatMenuButton error:', e?.message || e);
    }
  }

  await sendMessageSafe(msg.chat.id, 'Открой кнопку меню “Панель” рядом со строкой ввода.', { reply_markup: MAIN_KEYBOARD });
});

// =====================
// WebApp HTML/JS (dark, responsive, horizontal cards)
// =====================
const WEBAPP_HTML = `<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"/>
<title>ton-bot panel</title>
<style>
  :root{
    --bg:#0b0f14;
    --card:#111827;
    --text:#e5e7eb;
    --muted:#9ca3af;
    --border:#1f2937;
    --input:#0f172a;
    --btn:#1f2937;
    --btnText:#e5e7eb;
    --danger:#ef4444;
  }
  *{ box-sizing:border-box; }
  body{ margin:0; padding:14px; background:var(--bg); color:var(--text); font-family:system-ui, -apple-system, Segoe UI, Roboto, Arial; }
  h2{ margin:0 0 10px 0; }
  .card{ background:var(--card); border:1px solid var(--border); border-radius:14px; padding:12px; margin:10px 0; }
  label{ display:block; font-size:12px; opacity:.85; margin-bottom:4px; color:var(--muted); }
  input{
    width: min(320px, 86vw);
    padding:10px;
    border:1px solid var(--border);
    border-radius:12px;
    background:var(--input);
    color:var(--text);
    outline:none;
  }
  .row{ display:flex; gap:10px; flex-wrap:wrap; align-items:flex-end; }
  button{
    padding:10px 12px;
    border:1px solid var(--border);
    border-radius:12px;
    background:var(--btn);
    color:var(--btnText);
    cursor:pointer;
  }
  button:hover{ filter:brightness(1.07); }
  pre{ white-space: pre-wrap; word-break: break-word; background:rgba(255,255,255,.04); padding:10px; border-radius:12px; border:1px solid var(--border); }
  #err{ display:none; border-color:var(--danger); color:var(--danger); }

  .sug{ border:1px solid var(--border); border-radius:14px; margin-top:6px; overflow:hidden; max-width: 340px; background:var(--card); }
  .sug .item{ width:100%; text-align:left; border:0; background:transparent; padding:10px; display:flex; gap:10px; align-items:center; color:var(--text); }
  .sug .item:hover{ background:rgba(255,255,255,.06); }
  .thumb{ width:36px; height:36px; border-radius:12px; object-fit:cover; background:rgba(255,255,255,.08); border:1px solid var(--border); }
  .dot{ width:14px; height:14px; border-radius:50%; border:1px solid var(--border); display:inline-block; }

  .hscroll{ display:flex; gap:10px; overflow-x:auto; padding-bottom:6px; scroll-snap-type:x mandatory; }
  .lot{ min-width: 260px; max-width: 260px; flex: 0 0 auto; border:1px solid var(--border); border-radius:16px; padding:10px; background:rgba(255,255,255,.02); scroll-snap-align:start; }
  .lot img{ width:100%; height:160px; object-fit:cover; background:rgba(255,255,255,.03); border-radius:14px; border:1px solid var(--border); }
  .price{ font-size:18px; font-weight:800; margin-top:8px; }
  .muted{ color:var(--muted); font-size:13px; }
</style>
</head>
<body>
<h2>Панель управления</h2>

<div id="err" class="card"></div>
<div id="status" class="card">Загрузка…</div>

<div class="card">
  <h3 style="margin:0 0 8px 0">Фильтры</h3>
  <div class="row">
    <div>
      <label>Gift</label>
      <input id="gift" placeholder="Начни вводить…" autocomplete="off"/>
      <div id="giftSug" class="sug" style="display:none"></div>
    </div>
    <div>
      <label>Model</label>
      <input id="model" placeholder="Любая" autocomplete="off"/>
      <div id="modelSug" class="sug" style="display:none"></div>
    </div>
    <div>
      <label>Backdrop</label>
      <input id="backdrop" placeholder="Любой" autocomplete="off"/>
      <div id="backdropSug" class="sug" style="display:none"></div>
    </div>
  </div>

  <div class="row" style="margin-top:10px">
    <div>
      <label>Min TON</label>
      <input id="minPrice" type="number" step="0.001"/>
    </div>
    <div>
      <label>Max TON</label>
      <input id="maxPrice" type="number" step="0.001"/>
    </div>
  </div>

  <div class="row" style="margin-top:10px">
    <button id="save">Сохранить</button>
    <button id="toggleMonitor">Мониторинг: ?</button>
  </div>
</div>

<div class="card">
  <h3 style="margin:0 0 8px 0">Лоты MRKT</h3>
  <div class="row">
    <button id="lotsRefresh">🔄 Обновить</button>
  </div>
  <div id="lots" class="hscroll" style="margin-top:10px"></div>
</div>

<div class="card" id="adminBlock" style="display:none">
  <h3 style="margin:0 0 8px 0">Admin</h3>
  <div class="muted">Обнови MRKT токен прямо здесь (хранится в Redis).</div>
  <div class="row" style="margin-top:10px">
    <div>
      <label>Текущий токен (маска)</label>
      <input id="tokMask" disabled />
    </div>
    <div>
      <label>Новый MRKT_AUTH</label>
      <input id="tokNew" placeholder="вставь токен сюда"/>
    </div>
    <button id="tokSave">Сохранить токен</button>
  </div>
</div>

<div class="card">
  <h3 style="margin:0 0 8px 0">Логи</h3>
  <button id="logsRefresh">🔄 Обновить</button>
  <div id="logs" style="margin-top:10px"></div>
</div>

<script src="https://telegram.org/js/telegram-web-app.js"></script>
<script src="/app.js"></script>
</body>
</html>`;

const WEBAPP_JS = `(() => {
  const tg = window.Telegram?.WebApp;
  tg?.ready();

  // Apply Telegram theme if available
  function applyTheme() {
    const p = tg?.themeParams || {};
    const root = document.documentElement;
    if (p.bg_color) root.style.setProperty('--bg', p.bg_color);
    if (p.secondary_bg_color) root.style.setProperty('--card', p.secondary_bg_color);
    if (p.text_color) root.style.setProperty('--text', p.text_color);
    if (p.hint_color) root.style.setProperty('--muted', p.hint_color);
    if (p.button_color) root.style.setProperty('--btn', p.button_color);
    if (p.button_text_color) root.style.setProperty('--btnText', p.button_text_color);
  }
  applyTheme();
  tg?.onEvent?.('themeChanged', applyTheme);

  const initData = tg?.initData || '';
  const el = (id) => document.getElementById(id);

  function showErr(msg) {
    const box = el('err');
    box.style.display = 'block';
    box.textContent = msg;
  }
  function hideErr() {
    const box = el('err');
    box.style.display = 'none';
    box.textContent = '';
  }

  async function api(path, opts = {}) {
    const res = await fetch(path, {
      ...opts,
      headers: {
        'Content-Type': 'application/json',
        'X-Tg-Init-Data': initData,
        ...(opts.headers || {})
      }
    });
    const txt = await res.text();
    let data = null;
    try { data = txt ? JSON.parse(txt) : null; } catch { data = { raw: txt }; }
    if (!res.ok) throw new Error('HTTP ' + res.status + ': ' + JSON.stringify(data).slice(0, 400));
    return data;
  }

  function hideSug(id) { const b = el(id); b.style.display='none'; b.innerHTML=''; }
  function renderSug(id, items, onPick) {
    const b = el(id);
    if (!items || !items.length) { hideSug(id); return; }
    b.innerHTML = items.map((x) => {
      const thumb = x.imgUrl ? '<img class="thumb" src="' + x.imgUrl + '" referrerpolicy="no-referrer"/>' : '<div class="thumb"></div>';
      const dot = x.colorHex ? '<span class="dot" style="background:' + x.colorHex + '"></span>' : '';
      const right = '<div style="display:flex;flex-direction:column;gap:2px">' +
        '<div>' + (dot ? dot + '&nbsp;' : '') + x.label + '</div>' +
        (x.sub ? '<div class="muted">' + x.sub + '</div>' : '') +
      '</div>';
      return '<button type="button" class="item" data-v="' + x.value.replace(/"/g,'&quot;') + '">' + thumb + right + '</button>';
    }).join('');
    b.style.display = 'block';
    b.onclick = (e) => {
      const btn = e.target.closest('button[data-v]');
      if (!btn) return;
      onPick(btn.getAttribute('data-v'));
      hideSug(id);
    };
  }

  let collections = [];

  function renderStatus(st) {
    el('status').innerHTML =
      '<b>Статус</b><br>' +
      'MRKT_AUTH: <b>' + (st.api?.mrktAuthSet ? '✅' : '❌') + '</b><br>' +
      'Мониторинг: <b>' + (st.user.enabled ? 'ON' : 'OFF') + '</b><br>';
    el('toggleMonitor').textContent = 'Мониторинг: ' + (st.user.enabled ? 'ON' : 'OFF');

    if (st.api?.isAdmin) {
      el('adminBlock').style.display = 'block';
      el('tokMask').value = st.api?.mrktAuthMask || '';
    }
  }

  function renderLogs(st) {
    const logs = st.user.logs || [];
    if (!logs.length) {
      el('logs').innerHTML = '<i>Логов нет</i>';
      return;
    }
    el('logs').innerHTML = logs.map(l => '<pre>' + l.tsIso + ' | ' + l.type + '\\n' + l.text + '</pre>').join('');
  }

  function renderLots(resp) {
    const box = el('lots');
    if (resp.ok === false) {
      box.innerHTML = '<div style="color:#ef4444"><b>MRKT ошибка:</b> ' + (resp.reason || 'unknown') + '</div>';
      return;
    }
    const lots = resp.lots || [];
    if (!lots.length) {
      box.innerHTML = '<i>Лотов не найдено</i>';
      return;
    }
    box.innerHTML = lots.map(x => {
      const dot = x.backdropColorHex ? '<span class="dot" style="background:' + x.backdropColorHex + '"></span>' : '';
      const model = x.model ? ('<div class="muted">Model: ' + x.model + '</div>') : '';
      const backdrop = x.backdrop ? ('<div class="muted">Backdrop: ' + dot + ' ' + x.backdrop + '</div>') : '';
      const img = x.imgUrl
        ? ('<img src="' + x.imgUrl + '" referrerpolicy="no-referrer" loading="lazy"/>')
        : ('<div style="height:160px;border:1px solid rgba(255,255,255,.08);border-radius:14px"></div>');

      return (
        '<div class="lot">' +
          img +
          '<div class="price">' + x.priceTon.toFixed(3) + ' TON</div>' +
          '<div><b>' + x.name + '</b></div>' +
          model + backdrop +
          '<div class="row" style="margin-top:8px">' +
            '<button data-open="' + x.urlTelegram + '">NFT</button>' +
            '<button data-open="' + x.urlMarket + '">MRKT</button>' +
          '</div>' +
        '</div>'
      );
    }).join('');
  }

  async function loadCollections() {
    const meta = await api('/api/meta/collections');
    collections = meta.collections || [];
  }

  async function loadState() {
    const st = await api('/api/state');
    renderStatus(st);

    el('gift').value = st.user.filters.gift || '';
    el('model').value = st.user.filters.model || '';
    el('backdrop').value = st.user.filters.backdrop || '';
    el('minPrice').value = st.user.minPriceTon ?? 0;
    el('maxPrice').value = (st.user.maxPriceTon == null) ? '' : st.user.maxPriceTon;

    renderLogs(st);
  }

  async function loadLots() {
    const r = await api('/api/mrkt/lots');
    renderLots(r);
  }

  // Gift suggest local
  el('gift').addEventListener('input', () => {
    const q = el('gift').value.trim().toLowerCase();
    const items = collections
      .filter(x => !q || x.toLowerCase().includes(q))
      .slice(0, 12)
      .map(x => ({ label: x, value: x }));
    renderSug('giftSug', items, (v) => { el('gift').value = v; el('model').value=''; el('backdrop').value=''; });
  });

  // Model suggest from API
  let modelTimer = null;
  el('model').addEventListener('input', () => {
    if (modelTimer) clearTimeout(modelTimer);
    modelTimer = setTimeout(async () => {
      const gift = el('gift').value.trim();
      const q = el('model').value.trim();
      if (!gift) { hideSug('modelSug'); return; }
      try {
        const r = await api('/api/mrkt/suggest?kind=model&gift=' + encodeURIComponent(gift) + '&q=' + encodeURIComponent(q));
        renderSug('modelSug', r.items || [], (v) => { el('model').value = v; el('backdrop').value=''; });
      } catch { hideSug('modelSug'); }
    }, 200);
  });

  // Backdrop suggest from API
  let backdropTimer = null;
  el('backdrop').addEventListener('input', () => {
    if (backdropTimer) clearTimeout(backdropTimer);
    backdropTimer = setTimeout(async () => {
      const gift = el('gift').value.trim();
      const q = el('backdrop').value.trim();
      if (!gift) { hideSug('backdropSug'); return; }
      try {
        const r = await api('/api/mrkt/suggest?kind=backdrop&gift=' + encodeURIComponent(gift) + '&q=' + encodeURIComponent(q));
        renderSug('backdropSug', r.items || [], (v) => { el('backdrop').value = v; });
      } catch { hideSug('backdropSug'); }
    }, 220);
  });

  document.addEventListener('click', (e) => {
    if (!e.target.closest('#gift,#model,#backdrop,.sug')) {
      hideSug('giftSug'); hideSug('modelSug'); hideSug('backdropSug');
    }
  });

  // buttons
  el('save').onclick = async () => {
    hideErr();
    try {
      await api('/api/state/patch', {
        method: 'POST',
        body: JSON.stringify({
          filters: {
            gift: el('gift').value.trim(),
            model: el('model').value.trim(),
            backdrop: el('backdrop').value.trim(),
          },
          minPriceTon: Number(el('minPrice').value || 0),
          maxPriceTon: el('maxPrice').value === '' ? null : Number(el('maxPrice').value),
        })
      });
      await loadState();
      await loadLots();
    } catch (e) { showErr(String(e.message || e)); }
  };

  el('toggleMonitor').onclick = async () => {
    hideErr();
    try {
      await api('/api/action', { method: 'POST', body: JSON.stringify({ action: 'toggleMonitor' }) });
      await loadState();
    } catch (e) { showErr(String(e.message || e)); }
  };

  el('lotsRefresh').onclick = async () => { hideErr(); await loadLots().catch(e => showErr(e.message)); };
  el('logsRefresh').onclick = async () => { hideErr(); await loadState().catch(e => showErr(e.message)); };

  // admin token save
  const tokSave = el('tokSave');
  if (tokSave) {
    tokSave.onclick = async () => {
      hideErr();
      try {
        const t = el('tokNew').value.trim();
        if (!t) return showErr('Вставь токен.');
        await api('/api/admin/mrkt_auth', { method: 'POST', body: JSON.stringify({ token: t }) });
        el('tokNew').value = '';
        await loadState();
      } catch (e) {
        showErr(String(e.message || e));
      }
    };
  }

  document.body.addEventListener('click', (e) => {
    const btn = e.target.closest('button[data-open]');
    if (!btn) return;
    const url = btn.getAttribute('data-open');
    if (url) tg?.openTelegramLink ? tg.openTelegramLink(url) : window.open(url, '_blank');
  });

  (async () => {
    if (!initData) {
      showErr('Открой панель из Telegram (Menu Button). В браузере без initData API не работает.');
      return;
    }
    try {
      await loadCollections();
      await loadState();
      await loadLots();
    } catch (e) {
      showErr('Панель не может подключиться: ' + (e.message || e));
    }
  })();
})();`;

// =====================
// Web server + API
// =====================
function startWebServer() {
  const app = express();
  app.use(express.json({ limit: '600kb' }));

  app.get('/health', (req, res) => res.status(200).send('ok'));

  app.get('/', (req, res) => {
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.status(200).send(WEBAPP_HTML);
  });

  app.get('/app.js', (req, res) => {
    res.setHeader('Content-Type', 'application/javascript; charset=utf-8');
    res.status(200).send(WEBAPP_JS);
  });

  function auth(req, res, next) {
    const initData = String(req.headers['x-tg-init-data'] || '');
    const v = verifyTelegramWebAppInitData(initData);
    if (!v.ok) return res.status(401).json({ ok: false, reason: v.reason });
    req.userId = v.userId;
    next();
  }

  function isAdmin(userId) {
    return ADMIN_USER_ID && Number(userId) === Number(ADMIN_USER_ID);
  }

  // PROXY: gift image from fragment (fixes WebView issues)
  app.get('/img/gift', async (req, res) => {
    const name = String(req.query.name || '').trim();
    if (!name) return res.status(400).send('no name');

    const url = fragmentGiftRemoteUrl(name);
    const r = await fetchWithTimeout(url, { method: 'GET' }, 8000).catch(() => null);
    if (!r || !r.ok) return res.status(404).send('not found');

    const ct = r.headers.get('content-type') || 'image/jpeg';
    res.setHeader('Content-Type', ct);
    res.setHeader('Cache-Control', 'public, max-age=86400');

    // stream body
    const nodeStream = Readable.fromWeb(r.body);
    nodeStream.pipe(res);
  });

  app.get('/api/meta/collections', auth, (req, res) => {
    res.json({ ok: true, collections: MRKT_COLLECTIONS });
  });

  app.get('/api/state', auth, (req, res) => {
    const u = getOrCreateUser(req.userId);
    const mask = MRKT_AUTH_RUNTIME ? (MRKT_AUTH_RUNTIME.slice(0, 4) + '…' + MRKT_AUTH_RUNTIME.slice(-4)) : '';
    res.json({
      ok: true,
      api: {
        mrktAuthSet: !!MRKT_AUTH_RUNTIME,
        isAdmin: isAdmin(req.userId),
        mrktAuthMask: mask
      },
      user: {
        enabled: !!u.enabled,
        minPriceTon: u.minPriceTon ?? 0,
        maxPriceTon: u.maxPriceTon ?? null,
        filters: u.filters,
        logs: u.logs || []
      }
    });
  });

  app.post('/api/state/patch', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    const b = req.body || {};

    if (b.minPriceTon != null) u.minPriceTon = Number(b.minPriceTon) || 0;
    if (Object.prototype.hasOwnProperty.call(b, 'maxPriceTon')) {
      u.maxPriceTon = b.maxPriceTon == null ? null : Number(b.maxPriceTon);
    }

    if (b.filters && typeof b.filters === 'object') {
      if (typeof b.filters.gift === 'string') u.filters.gift = b.filters.gift.trim();
      if (typeof b.filters.model === 'string') u.filters.model = b.filters.model.trim();
      if (typeof b.filters.backdrop === 'string') u.filters.backdrop = b.filters.backdrop.trim();
    }

    pushUserLog(u, 'STATE', `gift=${u.filters.gift} model=${u.filters.model} backdrop=${u.filters.backdrop} min=${u.minPriceTon} max=${u.maxPriceTon}`);
    res.json({ ok: true });
  });

  app.post('/api/action', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    const action = req.body?.action;

    if (action === 'toggleMonitor') {
      u.enabled = !u.enabled;
      pushUserLog(u, 'MONITOR', `enabled=${u.enabled}`);
      return res.json({ ok: true });
    }

    res.status(400).json({ ok: false, reason: 'UNKNOWN_ACTION' });
  });

  // Suggest: model/backdrop
  app.get('/api/mrkt/suggest', auth, async (req, res) => {
    const kind = String(req.query.kind || '');
    const gift = String(req.query.gift || '').trim();
    const q = String(req.query.q || '').trim().toLowerCase();

    if (kind === 'model') {
      if (!gift) return res.json({ ok: true, items: [] });
      const models = await mrktGetModelsForGift(gift);
      const items = models
        .filter((m) => !q || m.name.toLowerCase().includes(q))
        .slice(0, WEBAPP_SUGGEST_LIMIT)
        .map((m) => ({
          label: m.name,
          value: m.name,
          imgUrl: m.thumbKey ? joinUrl(MRKT_CDN_BASE, m.thumbKey) : null,
          sub: m.rarityPerMille != null ? formatPctFromPermille(m.rarityPerMille) : null
        }));
      return res.json({ ok: true, items });
    }

    if (kind === 'backdrop') {
      if (!gift) return res.json({ ok: true, items: [] });
      const backs = await mrktGetBackdropsForGift(gift);
      const items = backs
        .filter((b) => !q || b.name.toLowerCase().includes(q))
        .slice(0, WEBAPP_SUGGEST_LIMIT)
        .map((b) => ({
          label: b.name,
          value: b.name,
          imgUrl: null,
          colorHex: b.centerHex || null,
          sub: b.rarityPerMille != null ? formatPctFromPermille(b.rarityPerMille) : null
        }));
      return res.json({ ok: true, items });
    }

    res.status(400).json({ ok: false, reason: 'BAD_KIND' });
  });

  // Lots (cards)
  app.get('/api/mrkt/lots', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    if (!MRKT_AUTH_RUNTIME) return res.json({ ok: false, reason: 'MRKT_AUTH_NOT_SET', lots: [] });
    if (!u.filters.gift) return res.json({ ok: true, lots: [] });

    const minP = u.minPriceTon != null ? Number(u.minPriceTon) : 0;
    const maxP = u.maxPriceTon != null ? Number(u.maxPriceTon) : null;

    const r = await mrktSearchLots(
      { gift: u.filters.gift, model: u.filters.model, backdrop: u.filters.backdrop },
      minP,
      maxP,
      WEBAPP_LOTS_PAGES
    );
    if (!r.ok) return res.json({ ok: false, reason: r.reason, lots: [] });

    const backs = await mrktGetBackdropsForGift(u.filters.gift);
    const backMap = new Map(backs.map(b => [norm(b.name), b]));

    const lots = (r.gifts || [])
      .slice(0, WEBAPP_LOTS_LIMIT)
      .map((x) => {
        const b = x.attrs?.backdrop ? backMap.get(norm(x.attrs.backdrop)) : null;
        const imgUrl = x.giftName ? (`/img/gift?name=${encodeURIComponent(x.giftName)}`) : null;

        return {
          id: x.id,
          name: x.name,
          priceTon: x.priceTon,
          urlTelegram: x.urlTelegram,
          urlMarket: x.urlMarket,
          model: x.attrs?.model || null,
          backdrop: x.attrs?.backdrop || null,
          symbol: x.attrs?.symbol || null,
          imgUrl,
          backdropColorHex: b?.centerHex || null
        };
      });

    res.json({ ok: true, lots });
  });

  // Admin: update MRKT_AUTH in Redis (no Railway)
  app.post('/api/admin/mrkt_auth', auth, async (req, res) => {
    if (!isAdmin(req.userId)) return res.status(403).json({ ok: false, reason: 'NOT_ADMIN' });

    const t = String(req.body?.token || '').trim();
    if (!t) return res.status(400).json({ ok: false, reason: 'EMPTY_TOKEN' });

    MRKT_AUTH_RUNTIME = t;
    try { await saveMrktAuthToRedis(t); } catch {}

    modelsCache.clear();
    backdropsCache.clear();

    res.json({ ok: true });
  });

  const port = Number(process.env.PORT || 3000);
  app.listen(port, '0.0.0.0', () => console.log('WebApp listening on port', port));
}

startWebServer();

// =====================
// Intervals + bootstrap
// =====================
setInterval(() => {
  checkMarketsForAllUsers().catch((e) => console.error('monitor error:', e));
}, CHECK_INTERVAL_MS);

(async () => {
  if (REDIS_URL) {
    await initRedis();
    if (redis) await loadMrktAuthFromRedis();
  }
  console.log('Bot started. /start');
})();
