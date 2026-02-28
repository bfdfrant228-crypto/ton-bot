/**
 * ton-bot MRKT + WebApp Panel (single-file)
 * version: 2026-02-28-webapp-suggest-lots-fix401-v3
 */

const TelegramBot = require('node-telegram-bot-api');
const express = require('express');
const crypto = require('crypto');

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

// Redis (optional)
const REDIS_URL = process.env.REDIS_URL || process.env.REDIS_PRIVATE_URL || null;

// MRKT
const MRKT_API_URL = 'https://api.tgmrkt.io/api/v1';
const MRKT_AUTH = (process.env.MRKT_AUTH || '').trim() || null;

const MRKT_COUNT = Number(process.env.MRKT_COUNT || 50);
const MRKT_PAGES = Number(process.env.MRKT_PAGES || 6);

// monitor
const CHECK_INTERVAL_MS = Number(process.env.CHECK_INTERVAL_MS || 5000);
const MRKT_TIMEOUT_MS = Number(process.env.MRKT_TIMEOUT_MS || 8000);
const MRKT_PAGES_MONITOR = Number(process.env.MRKT_PAGES_MONITOR || 1);
const ONLY_CHEAPEST_PER_CHECK = String(process.env.ONLY_CHEAPEST_PER_CHECK || '1') !== '0';
const SEND_DELAY_MS = Number(process.env.SEND_DELAY_MS || 80);
const SENT_TTL_MS = Number(process.env.SENT_TTL_MS || 24 * 60 * 60 * 1000);

// subscriptions
const SUBS_CHECK_INTERVAL_MS = Number(process.env.SUBS_CHECK_INTERVAL_MS || 9000);
const SUBS_MAX_NOTIFICATIONS_PER_CYCLE = Number(process.env.SUBS_MAX_NOTIFICATIONS_PER_CYCLE || 8);
const SUBS_EMPTY_CONFIRM = Number(process.env.SUBS_EMPTY_CONFIRM || 2);
const SUBS_MRKT_FEED_MAX_EVENTS_PER_CYCLE = Number(process.env.SUBS_MRKT_FEED_MAX_EVENTS_PER_CYCLE || 12);

// feed
const MRKT_FEED_COUNT = Number(process.env.MRKT_FEED_COUNT || 80);
const MRKT_FEED_THROTTLE_MS = Number(process.env.MRKT_FEED_THROTTLE_MS || 110);

const MRKT_FEED_NOTIFY_TYPES_RAW = String(process.env.MRKT_FEED_NOTIFY_TYPES || 'sale,listing,change_price');
const MRKT_FEED_NOTIFY_TYPES = new Set(
  MRKT_FEED_NOTIFY_TYPES_RAW.split(',').map((s) => s.trim().toLowerCase()).filter(Boolean)
);

// fees
const MRKT_FEE = Number(process.env.MRKT_FEE || 0);

// ===== AUTO BUY =====
const AUTO_BUY_GLOBAL = String(process.env.AUTO_BUY_GLOBAL || '0') === '1';
const AUTO_BUY_DRY_RUN = String(process.env.AUTO_BUY_DRY_RUN || '1') !== '0';
const AUTO_BUY_ONLY_NEW_LISTINGS = String(process.env.AUTO_BUY_ONLY_NEW_LISTINGS || '1') !== '0';

const AUTO_BUY_DISABLE_AFTER_SUCCESS = String(process.env.AUTO_BUY_DISABLE_AFTER_SUCCESS || '0') === '1';
const AUTO_BUY_ATTEMPT_TTL_MS = Number(process.env.AUTO_BUY_ATTEMPT_TTL_MS || 30_000);
const AUTO_BUY_MAX_PER_CHECK = Number(process.env.AUTO_BUY_MAX_PER_CHECK || 1);
const AUTO_BUY_NO_FUNDS_PAUSE_MS = Number(process.env.AUTO_BUY_NO_FUNDS_PAUSE_MS || 10 * 60 * 1000);

// WebApp lots/suggest
const WEBAPP_LOTS_LIMIT = Number(process.env.WEBAPP_LOTS_LIMIT || 25);
const WEBAPP_LOTS_PAGES = Number(process.env.WEBAPP_LOTS_PAGES || 1);
const WEBAPP_SUGGEST_LIMIT = Number(process.env.WEBAPP_SUGGEST_LIMIT || 20);
const WEBAPP_BACKDROP_SCAN_PAGES = Number(process.env.WEBAPP_BACKDROP_SCAN_PAGES || 3);

const MRKT_COLLECTIONS = String(process.env.MRKT_COLLECTIONS || '')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);

console.log('Bot version 2026-02-28-webapp-suggest-lots-fix401-v3');
console.log('MODE=', MODE);
console.log('WEBAPP_URL=', WEBAPP_URL || 'not set');
console.log('REDIS_URL=', REDIS_URL ? 'set' : 'not set');
console.log('MRKT_AUTH=', MRKT_AUTH ? 'set' : 'not set');
console.log('MRKT_COLLECTIONS=', MRKT_COLLECTIONS.length ? MRKT_COLLECTIONS.length : 'not set');
console.log('AUTO_BUY_GLOBAL=', AUTO_BUY_GLOBAL, 'AUTO_BUY_DRY_RUN=', AUTO_BUY_DRY_RUN, 'ONLY_NEW=', AUTO_BUY_ONLY_NEW_LISTINGS);

const bot = new TelegramBot(token, { polling: true });

// =====================
// UI (Telegram)
// =====================
const MAIN_KEYBOARD = {
  keyboard: [
    [{ text: '🔍 Запустить поиск' }, { text: '⏹ Остановить поиск' }],
    [{ text: '💰 Макс. цена' }, { text: '💵 Мин. цена' }],
    [{ text: '🤖 Автопокупка' }, { text: '📡 Подписки' }],
    [{ text: '📌 Статус' }]
  ],
  resize_keyboard: true,
};

// =====================
// State
// =====================
const users = new Map();
const sentDeals = new Map();
const subStates = new Map();

let isChecking = false;
let isSubsChecking = false;

const autoBuyLocks = new Set();
const autoBuyRecentAttempts = new Map();

const mrktAuthState = { ok: null, lastOkAt: 0, lastFailAt: 0, lastFailCode: null };

// caches for suggest
const modelsCache = new Map();   // gift -> { time, items:[{name, rarityPerMille}] }
const backdropsCache = new Map(); // key gift|model -> { time, items:[string] }
const CACHE_TTL_MS = 3 * 60_000;

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
function pad2(x) { return String(x).padStart(2, '0'); }
function pad3(x) { return String(x).padStart(3, '0'); }
function fmtClockMsUTC(d) {
  const x = (d instanceof Date) ? d : new Date(d);
  return `${pad2(x.getUTCHours())}:${pad2(x.getUTCMinutes())}:${pad2(x.getUTCSeconds())}.${pad3(x.getUTCMilliseconds())} UTC`;
}
function fmtIsoMs(d) {
  const x = (d instanceof Date) ? d : new Date(d);
  return x.toISOString();
}
function mrktLotUrlFromId(id) {
  if (!id) return 'https://t.me/mrkt';
  const appId = String(id).replace(/-/g, '');
  return `https://t.me/mrkt/app?startapp=${appId}`;
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
function percentChange(oldV, newV) {
  if (!oldV || !Number.isFinite(oldV) || oldV <= 0) return null;
  return ((newV - oldV) / oldV) * 100;
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
// Redis persistence (optional)
// =====================
let redis = null;

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

function getOrCreateUser(userId) {
  if (!users.has(userId)) {
    users.set(userId, {
      enabled: true,
      minPriceTon: 0,
      maxPriceTon: null,
      state: null, // awaiting_max | awaiting_min
      autoBuyEnabled: false,
      autoBuyFeedLastId: null,
      autoBuyPausedUntil: 0,
      filters: { gift: '', model: '', backdrop: '' },
      subscriptions: [],
      logs: [],
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

function exportState() {
  const out = { users: {} };
  for (const [userId, u] of users.entries()) {
    out.users[String(userId)] = {
      enabled: !!u.enabled,
      minPriceTon: typeof u.minPriceTon === 'number' ? u.minPriceTon : 0,
      maxPriceTon: typeof u.maxPriceTon === 'number' ? u.maxPriceTon : null,
      filters: u.filters,
      subscriptions: u.subscriptions || [],
      autoBuyEnabled: !!u.autoBuyEnabled,
      autoBuyFeedLastId: u.autoBuyFeedLastId || null,
      autoBuyPausedUntil: Number(u.autoBuyPausedUntil || 0),
      logs: u.logs || [],
    };
  }
  return out;
}

function renumberSubs(user) {
  const subs = Array.isArray(user.subscriptions) ? user.subscriptions : [];
  subs.forEach((s, idx) => { if (s) s.num = idx + 1; });
}

function importState(parsed) {
  const objUsers = parsed?.users && typeof parsed.users === 'object' ? parsed.users : {};
  for (const [idStr, u] of Object.entries(objUsers)) {
    const userId = Number(idStr);
    if (!Number.isFinite(userId)) continue;

    const safe = {
      enabled: u?.enabled !== false,
      minPriceTon: typeof u?.minPriceTon === 'number' ? u.minPriceTon : 0,
      maxPriceTon: typeof u?.maxPriceTon === 'number' ? u.maxPriceTon : null,
      state: null,
      autoBuyEnabled: !!u?.autoBuyEnabled,
      autoBuyFeedLastId: u?.autoBuyFeedLastId || null,
      autoBuyPausedUntil: Number(u?.autoBuyPausedUntil || 0),
      filters: {
        gift: typeof u?.filters?.gift === 'string' ? u.filters.gift : '',
        model: typeof u?.filters?.model === 'string' ? u.filters.model : '',
        backdrop: typeof u?.filters?.backdrop === 'string' ? u.filters.backdrop : '',
      },
      subscriptions: Array.isArray(u?.subscriptions) ? u.subscriptions : [],
      logs: Array.isArray(u?.logs) ? u.logs : [],
    };

    for (const s of safe.subscriptions) {
      if (!s || typeof s !== 'object') continue;
      if (!s.id) s.id = `sub_${Date.now()}_${Math.random().toString(16).slice(2, 6)}`;
      if (s.enabled == null) s.enabled = true;
      if (!s.filters) s.filters = {};
      if (typeof s.filters.gift !== 'string') s.filters.gift = safe.filters.gift || '';
      if (typeof s.filters.model !== 'string') s.filters.model = safe.filters.model || '';
      if (typeof s.filters.backdrop !== 'string') s.filters.backdrop = safe.filters.backdrop || '';
      if (s.maxPriceTon != null && !Number.isFinite(Number(s.maxPriceTon))) s.maxPriceTon = null;
      if (typeof s.num !== 'number') s.num = 0;
    }

    renumberSubs(safe);
    users.set(userId, safe);
  }
}

async function loadState() {
  if (!redis) return;
  const raw = await redis.get('bot:state:webapp:mrkt:v3');
  if (!raw) return;
  importState(JSON.parse(raw));
  console.log('Loaded state from Redis. users:', users.size);
}

let saveTimer = null;
function scheduleSave() {
  if (!redis) return;
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    saveState().catch((e) => console.error('saveState error:', e));
  }, 350);
}
async function saveState() {
  if (!redis) return;
  await redis.set('bot:state:webapp:mrkt:v3', JSON.stringify(exportState()));
}

// =====================
// MRKT headers (важно для 401/403)
// =====================
function mrktHeaders() {
  // MRKT иногда начинает требовать origin/referer как у WebApp
  return {
    Authorization: MRKT_AUTH || '',
    'Content-Type': 'application/json',
    Accept: 'application/json',
    Origin: 'https://cdn.tgmrkt.io',
    Referer: 'https://cdn.tgmrkt.io/',
    'User-Agent': 'Mozilla/5.0',
  };
}

// =====================
// MRKT API
// =====================
async function markMrktFail(statusCode) {
  mrktAuthState.ok = false;
  mrktAuthState.lastFailAt = nowMs();
  mrktAuthState.lastFailCode = statusCode;
}
function markMrktOk() {
  mrktAuthState.ok = true;
  mrktAuthState.lastOkAt = nowMs();
}

async function mrktFetchSalingPage({ collectionName, modelName, backdropName, cursor }) {
  if (!MRKT_AUTH) return { ok: false, reason: 'NO_AUTH', gifts: [], cursor: '' };

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
    promotedFirst: false,
  };

  const res = await fetchWithTimeout(`${MRKT_API_URL}/gifts/saling`, {
    method: 'POST',
    headers: mrktHeaders(),
    body: JSON.stringify(body),
  }, MRKT_TIMEOUT_MS).catch(() => null);

  if (!res) return { ok: false, reason: 'FETCH_ERROR', gifts: [], cursor: '' };
  if (!res.ok) {
    await markMrktFail(res.status);
    return { ok: false, reason: `HTTP_${res.status}`, gifts: [], cursor: '' };
  }
  markMrktOk();

  const data = await res.json().catch(() => null);
  const gifts = Array.isArray(data?.gifts) ? data.gifts : [];
  const nextCursor = data?.cursor || '';
  return { ok: true, reason: 'OK', gifts, cursor: nextCursor };
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
      cursor,
    });
    if (!r.ok) return { ok: false, reason: r.reason, gifts: [] };

    for (const g of r.gifts) {
      const nano =
        (g?.salePriceWithoutFee && Number(g.salePriceWithoutFee) > 0) ? g.salePriceWithoutFee : g?.salePrice;

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

      const urlTelegram = g.name && String(g.name).includes('-') ? `https://t.me/nft/${g.name}` : 'https://t.me/mrkt';
      const urlMarket = g.id ? mrktLotUrlFromId(g.id) : 'https://t.me/mrkt';

      out.push({
        id: g.id,
        name: displayName,
        priceTon,
        priceNano: Number(nano),
        urlTelegram,
        urlMarket,
        attrs: { model: modelName || null, backdrop: backdropName || null, symbol: symbolName || null },
        raw: g,
      });

      if (out.length >= 300) break;
    }

    if (out.length >= 300) break;
    cursor = r.cursor || '';
    if (!cursor) break;
  }

  out.sort((a, b) => a.priceTon - b.priceTon);
  return { ok: true, reason: 'OK', gifts: out };
}

async function mrktFeedFetch({ gift, model, backdrop, cursor, count, types }) {
  if (!MRKT_AUTH) return { ok: false, reason: 'NO_AUTH', items: [], cursor: '' };

  const body = {
    count: Number(count || MRKT_FEED_COUNT),
    cursor: cursor || '',
    collectionNames: gift ? [gift] : [],
    modelNames: model ? [model] : [],
    backdropNames: backdrop ? [backdrop] : [],
    lowToHigh: false,
    maxPrice: null,
    minPrice: null,
    number: null,
    ordering: 'Latest',
    query: null,
    type: Array.isArray(types) ? types : [],
  };

  const res = await fetchWithTimeout(`${MRKT_API_URL}/feed`, {
    method: 'POST',
    headers: mrktHeaders(),
    body: JSON.stringify(body),
  }, MRKT_TIMEOUT_MS).catch(() => null);

  if (!res) return { ok: false, reason: 'FETCH_ERROR', items: [], cursor: '' };
  if (!res.ok) {
    await markMrktFail(res.status);
    return { ok: false, reason: `HTTP_${res.status}`, items: [], cursor: '' };
  }
  markMrktOk();

  const data = await res.json().catch(() => null);
  const items = Array.isArray(data?.items) ? data.items : [];
  const nextCursor = data?.cursor || '';
  return { ok: true, reason: 'OK', items, cursor: nextCursor };
}

async function mrktBuy({ id, priceNano }) {
  if (!MRKT_AUTH) return { ok: false, reason: 'NO_AUTH' };

  const body = { ids: [id], prices: { [id]: priceNano } };

  const res = await fetchWithTimeout(`${MRKT_API_URL}/gifts/buy`, {
    method: 'POST',
    headers: mrktHeaders(),
    body: JSON.stringify(body),
  }, MRKT_TIMEOUT_MS).catch(() => null);

  if (!res) return { ok: false, reason: 'FETCH_ERROR' };

  const txt = await res.text().catch(() => '');
  let data = null;
  try { data = txt ? JSON.parse(txt) : null; } catch { data = null; }

  if (!res.ok) return { ok: false, reason: `HTTP_${res.status}`, data, text: txt.slice(0, 500) };

  const okItem =
    Array.isArray(data)
      ? data.find((x) =>
          x?.source?.type === 'buy_gift' &&
          x?.userGift?.isMine === true &&
          String(x?.userGift?.id || '') === String(id)
        )
      : null;

  if (!okItem) return { ok: false, reason: 'BUY_NOT_CONFIRMED', data };

  return { ok: true, data, okItem };
}

function isNoFundsError(r) {
  const raw = JSON.stringify(r?.data || r?.text || '').toLowerCase();
  return (
    raw.includes('not enough') ||
    raw.includes('insufficient') ||
    raw.includes('no funds') ||
    raw.includes('low balance') ||
    raw.includes('low_balance') ||
    raw.includes('insufficient_funds') ||
    raw.includes('balance')
  );
}

// =====================
// Suggest helpers
// =====================
async function mrktGetModelsForGift(gift) {
  if (!gift) return [];
  const cached = modelsCache.get(gift);
  if (cached && (nowMs() - cached.time < CACHE_TTL_MS)) return cached.items;

  // /gifts/models часто работает и без auth, но иногда лучше с auth
  const body = { collections: [gift] };

  const tryReq = async (useAuth) => {
    const headers = useAuth && MRKT_AUTH ? { ...mrktHeaders() } : { 'Content-Type': 'application/json', Accept: 'application/json' };
    const res = await fetchWithTimeout(`${MRKT_API_URL}/gifts/models`, {
      method: 'POST',
      headers,
      body: JSON.stringify(body),
    }, MRKT_TIMEOUT_MS).catch(() => null);
    if (!res || !res.ok) return null;
    const data = await res.json().catch(() => null);
    return Array.isArray(data) ? data : null;
  };

  const data = (await tryReq(false)) || (await tryReq(true)) || [];
  const map = new Map();
  for (const it of data) {
    const name = it.modelTitle || it.modelName;
    if (!name) continue;
    const rarityPerMille = it.rarityPerMille ?? null;
    const key = norm(name);
    if (!map.has(key)) map.set(key, { name: String(name), rarityPerMille });
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

async function mrktScanBackdrops(gift, model) {
  if (!gift) return [];
  const key = `${gift}||${model || ''}`.toLowerCase();
  const cached = backdropsCache.get(key);
  if (cached && (nowMs() - cached.time < CACHE_TTL_MS)) return cached.items;

  const uniq = new Map();
  let cursor = '';
  for (let page = 0; page < WEBAPP_BACKDROP_SCAN_PAGES; page++) {
    const r = await mrktFetchSalingPage({
      collectionName: gift,
      modelName: model || null,
      backdropName: null,
      cursor,
    });
    if (!r.ok) break;

    for (const g of r.gifts) {
      const b = g.backdropName;
      if (!b) continue;
      const k = normTraitName(b);
      if (!uniq.has(k)) uniq.set(k, b);
      if (uniq.size >= 200) break;
    }

    cursor = r.cursor || '';
    if (!cursor || uniq.size >= 200) break;
  }

  const items = Array.from(uniq.values()).sort((a, b) => a.localeCompare(b));
  backdropsCache.set(key, { time: nowMs(), items });
  return items;
}

// =====================
// Subscriptions
// =====================
function findSub(user, subId) {
  return (user.subscriptions || []).find((s) => s && s.id === subId) || null;
}
function makeSubFromCurrentFilters(user) {
  if (!user.filters.gift) return { ok: false, reason: 'NO_GIFT' };
  if (!Array.isArray(user.subscriptions)) user.subscriptions = [];
  const sub = {
    id: `sub_${Date.now()}_${Math.random().toString(16).slice(2, 6)}`,
    num: user.subscriptions.length + 1,
    enabled: true,
    maxPriceTon: user.maxPriceTon ?? null,
    filters: {
      gift: user.filters.gift,
      model: user.filters.model || '',
      backdrop: user.filters.backdrop || '',
    },
  };
  return { ok: true, sub };
}

async function notifySubFloor(userId, sub, prevFloor, newFloor, lot) {
  let text = `#${sub.num} ${sub.filters.gift}\n`;
  if (prevFloor == null) text += `Новый флор: ${newFloor.toFixed(3)} TON\n`;
  else {
    const pct = percentChange(prevFloor, newFloor);
    text += `Флор изменился: ${prevFloor.toFixed(3)} -> ${newFloor.toFixed(3)} TON${pct == null ? '' : ` (${pct.toFixed(1)}%)`}\n`;
  }
  if (sub.filters.model) text += `Model: ${sub.filters.model}\n`;
  if (sub.filters.backdrop) text += `Backdrop: ${sub.filters.backdrop}\n`;
  if (sub.maxPriceTon != null) text += `Max: ${Number(sub.maxPriceTon).toFixed(3)} TON\n`;
  if (lot?.urlTelegram) text += lot.urlTelegram;

  const reply_markup = lot?.urlMarket
    ? { inline_keyboard: [[{ text: 'Открыть MRKT', url: lot.urlMarket }]] }
    : undefined;

  await sendMessageSafe(userId, text.trim(), { disable_web_page_preview: false, reply_markup });
}

async function notifySubMrktEvent(userId, sub, item) {
  const type = String(item?.type || '').toLowerCase();
  if (!MRKT_FEED_NOTIFY_TYPES.has(type)) return;

  const g = item?.gift;
  if (!g) return;

  const title = g.collectionTitle || g.collectionName || g.title || 'MRKT Gift';
  const number = g.number ?? null;

  const amountNano = item.amount ?? g.salePrice ?? g.salePriceWithoutFee ?? null;
  const amountTon = Number(amountNano) / 1e9;
  const dateStr = item?.date ? String(item.date) : null;

  let text = `MRKT событие: ${type}\n`;
  text += `${title}${number != null ? ` #${number}` : ''}\n`;
  if (g.modelTitle || g.modelName) text += `Model: ${(g.modelTitle || g.modelName)}\n`;
  if (g.backdropName) text += `Backdrop: ${g.backdropName}\n`;
  if (Number.isFinite(amountTon) && amountTon > 0) text += `Amount: ${amountTon.toFixed(3)} TON\n`;
  if (dateStr) text += `Time: ${fmtClockMsUTC(dateStr)}\n`;

  const urlTelegram = g.name && String(g.name).includes('-') ? `https://t.me/nft/${g.name}` : 'https://t.me/mrkt';
  const urlMarket = g.id ? mrktLotUrlFromId(g.id) : 'https://t.me/mrkt';
  text += urlTelegram;

  await sendMessageSafe(userId, text.trim(), {
    disable_web_page_preview: false,
    reply_markup: { inline_keyboard: [[{ text: 'Открыть MRKT', url: urlMarket }]] }
  });
}

async function getFloorForSub(sub) {
  const r = await mrktSearchLots(
    { gift: sub.filters.gift, model: sub.filters.model, backdrop: sub.filters.backdrop },
    null,
    null,
    1
  );
  if (!r.ok) return { ok: false, lot: null };
  return { ok: true, lot: r.gifts[0] || null };
}

async function processMrktFeedForSub(userId, sub, stateKey, budgetEvents) {
  if (budgetEvents <= 0) return 0;

  const st = subStates.get(stateKey) || { floor: null, emptyStreak: 0, lastNotifiedFloor: null, feedLastId: null };

  const r = await mrktFeedFetch({
    gift: sub.filters.gift,
    model: sub.filters.model || null,
    backdrop: sub.filters.backdrop || null,
    cursor: '',
    count: MRKT_FEED_COUNT,
    types: [],
  });

  if (!r.ok || !r.items.length) return 0;

  const latestId = r.items[0]?.id || null;
  if (!latestId) return 0;

  if (!st.feedLastId) {
    subStates.set(stateKey, { ...st, feedLastId: latestId });
    return 0;
  }

  const newItems = [];
  for (const it of r.items) {
    if (!it?.id) continue;
    if (it.id === st.feedLastId) break;
    newItems.push(it);
  }

  if (!newItems.length) {
    subStates.set(stateKey, { ...st, feedLastId: latestId });
    return 0;
  }

  newItems.reverse();

  let sent = 0;
  for (const it of newItems) {
    if (sent >= budgetEvents) break;
    await notifySubMrktEvent(userId, sub, it);
    sent++;
  }

  subStates.set(stateKey, { ...st, feedLastId: latestId });
  return sent;
}

async function checkSubscriptionsForAllUsers({ manual = false } = {}) {
  if (MODE !== 'real') return { processedSubs: 0, floorNotifs: 0, feedNotifs: 0 };
  if (isSubsChecking && !manual) return { processedSubs: 0, floorNotifs: 0, feedNotifs: 0 };

  isSubsChecking = true;
  try {
    let processedSubs = 0;
    let floorNotifs = 0;
    let feedNotifs = 0;
    let globalFeedBudget = SUBS_MRKT_FEED_MAX_EVENTS_PER_CYCLE;

    for (const [userId, user] of users.entries()) {
      const subs = Array.isArray(user.subscriptions) ? user.subscriptions : [];
      const active = subs.filter((s) => s && s.enabled);
      if (!active.length) continue;

      for (const sub of active) {
        processedSubs++;
        if (floorNotifs >= SUBS_MAX_NOTIFICATIONS_PER_CYCLE) break;

        const stateKey = `${userId}:${sub.id}:MRKT`;
        const prevState = subStates.get(stateKey) || { floor: null, emptyStreak: 0, lastNotifiedFloor: null, feedLastId: null };

        const floorRes = await getFloorForSub(sub);
        if (!floorRes.ok) continue;

        const lot = floorRes.lot;
        const newFloor = lot ? lot.priceTon : null;

        let emptyStreak = prevState.emptyStreak || 0;
        if (newFloor == null) {
          emptyStreak++;
          if (emptyStreak < SUBS_EMPTY_CONFIRM) {
            subStates.set(stateKey, { ...prevState, emptyStreak });
            continue;
          }
        } else {
          emptyStreak = 0;
        }

        const prevFloor = prevState.floor;
        const max = sub.maxPriceTon != null ? Number(sub.maxPriceTon) : null;
        const canNotify = (newFloor != null) && (max == null || newFloor <= max);

        if (prevFloor == null && newFloor != null && canNotify) {
          await notifySubFloor(userId, sub, null, newFloor, lot);
          floorNotifs++;
          subStates.set(stateKey, { ...prevState, floor: newFloor, emptyStreak, lastNotifiedFloor: newFloor });
        } else if (prevFloor != null && newFloor != null && Number(prevFloor) !== Number(newFloor) && canNotify) {
          await notifySubFloor(userId, sub, prevFloor, newFloor, lot);
          floorNotifs++;
          subStates.set(stateKey, { ...prevState, floor: newFloor, emptyStreak, lastNotifiedFloor: newFloor });
        } else {
          subStates.set(stateKey, { ...prevState, floor: newFloor, emptyStreak });
        }

        if (globalFeedBudget > 0) {
          const sent = await processMrktFeedForSub(userId, sub, stateKey, globalFeedBudget);
          globalFeedBudget -= sent;
          feedNotifs += sent;
        }

        if (MRKT_FEED_THROTTLE_MS > 0) await sleep(MRKT_FEED_THROTTLE_MS);
      }
    }

    return { processedSubs, floorNotifs, feedNotifs };
  } catch (e) {
    console.error('subs error:', e);
    return { processedSubs: 0, floorNotifs: 0, feedNotifs: 0 };
  } finally {
    isSubsChecking = false;
  }
}

// =====================
// Deal notify
// =====================
async function sendDeal(userId, gift) {
  const lines = [];
  lines.push(`Price: ${gift.priceTon.toFixed(3)} TON`);
  lines.push(`Gift: ${gift.name}`);
  if (gift.attrs?.model) lines.push(`Model: ${gift.attrs.model}`);
  if (gift.attrs?.symbol) lines.push(`Symbol: ${gift.attrs.symbol}`);
  if (gift.attrs?.backdrop) lines.push(`Backdrop: ${gift.attrs.backdrop}`);
  lines.push(`Market: MRKT`);
  if (gift.urlTelegram) lines.push(gift.urlTelegram);

  const reply_markup = gift.urlMarket
    ? { inline_keyboard: [[{ text: 'Открыть MRKT', url: gift.urlMarket }]] }
    : undefined;

  await sendMessageSafe(userId, lines.join('\n'), { disable_web_page_preview: false, reply_markup });
}

// =====================
// AUTO BUY (only new listings) + speed timing
// =====================
async function initAutoBuyBaseline(userId, user) {
  if (!AUTO_BUY_ONLY_NEW_LISTINGS) return;
  if (!user.filters.gift) return;

  const r = await mrktFeedFetch({
    gift: user.filters.gift,
    model: user.filters.model || null,
    backdrop: user.filters.backdrop || null,
    cursor: '',
    count: 25,
    types: ['listing'],
  });

  user.autoBuyFeedLastId = (r.ok && r.items.length) ? (r.items[0]?.id || null) : null;
  scheduleSave();
}

async function attemptAutoBuyFromNewListings(userId, user, minP, maxP) {
  if (!AUTO_BUY_GLOBAL) return false;
  if (!user.autoBuyEnabled) return false;
  if (!user.filters.gift) return false;

  const now = nowMs();
  if (user.autoBuyPausedUntil && now < user.autoBuyPausedUntil) return false;

  if (autoBuyLocks.has(userId)) return false;
  autoBuyLocks.add(userId);

  try {
    const r = await mrktFeedFetch({
      gift: user.filters.gift,
      model: user.filters.model || null,
      backdrop: user.filters.backdrop || null,
      cursor: '',
      count: MRKT_FEED_COUNT,
      types: ['listing'],
    });

    if (!r.ok || !r.items.length) return false;

    const latestId = r.items[0]?.id || null;
    if (!latestId) return false;

    if (AUTO_BUY_ONLY_NEW_LISTINGS && !user.autoBuyFeedLastId) {
      user.autoBuyFeedLastId = latestId;
      scheduleSave();
      return false;
    }

    const newItems = [];
    for (const it of r.items) {
      if (!it?.id) continue;
      if (AUTO_BUY_ONLY_NEW_LISTINGS && user.autoBuyFeedLastId && it.id === user.autoBuyFeedLastId) break;
      newItems.push(it);
      if (newItems.length >= 120) break;
    }

    if (!newItems.length) {
      user.autoBuyFeedLastId = latestId;
      scheduleSave();
      return false;
    }

    newItems.reverse();

    let boughtCount = 0;

    for (const it of newItems) {
      if (boughtCount >= AUTO_BUY_MAX_PER_CHECK) break;

      const g = it.gift;
      if (!g?.id) continue;

      const priceNano = Number(it.amount ?? g.salePriceWithoutFee ?? g.salePrice ?? 0);
      if (!Number.isFinite(priceNano) || priceNano <= 0) continue;

      const priceTon = priceNano / 1e9;
      if (!inRange(priceTon, minP, maxP)) continue;

      const attemptKey = `${userId}:${g.id}`;
      const last = autoBuyRecentAttempts.get(attemptKey);
      if (last && nowMs() - last < AUTO_BUY_ATTEMPT_TTL_MS) continue;
      autoBuyRecentAttempts.set(attemptKey, nowMs());

      const title = `${g.collectionTitle || g.collectionName || g.title || 'Gift'}${g.number != null ? ` #${g.number}` : ''}`;
      const urlTelegram = g.name && String(g.name).includes('-') ? `https://t.me/nft/${g.name}` : 'https://t.me/mrkt';
      const urlMarket = mrktLotUrlFromId(g.id);

      const listingDate = it.date ? new Date(it.date) : null;
      const tStart = new Date();

      if (AUTO_BUY_DRY_RUN) {
        const msg =
          `🤖 DRY RUN (new listing)\n${title}\nЦена: ${priceTon.toFixed(3)} TON\n` +
          (listingDate ? `Listing: ${fmtClockMsUTC(listingDate)}\n` : '') +
          `Buy start: ${fmtClockMsUTC(tStart)}\n` +
          urlTelegram;

        pushUserLog(user, 'DRY_LISTING', msg.replace(/\n/g, ' | '));
        scheduleSave();

        await sendMessageSafe(userId, msg, {
          disable_web_page_preview: true,
          reply_markup: { inline_keyboard: [[{ text: 'Открыть MRKT', url: urlMarket }]] }
        });

        boughtCount++;
        continue;
      }

      await sendMessageSafe(
        userId,
        `🤖 Покупаю (new listing): ${title} за ${priceTon.toFixed(3)} TON...\nBuy start: ${fmtClockMsUTC(tStart)}`,
        { disable_web_page_preview: true }
      );

      const reqStartMs = nowMs();
      const buyRes = await mrktBuy({ id: g.id, priceNano });
      const reqEndMs = nowMs();
      const tEnd = new Date();

      const rttMs = reqEndMs - reqStartMs;
      const lagFromListingMs = listingDate ? (tEnd.getTime() - listingDate.getTime()) : null;

      if (buyRes.ok) {
        let timeBlock = '';
        if (listingDate) timeBlock += `Listing: ${fmtClockMsUTC(listingDate)} (${fmtIsoMs(listingDate)})\n`;
        timeBlock += `Buy start: ${fmtClockMsUTC(tStart)} (${fmtIsoMs(tStart)})\n`;
        timeBlock += `Buy end:   ${fmtClockMsUTC(tEnd)} (${fmtIsoMs(tEnd)})\n`;
        timeBlock += `RTT buy: ${rttMs} ms\n`;
        if (lagFromListingMs != null) timeBlock += `Δ listing→buy: ${lagFromListingMs} ms\n`;

        const okMsg = `✅ Куплено!\n${title}\nЦена: ${priceTon.toFixed(3)} TON\n\n${timeBlock}${urlTelegram}`;
        pushUserLog(user, 'BUY_OK', okMsg.replace(/\n/g, ' | '));
        scheduleSave();

        await sendMessageSafe(userId, okMsg, {
          disable_web_page_preview: true,
          reply_markup: { inline_keyboard: [[{ text: 'Открыть MRKT', url: urlMarket }]] }
        });

        boughtCount++;

        if (AUTO_BUY_DISABLE_AFTER_SUCCESS) {
          user.autoBuyEnabled = false;
          pushUserLog(user, 'AUTO_BUY_OFF', 'disabled after success');
          scheduleSave();
          await sendMessageSafe(userId, `Автопокупка выключена после успешной покупки (safety).`, { disable_web_page_preview: true });
          break;
        }
      } else {
        if (isNoFundsError(buyRes)) {
          user.autoBuyEnabled = false;
          user.autoBuyPausedUntil = nowMs() + AUTO_BUY_NO_FUNDS_PAUSE_MS;

          const nfMsg =
            `❌ Не хватает денег на балансе MRKT (или мало денег для этой покупки).\n` +
            `Автопокупка выключена.\n` +
            `Пауза: ${Math.round(AUTO_BUY_NO_FUNDS_PAUSE_MS / 60000)} мин.\n` +
            `Reason: ${buyRes.reason}`;

          pushUserLog(user, 'NO_FUNDS', nfMsg.replace(/\n/g, ' | '));
          scheduleSave();

          await sendMessageSafe(userId, nfMsg, { disable_web_page_preview: true });
          break;
        }

        const failMsg =
          `❌ Покупка не удалась: ${buyRes.reason}\n` +
          (buyRes.data?.message ? `message: ${buyRes.data.message}\n` : '') +
          (buyRes.text ? `body: ${buyRes.text}` : '');

        pushUserLog(user, 'BUY_FAIL', failMsg.replace(/\n/g, ' | ').slice(0, 1500));
        scheduleSave();

        await sendMessageSafe(userId, failMsg, { disable_web_page_preview: true });
      }
    }

    user.autoBuyFeedLastId = latestId;
    scheduleSave();
    return boughtCount > 0;
  } finally {
    autoBuyLocks.delete(userId);
  }
}

// =====================
// Monitor loop
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

      if (AUTO_BUY_GLOBAL && user.autoBuyEnabled) {
        await attemptAutoBuyFromNewListings(userId, user, minP, maxP);
      }

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
        await sendDeal(userId, g);

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
// Telegram commands / messages
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
  const user = getOrCreateUser(msg.from.id);

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

  await sendMessageSafe(
    msg.chat.id,
    'Бот запущен.\nОткрой кнопку меню “Панель” рядом со строкой ввода — там интерфейс.\n' +
      'Если не появилась: BotFather → /setmenubutton → WebApp → URL.',
    { reply_markup: MAIN_KEYBOARD }
  );

  if (user.autoBuyEnabled && AUTO_BUY_ONLY_NEW_LISTINGS) {
    user.autoBuyFeedLastId = null;
    await initAutoBuyBaseline(msg.from.id, user);
  }
});

bot.on('message', async (msg) => {
  const userId = msg.from?.id;
  const chatId = msg.chat?.id;
  const text = msg.text;
  if (!userId || !chatId || !text) return;
  if (text.startsWith('/')) return;

  const user = getOrCreateUser(userId);
  const t = text.trim();

  if (user.state === 'awaiting_max') {
    const v = n(t);
    if (!Number.isFinite(v) || v <= 0) return sendMessageSafe(chatId, 'Введи MAX TON (пример: 5)', { reply_markup: MAIN_KEYBOARD });
    user.maxPriceTon = v;
    user.state = null;
    scheduleSave();
    return sendMessageSafe(chatId, `Ок. MAX: ${v.toFixed(3)} TON`, { reply_markup: MAIN_KEYBOARD });
  }

  if (user.state === 'awaiting_min') {
    const v = n(t);
    if (!Number.isFinite(v) || v < 0) return sendMessageSafe(chatId, 'Введи MIN TON (0 = убрать)', { reply_markup: MAIN_KEYBOARD });
    user.minPriceTon = v;
    user.state = null;
    scheduleSave();
    return sendMessageSafe(chatId, `Ок. MIN: ${v.toFixed(3)} TON`, { reply_markup: MAIN_KEYBOARD });
  }

  if (t === '🔍 Запустить поиск') {
    user.enabled = true; scheduleSave();
    return sendMessageSafe(chatId, 'Мониторинг включён.', { reply_markup: MAIN_KEYBOARD });
  }
  if (t === '⏹ Остановить поиск') {
    user.enabled = false; scheduleSave();
    return sendMessageSafe(chatId, 'Мониторинг остановлен.', { reply_markup: MAIN_KEYBOARD });
  }
  if (t === '💰 Макс. цена') {
    user.state = 'awaiting_max'; scheduleSave();
    return sendMessageSafe(chatId, 'Введи MAX TON:', { reply_markup: MAIN_KEYBOARD });
  }
  if (t === '💵 Мин. цена') {
    user.state = 'awaiting_min'; scheduleSave();
    return sendMessageSafe(chatId, 'Введи MIN TON:', { reply_markup: MAIN_KEYBOARD });
  }
  if (t === '🤖 Автопокупка') {
    user.autoBuyEnabled = !user.autoBuyEnabled;
    if (user.autoBuyEnabled) {
      user.autoBuyPausedUntil = 0;
      user.autoBuyFeedLastId = null;
      await initAutoBuyBaseline(userId, user);
      pushUserLog(user, 'AUTO_BUY_ON', 'enabled from Telegram');
    } else {
      pushUserLog(user, 'AUTO_BUY_OFF', 'disabled from Telegram');
    }
    scheduleSave();
    return sendMessageSafe(chatId, `Автопокупка: ${user.autoBuyEnabled ? 'ON' : 'OFF'}`, { reply_markup: MAIN_KEYBOARD });
  }
  if (t === '📌 Статус') {
    const txt =
      `Status:\n` +
      `MRKT_AUTH: ${MRKT_AUTH ? '✅' : '❌'}\n` +
      `MRKT last ok: ${mrktAuthState.lastOkAt ? new Date(mrktAuthState.lastOkAt).toLocaleString() : '-'}\n` +
      `MRKT last fail: ${mrktAuthState.lastFailAt ? `HTTP ${mrktAuthState.lastFailCode}` : '-'}\n` +
      `Collections: ${MRKT_COLLECTIONS.length}\n` +
      `WebApp: ${WEBAPP_URL || 'not set'}\n`;
    return sendMessageSafe(chatId, txt, { reply_markup: MAIN_KEYBOARD });
  }

  return sendMessageSafe(chatId, 'Открой “Панель” (Menu Button) — там всё удобно.', { reply_markup: MAIN_KEYBOARD });
});

// =====================
// WebApp HTML/JS
// =====================
const WEBAPP_HTML = `<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>ton-bot panel</title>
<style>
  body { font-family: system-ui, Arial; margin: 14px; }
  .row { display:flex; gap:10px; flex-wrap:wrap; align-items:flex-end; }
  .card { border:1px solid #ddd; border-radius:10px; padding:12px; margin:10px 0; }
  label { display:block; font-size:12px; opacity:.75; margin-bottom:4px; }
  input { padding:8px; min-width: 240px; }
  button { padding:10px 12px; cursor:pointer; }
  pre { white-space: pre-wrap; word-break: break-word; background:#f7f7f7; padding:10px; border-radius:10px; }
  .ok { color:#0a7; } .bad { color:#c33; }
  #err { display:none; }
  .sug { border:1px solid #eee; border-radius:10px; margin-top:6px; overflow:hidden; }
  .sug button { width:100%; text-align:left; border:0; background:#fff; padding:10px; }
  .sug button:hover { background:#f3f3f3; }
</style>
</head>
<body>
<h2>Панель управления</h2>

<div id="err" class="card" style="border-color:#c33"></div>
<div id="status" class="card">Загрузка…</div>

<div class="card">
  <h3>Фильтры</h3>
  <div class="row">
    <div>
      <label>Gift (collection)</label>
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
    <button id="toggleAutobuy">Автопокупка: ?</button>
  </div>
</div>

<div class="card">
  <h3>Лоты MRKT (по фильтрам)</h3>
  <div class="row">
    <button id="lotsRefresh">🔄 Обновить лоты</button>
  </div>
  <div id="lots" style="margin-top:10px"></div>
</div>

<div class="card">
  <h3>Подписки</h3>
  <div class="row">
    <button id="subCreate">➕ Создать из текущих фильтров</button>
    <button id="subCheckNow">🔎 Проверить подписки сейчас</button>
    <button id="refresh">🔄 Обновить</button>
  </div>
  <div id="subs" style="margin-top:10px"></div>
</div>

<div class="card">
  <h3>Логи</h3>
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

  function sugBox(id) { return el(id); }
  function hideSug(id) { const b = sugBox(id); b.style.display='none'; b.innerHTML=''; }
  function renderSug(id, items, onPick) {
    const b = sugBox(id);
    if (!items || !items.length) { hideSug(id); return; }
    b.innerHTML = items.map((x) => '<button type="button" data-v="' + String(x).replace(/"/g,'&quot;') + '">' + x + '</button>').join('');
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
    const okAuth = st.api?.mrktAuthSet ? 'ok' : 'bad';
    el('status').innerHTML =
      '<b>Статус</b><br>' +
      'MRKT_AUTH: <span class="' + okAuth + '">' + (st.api?.mrktAuthSet ? '✅' : '❌') + '</span><br>' +
      'MRKT last fail: <b>' + (st.api?.mrktLastFailCode ?? '-') + '</b><br>' +
      'Мониторинг: <b>' + (st.user.enabled ? 'ON' : 'OFF') + '</b><br>' +
      'Автопокупка: <b>' + (st.user.autoBuyEnabled ? 'ON' : 'OFF') + '</b><br>' +
      ((st.user.autoBuyPausedUntil && st.user.autoBuyPausedUntil > Date.now())
        ? ('Пауза автопокупки до: <b>' + new Date(st.user.autoBuyPausedUntil).toISOString() + '</b><br>')
        : '');
  }

  function renderSubs(st) {
    const subs = st.user.subscriptions || [];
    if (!subs.length) {
      el('subs').innerHTML = '<i>Подписок нет</i>';
      return;
    }
    el('subs').innerHTML = subs.map(s => \`
      <div class="card">
        <div><b>#\${s.num}</b> \${s.enabled ? 'ON' : 'OFF'}</div>
        <div>Gift: \${s.filters.gift}</div>
        <div>Model: \${s.filters.model || 'any'}</div>
        <div>Backdrop: \${s.filters.backdrop || 'any'}</div>
        <div>Max: \${s.maxPriceTon ?? '∞'}</div>
        <div class="row" style="margin-top:8px">
          <button data-act="subToggle" data-id="\${s.id}">\${s.enabled ? '⏸' : '▶️'} Toggle</button>
          <button data-act="subMax" data-id="\${s.id}">💰 Set Max</button>
          <button data-act="subDel" data-id="\${s.id}">🗑 Delete</button>
        </div>
      </div>
    \`).join('');
  }

  function renderLogs(st) {
    const logs = st.user.logs || [];
    if (!logs.length) {
      el('logs').innerHTML = '<i>Логов нет</i>';
      return;
    }
    el('logs').innerHTML = logs.map(l => \`<pre>\${l.tsIso} | \${l.type}\\n\${l.text}</pre>\`).join('');
  }

  function renderLots(resp) {
    const lots = resp.lots || [];
    if (resp.ok === false) {
      el('lots').innerHTML = '<b style="color:#c33">MRKT ошибка: ' + (resp.reason || 'unknown') + '</b>';
      return;
    }
    if (!lots.length) {
      el('lots').innerHTML = '<i>Лотов по фильтрам не найдено</i>';
      return;
    }
    el('lots').innerHTML = lots.map(x => \`
      <div class="card">
        <div><b>\${x.priceTon.toFixed(3)} TON</b> — \${x.name}</div>
        <div>\${x.model ? ('Model: ' + x.model) : ''}</div>
        <div>\${x.backdrop ? ('Backdrop: ' + x.backdrop) : ''}</div>
        <div>\${x.symbol ? ('Symbol: ' + x.symbol) : ''}</div>
        <div class="row" style="margin-top:8px">
          <button data-act="open" data-url="\${x.urlTelegram}">Открыть NFT</button>
          <button data-act="open" data-url="\${x.urlMarket}">Открыть MRKT</button>
        </div>
      </div>
    \`).join('');
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

    el('toggleMonitor').textContent = 'Мониторинг: ' + (st.user.enabled ? 'ON' : 'OFF');
    el('toggleAutobuy').textContent = 'Автопокупка: ' + (st.user.autoBuyEnabled ? 'ON' : 'OFF');

    renderSubs(st);
    renderLogs(st);
  }

  async function loadLots() {
    // IMPORTANT: lots errors should NOT kill whole panel
    try {
      const r = await api('/api/mrkt/lots');
      renderLots(r);
    } catch (e) {
      el('lots').innerHTML = '<b style="color:#c33">Ошибка загрузки лотов: ' + (e.message || e) + '</b>';
    }
  }

  // ===== suggest UI =====
  function filterLocal(list, q) {
    const qq = (q || '').trim().toLowerCase();
    if (!qq) return list.slice(0, 12);
    return list.filter(x => String(x).toLowerCase().includes(qq)).slice(0, 12);
  }

  let modelTimer = null;
  let backdropTimer = null;

  el('gift').addEventListener('input', () => {
    const q = el('gift').value;
    const items = filterLocal(collections, q);
    renderSug('giftSug', items, (v) => { el('gift').value = v; });
  });

  el('model').addEventListener('input', () => {
    if (modelTimer) clearTimeout(modelTimer);
    modelTimer = setTimeout(async () => {
      const gift = el('gift').value.trim();
      const q = el('model').value.trim();
      if (!gift) { hideSug('modelSug'); return; }
      try {
        const r = await api('/api/mrkt/suggest?kind=model&gift=' + encodeURIComponent(gift) + '&q=' + encodeURIComponent(q));
        renderSug('modelSug', (r.items || []).map(x => x.label), (v) => { el('model').value = v; });
      } catch { hideSug('modelSug'); }
    }, 250);
  });

  el('backdrop').addEventListener('input', () => {
    if (backdropTimer) clearTimeout(backdropTimer);
    backdropTimer = setTimeout(async () => {
      const gift = el('gift').value.trim();
      const model = el('model').value.trim();
      const q = el('backdrop').value.trim();
      if (!gift) { hideSug('backdropSug'); return; }
      try {
        const url = '/api/mrkt/suggest?kind=backdrop&gift=' + encodeURIComponent(gift) +
          '&model=' + encodeURIComponent(model) + '&q=' + encodeURIComponent(q);
        const r = await api(url);
        renderSug('backdropSug', (r.items || []).map(x => x.label), (v) => { el('backdrop').value = v; });
      } catch { hideSug('backdropSug'); }
    }, 300);
  });

  document.addEventListener('click', (e) => {
    // close suggest when click outside inputs
    const isInp = e.target.closest('#gift,#model,#backdrop,.sug');
    if (!isInp) {
      hideSug('giftSug'); hideSug('modelSug'); hideSug('backdropSug');
    }
  });

  // actions
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

  el('toggleAutobuy').onclick = async () => {
    hideErr();
    try {
      await api('/api/action', { method: 'POST', body: JSON.stringify({ action: 'toggleAutoBuy' }) });
      await loadState();
    } catch (e) { showErr(String(e.message || e)); }
  };

  el('subCreate').onclick = async () => {
    hideErr();
    try {
      await api('/api/sub/create', { method: 'POST' });
      await loadState();
    } catch (e) { showErr(String(e.message || e)); }
  };

  el('subCheckNow').onclick = async () => {
    hideErr();
    try {
      const r = await api('/api/sub/checknow', { method: 'POST' });
      alert('Готово: subs=' + r.processedSubs + ', floorNotifs=' + r.floorNotifs + ', feedNotifs=' + r.feedNotifs);
      await loadState();
    } catch (e) { showErr(String(e.message || e)); }
  };

  el('refresh').onclick = async () => { hideErr(); await loadState().catch(e => showErr(e.message)); };
  el('logsRefresh').onclick = async () => { hideErr(); await loadState().catch(e => showErr(e.message)); };
  el('lotsRefresh').onclick = async () => { hideErr(); await loadLots(); };

  document.body.addEventListener('click', async (e) => {
    const openBtn = e.target.closest('button[data-act="open"]');
    if (openBtn) {
      const url = openBtn.getAttribute('data-url');
      if (url) tg?.openTelegramLink ? tg.openTelegramLink(url) : window.open(url, '_blank');
      return;
    }

    const btn = e.target.closest('button[data-act]');
    if (!btn) return;
    const act = btn.dataset.act;
    const id = btn.dataset.id;
    if (!id) return;

    hideErr();
    try {
      if (act === 'subToggle') {
        await api('/api/sub/toggle', { method: 'POST', body: JSON.stringify({ id }) });
      } else if (act === 'subDel') {
        await api('/api/sub/delete', { method: 'POST', body: JSON.stringify({ id }) });
      } else if (act === 'subMax') {
        const v = prompt('MAX TON для подписки:', '5');
        if (v == null) return;
        await api('/api/sub/setmax', { method: 'POST', body: JSON.stringify({ id, maxPriceTon: Number(v) }) });
      }
      await loadState();
    } catch (e2) {
      showErr(String(e2.message || e2));
    }
  });

  (async () => {
    if (!initData) {
      showErr('Открой панель из Telegram (Menu Button). В браузере без Telegram initData API не работает.');
      return;
    }
    try {
      await loadCollections();
      await loadState();
      await loadLots();
    } catch (e) {
      showErr('Панель не может подключиться к API: ' + (e.message || e));
    }
  })();
})();`;

// =====================
// Web server (WebApp + API)
// =====================
function startWebServer() {
  const app = express();
  app.use(express.json({ limit: '300kb' }));

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

  app.get('/api/meta/collections', auth, (req, res) => {
    res.json({ ok: true, collections: MRKT_COLLECTIONS });
  });

  app.get('/api/state', auth, (req, res) => {
    const u = getOrCreateUser(req.userId);
    res.json({
      ok: true,
      api: {
        mrktAuthSet: !!MRKT_AUTH,
        mrktLastFailCode: mrktAuthState.lastFailCode ?? null,
      },
      user: {
        enabled: !!u.enabled,
        minPriceTon: u.minPriceTon ?? 0,
        maxPriceTon: u.maxPriceTon ?? null,
        filters: u.filters,
        autoBuyEnabled: !!u.autoBuyEnabled,
        autoBuyPausedUntil: Number(u.autoBuyPausedUntil || 0),
        subscriptions: u.subscriptions || [],
        logs: u.logs || [],
      }
    });
  });

  // lots for panel: never throw 502 to UI, return ok:false in body instead
  app.get('/api/mrkt/lots', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    if (!MRKT_AUTH) return res.json({ ok: false, reason: 'MRKT_AUTH_NOT_SET', lots: [] });
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

    const lots = (r.gifts || [])
      .slice(0, WEBAPP_LOTS_LIMIT)
      .map((x) => ({
        id: x.id,
        name: x.name,
        priceTon: x.priceTon,
        urlTelegram: x.urlTelegram,
        urlMarket: x.urlMarket,
        model: x.attrs?.model || null,
        backdrop: x.attrs?.backdrop || null,
        symbol: x.attrs?.symbol || null,
      }));

    res.json({ ok: true, lots });
  });

  // suggest endpoint for UI dropdowns
  app.get('/api/mrkt/suggest', auth, async (req, res) => {
    const kind = String(req.query.kind || '');
    const gift = String(req.query.gift || '').trim();
    const model = String(req.query.model || '').trim();
    const q = String(req.query.q || '').trim().toLowerCase();

    if (kind === 'gift') {
      const items = MRKT_COLLECTIONS
        .filter((x) => !q || x.toLowerCase().includes(q))
        .slice(0, WEBAPP_SUGGEST_LIMIT)
        .map((x) => ({ label: x }));
      return res.json({ ok: true, items });
    }

    if (kind === 'model') {
      if (!gift) return res.json({ ok: true, items: [] });
      const models = await mrktGetModelsForGift(gift);
      const items = models
        .filter((m) => !q || m.name.toLowerCase().includes(q))
        .slice(0, WEBAPP_SUGGEST_LIMIT)
        .map((m) => ({ label: m.name }));
      return res.json({ ok: true, items });
    }

    if (kind === 'backdrop') {
      if (!gift) return res.json({ ok: true, items: [] });
      const backs = await mrktScanBackdrops(gift, model || null);
      const items = backs
        .filter((b) => !q || String(b).toLowerCase().includes(q))
        .slice(0, WEBAPP_SUGGEST_LIMIT)
        .map((b) => ({ label: b }));
      return res.json({ ok: true, items });
    }

    res.status(400).json({ ok: false, reason: 'BAD_KIND' });
  });

  app.post('/api/state/patch', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    const b = req.body || {};

    if (b.minPriceTon != null) u.minPriceTon = Number(b.minPriceTon) || 0;
    if (Object.prototype.hasOwnProperty.call(b, 'maxPriceTon')) {
      u.maxPriceTon = b.maxPriceTon == null ? null : Number(b.maxPriceTon);
    }

    let filtersChanged = false;

    if (b.filters && typeof b.filters === 'object') {
      u.filters = u.filters || { gift: '', model: '', backdrop: '' };
      if (typeof b.filters.gift === 'string') { u.filters.gift = b.filters.gift.trim(); filtersChanged = true; }
      if (typeof b.filters.model === 'string') { u.filters.model = b.filters.model.trim(); filtersChanged = true; }
      if (typeof b.filters.backdrop === 'string') { u.filters.backdrop = b.filters.backdrop.trim(); filtersChanged = true; }
    }

    if (filtersChanged && u.autoBuyEnabled && AUTO_BUY_ONLY_NEW_LISTINGS) {
      u.autoBuyFeedLastId = null;
      await initAutoBuyBaseline(req.userId, u).catch(() => {});
    }

    pushUserLog(u, 'STATE_PATCH', `gift=${u.filters.gift} model=${u.filters.model} backdrop=${u.filters.backdrop} min=${u.minPriceTon} max=${u.maxPriceTon}`);
    scheduleSave?.();

    res.json({ ok: true });
  });

  app.post('/api/action', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    const action = req.body?.action;

    if (action === 'toggleMonitor') {
      u.enabled = !u.enabled;
      pushUserLog(u, 'MONITOR', `enabled=${u.enabled}`);
      scheduleSave?.();
      return res.json({ ok: true });
    }

    if (action === 'toggleAutoBuy') {
      u.autoBuyEnabled = !u.autoBuyEnabled;
      if (u.autoBuyEnabled) {
        u.autoBuyPausedUntil = 0;
        u.autoBuyFeedLastId = null;
        pushUserLog(u, 'AUTO_BUY_ON', 'enabled from WebApp');
        await initAutoBuyBaseline(req.userId, u).catch(() => {});
      } else {
        pushUserLog(u, 'AUTO_BUY_OFF', 'disabled from WebApp');
      }
      scheduleSave?.();
      return res.json({ ok: true });
    }

    res.status(400).json({ ok: false, reason: 'UNKNOWN_ACTION' });
  });

  // subs
  app.post('/api/sub/create', auth, (req, res) => {
    const u = getOrCreateUser(req.userId);
    const r = makeSubFromCurrentFilters(u);
    if (!r.ok) return res.status(400).json({ ok: false, reason: r.reason });
    u.subscriptions.push(r.sub);
    renumberSubs(u);
    pushUserLog(u, 'SUB_CREATE', `#${r.sub.num} gift=${r.sub.filters.gift}`);
    scheduleSave?.();
    res.json({ ok: true });
  });

  app.post('/api/sub/toggle', auth, (req, res) => {
    const u = getOrCreateUser(req.userId);
    const id = req.body?.id;
    const s = findSub(u, id);
    if (!s) return res.status(404).json({ ok: false, reason: 'NOT_FOUND' });
    s.enabled = !s.enabled;
    pushUserLog(u, 'SUB_TOGGLE', `#${s.num} enabled=${s.enabled}`);
    scheduleSave?.();
    res.json({ ok: true });
  });

  app.post('/api/sub/delete', auth, (req, res) => {
    const u = getOrCreateUser(req.userId);
    const id = req.body?.id;
    const before = (u.subscriptions || []).length;
    u.subscriptions = (u.subscriptions || []).filter(x => x && x.id !== id);
    renumberSubs(u);
    pushUserLog(u, 'SUB_DELETE', `before=${before} after=${u.subscriptions.length}`);
    scheduleSave?.();
    res.json({ ok: true });
  });

  app.post('/api/sub/setmax', auth, (req, res) => {
    const u = getOrCreateUser(req.userId);
    const id = req.body?.id;
    const v = Number(req.body?.maxPriceTon);
    const s = findSub(u, id);
    if (!s) return res.status(404).json({ ok: false, reason: 'NOT_FOUND' });
    if (!Number.isFinite(v) || v <= 0) return res.status(400).json({ ok: false, reason: 'BAD_MAX' });
    s.maxPriceTon = v;
    pushUserLog(u, 'SUB_MAX', `#${s.num} max=${v}`);
    scheduleSave?.();
    res.json({ ok: true });
  });

  app.post('/api/sub/checknow', auth, async (req, res) => {
    const st = await checkSubscriptionsForAllUsers({ manual: true });
    res.json(st);
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

setInterval(() => {
  checkSubscriptionsForAllUsers().catch((e) => console.error('subs error:', e));
}, SUBS_CHECK_INTERVAL_MS);

(async () => {
  if (REDIS_URL) {
    await initRedis();
    if (redis) await loadState();
  }
  console.log('Bot started. /start');
})();
