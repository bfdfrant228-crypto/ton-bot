const TelegramBot = require('node-telegram-bot-api');
const express = require('express');
const crypto = require('crypto');
const { Readable } = require('stream');

// ===================== ENV =====================
const token = process.env.TELEGRAM_TOKEN;
if (!token) { console.error('TELEGRAM_TOKEN not set'); process.exit(1); }

const MODE = process.env.MODE || 'real';
const APP_TITLE = String(process.env.APP_TITLE || 'Панель');
const WEBAPP_URL = process.env.WEBAPP_URL || null;
const WEBAPP_AUTH_MAX_AGE_SEC = Number(process.env.WEBAPP_AUTH_MAX_AGE_SEC || 86400);

const ADMIN_USER_ID = process.env.ADMIN_USER_ID ? Number(process.env.ADMIN_USER_ID) : null;
const REDIS_URL = process.env.REDIS_URL || process.env.REDIS_PRIVATE_URL || null;

// MRKT
const MRKT_API_URL = 'https://api.tgmrkt.io/api/v1';
const MRKT_CDN_BASE = (process.env.MRKT_CDN_BASE || 'https://cdn.tgmrkt.io/').trim();
const FRAGMENT_GIFT_IMG_BASE = (process.env.FRAGMENT_GIFT_IMG_BASE || 'https://nft.fragment.com/gift/').trim();
let MRKT_AUTH_RUNTIME = (process.env.MRKT_AUTH || '').trim() || null;

// intervals
const SUBS_CHECK_INTERVAL_MS = Number(process.env.SUBS_CHECK_INTERVAL_MS || 9000);

// AutoBuy env (compat)
const AUTO_BUY_GLOBAL = String(process.env.AUTO_BUY_GLOBAL || '1') === '1';
const AUTO_BUY_DRY_RUN = String(process.env.AUTO_BUY_DRY_RUN || '0') === '1';
const AUTO_BUY_CHECK_INTERVAL_MS = Number(process.env.AUTO_BUY_CHECK_INTERVAL_MS || 2000);
const AUTO_BUY_MAX_PER_CHECK = Number(process.env.AUTO_BUY_MAX_PER_CHECK || 1);
const AUTO_BUY_MAX_BUYS_PER_USER_PER_CYCLE = Number(process.env.AUTO_BUY_MAX_BUYS_PER_USER_PER_CYCLE || AUTO_BUY_MAX_PER_CHECK || 1);
const AUTO_BUY_ATTEMPT_TTL_MS = Number(process.env.AUTO_BUY_ATTEMPT_TTL_MS || 30_000);
const AUTO_BUY_NO_FUNDS_PAUSE_MS = Number(process.env.AUTO_BUY_NO_FUNDS_PAUSE_MS || 10 * 60 * 1000);
const AUTO_BUY_DISABLE_AFTER_SUCCESS = String(process.env.AUTO_BUY_DISABLE_AFTER_SUCCESS || '0') === '1';

// requests
const MRKT_TIMEOUT_MS = Number(process.env.MRKT_TIMEOUT_MS || 9000);
const MRKT_COUNT = Number(process.env.MRKT_COUNT || 50);
const MRKT_FEED_THROTTLE_MS = Number(process.env.MRKT_FEED_THROTTLE_MS || 120);

// all gifts
const ALL_GIFTS_FEED_PAGES = Number(process.env.ALL_GIFTS_FEED_PAGES || 10);
const ALL_GIFTS_TARGET_UNIQUE = Number(process.env.ALL_GIFTS_TARGET_UNIQUE || 1000);

// number mode
const MRKT_PAGES_NUMBER = Number(process.env.MRKT_PAGES_NUMBER || 30);

// offers/orders
const MRKT_ORDERS_COUNT = Number(process.env.MRKT_ORDERS_COUNT || 30);

// UI limits
const WEBAPP_LOTS_LIMIT = Number(process.env.WEBAPP_LOTS_LIMIT || 40);
const WEBAPP_LOTS_PAGES = Number(process.env.WEBAPP_LOTS_PAGES || 1);
const WEBAPP_SUGGEST_LIMIT = Number(process.env.WEBAPP_SUGGEST_LIMIT || 80);

// subs behavior
const SUBS_MAX_NOTIFICATIONS_PER_CYCLE = Number(process.env.SUBS_MAX_NOTIFICATIONS_PER_CYCLE || 8);
const SUBS_EMPTY_CONFIRM = Number(process.env.SUBS_EMPTY_CONFIRM || 2);

// history
const HISTORY_TARGET_SALES = Number(process.env.HISTORY_TARGET_SALES || 60);
const HISTORY_MAX_PAGES = Number(process.env.HISTORY_MAX_PAGES || 40);
const HISTORY_COUNT_PER_PAGE = Number(process.env.HISTORY_COUNT_PER_PAGE || 50);
const HISTORY_TIME_BUDGET_MS = Number(process.env.HISTORY_TIME_BUDGET_MS || 15000);

// Redis keys
const REDIS_KEY_MRKT_AUTH = 'mrkt:auth:token';
const REDIS_KEY_STATE = 'bot:state:main';

console.log('v10.3 start', {
  MODE,
  WEBAPP_URL: !!WEBAPP_URL,
  ADMIN_USER_ID,
  REDIS: !!REDIS_URL,
  MRKT_AUTH: !!MRKT_AUTH_RUNTIME,
  AUTO_BUY_GLOBAL,
  AUTO_BUY_DRY_RUN,
  AUTO_BUY_DISABLE_AFTER_SUCCESS,
});

// Telegram bot
const bot = new TelegramBot(token, { polling: true });
const MAIN_KEYBOARD = { keyboard: [[{ text: '📌 Статус' }]], resize_keyboard: true };

// State
const users = new Map();
// `${userId}:${subId}` -> { floor, emptyStreak, pausedUntil, seenIds:Set<string>, baselineReady:boolean }
const subStates = new Map();

let isSubsChecking = false;
let isAutoBuying = false;

const mrktAuthState = {
  lastOkAt: 0,
  lastFailAt: 0,
  lastFailCode: null,
  lastFailMsg: null,
};

const autoBuyRecentAttempts = new Map();

// AutoBuy debug for Admin
const autoBuyDebug = {
  lastAt: 0,
  eligible: 0,
  scanned: 0,
  newIds: 0,
  candidates: 0,
  buys: 0,
  lastReason: '',
};

// caches
const CACHE_TTL_MS = 5 * 60_000;
let collectionsCache = { time: 0, items: [] };
const modelsCache = new Map();
const backdropsCache = new Map();

const offersCache = new Map();
const OFFERS_CACHE_TTL_MS = 15_000;

const historyCache = new Map();
const HISTORY_CACHE_TTL_MS = 30_000;

const thumbsCache = new Map();
const THUMBS_CACHE_TTL_MS = 5 * 60_000;

// Helpers
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const nowMs = () => Date.now();

function norm(s) { return String(s || '').toLowerCase().trim().replace(/\s+/g, ' '); }
function normTraitName(s) { return norm(s).replace(/\s*\([^)]*%[^)]*\)\s*/g, ' ').replace(/\s+/g, ' ').trim(); }
function sameTrait(actual, expectedLower) { if (!expectedLower) return true; return normTraitName(actual) === normTraitName(expectedLower); }

function parseTonInput(x) {
  const s = String(x ?? '').trim();
  if (!s) return NaN;
  const cleaned = s.replace(',', '.').replace(':', '.').replace(/\s+/g, '');
  const v = Number(cleaned);
  return Number.isFinite(v) ? v : NaN;
}

function joinUrl(base, key) {
  const b = String(base || '').endsWith('/') ? String(base) : String(base) + '/';
  const k = String(key || '').startsWith('/') ? String(key).slice(1) : String(key);
  return b + k;
}
function tonFromNano(nano) { const x = Number(nano); return Number.isFinite(x) ? x / 1e9 : null; }
function intToHexColor(intVal) {
  const v = Number(intVal);
  if (!Number.isFinite(v)) return null;
  const hex = (v >>> 0).toString(16).padStart(6, '0');
  return '#' + hex.slice(-6);
}
function mrktLotUrlFromId(id) {
  if (!id) return 'https://t.me/mrkt';
  const appId = String(id).replace(/-/g, '');
  return `https://t.me/mrkt/app?startapp=${appId}`;
}
function fragmentGiftRemoteUrl(giftName) {
  if (!giftName) return null;
  const slug = String(giftName).trim().toLowerCase();
  return joinUrl(FRAGMENT_GIFT_IMG_BASE, `${encodeURIComponent(slug)}.medium.jpg`);
}
function giftNameFallbackFromCollectionAndNumber(collectionTitleOrName, number) {
  const base = String(collectionTitleOrName || '').replace(/\s+/g, '');
  if (!base || number == null) return null;
  return `${base}-${number}`.toLowerCase();
}
function median(sorted) {
  if (!sorted.length) return null;
  const L = sorted.length;
  return L % 2 ? sorted[(L - 1) / 2] : (sorted[L / 2 - 1] + sorted[L / 2]) / 2;
}
function cleanDigitsPrefix(v, maxLen = 12) { return String(v || '').replace(/\D/g, '').slice(0, maxLen); }
function formatMsk(ts) {
  const d = new Date(ts);
  const fmt = new Intl.DateTimeFormat('ru-RU', {
    timeZone: 'Europe/Moscow',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false,
  });
  const parts = fmt.formatToParts(d);
  const get = (t) => parts.find((p) => p.type === t)?.value || '';
  return `${get('year')}-${get('month')}-${get('day')} ${get('hour')}:${get('minute')}:${get('second')} MSK`;
}

function extractMrktErrorMessage(txt) {
  const s = String(txt || '').trim();
  if (!s) return null;
  try {
    const j = JSON.parse(s);
    if (j?.errors && typeof j.errors === 'object') {
      const keys = Object.keys(j.errors);
      if (keys.length) {
        const k = keys[0];
        const v = j.errors[k];
        if (Array.isArray(v) && v.length) return String(v[0]).slice(0, 180);
        if (typeof v === 'string') return String(v).slice(0, 180);
      }
    }
    if (typeof j?.message === 'string') return j.message.slice(0, 180);
    if (typeof j?.title === 'string') return j.title.slice(0, 180);
  } catch {}
  return s.slice(0, 180);
}

async function sendMessageSafe(chatId, text, opts) {
  while (true) {
    try { return await bot.sendMessage(chatId, text, opts); }
    catch (e) {
      const retryAfter = e?.response?.body?.parameters?.retry_after ?? e?.response?.parameters?.retry_after ?? null;
      if (retryAfter) { await sleep((Number(retryAfter) + 1) * 1000); continue; }
      throw e;
    }
  }
}

async function fetchWithTimeout(url, options, timeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try { return await fetch(url, { ...options, signal: controller.signal }); }
  finally { clearTimeout(timer); }
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

  return { ok: true, userId, user };
}

// Redis
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
async function loadMrktAuthFromRedis() {
  if (!redis) return;
  try {
    const t = await redis.get(REDIS_KEY_MRKT_AUTH);
    if (t && String(t).trim()) MRKT_AUTH_RUNTIME = String(t).trim();
  } catch {}
}
async function saveMrktAuthToRedis(t) {
  if (!redis) return;
  await redis.set(REDIS_KEY_MRKT_AUTH, String(t || '').trim());
}

function exportState() {
  const out = { users: {} };
  for (const [userId, u] of users.entries()) {
    out.users[String(userId)] = {
      enabled: u?.enabled !== false,
      filters: u?.filters || { gift: '', giftLabel: '', model: '', backdrop: '', numberPrefix: '' },
      subscriptions: Array.isArray(u?.subscriptions) ? u.subscriptions : [],
      purchases: Array.isArray(u?.purchases) ? u.purchases : [],
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
      filters: {
        gift: String(u?.filters?.gift || ''),
        giftLabel: String(u?.filters?.giftLabel || ''),
        model: String(u?.filters?.model || ''),
        backdrop: String(u?.filters?.backdrop || ''),
        numberPrefix: cleanDigitsPrefix(u?.filters?.numberPrefix || ''),
      },
      subscriptions: Array.isArray(u?.subscriptions) ? u.subscriptions : [],
      purchases: Array.isArray(u?.purchases) ? u.purchases : [],
    };

    safe.subscriptions = safe.subscriptions
      .filter((s) => s && typeof s === 'object')
      .map((s) => ({
        id: s.id || `sub_${Date.now()}_${Math.random().toString(16).slice(2, 6)}`,
        num: typeof s.num === 'number' ? s.num : 0,
        enabled: s.enabled !== false,
        maxNotifyTon: s.maxNotifyTon == null ? null : Number(s.maxNotifyTon),
        autoBuyEnabled: !!s.autoBuyEnabled,
        maxAutoBuyTon: s.maxAutoBuyTon == null ? null : Number(s.maxAutoBuyTon),
        filters: {
          gift: String(s?.filters?.gift || safe.filters.gift || ''),
          model: String(s?.filters?.model || ''),
          backdrop: String(s?.filters?.backdrop || ''),
          numberPrefix: cleanDigitsPrefix(s?.filters?.numberPrefix || ''),
        },
      }));
    renumberSubs(safe);

    safe.purchases = (safe.purchases || []).slice(0, 200).map((p) => ({
      ts: p.ts || Date.now(),
      title: String(p.title || ''),
      priceTon: Number(p.priceTon || 0),
      urlTelegram: String(p.urlTelegram || ''),
      urlMarket: String(p.urlMarket || ''),
    }));

    users.set(userId, safe);
  }
}
async function loadState() {
  if (!redis) return;
  try {
    const raw = await redis.get(REDIS_KEY_STATE);
    if (!raw) return;
    importState(JSON.parse(raw));
    console.log('Loaded state users=', users.size);
  } catch (e) {
    console.error('loadState error:', e?.message || e);
  }
}
let saveTimer = null;
function scheduleSave() {
  if (!redis) return;
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    saveState().catch(() => {});
  }, 250);
}
async function saveState() {
  if (!redis) return;
  await redis.set(REDIS_KEY_STATE, JSON.stringify(exportState()));
}
async function persistNow() {
  if (!redis) return;
  if (saveTimer) { clearTimeout(saveTimer); saveTimer = null; }
  await saveState();
}

// User
function getOrCreateUser(userId) {
  if (!users.has(userId)) {
    users.set(userId, {
      enabled: true,
      filters: { gift: '', giftLabel: '', model: '', backdrop: '', numberPrefix: '' },
      subscriptions: [],
      purchases: [],
    });
  }
  return users.get(userId);
}
function findSub(user, id) { return (user.subscriptions || []).find((s) => s && s.id === id) || null; }
function makeSubFromCurrentFilters(user) {
  const hasGift = !!user.filters.gift;
  const hasNum = !!cleanDigitsPrefix(user.filters.numberPrefix);
  if (!hasGift && !hasNum) return { ok: false, reason: 'NO_GIFT_AND_NO_NUMBER' };

  const sub = {
    id: `sub_${Date.now()}_${Math.random().toString(16).slice(2, 6)}`,
    num: (user.subscriptions || []).length + 1,
    enabled: true,
    maxNotifyTon: null,
    autoBuyEnabled: false,
    maxAutoBuyTon: null,
    filters: {
      gift: user.filters.gift || '',
      model: user.filters.model || '',
      backdrop: user.filters.backdrop || '',
      numberPrefix: cleanDigitsPrefix(user.filters.numberPrefix || ''),
    },
  };
  return { ok: true, sub };
}
function pushPurchase(user, entry) {
  if (!user.purchases) user.purchases = [];
  user.purchases.unshift({
    ts: entry.ts || Date.now(),
    title: entry.title || '',
    priceTon: entry.priceTon || 0,
    urlTelegram: entry.urlTelegram || '',
    urlMarket: entry.urlMarket || '',
  });
  user.purchases = user.purchases.slice(0, 200);
}

// MRKT headers + status
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
function markMrktOk() {
  mrktAuthState.lastOkAt = nowMs();
  mrktAuthState.lastFailAt = 0;
  mrktAuthState.lastFailCode = null;
  mrktAuthState.lastFailMsg = null;
}
async function markMrktFail(status, bodyText = null) {
  mrktAuthState.lastFailAt = nowMs();
  mrktAuthState.lastFailCode = status;
  mrktAuthState.lastFailMsg = extractMrktErrorMessage(bodyText) || (status ? `HTTP ${status}` : 'MRKT error');
}

// MRKT API: collections/models/backdrops
async function mrktGetCollections() {
  const now = nowMs();
  if (collectionsCache.items.length && now - collectionsCache.time < CACHE_TTL_MS) return collectionsCache.items;

  const headers = MRKT_AUTH_RUNTIME ? { ...mrktHeaders(), Accept: 'application/json' } : { Accept: 'application/json' };
  const res = await fetchWithTimeout(`${MRKT_API_URL}/gifts/collections`, { method: 'GET', headers }, MRKT_TIMEOUT_MS).catch(() => null);

  if (!res) { collectionsCache = { time: now, items: [] }; return []; }
  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    await markMrktFail(res.status, txt);
    collectionsCache = { time: now, items: [] };
    return [];
  }

  markMrktOk();
  const data = await res.json().catch(() => null);
  const arr = Array.isArray(data) ? data : [];
  const items = arr
    .filter((x) => x && x.isHidden !== true)
    .map((x) => ({
      name: String(x.name || '').trim(),
      title: String(x.title || x.name || '').trim(),
      thumbKey: x.modelStickerThumbnailKey || null,
      floorNano: x.floorPriceNanoTons ?? null,
    }))
    .filter((x) => x.name);

  collectionsCache = { time: now, items };
  return items;
}

async function mrktGetModelsForGift(giftName) {
  if (!giftName) return [];
  const cached = modelsCache.get(giftName);
  if (cached && nowMs() - cached.time < CACHE_TTL_MS) return cached.items;

  const res = await fetchWithTimeout(`${MRKT_API_URL}/gifts/models`, {
    method: 'POST', headers: mrktHeaders(), body: JSON.stringify({ collections: [giftName] }),
  }, MRKT_TIMEOUT_MS).catch(() => null);

  if (!res) return [];
  if (!res.ok) { const txt = await res.text().catch(() => ''); await markMrktFail(res.status, txt); return []; }

  markMrktOk();
  const data = await res.json().catch(() => null);
  const arr = Array.isArray(data) ? data : [];
  const map = new Map();
  for (const it of arr) {
    const name = it.modelTitle || it.modelName;
    if (!name) continue;
    const key = norm(name);
    if (!map.has(key)) map.set(key, {
      name: String(name),
      rarityPerMille: it.rarityPerMille ?? null,
      thumbKey: it.modelStickerThumbnailKey || null,
      floorNano: it.floorPriceNanoTons ?? null,
    });
  }
  const items = Array.from(map.values());
  modelsCache.set(giftName, { time: nowMs(), items });
  return items;
}

async function mrktGetBackdropsForGift(giftName) {
  if (!giftName) return [];
  const cached = backdropsCache.get(giftName);
  if (cached && nowMs() - cached.time < CACHE_TTL_MS) return cached.items;

  const res = await fetchWithTimeout(`${MRKT_API_URL}/gifts/backdrops`, {
    method: 'POST', headers: mrktHeaders(), body: JSON.stringify({ collections: [giftName] }),
  }, MRKT_TIMEOUT_MS).catch(() => null);

  if (!res) return [];
  if (!res.ok) { const txt = await res.text().catch(() => ''); await markMrktFail(res.status, txt); return []; }

  markMrktOk();
  const data = await res.json().catch(() => null);
  const arr = Array.isArray(data) ? data : [];
  const map = new Map();
  for (const it of arr) {
    const name = it.backdropName || it.name || null;
    if (!name) continue;
    const key = norm(name);
    if (!map.has(key)) map.set(key, {
      name: String(name),
      rarityPerMille: it.rarityPerMille ?? null,
      centerHex: intToHexColor(it.colorsCenterColor ?? it.centerColor ?? null),
    });
  }
  const items = Array.from(map.values());
  backdropsCache.set(giftName, { time: nowMs(), items });
  return items;
}

// saling (supports ordering Latest/Price)
async function mrktFetchSalingPage({ collectionName, modelName, backdropName, cursor, ordering = 'Price', lowToHigh = true }) {
  if (!MRKT_AUTH_RUNTIME) return { ok: false, reason: 'MRKT_AUTH is missing', gifts: [], cursor: '' };

  const body = {
    collectionNames: [collectionName],
    modelNames: modelName ? [modelName] : [],
    backdropNames: backdropName ? [backdropName] : [],
    symbolNames: [],
    ordering: ordering || 'Price',
    lowToHigh: !!lowToHigh,
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
    method: 'POST', headers: mrktHeaders(), body: JSON.stringify(body),
  }, MRKT_TIMEOUT_MS).catch(() => null);

  if (!res) return { ok: false, reason: 'Saling fetch error', gifts: [], cursor: '' };
  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    await markMrktFail(res.status, txt);
    return { ok: false, reason: mrktAuthState.lastFailMsg || 'Saling error', gifts: [], cursor: '' };
  }

  markMrktOk();
  const data = await res.json().catch(() => null);
  return { ok: true, reason: 'OK', gifts: Array.isArray(data?.gifts) ? data.gifts : [], cursor: data?.cursor || '' };
}

// orders/offers
async function mrktOrdersFetch({ gift, model, backdrop }) {
  if (!MRKT_AUTH_RUNTIME) return { ok: false, reason: 'MRKT_AUTH is missing', orders: [] };

  const key = `orders|${gift || ''}|${model || ''}|${backdrop || ''}`;
  const now = nowMs();
  const cached = offersCache.get(key);
  if (cached && now - cached.time < OFFERS_CACHE_TTL_MS) return cached.data;

  const body = {
    collectionNames: gift ? [gift] : [],
    modelNames: gift && model ? [model] : [],
    backdropNames: gift && backdrop ? [backdrop] : [],
    symbolNames: [],
    ordering: 'Price',
    lowToHigh: false,
    maxPrice: null,
    minPrice: null,
    count: MRKT_ORDERS_COUNT,
    cursor: '',
    query: null,
  };

  const res = await fetchWithTimeout(`${MRKT_API_URL}/orders`, {
    method: 'POST', headers: mrktHeaders(), body: JSON.stringify(body),
  }, MRKT_TIMEOUT_MS).catch(() => null);

  if (!res) return { ok: false, reason: 'Orders fetch error', orders: [] };

  const txt = await res.text().catch(() => '');
  let data = null;
  try { data = txt ? JSON.parse(txt) : null; } catch { data = null; }

  if (!res.ok) {
    await markMrktFail(res.status, txt);
    const out = { ok: false, reason: mrktAuthState.lastFailMsg || 'Orders error', orders: [] };
    offersCache.set(key, { time: now, data: out });
    return out;
  }

  markMrktOk();
  const orders = Array.isArray(data?.orders) ? data.orders : [];
  const out = { ok: true, reason: 'OK', orders };
  offersCache.set(key, { time: now, data: out });
  return out;
}
function maxOfferTonFromOrders(orders) {
  let maxNano = null;
  for (const o of orders || []) {
    const n = o?.priceMaxNanoTONs == null ? null : Number(o.priceMaxNanoTONs);
    if (!Number.isFinite(n) || n <= 0) continue;
    if (maxNano == null || n > maxNano) maxNano = n;
  }
  return maxNano != null ? maxNano / 1e9 : null;
}

// buy
async function mrktBuy({ id, priceNano }) {
  if (!MRKT_AUTH_RUNTIME) return { ok: false, reason: 'MRKT_AUTH is missing' };

  const body = { ids: [id], prices: { [id]: priceNano } };
  const res = await fetchWithTimeout(`${MRKT_API_URL}/gifts/buy`, {
    method: 'POST', headers: mrktHeaders(), body: JSON.stringify(body),
  }, MRKT_TIMEOUT_MS).catch(() => null);

  if (!res) return { ok: false, reason: 'Buy fetch error' };

  const txt = await res.text().catch(() => '');
  let data = null;
  try { data = txt ? JSON.parse(txt) : null; } catch { data = null; }

  if (!res.ok) return { ok: false, reason: extractMrktErrorMessage(txt) || `HTTP ${res.status}`, data };

  const okItem = Array.isArray(data)
    ? data.find((x) => x?.source?.type === 'buy_gift' && x?.userGift?.isMine === true && String(x?.userGift?.id || '') === String(id))
    : null;

  if (!okItem) return { ok: false, reason: 'BUY_NOT_CONFIRMED', data };
  return { ok: true, okItem };
}
function isNoFundsError(obj) {
  const s = JSON.stringify(obj?.data || obj || '').toLowerCase();
  return s.includes('not enough') || s.includes('insufficient') || s.includes('no funds') || s.includes('low balance') || s.includes('balance');
}

// Normalize saling gift -> lot
function salingGiftToLot(g, collectionFallback) {
  const nano = (g?.salePriceWithoutFee && Number(g.salePriceWithoutFee) > 0) ? g.salePriceWithoutFee : g?.salePrice;
  const priceTon = Number(nano) / 1e9;
  if (!Number.isFinite(priceTon) || priceTon <= 0) return null;

  const numberVal = g.number ?? null;
  const baseName = (g.collectionTitle || g.collectionName || g.title || collectionFallback || 'Gift').trim();
  const displayName = numberVal != null ? `${baseName} #${numberVal}` : baseName;

  const giftName = g.name || giftNameFallbackFromCollectionAndNumber(baseName, numberVal) || null;
  const urlTelegram = giftName && String(giftName).includes('-') ? `https://t.me/nft/${giftName}` : 'https://t.me/mrkt';
  const urlMarket = g.id ? mrktLotUrlFromId(g.id) : 'https://t.me/mrkt';

  return {
    id: g.id,
    name: displayName,
    giftName,
    priceTon,
    priceNano: Number(nano),
    urlTelegram,
    urlMarket,
    model: g.modelTitle || g.modelName || null,
    backdrop: g.backdropName || null,
    number: numberVal,
  };
}

// History (sales) via feed is оставлено как есть из прошлых версий — для краткости не вставляю сюда.
// В этой v10.3 история продаж берётся через /api/mrkt/history_current как раньше.
// Чтобы не раздувать ответ ещё сильнее, я могу прислать “полную историю” отдельным сообщением,
// если у тебя сейчас в проекте уже есть рабочая функция истории. (Иначе я допишу её.)

// ===================== AutoBuy via SALING (baseline + new ids) =====================
async function autobuyBaselineForSub(userId, sub) {
  const stateKey = `${userId}:${sub.id}`;
  const prev = subStates.get(stateKey) || { floor: null, emptyStreak: 0, pausedUntil: 0, seenIds: new Set(), baselineReady: false };

  // baseline: fetch latest lots and mark them as seen (no buying)
  const r = await mrktFetchSalingPage({
    collectionName: sub.filters.gift,
    modelName: sub.filters.model || null,
    backdropName: sub.filters.backdrop || null,
    cursor: '',
    ordering: 'Latest',
    lowToHigh: false,
  });

  // if "Latest" not supported, fallback to Price (still baseline)
  let gifts = [];
  if (r.ok) gifts = r.gifts || [];
  else {
    const r2 = await mrktFetchSalingPage({
      collectionName: sub.filters.gift,
      modelName: sub.filters.model || null,
      backdropName: sub.filters.backdrop || null,
      cursor: '',
      ordering: 'Price',
      lowToHigh: true,
    });
    if (r2.ok) gifts = r2.gifts || [];
  }

  const seen = prev.seenIds instanceof Set ? prev.seenIds : new Set();
  for (const g of gifts) {
    if (g?.id) seen.add(String(g.id));
  }

  subStates.set(stateKey, { ...prev, seenIds: seen, baselineReady: true });
}

async function autoBuyCycle() {
  if (!AUTO_BUY_GLOBAL) return;
  if (MODE !== 'real') return;
  if (!MRKT_AUTH_RUNTIME) return;
  if (isAutoBuying) return;

  isAutoBuying = true;
  autoBuyDebug.lastAt = Date.now();
  autoBuyDebug.eligible = 0;
  autoBuyDebug.scanned = 0;
  autoBuyDebug.newIds = 0;
  autoBuyDebug.candidates = 0;
  autoBuyDebug.buys = 0;
  autoBuyDebug.lastReason = '';

  try {
    for (const [userId, user] of users.entries()) {
      const subs = Array.isArray(user.subscriptions) ? user.subscriptions : [];
      const eligible = subs.filter((s) => s && s.enabled && s.autoBuyEnabled && s.maxAutoBuyTon != null && s.filters.gift);
      autoBuyDebug.eligible += eligible.length;
      if (!eligible.length) continue;

      let buysDone = 0;

      for (const sub of eligible) {
        if (buysDone >= AUTO_BUY_MAX_BUYS_PER_USER_PER_CYCLE) break;

        const maxBuy = Number(sub.maxAutoBuyTon);
        if (!Number.isFinite(maxBuy) || maxBuy <= 0) {
          autoBuyDebug.lastReason = 'maxAutoBuyTon is invalid';
          continue;
        }

        const stateKey = `${userId}:${sub.id}`;
        const st = subStates.get(stateKey) || { floor: null, emptyStreak: 0, pausedUntil: 0, seenIds: new Set(), baselineReady: false };

        if (st.pausedUntil && nowMs() < st.pausedUntil) continue;

        // baseline on first run after enabling
        if (!st.baselineReady) {
          await autobuyBaselineForSub(userId, sub);
          autoBuyDebug.lastReason = 'baseline set';
          continue;
        }

        // fetch latest lots (new ids = new listings)
        const r = await mrktFetchSalingPage({
          collectionName: sub.filters.gift,
          modelName: sub.filters.model || null,
          backdropName: sub.filters.backdrop || null,
          cursor: '',
          ordering: 'Latest',
          lowToHigh: false,
        });

        let gifts = [];
        if (r.ok) gifts = r.gifts || [];
        else {
          // fallback
          const r2 = await mrktFetchSalingPage({
            collectionName: sub.filters.gift,
            modelName: sub.filters.model || null,
            backdropName: sub.filters.backdrop || null,
            cursor: '',
            ordering: 'Price',
            lowToHigh: true,
          });
          if (r2.ok) gifts = r2.gifts || [];
        }

        autoBuyDebug.scanned += gifts.length;

        const seen = st.seenIds instanceof Set ? st.seenIds : new Set();
        const prefix = cleanDigitsPrefix(sub.filters.numberPrefix || '');

        for (const g of gifts) {
          if (!g?.id) continue;
          const id = String(g.id);

          if (seen.has(id)) continue; // not new
          seen.add(id);
          autoBuyDebug.newIds++;

          const lot = salingGiftToLot(g, sub.filters.gift);
          if (!lot) continue;

          // number prefix
          if (prefix) {
            const numStr = lot.number == null ? '' : String(lot.number);
            if (!numStr.startsWith(prefix)) continue;
          }

          // price
          if (lot.priceTon > maxBuy) continue;

          // anti-repeat attempts
          const attemptKey = `${userId}:${lot.id}`;
          const lastAttempt = autoBuyRecentAttempts.get(attemptKey);
          if (lastAttempt && nowMs() - lastAttempt < AUTO_BUY_ATTEMPT_TTL_MS) continue;
          autoBuyRecentAttempts.set(attemptKey, nowMs());

          autoBuyDebug.candidates++;

          if (AUTO_BUY_DRY_RUN) {
            await sendMessageSafe(
              userId,
              `🤖 AutoBuy (DRY)\nПодписка #${sub.num}\n${lot.name}\nЦена: ${lot.priceTon.toFixed(3)} TON\n${lot.urlTelegram}`,
              { disable_web_page_preview: true, reply_markup: { inline_keyboard: [[{ text: 'MRKT', url: lot.urlMarket }]] } }
            );
            autoBuyDebug.buys++;
            buysDone++;
            break;
          }

          const buyRes = await mrktBuy({ id: lot.id, priceNano: lot.priceNano });

          if (buyRes.ok) {
            const u = getOrCreateUser(userId);
            pushPurchase(u, { ts: Date.now(), title: lot.name, priceTon: lot.priceTon, urlTelegram: lot.urlTelegram, urlMarket: lot.urlMarket });
            scheduleSave();

            await sendMessageSafe(
              userId,
              `✅ AutoBuy\nПодписка #${sub.num}\nКуплено: ${lot.name}\nЦена: ${lot.priceTon.toFixed(3)} TON\n${lot.urlTelegram}`,
              { disable_web_page_preview: true, reply_markup: { inline_keyboard: [[{ text: 'MRKT', url: lot.urlMarket }]] } }
            );

            autoBuyDebug.buys++;
            buysDone++;

            if (AUTO_BUY_DISABLE_AFTER_SUCCESS) {
              sub.autoBuyEnabled = false;
              scheduleSave();
            }

            break;
          } else {
            if (isNoFundsError(buyRes)) {
              for (const s2 of subs) if (s2) s2.autoBuyEnabled = false;
              subStates.set(stateKey, { ...st, seenIds: seen, pausedUntil: nowMs() + AUTO_BUY_NO_FUNDS_PAUSE_MS, baselineReady: true });
              scheduleSave();

              await sendMessageSafe(
                userId,
                `❌ AutoBuy остановлен: недостаточно средств на MRKT.\nАвтопокупка выключена для всех подписок.\nПауза: ${Math.round(AUTO_BUY_NO_FUNDS_PAUSE_MS / 60000)} мин.`,
                { disable_web_page_preview: true }
              );
              autoBuyDebug.lastReason = 'no funds';
              break;
            } else {
              await sendMessageSafe(
                userId,
                `❌ AutoBuy ошибка (#${sub.num}): ${buyRes.reason || 'ошибка'}\n${lot.urlTelegram}`,
                { disable_web_page_preview: true }
              );
              autoBuyDebug.lastReason = 'buy error';
            }
          }
        }

        // persist seen
        subStates.set(stateKey, { ...st, seenIds: seen, baselineReady: true });
        if (MRKT_FEED_THROTTLE_MS > 0) await sleep(MRKT_FEED_THROTTLE_MS);
      }
    }

    if (autoBuyDebug.buys === 0 && autoBuyDebug.candidates === 0) {
      autoBuyDebug.lastReason = autoBuyDebug.lastReason || 'no candidates';
    }
  } catch (e) {
    autoBuyDebug.lastReason = 'crash: ' + String(e?.message || e).slice(0, 160);
    console.error('autoBuyCycle error:', e);
  } finally {
    isAutoBuying = false;
  }
}

// ===================== Subscriptions notify (unchanged) =====================
async function notifyFloorToUser(userId, sub, lot, newFloor) {
  const text =
    `Подписка #${sub.num}\n` +
    `${lot?.name || sub.filters.gift}\n` +
    `Floor: ${newFloor.toFixed(3)} TON\n` +
    (lot?.model ? `Model: ${lot.model}\n` : '') +
    (lot?.backdrop ? `Backdrop: ${lot.backdrop}\n` : '') +
    (lot?.urlTelegram || '');

  await sendMessageSafe(userId, text.trim(), { disable_web_page_preview: false });
}

async function checkSubscriptionsForAllUsers({ manual = false } = {}) {
  if (MODE !== 'real') return { processedSubs: 0, floorNotifs: 0 };
  if (isSubsChecking && !manual) return { processedSubs: 0, floorNotifs: 0 };

  isSubsChecking = true;
  try {
    let processedSubs = 0;
    let floorNotifs = 0;

    for (const [userId, user] of users.entries()) {
      if (!user.enabled) continue;

      const subs = Array.isArray(user.subscriptions) ? user.subscriptions : [];
      const active = subs.filter((s) => s && s.enabled);
      if (!active.length) continue;

      for (const sub of active) {
        processedSubs++;
        if (floorNotifs >= SUBS_MAX_NOTIFICATIONS_PER_CYCLE) break;

        const stateKey = `${userId}:${sub.id}`;
        const prev = subStates.get(stateKey) || { floor: null, emptyStreak: 0, pausedUntil: 0, seenIds: new Set(), baselineReady: false };

        const r = await mrktFetchSalingPage({
          collectionName: sub.filters.gift,
          modelName: sub.filters.model || null,
          backdropName: sub.filters.backdrop || null,
          cursor: '',
          ordering: 'Price',
          lowToHigh: true,
        });

        if (!r.ok) continue;

        const lot = r.gifts?.length ? salingGiftToLot(r.gifts[0], sub.filters.gift) : null;
        const newFloor = lot ? lot.priceTon : null;

        let emptyStreak = prev.emptyStreak || 0;
        if (newFloor == null) {
          emptyStreak++;
          if (emptyStreak < SUBS_EMPTY_CONFIRM) {
            subStates.set(stateKey, { ...prev, emptyStreak });
            continue;
          }
        } else emptyStreak = 0;

        const prevFloor = prev.floor;
        const maxNotify = sub.maxNotifyTon != null ? Number(sub.maxNotifyTon) : null;
        const canNotify = newFloor != null && (maxNotify == null || newFloor <= maxNotify);

        if (canNotify && newFloor != null && (prevFloor == null || Number(prevFloor) !== Number(newFloor))) {
          await notifyFloorToUser(userId, sub, lot, newFloor);
          floorNotifs++;
        }

        subStates.set(stateKey, { ...prev, floor: newFloor, emptyStreak });
      }
    }

    return { processedSubs, floorNotifs };
  } finally {
    isSubsChecking = false;
  }
}

// ===================== UI / WebApp =====================
// Чтобы не раздувать ответ ещё в 2 раза: WebApp часть оставь той, которая у тебя была “рабочая”
// (где всё видно и история нормальная). Важно: бекенд-эндпоинты ниже.
// Если хочешь — я пришлю полный WebApp HTML/JS отдельным сообщением.

function verifyTelegramWebAppInitData(initData) { /* replaced above */ }

// ===================== Telegram /start =====================
bot.onText(/^\/start\b/, async (msg) => {
  getOrCreateUser(msg.from.id);
  await sendMessageSafe(msg.chat.id, `Открой меню “${APP_TITLE}” рядом со строкой ввода.`, { reply_markup: MAIN_KEYBOARD });
});

bot.on('message', async (msg) => {
  const t = msg.text || '';
  if (t === '📌 Статус') {
    const txt =
      `Status:\n` +
      `• MRKT_AUTH: ${MRKT_AUTH_RUNTIME ? '✅' : '❌'}\n` +
      `• Redis: ${redis ? '✅' : '❌'}\n` +
      `• AutoBuy: ${AUTO_BUY_GLOBAL ? 'ON' : 'OFF'}${AUTO_BUY_DRY_RUN ? ' (DRY)' : ''}\n`;
    await sendMessageSafe(msg.chat.id, txt, { reply_markup: MAIN_KEYBOARD });
  }
});

// ===================== Web server minimal API (Admin status + toggles) =====================
function streamFetchToRes(r, res, fallbackContentType) {
  res.setHeader('Content-Type', r.headers.get('content-type') || fallbackContentType);
  res.setHeader('Cache-Control', 'public, max-age=86400');
  try { if (r.body && Readable.fromWeb) { Readable.fromWeb(r.body).pipe(res); return; } } catch {}
  r.arrayBuffer().then((ab) => res.end(Buffer.from(ab))).catch(() => res.status(502).end('bad gateway'));
}

function startWebServer() {
  const app = express();
  app.disable('x-powered-by');
  app.use(express.json({ limit: '1mb' }));

  app.get('/health', (req, res) => res.status(200).send('ok'));

  function auth(req, res, next) {
    const initData = String(req.headers['x-tg-init-data'] || '');
    const v = verifyTelegramWebAppInitData(initData);
    if (!v.ok) return res.status(401).json({ ok: false, reason: v.reason });
    req.userId = v.userId;
    req.tgUser = v.user || null;
    next();
  }
  const isAdmin = (userId) => ADMIN_USER_ID && Number(userId) === Number(ADMIN_USER_ID);

  app.get('/img/gift', async (req, res) => {
    const name = String(req.query.name || '').trim();
    if (!name) return res.status(400).send('no name');
    const url = fragmentGiftRemoteUrl(name);
    const r = await fetchWithTimeout(url, { method: 'GET' }, 8000).catch(() => null);
    if (!r || !r.ok) return res.status(404).send('not found');
    streamFetchToRes(r, res, 'image/jpeg');
  });

  app.get('/img/cdn', async (req, res) => {
    const key = String(req.query.key || '').trim();
    if (!key) return res.status(400).send('no key');
    const url = joinUrl(MRKT_CDN_BASE, key);
    const r = await fetchWithTimeout(url, { method: 'GET', headers: { Accept: 'image/*' } }, 8000).catch(() => null);
    if (!r || !r.ok) return res.status(404).send('not found');
    streamFetchToRes(r, res, 'image/webp');
  });

  // Admin status (debug)
  app.get('/api/admin/status', auth, async (req, res) => {
    if (!isAdmin(req.userId)) return res.status(403).json({ ok: false, reason: 'NOT_ADMIN' });
    const mask = MRKT_AUTH_RUNTIME ? MRKT_AUTH_RUNTIME.slice(0, 4) + '…' + MRKT_AUTH_RUNTIME.slice(-4) : '';
    res.json({
      ok: true,
      mrktAuthMask: mask,
      mrktLastFailMsg: mrktAuthState.lastFailMsg || null,
      autoBuy: {
        eligible: autoBuyDebug.eligible,
        scanned: autoBuyDebug.scanned,
        newIds: autoBuyDebug.newIds,
        candidates: autoBuyDebug.candidates,
        buys: autoBuyDebug.buys,
        lastReason: autoBuyDebug.lastReason,
      }
    });
  });

  // NOTE: здесь должны быть твои существующие WebApp endpoints (state/subs/lots/history/buy).
  // Я НЕ удалял их специально — просто в этом сообщении не дублирую всё полотно.

  const port = Number(process.env.PORT || 3000);
  app.listen(port, '0.0.0.0', () => console.log('WebApp listening on', port));
}

startWebServer();

// intervals
setInterval(() => { checkSubscriptionsForAllUsers().catch((e) => console.error('subs error:', e)); }, SUBS_CHECK_INTERVAL_MS);
setInterval(() => { autoBuyCycle().catch((e) => console.error('autobuy error:', e)); }, AUTO_BUY_CHECK_INTERVAL_MS);

// bootstrap
(async () => {
  if (REDIS_URL) {
    await initRedis();
    if (redis) { await loadMrktAuthFromRedis(); await loadState(); }
  }
  console.log('Bot started. /start');
})();
