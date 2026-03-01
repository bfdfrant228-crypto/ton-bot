/**
 * MRKT Panel + Bot (single file)
 * v9.8:
 * - FIX: mrktFeedFetch defined (no more 502 from ReferenceError)
 * - FIX: "All gifts" lots from /feed listing filtered by gift.isOnSale === true (avoid stale 0.5)
 * - FIX: hover transform no longer breaks clear (x) buttons
 * - UI: offers pill calmer
 * - keeps Profile + purchases persisted in Redis
 * - restores AutoBuy settings controls in subscriptions (settings only)
 *
 * 2026-03-01 v9.8
 */

const TelegramBot = require('node-telegram-bot-api');
const express = require('express');
const crypto = require('crypto');
const { Readable } = require('stream');

// ===================== ENV =====================
const token = process.env.TELEGRAM_TOKEN;
if (!token) {
  console.error('TELEGRAM_TOKEN not set');
  process.exit(1);
}

const MODE = process.env.MODE || 'real';
const APP_TITLE = String(process.env.APP_TITLE || 'Панель');
const WEBAPP_URL = process.env.WEBAPP_URL || null;
const WEBAPP_AUTH_MAX_AGE_SEC = Number(process.env.WEBAPP_AUTH_MAX_AGE_SEC || 86400);

const ADMIN_USER_ID = process.env.ADMIN_USER_ID ? Number(process.env.ADMIN_USER_ID) : null;

const REDIS_URL = process.env.REDIS_URL || process.env.REDIS_PRIVATE_URL || null;

const MRKT_API_URL = 'https://api.tgmrkt.io/api/v1';
const MRKT_CDN_BASE = (process.env.MRKT_CDN_BASE || 'https://cdn.tgmrkt.io/').trim();
const FRAGMENT_GIFT_IMG_BASE = (process.env.FRAGMENT_GIFT_IMG_BASE || 'https://nft.fragment.com/gift/').trim();

let MRKT_AUTH_RUNTIME = (process.env.MRKT_AUTH || '').trim() || null;

// intervals
const SUBS_CHECK_INTERVAL_MS = Number(process.env.SUBS_CHECK_INTERVAL_MS || 9000);

// requests
const MRKT_TIMEOUT_MS = Number(process.env.MRKT_TIMEOUT_MS || 9000);
const MRKT_COUNT = Number(process.env.MRKT_COUNT || 50);
const MRKT_FEED_COUNT = Number(process.env.MRKT_FEED_COUNT || 60);
const MRKT_FEED_THROTTLE_MS = Number(process.env.MRKT_FEED_THROTTLE_MS || 120);

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

// notify throttling
const MRKT_AUTH_NOTIFY_COOLDOWN_MS = Number(process.env.MRKT_AUTH_NOTIFY_COOLDOWN_MS || 60 * 60 * 1000);

// Redis keys (STABLE key to not "lose" state between versions)
const REDIS_KEY_MRKT_AUTH = 'mrkt:auth:token';
const REDIS_KEY_STATE = 'bot:state:main';

console.log('v9.8 start', {
  MODE,
  APP_TITLE,
  WEBAPP_URL: !!WEBAPP_URL,
  ADMIN_USER_ID,
  REDIS: !!REDIS_URL,
  MRKT_AUTH: !!MRKT_AUTH_RUNTIME,
});

// ===================== Telegram bot =====================
const bot = new TelegramBot(token, { polling: true });

const MAIN_KEYBOARD = {
  keyboard: [[{ text: '📌 Статус' }]],
  resize_keyboard: true,
};

// ===================== State =====================
const users = new Map(); // userId -> state
const subStates = new Map(); // `${userId}:${subId}` -> { floor, emptyStreak }
let isSubsChecking = false;

const mrktAuthState = {
  lastOkAt: 0,
  lastFailAt: 0,
  lastFailCode: null,
  lastFailBody: null,
  lastNotifiedAt: 0,
};

// caches
const CACHE_TTL_MS = 5 * 60_000;
let collectionsCache = { time: 0, items: [] }; // [{name,title,thumbKey,floorNano}]
const modelsCache = new Map(); // gift -> { time, items }
const backdropsCache = new Map(); // gift -> { time, items }

const historyCache = new Map();
const HISTORY_CACHE_TTL_MS = 30_000;

const offersCache = new Map();
const OFFERS_CACHE_TTL_MS = 15_000;

// ===================== Helpers =====================
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const nowMs = () => Date.now();

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
function joinUrl(base, key) {
  const b = String(base || '').endsWith('/') ? String(base) : String(base) + '/';
  const k = String(key || '').startsWith('/') ? String(key).slice(1) : String(key);
  return b + k;
}
function tonFromNano(nano) {
  const x = Number(nano);
  if (!Number.isFinite(x)) return null;
  return x / 1e9;
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
  const s = pct % 1 === 0 ? pct.toFixed(0) : pct.toFixed(1);
  return `${s}%`;
}
function mrktLotUrlFromId(id) {
  if (!id) return 'https://t.me/mrkt';
  const appId = String(id).replace(/-/g, '');
  return `https://t.me/mrkt/app?startapp=${appId}`;
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
function median(sorted) {
  if (!sorted.length) return null;
  const L = sorted.length;
  return L % 2 ? sorted[(L - 1) / 2] : (sorted[L / 2 - 1] + sorted[L / 2]) / 2;
}
function cleanDigitsPrefix(v, maxLen = 12) {
  return String(v || '').replace(/\D/g, '').slice(0, maxLen);
}
function formatMsk(ts) {
  const d = new Date(ts);
  const fmt = new Intl.DateTimeFormat('ru-RU', {
    timeZone: 'Europe/Moscow',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  });
  const parts = fmt.formatToParts(d);
  const get = (t) => parts.find((p) => p.type === t)?.value || '';
  return `${get('year')}-${get('month')}-${get('day')} ${get('hour')}:${get('minute')}:${get('second')} MSK`;
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

  return { ok: true, userId, user };
}

// ===================== Redis persistence =====================
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

    safe.purchases = safe.purchases
      .filter((p) => p && typeof p === 'object')
      .slice(0, 200)
      .map((p) => ({
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
  if (saveTimer) {
    clearTimeout(saveTimer);
    saveTimer = null;
  }
  await saveState();
}

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

function findSub(user, id) {
  return (user.subscriptions || []).find((s) => s && s.id === id) || null;
}

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

// ===================== MRKT auth state =====================
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

async function notifyAdminMrktAuthExpired(status) {
  if (!ADMIN_USER_ID) return;
  const now = nowMs();
  if (now - mrktAuthState.lastNotifiedAt < MRKT_AUTH_NOTIFY_COOLDOWN_MS) return;
  mrktAuthState.lastNotifiedAt = now;
  try {
    await sendMessageSafe(
      ADMIN_USER_ID,
      `⚠️ MRKT_AUTH не работает (HTTP ${status}).\nПанель → Admin → вставь новый токен.`,
      { disable_web_page_preview: true }
    );
  } catch {}
}

function markMrktOk() {
  mrktAuthState.lastOkAt = nowMs();
  mrktAuthState.lastFailAt = 0;
  mrktAuthState.lastFailCode = null;
  mrktAuthState.lastFailBody = null;
}
async function markMrktFail(status, bodyText = null) {
  mrktAuthState.lastFailAt = nowMs();
  mrktAuthState.lastFailCode = status;
  if (bodyText) mrktAuthState.lastFailBody = String(bodyText).slice(0, 200);
  if (status === 401 || status === 403) await notifyAdminMrktAuthExpired(status);
}

// ===================== MRKT API: collections/models/backdrops =====================
async function mrktGetCollections() {
  const now = nowMs();
  if (collectionsCache.items.length && now - collectionsCache.time < CACHE_TTL_MS) return collectionsCache.items;

  const headers = MRKT_AUTH_RUNTIME ? { ...mrktHeaders(), Accept: 'application/json' } : { Accept: 'application/json' };
  const res = await fetchWithTimeout(`${MRKT_API_URL}/gifts/collections`, { method: 'GET', headers }, MRKT_TIMEOUT_MS).catch(() => null);

  if (!res) { collectionsCache = { time: now, items: [] }; return []; }
  if (!res.ok) { const txt = await res.text().catch(() => ''); await markMrktFail(res.status, txt); collectionsCache = { time: now, items: [] }; return []; }

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

  const res = await fetchWithTimeout(
    `${MRKT_API_URL}/gifts/models`,
    { method: 'POST', headers: mrktHeaders(), body: JSON.stringify({ collections: [giftName] }) },
    MRKT_TIMEOUT_MS
  ).catch(() => null);

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

  const res = await fetchWithTimeout(
    `${MRKT_API_URL}/gifts/backdrops`,
    { method: 'POST', headers: mrktHeaders(), body: JSON.stringify({ collections: [giftName] }) },
    MRKT_TIMEOUT_MS
  ).catch(() => null);

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

// ===================== MRKT API: FEED (FIXED) =====================
async function mrktFeedFetch({ gift, model, backdrop, cursor, count, types, ordering = 'Latest', lowToHigh = false }) {
  if (!MRKT_AUTH_RUNTIME) return { ok: false, reason: 'NO_AUTH', items: [], cursor: '' };

  const body = {
    count: Number(count || MRKT_FEED_COUNT),
    cursor: cursor || '',
    collectionNames: gift ? [gift] : [],
    modelNames: gift && model ? [model] : [],
    backdropNames: gift && backdrop ? [backdrop] : [],
    lowToHigh: !!lowToHigh,
    maxPrice: null,
    minPrice: null,
    number: null,
    ordering: ordering || 'Latest',
    query: null,
    type: Array.isArray(types) ? types : [],
  };

  const res = await fetchWithTimeout(
    `${MRKT_API_URL}/feed`,
    { method: 'POST', headers: mrktHeaders(), body: JSON.stringify(body) },
    MRKT_TIMEOUT_MS
  ).catch(() => null);

  if (!res) return { ok: false, reason: 'FETCH_ERROR', items: [], cursor: '' };
  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    await markMrktFail(res.status, txt);
    return { ok: false, reason: `HTTP_${res.status}`, items: [], cursor: '' };
  }

  markMrktOk();
  const data = await res.json().catch(() => null);
  return {
    ok: true,
    reason: 'OK',
    items: Array.isArray(data?.items) ? data.items : [],
    cursor: data?.cursor || '',
  };
}

// ===================== MRKT API: saling =====================
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
    promotedFirst: false,
  };

  const res = await fetchWithTimeout(
    `${MRKT_API_URL}/gifts/saling`,
    { method: 'POST', headers: mrktHeaders(), body: JSON.stringify(body) },
    MRKT_TIMEOUT_MS
  ).catch(() => null);

  if (!res) return { ok: false, reason: 'FETCH_ERROR', gifts: [], cursor: '' };
  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    await markMrktFail(res.status, txt);
    return { ok: false, reason: `HTTP_${res.status}`, gifts: [], cursor: '' };
  }

  markMrktOk();
  const data = await res.json().catch(() => null);
  return { ok: true, reason: 'OK', gifts: Array.isArray(data?.gifts) ? data.gifts : [], cursor: data?.cursor || '' };
}

// ===================== MRKT: All gifts cheapest listings (feed) =====================
async function mrktFeedListingsAll({ cursor = '', count = MRKT_FEED_COUNT }) {
  // We try Price+lowToHigh, fallback to Latest. Then FILTER by gift.isOnSale === true.
  let r = await mrktFeedFetch({
    gift: null,
    model: null,
    backdrop: null,
    cursor,
    count,
    types: ['listing'],
    ordering: 'Price',
    lowToHigh: true,
  });

  if (!r.ok) {
    r = await mrktFeedFetch({
      gift: null,
      model: null,
      backdrop: null,
      cursor,
      count,
      types: ['listing'],
      ordering: 'Latest',
      lowToHigh: false,
    });
  }
  if (!r.ok) return r;

  // FILTER stale listings
  const items = (r.items || []).filter((it) => {
    const g = it?.gift;
    if (!g) return false;
    // must be currently on sale
    if (g.isOnSale !== true) return false;
    if (g.isOnPlatform === false) return false;
    const amountNano = it.amount ?? g.salePriceWithoutFee ?? g.salePrice ?? null;
    const n = amountNano == null ? NaN : Number(amountNano);
    return Number.isFinite(n) && n > 0;
  });

  return { ...r, items };
}

function mapListingItemToLot(it) {
  const g = it?.gift;
  if (!g?.id) return null;

  if (g.isOnSale !== true) return null;

  const amountNano = it.amount ?? g.salePriceWithoutFee ?? g.salePrice ?? null;
  const priceNano = amountNano == null ? NaN : Number(amountNano);
  if (!Number.isFinite(priceNano) || priceNano <= 0) return null;

  const priceTon = priceNano / 1e9;

  const titleBase = g.collectionTitle || g.collectionName || g.title || 'Gift';
  const numberVal = g.number ?? null;
  const name = numberVal != null ? `${titleBase} #${numberVal}` : titleBase;

  const giftName = g.name || giftNameFallbackFromCollectionAndNumber(titleBase, numberVal) || null;
  const urlTelegram = giftName && String(giftName).includes('-') ? `https://t.me/nft/${giftName}` : 'https://t.me/mrkt';
  const urlMarket = mrktLotUrlFromId(g.id);

  return {
    id: g.id,
    name,
    giftName,
    priceTon,
    priceNano,
    urlTelegram,
    urlMarket,
    model: g.modelTitle || g.modelName || null,
    backdrop: g.backdropName || null,
    number: numberVal,
  };
}

// ===================== MRKT: search lots by filters =====================
async function mrktSearchLotsByFilters({ gift, model, backdrop, numberPrefix }, pagesLimit) {
  const prefix = cleanDigitsPrefix(numberPrefix || '');

  // no gift -> global feed listings
  if (!gift) {
    let cursor = '';
    const out = [];
    for (let page = 0; page < Math.max(2, pagesLimit); page++) {
      const r = await mrktFeedListingsAll({ cursor, count: MRKT_FEED_COUNT });
      if (!r.ok) return { ok: false, reason: r.reason, gifts: [] };
      if (!r.items.length) break;

      for (const it of r.items) {
        const lot = mapListingItemToLot(it);
        if (!lot) continue;

        const numStr = lot.number == null ? '' : String(lot.number);
        if (prefix && !numStr.startsWith(prefix)) continue;

        // best effort model/backdrop filtering
        if (model && lot.model && !sameTrait(lot.model, norm(model))) continue;
        if (backdrop && lot.backdrop && !sameTrait(lot.backdrop, norm(backdrop))) continue;

        out.push(lot);
        if (out.length >= 220) break;
      }

      if (out.length >= 220) break;
      cursor = r.cursor || '';
      if (!cursor) break;
      await sleep(MRKT_FEED_THROTTLE_MS);
    }

    out.sort((a, b) => a.priceTon - b.priceTon);
    return { ok: true, reason: 'OK', gifts: out };
  }

  // gift selected -> saling
  const extraPages = prefix && prefix.length <= 2 ? 3 : (prefix ? 2 : 0);
  const maxPages = Math.max(pagesLimit, extraPages);

  let cursor = '';
  const out = [];

  for (let page = 0; page < maxPages; page++) {
    const r = await mrktFetchSalingPage({
      collectionName: gift,
      modelName: model || null,
      backdropName: backdrop || null,
      cursor,
    });
    if (!r.ok) return { ok: false, reason: r.reason, gifts: [] };

    for (const g of r.gifts) {
      const nano = (g?.salePriceWithoutFee && Number(g.salePriceWithoutFee) > 0) ? g.salePriceWithoutFee : g?.salePrice;
      if (nano == null) continue;

      const priceTon = Number(nano) / 1e9;
      if (!Number.isFinite(priceTon) || priceTon <= 0) continue;

      const numberVal = g.number ?? null;
      const numStr = numberVal == null ? '' : String(numberVal);
      if (prefix && !numStr.startsWith(prefix)) continue;

      const baseName = (g.collectionTitle || g.collectionName || g.title || gift).trim();
      const displayName = numberVal ? `${baseName} #${numberVal}` : baseName;

      const giftName = g.name || giftNameFallbackFromCollectionAndNumber(baseName, numberVal) || null;
      const urlTelegram = giftName && String(giftName).includes('-') ? `https://t.me/nft/${giftName}` : 'https://t.me/mrkt';
      const urlMarket = g.id ? mrktLotUrlFromId(g.id) : 'https://t.me/mrkt';

      out.push({
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
      });

      if (out.length >= 500) break;
    }

    cursor = r.cursor || '';
    if (!cursor || out.length >= 500) break;
  }

  out.sort((a, b) => a.priceTon - b.priceTon);
  return { ok: true, reason: 'OK', gifts: out };
}

// ===================== MRKT: orders/offers =====================
async function mrktOrdersFetch({ gift, model, backdrop }) {
  if (!MRKT_AUTH_RUNTIME) return { ok: false, reason: 'NO_AUTH', orders: [] };

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

  const res = await fetchWithTimeout(`${MRKT_API_URL}/orders`, { method: 'POST', headers: mrktHeaders(), body: JSON.stringify(body) }, MRKT_TIMEOUT_MS).catch(() => null);
  if (!res) return { ok: false, reason: 'FETCH_ERROR', orders: [] };

  const txt = await res.text().catch(() => '');
  let data = null;
  try { data = txt ? JSON.parse(txt) : null; } catch { data = null; }

  if (!res.ok) {
    await markMrktFail(res.status, txt);
    const out = { ok: false, reason: `HTTP_${res.status}`, orders: [] };
    offersCache.set(key, { time: now, data: out });
    return out;
  }

  markMrktOk();
  const orders = Array.isArray(data?.orders) ? data.orders : [];
  const out = { ok: true, reason: 'OK', orders };
  offersCache.set(key, { time: now, data: out });
  return out;
}

// ===================== MRKT: buy =====================
async function mrktBuy({ id, priceNano }) {
  if (!MRKT_AUTH_RUNTIME) return { ok: false, reason: 'NO_AUTH' };

  const body = { ids: [id], prices: { [id]: priceNano } };
  const res = await fetchWithTimeout(`${MRKT_API_URL}/gifts/buy`, { method: 'POST', headers: mrktHeaders(), body: JSON.stringify(body) }, MRKT_TIMEOUT_MS).catch(() => null);
  if (!res) return { ok: false, reason: 'FETCH_ERROR' };

  const txt = await res.text().catch(() => '');
  let data = null;
  try { data = txt ? JSON.parse(txt) : null; } catch { data = null; }

  if (!res.ok) return { ok: false, reason: `HTTP_${res.status}`, data, text: txt.slice(0, 600) };

  const okItem = Array.isArray(data)
    ? data.find((x) => x?.source?.type === 'buy_gift' && x?.userGift?.isMine === true && String(x?.userGift?.id || '') === String(id))
    : null;

  if (!okItem) return { ok: false, reason: 'BUY_NOT_CONFIRMED', data };
  return { ok: true, data, okItem };
}

// ===================== History (uses mrktFeedFetch now works) =====================
async function mrktHistorySales({ gift, model, backdrop, numberPrefix }) {
  const prefix = cleanDigitsPrefix(numberPrefix || '');
  const key = `hist|${gift || ''}|${model || ''}|${backdrop || ''}|${prefix}`;
  const now = nowMs();
  const cached = historyCache.get(key);
  if (cached && now - cached.time < HISTORY_CACHE_TTL_MS) return cached.data;

  const started = nowMs();
  let cursor = '';
  let pages = 0;
  const prices = [];
  const lastSales = [];

  while (pages < HISTORY_MAX_PAGES && prices.length < HISTORY_TARGET_SALES) {
    if (nowMs() - started > HISTORY_TIME_BUDGET_MS) break;

    const r = await mrktFeedFetch({
      gift: gift || null,
      model: gift && model ? model : null,
      backdrop: gift && backdrop ? backdrop : null,
      cursor,
      count: HISTORY_COUNT_PER_PAGE,
      types: ['sale'],
      ordering: 'Latest',
      lowToHigh: false,
    });

    if (!r.ok) break;
    if (!r.items.length) break;

    for (const it of r.items) {
      const g = it.gift;
      if (!g) continue;

      const num = g.number ?? null;
      const numStr = num == null ? '' : String(num);
      if (prefix && !numStr.startsWith(prefix)) continue;

      const amountNano = it.amount ?? g.salePrice ?? g.salePriceWithoutFee ?? null;
      const ton = amountNano != null ? Number(amountNano) / 1e9 : NaN;
      if (!Number.isFinite(ton) || ton <= 0) continue;

      prices.push(ton);

      if (lastSales.length < 35) {
        const titleBase = g.collectionTitle || g.collectionName || g.title || (gift || 'Gift');
        const giftName = g.name || giftNameFallbackFromCollectionAndNumber(titleBase, g.number) || null;
        const urlTelegram = giftName && String(giftName).includes('-') ? `https://t.me/nft/${giftName}` : null;
        const urlMarket = g.id ? mrktLotUrlFromId(g.id) : null;

        lastSales.push({
          tsMsk: it.date ? formatMsk(it.date) : '',
          priceTon: ton,
          number: g.number ?? null,
          model: g.modelTitle || g.modelName || null,
          backdrop: g.backdropName || null,
          imgUrl: giftName ? `/img/gift?name=${encodeURIComponent(giftName)}` : null,
          urlTelegram,
          urlMarket,
        });
      }

      if (prices.length >= HISTORY_TARGET_SALES) break;
    }

    cursor = r.cursor || '';
    pages++;
    if (!cursor) break;
    await sleep(MRKT_FEED_THROTTLE_MS);
  }

  prices.sort((a, b) => a - b);
  const data = { ok: true, median: median(prices), count: prices.length, lastSales };
  historyCache.set(key, { time: now, data });
  return data;
}

// ===================== Sub notifications =====================
async function notifyFloorToUser(userId, sub, lot, newFloor) {
  const title = lot?.name || sub?.filters?.gift || 'Gift';
  const urlTelegram = lot?.urlTelegram || 'https://t.me/mrkt';
  const urlMarket = lot?.urlMarket || 'https://t.me/mrkt';
  const num = lot?.number != null ? `#${lot.number}` : (sub?.filters?.numberPrefix ? `#${sub.filters.numberPrefix}*` : '');

  const lines = [];
  lines.push(`Gift: ${title} ${num}`.trim());
  lines.push(`Floor: ${newFloor.toFixed(3)} TON`);
  if (lot?.model) lines.push(`Model: ${lot.model}`);
  if (lot?.backdrop) lines.push(`Backdrop: ${lot.backdrop}`);
  lines.push(urlTelegram); // preview

  const webUrl = WEBAPP_URL ? `${WEBAPP_URL}?tab=subs&subId=${encodeURIComponent(sub.id)}` : null;

  const openSubBtn = webUrl
    ? { text: 'Открыть подписку', web_app: { url: webUrl } }
    : { text: 'Открыть подписку', url: urlMarket };

  const reply_markup = {
    inline_keyboard: [[
      openSubBtn,
      { text: 'MRKT', url: urlMarket },
    ]],
  };

  await sendMessageSafe(userId, lines.join('\n'), {
    disable_web_page_preview: false,
    reply_markup,
  });
}

// ===================== subs worker =====================
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
        const prev = subStates.get(stateKey) || { floor: null, emptyStreak: 0 };

        const lots = await mrktSearchLotsByFilters({
          gift: sub.filters.gift || '',
          model: sub.filters.model || '',
          backdrop: sub.filters.backdrop || '',
          numberPrefix: sub.filters.numberPrefix || '',
        }, 1);

        if (!lots.ok) continue;

        const lot = lots.gifts[0] || null;
        const newFloor = lot ? lot.priceTon : null;

        let emptyStreak = prev.emptyStreak || 0;
        if (newFloor == null) {
          emptyStreak++;
          if (emptyStreak < SUBS_EMPTY_CONFIRM) {
            subStates.set(stateKey, { ...prev, emptyStreak });
            continue;
          }
        } else {
          emptyStreak = 0;
        }

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

// ===================== Telegram menu button =====================
async function tgApi(method, body) {
  const res = await fetch(`https://api.telegram.org/bot${token}/${method}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  }).catch(() => null);

  const data = res ? await res.json().catch(() => null) : null;
  if (!data?.ok) throw new Error(`Telegram API ${method} failed`);
  return data.result;
}

bot.onText(/^\/start\b/, async (msg) => {
  getOrCreateUser(msg.from.id);

  if (WEBAPP_URL) {
    try {
      await tgApi('setChatMenuButton', {
        chat_id: msg.chat.id,
        menu_button: { type: 'web_app', text: APP_TITLE, web_app: { url: WEBAPP_URL } },
      });
    } catch {}
  }

  await sendMessageSafe(msg.chat.id, `Открой меню “${APP_TITLE}” рядом со строкой ввода.`, { reply_markup: MAIN_KEYBOARD });
});

bot.on('message', async (msg) => {
  const t = msg.text || '';
  if (t === '📌 Статус') {
    const txt =
      `Status:\n` +
      `• MRKT_AUTH: ${MRKT_AUTH_RUNTIME ? '✅' : '❌'}\n` +
      `• Redis: ${redis ? '✅' : '❌'}\n` +
      `• Persist key: ${REDIS_KEY_STATE}\n` +
      `• last fail: ${mrktAuthState.lastFailAt ? `HTTP ${mrktAuthState.lastFailCode}` : '-'}\n`;
    await sendMessageSafe(msg.chat.id, txt, { reply_markup: MAIN_KEYBOARD });
  }
});

// ===================== WebApp HTML/JS =====================
// To keep this message shorter, WebApp is minimal but with FIXED hover and calmer offer pill.
const WEBAPP_HTML = `<!doctype html><html lang="ru"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"/>
<title>${APP_TITLE}</title>
<style>
:root{
  --bg:#0b0f14; --card:#101826; --text:#e5e7eb; --muted:#9ca3af; --border:#223044;
  --input:#0f172a; --btn:#182235; --btnText:#e5e7eb; --accent:#22c55e; --accent2:#38bdf8; --danger:#ef4444;
}
*{box-sizing:border-box}
body{margin:0;padding:14px;background:var(--bg);color:var(--text);font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial}
h2{margin:0 0 10px 0}
.card{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:12px;margin:10px 0}
label{display:block;font-size:12px;color:var(--muted);margin-bottom:4px}
.row{display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end}
.field{position:relative;flex:1 1 200px;min-width:140px}
.inpWrap{position:relative}
input{width:100%;padding:10px;border:1px solid var(--border);border-radius:12px;background:var(--input);color:var(--text);outline:none}
input.compact{height:38px;padding:8px 32px 8px 10px;font-size:13px}
input.compact.expanded{height:44px;font-size:14px}
button{padding:10px 12px;border:1px solid var(--border);border-radius:12px;background:var(--btn);color:var(--btnText);cursor:pointer;transition:filter .12s ease}
button:hover{filter:brightness(1.08)} /* FIX: no transform here */
.lift{transition:filter .12s ease, transform .08s ease}
.lift:hover{transform:translateY(-1px)}
.primary{border-color:var(--accent);color:#052e16;background:var(--accent);font-weight:950}
.small{padding:8px 10px;border-radius:10px;font-size:13px}
.xbtn{position:absolute;right:8px;top:50%;transform:translateY(-50%);width:20px;height:20px;border-radius:8px;border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.05);color:rgba(255,255,255,.55);cursor:pointer;display:flex;align-items:center;justify-content:center;line-height:1}
.xbtn:hover{filter:brightness(1.2);color:rgba(255,255,255,.85)}
.badge{padding:4px 8px;border-radius:999px;border:1px solid var(--border);color:var(--muted);font-size:12px}
.hr{height:1px;background:var(--border);margin:10px 0}
.muted{color:var(--muted);font-size:13px}
#err{display:none;border-color:var(--danger);color:#ffd1d1;white-space:pre-wrap;word-break:break-word}

.tabs{display:flex;gap:8px;flex-wrap:wrap}
.tabbtn{border-radius:999px;padding:8px 12px}
.tabbtn.active{border-color:var(--accent);color:var(--accent)}

.sug{border:1px solid var(--border);border-radius:14px;overflow:auto;background:var(--card);max-height:360px;position:absolute;top:calc(100% + 6px);left:0;right:0;z-index:9999;box-shadow:0 14px 40px rgba(0,0,0,.45)}
.sug .item{width:100%;text-align:left;border:0;background:transparent;padding:10px;display:flex;gap:10px;align-items:center}
.sug .item:hover{background:rgba(255,255,255,.06)}
.thumb{width:54px;height:54px;border-radius:16px;object-fit:contain;background:rgba(255,255,255,.06);border:1px solid var(--border);flex:0 0 auto}

.offerPill{display:inline-flex;align-items:center;gap:10px;padding:6px 10px;border-radius:999px;border:1px solid rgba(56,189,248,.22);background:rgba(56,189,248,.04);color:rgba(234,247,255,.86);font-weight:800;font-size:13px}

.grid{display:grid;gap:10px;margin-top:10px;grid-template-columns: repeat(auto-fill, minmax(170px, 1fr))}
@media (max-width: 520px){.grid{grid-template-columns: repeat(2, minmax(0, 1fr));}}
.lot{border:1px solid var(--border);border-radius:16px;padding:10px;background:rgba(255,255,255,.02)}
.lot img{width:100%;aspect-ratio:1/1;height:auto;object-fit:cover;background:rgba(255,255,255,.03);border-radius:14px;border:1px solid var(--border)}
.price{font-size:15px;font-weight:950;margin-top:8px}

.sheetWrap{position:fixed;inset:0;background:rgba(0,0,0,.55);display:none;align-items:flex-end;justify-content:center;z-index:50000;padding:12px}
.sheet{width:min(980px, 96vw);height:min(70vh, 720px);background:var(--card);border:1px solid var(--border);border-radius:18px;padding:12px;box-shadow:0 30px 90px rgba(0,0,0,.5);display:flex;flex-direction:column;gap:10px}
.sheetHeader{display:flex;justify-content:space-between;gap:12px;align-items:center}
.sheetBody{overflow:auto;padding-right:4px}

/* dark scrollbar */
.sheetBody::-webkit-scrollbar,.sug::-webkit-scrollbar{width:10px;height:10px}
.sheetBody::-webkit-scrollbar-track,.sug::-webkit-scrollbar-track{background:rgba(255,255,255,.03);border-radius:999px}
.sheetBody::-webkit-scrollbar-thumb,.sug::-webkit-scrollbar-thumb{background:rgba(255,255,255,.18);border-radius:999px}
.sheetBody::-webkit-scrollbar-button,.sug::-webkit-scrollbar-button{display:none}

.modalWrap{position:fixed;inset:0;background:rgba(0,0,0,.55);display:none;align-items:flex-end;justify-content:center;z-index:60000;padding:12px}
.modal{width:min(560px, 96vw);background:var(--card);border:1px solid var(--border);border-radius:18px;padding:12px;box-shadow:0 30px 90px rgba(0,0,0,.5)}
.modalHeader{display:flex;justify-content:space-between;gap:12px;align-items:center}
.modalTitle{font-weight:950}
.modalImg{width:100%;max-height:240px;height:auto;object-fit:contain;border-radius:14px;border:1px solid var(--border);background:rgba(255,255,255,.03);margin-top:10px}
.buy{border-color:rgba(56,189,248,.65); background:rgba(56,189,248,.14); color:#eaf7ff; font-weight:950}
.buy:hover{border-color:rgba(56,189,248,1); background:rgba(56,189,248,.20)}
.saleRow{display:flex;gap:10px;align-items:stretch}
.saleImg{width:88px;height:88px;border-radius:14px;border:1px solid var(--border);object-fit:cover;background:rgba(255,255,255,.03);flex:0 0 auto}
.saleCard{flex:1;border:1px solid var(--border);border-radius:14px;padding:10px;background:rgba(255,255,255,.02)}
.saleBtns{display:flex;gap:8px;margin-top:8px;flex-wrap:wrap}
.thumbWrap{width:54px;height:54px;border-radius:16px;border:1px solid var(--border);background:rgba(255,255,255,.04);display:flex;align-items:center;justify-content:center;overflow:hidden}
.thumbWrap img{width:54px;height:54px;object-fit:contain}
</style></head><body>
<h2>${APP_TITLE}</h2>
<div id="err" class="card"></div>

<div class="tabs">
  <button class="tabbtn active lift" data-tab="market">Market</button>
  <button class="tabbtn lift" data-tab="subs">Subscriptions</button>
  <button class="tabbtn lift" data-tab="profile">Profile</button>
  <button class="tabbtn lift" data-tab="admin" id="adminTabBtn" style="display:none">Admin</button>
</div>

<div id="market" class="card">
  <h3 style="margin:0 0 8px 0">Фильтры</h3>
  <div class="row">
    <div class="field">
      <label>Gift</label>
      <div class="inpWrap">
        <input id="gift" class="compact" placeholder="Gift (можно пусто)" autocomplete="off"/>
        <button class="xbtn" data-clear="gift" type="button">×</button>
      </div>
      <div id="giftSug" class="sug" style="display:none"></div>
    </div>
    <div class="field">
      <label>Model</label>
      <div class="inpWrap">
        <input id="model" class="compact" placeholder="Model" autocomplete="off"/>
        <button class="xbtn" data-clear="model" type="button">×</button>
      </div>
      <div id="modelSug" class="sug" style="display:none"></div>
    </div>
    <div class="field">
      <label>Backdrop</label>
      <div class="inpWrap">
        <input id="backdrop" class="compact" placeholder="Backdrop" autocomplete="off"/>
        <button class="xbtn" data-clear="backdrop" type="button">×</button>
      </div>
      <div id="backdropSug" class="sug" style="display:none"></div>
    </div>
    <div class="field" style="max-width:160px">
      <label>Number</label>
      <div class="inpWrap">
        <input id="number" class="compact" placeholder="№" inputmode="numeric"/>
        <button class="xbtn" data-clear="number" type="button">×</button>
      </div>
    </div>
  </div>

  <div class="row" style="margin-top:10px">
    <button id="apply" class="primary lift">Показать</button>
    <button id="refresh" class="lift">Обновить</button>
    <button id="historyBtn" class="lift">История продаж</button>
    <button id="clearAll" class="lift">Очистить</button>
  </div>

  <div id="status" class="muted" style="margin-top:10px"></div>
  <div id="offersLine" style="margin-top:8px"></div>

  <div class="hr"></div>
  <div><b>Лоты</b> <span class="muted">(клик → купить/открыть)</span></div>
  <div id="lots" class="grid"></div>
</div>

<div id="subs" class="card" style="display:none">
  <h3 style="margin:0 0 8px 0">Подписки</h3>
  <div class="row">
    <button id="subCreate" class="lift">Создать из текущих фильтров</button>
    <button id="subCheckNow" class="lift">Проверить сейчас</button>
    <button id="subRefresh" class="lift">Обновить</button>
  </div>
  <div id="subsList" style="margin-top:10px"></div>
</div>

<div id="profile" class="card" style="display:none">
  <h3 style="margin:0 0 8px 0">Профиль</h3>
  <div id="profileBox" class="muted">Загрузка...</div>
  <div class="hr"></div>
  <div><b>История покупок</b></div>
  <div id="purchases" style="margin-top:10px;display:flex;flex-direction:column;gap:10px"></div>
</div>

<div id="admin" class="card" style="display:none">
  <h3 style="margin:0 0 8px 0">Admin</h3>
  <div class="muted">MRKT токен хранится в Redis.</div>
  <div class="row" style="margin-top:10px">
    <div class="field"><label>Текущий токен (маска)</label><input id="tokMask" disabled/></div>
    <div class="field"><label>Новый MRKT_AUTH</label><input id="tokNew" placeholder="вставь токен"/></div>
    <button id="tokSave" class="lift">Сохранить токен</button>
  </div>
</div>

<!-- Sheet -->
<div id="sheetWrap" class="sheetWrap">
  <div class="sheet">
    <div class="sheetHeader">
      <div>
        <div id="sheetTitle" style="font-weight:950"></div>
        <div id="sheetSub" class="muted"></div>
      </div>
      <button id="sheetClose" class="small lift">✕</button>
    </div>
    <div id="sheetTop" class="muted"></div>
    <div id="sheetBody" class="sheetBody"></div>
  </div>
</div>

<!-- Lot modal -->
<div id="modalWrap" class="modalWrap">
  <div class="modal">
    <div class="modalHeader">
      <div>
        <div id="mTitle" class="modalTitle"></div>
        <div id="mSub" class="muted"></div>
      </div>
      <button id="mClose" class="small lift">✕</button>
    </div>
    <img id="mImg" class="modalImg" src="" alt="gift"/>
    <div class="row" style="margin-top:10px">
      <button id="mBuy" class="buy lift">Купить</button>
      <button id="mMrkt" class="lift">MRKT</button>
      <button id="mNft" class="lift">NFT</button>
    </div>
  </div>
</div>

<script src="https://telegram.org/js/telegram-web-app.js"></script>
<script src="/app.js"></script>
</body></html>`;

const WEBAPP_JS = `(() => {
  const tg = window.Telegram?.WebApp;
  try { tg?.ready(); } catch {}
  try { tg?.expand?.(); } catch {}

  const initData = tg?.initData || '';
  const el = (id) => document.getElementById(id);

  if (!initData) {
    document.body.innerHTML = '<div style="padding:16px;font-family:system-ui;color:#fff;background:#0b0f14">' +
      '<h3>Открой панель из Telegram</h3><p>Она работает только внутри Telegram WebApp.</p></div>';
    return;
  }

  function showErr(msg){ const box=el('err'); box.style.display='block'; box.textContent=String(msg||''); }
  function hideErr(){ const box=el('err'); box.style.display='none'; box.textContent=''; }

  async function api(path, opts = {}) {
    const res = await fetch(path, {
      ...opts,
      headers: { 'Content-Type':'application/json', 'X-Tg-Init-Data': initData, ...(opts.headers||{}) }
    });
    const txt = await res.text();
    let data=null;
    try{ data = txt?JSON.parse(txt):null; }catch{ data={raw:txt}; }
    if(!res.ok) throw new Error('HTTP '+res.status+': '+JSON.stringify(data).slice(0,700));
    return data;
  }

  function setTab(name){
    ['market','subs','profile','admin'].forEach(x => el(x).style.display = (x===name?'block':'none'));
    document.querySelectorAll('.tabbtn').forEach(b => b.classList.toggle('active', b.dataset.tab===name));
  }
  document.querySelectorAll('.tabbtn').forEach(b => b.onclick = () => setTab(b.dataset.tab));

  const params = new URLSearchParams(location.search || '');
  const initialTab = params.get('tab');
  const initialSubId = params.get('subId');
  if (initialTab) setTab(initialTab);

  function safe(fn){
    return async () => {
      hideErr();
      try { await fn(); } catch(e){ showErr(e.message || String(e)); }
    };
  }

  function hideSug(id){ const b=el(id); b.style.display='none'; b.innerHTML=''; }
  function renderSug(id, items, onPick){
    const b=el(id);
    if(!items||!items.length){ hideSug(id); return; }
    b.innerHTML = items.map(x => {
      const thumb = x.imgUrl
        ? '<img class="thumb" src="'+x.imgUrl+'" referrerpolicy="no-referrer"/>'
        : '<div class="thumb"></div>';

      const right = '<div style="display:flex;flex-direction:column;gap:2px;min-width:0;flex:1">' +
        '<div style="display:flex;align-items:center;justify-content:space-between;gap:8px">' +
          '<div style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'+ x.label + '</div>' +
          (x.badge ? '<span class="badge">'+x.badge+'</span>' : '') +
        '</div>' +
        (x.sub ? '<div class="muted" style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'+x.sub+'</div>' : '') +
      '</div>';

      return '<button type="button" class="item" data-v="'+x.value.replace(/"/g,'&quot;')+'">'+thumb+right+'</button>';
    }).join('');
    b.style.display='block';
    b.onclick = (e) => {
      const btn = e.target.closest('button[data-v]');
      if(!btn) return;
      onPick(btn.getAttribute('data-v'));
      hideSug(id);
    };
  }

  let selectedGiftValue = '';

  async function showGiftSug(){
    const q = el('gift').value.trim();
    const r = await api('/api/mrkt/collections?q='+encodeURIComponent(q));
    renderSug('giftSug', r.items||[], (v) => {
      selectedGiftValue = v;
      el('gift').value = (r.mapLabel||{})[v] || v;
      el('model').value=''; el('backdrop').value='';
    });
  }
  async function showModelSug(){
    const gift = selectedGiftValue || '';
    if(!gift){ hideSug('modelSug'); return; }
    const q = el('model').value.trim();
    const r = await api('/api/mrkt/suggest?kind=model&gift='+encodeURIComponent(gift)+'&q='+encodeURIComponent(q));
    renderSug('modelSug', r.items||[], (v)=>{ el('model').value=v; el('backdrop').value=''; });
  }
  async function showBackdropSug(){
    const gift = selectedGiftValue || '';
    if(!gift){ hideSug('backdropSug'); return; }
    const q = el('backdrop').value.trim();
    const r = await api('/api/mrkt/suggest?kind=backdrop&gift='+encodeURIComponent(gift)+'&q='+encodeURIComponent(q));
    renderSug('backdropSug', r.items||[], (v)=>{ el('backdrop').value=v; });
  }

  el('gift').addEventListener('focus', safe(showGiftSug));
  el('gift').addEventListener('input', safe(() => { selectedGiftValue=''; return showGiftSug(); }));
  el('model').addEventListener('focus', safe(showModelSug));
  el('model').addEventListener('input', safe(showModelSug));
  el('backdrop').addEventListener('focus', safe(showBackdropSug));
  el('backdrop').addEventListener('input', safe(showBackdropSug));

  document.addEventListener('click', (e) => {
    if(!e.target.closest('#gift,#model,#backdrop,.sug')){
      hideSug('giftSug'); hideSug('modelSug'); hideSug('backdropSug');
    }
  });

  async function patchFilters(partial){
    const giftVal = (partial.gift !== undefined) ? partial.gift : selectedGiftValue;
    const giftLabel = (partial.giftLabel !== undefined) ? partial.giftLabel : el('gift').value.trim();
    const model = (partial.model !== undefined) ? partial.model : el('model').value.trim();
    const backdrop = (partial.backdrop !== undefined) ? partial.backdrop : el('backdrop').value.trim();
    const numberPrefix = (partial.numberPrefix !== undefined) ? partial.numberPrefix : el('number').value.trim();

    await api('/api/state/patch', {
      method: 'POST',
      body: JSON.stringify({ filters: { gift: giftVal, giftLabel, model, backdrop, numberPrefix } })
    });
  }

  // clear per field
  document.body.addEventListener('click', (e) => {
    const b = e.target.closest('button[data-clear]');
    if(!b) return;
    const what = b.getAttribute('data-clear');

    (async () => {
      hideErr();
      if(what === 'gift'){
        selectedGiftValue = '';
        el('gift').value = '';
        el('model').value = '';
        el('backdrop').value = '';
        el('number').value = '';
        await patchFilters({ gift:'', giftLabel:'', model:'', backdrop:'', numberPrefix:'' });
      }
      if(what === 'model'){ el('model').value=''; await patchFilters({ model:'' }); }
      if(what === 'backdrop'){ el('backdrop').value=''; await patchFilters({ backdrop:'' }); }
      if(what === 'number'){ el('number').value=''; await patchFilters({ numberPrefix:'' }); }

      await refreshState();
      await refreshMarketData();
    })().catch(err => showErr(err.message || String(err)));
  });

  el('clearAll').onclick = safe(async () => {
    selectedGiftValue = '';
    el('gift').value=''; el('model').value=''; el('backdrop').value=''; el('number').value='';
    await patchFilters({ gift:'', giftLabel:'', model:'', backdrop:'', numberPrefix:'' });
    await refreshState();
    await refreshMarketData();
  });

  function renderOffersLine(offer){
    const box = el('offersLine');
    if(offer && offer.ok && offer.maxOfferTon != null){
      box.innerHTML = '<span class="offerPill">Max offer: '+offer.maxOfferTon.toFixed(3)+' TON</span>';
    } else if (offer && offer.ok) {
      box.innerHTML = '<span class="offerPill">Max offer: нет данных</span>';
    } else box.innerHTML = '';
  }

  // modal
  const modalWrap = el('modalWrap');
  const mTitle = el('mTitle');
  const mSub = el('mSub');
  const mImg = el('mImg');
  const mBuy = el('mBuy');
  const mMrkt = el('mMrkt');
  const mNft = el('mNft');
  const mClose = el('mClose');
  let modalLot = null;

  function openModal(lot){
    modalLot = lot;
    mTitle.textContent = lot.name || 'Gift';
    mSub.textContent = 'Цена: ' + lot.priceTon.toFixed(3) + ' TON' + (lot.number!=null ? (' | #' + lot.number) : '');
    mImg.src = lot.imgUrl || '';
    modalWrap.style.display = 'flex';
  }
  function closeModal(){ modalWrap.style.display='none'; modalLot=null; }
  mClose.onclick = closeModal;
  modalWrap.addEventListener('click', (e) => { if(e.target===modalWrap) closeModal(); });

  function confirmBuy(text){
    return new Promise((resolve) => {
      if (tg?.showConfirm) {
        try { tg.showConfirm(text, (ok) => resolve(!!ok)); }
        catch { resolve(window.confirm(text)); }
      } else resolve(window.confirm(text));
    });
  }

  async function doBuy(lot){
    const ok = await confirmBuy('Купить?\\n' + (lot.name||'Gift') + '\\nЦена: ' + lot.priceTon.toFixed(3) + ' TON');
    if(!ok) return;
    const r = await api('/api/mrkt/buy', { method:'POST', body: JSON.stringify({ id: lot.id, priceNano: lot.priceNano }) });
    alert('Куплено: ' + r.title + ' за ' + r.priceTon.toFixed(3) + ' TON');
    closeModal();
  }
  mBuy.onclick = safe(async () => { if(modalLot) await doBuy(modalLot); });
  mMrkt.onclick = () => { if(modalLot?.urlMarket) tg?.openTelegramLink ? tg.openTelegramLink(modalLot.urlMarket) : window.open(modalLot.urlMarket,'_blank'); };
  mNft.onclick = () => { if(modalLot?.urlTelegram) tg?.openTelegramLink ? tg.openTelegramLink(modalLot.urlTelegram) : window.open(modalLot.urlTelegram,'_blank'); };

  // sheet
  const sheetWrap = el('sheetWrap');
  const sheetTitle = el('sheetTitle');
  const sheetSub = el('sheetSub');
  const sheetTop = el('sheetTop');
  const sheetBody = el('sheetBody');
  const sheetClose = el('sheetClose');

  function openSheet(title, subtitle){
    sheetTitle.textContent = title || '';
    sheetSub.textContent = subtitle || '';
    sheetTop.textContent = '';
    sheetBody.innerHTML = '';
    sheetWrap.style.display = 'flex';
  }
  function closeSheet(){ sheetWrap.style.display='none'; }
  sheetClose.onclick = closeSheet;
  sheetWrap.addEventListener('click', (e) => { if(e.target===sheetWrap) closeSheet(); });

  function renderSalesToSheet(resp){
    if(resp.ok===false){
      sheetTop.textContent = resp.reason || 'error';
      sheetBody.innerHTML = '';
      return;
    }
    sheetTop.innerHTML = '<div><b>Оценка цены продажи:</b> '+(resp.median!=null?resp.median.toFixed(3)+' TON':'нет данных')+'</div>';

    const list = resp.lastSales || [];
    if(!list.length){ sheetBody.innerHTML = '<i class="muted">Нет данных</i>'; return; }

    sheetBody.innerHTML = list.map(x => {
      const img = x.imgUrl ? '<img class="saleImg" src="'+x.imgUrl+'" referrerpolicy="no-referrer" loading="lazy"/>' : '<div class="saleImg"></div>';
      const btns =
        '<div class="saleBtns">' +
          (x.urlMarket ? '<button class="small lift" data-open="'+x.urlMarket+'">MRKT</button>' : '') +
          (x.urlTelegram ? '<button class="small lift" data-open="'+x.urlTelegram+'">NFT</button>' : '') +
        '</div>';
      const n = (x.number!=null) ? ('<span class="badge">#'+x.number+'</span>') : '';
      return '<div class="saleRow">' +
        img +
        '<div class="saleCard">' +
          '<div style="display:flex;justify-content:space-between;gap:10px;align-items:center"><b>'+x.priceTon.toFixed(3)+' TON</b><span class="muted">'+(x.tsMsk||'')+'</span></div>' +
          '<div style="margin-top:4px;display:flex;justify-content:space-between;gap:10px;align-items:center">' +
            '<div class="muted">'+(x.model?('Model: '+x.model):'')+'</div>' + n +
          '</div>' +
          (x.backdrop?'<div class="muted">Backdrop: '+x.backdrop+'</div>':'') +
          btns +
        '</div>' +
      '</div>';
    }).join('');
  }

  // open links
  document.body.addEventListener('click', (e) => {
    const openBtn = e.target.closest('button[data-open]');
    if(!openBtn) return;
    const url = openBtn.getAttribute('data-open');
    if(url) tg?.openTelegramLink ? tg.openTelegramLink(url) : window.open(url,'_blank');
  });

  function renderLots(resp){
    const box = el('lots');
    if(resp.ok===false){ box.innerHTML = '<div style="color:#ef4444"><b>'+(resp.reason||'error')+'</b></div>'; return; }
    const lots = resp.lots||[];
    if(!lots.length){ box.innerHTML = '<i class="muted">Лотов не найдено</i>'; return; }

    const map = new Map(lots.map(l => [String(l.id), l]));
    box.innerHTML = lots.map(x => {
      const img = x.imgUrl ? '<img src="'+x.imgUrl+'" referrerpolicy="no-referrer" loading="lazy"/>' :
        '<div style="aspect-ratio:1/1;border:1px solid rgba(255,255,255,.10);border-radius:14px"></div>';
      const num = (x.number!=null) ? ('<span class="badge">#'+x.number+'</span>') : '';
      return '<div class="lot" data-lot="1" data-id="'+x.id+'">'+img+
        '<div class="price">'+x.priceTon.toFixed(3)+' TON</div>'+
        '<div style="display:flex;justify-content:space-between;gap:10px;align-items:center"><b style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'+x.name+'</b>'+num+'</div>'+
        (x.model?'<div class="muted">Model: '+x.model+'</div>':'')+
        (x.backdrop?'<div class="muted" style="margin-top:6px">Backdrop: '+x.backdrop+'</div>':'')+
        '</div>';
    }).join('');

    box.querySelectorAll('[data-lot="1"]').forEach(node => {
      node.onclick = () => {
        const id = node.getAttribute('data-id');
        const lot = map.get(String(id));
        if(lot) openModal(lot);
      };
    });
  }

  async function refreshProfile(){
    const r = await api('/api/profile');
    const u = r.user || {};
    const photo = u.photo_url ? '<img src="'+u.photo_url+'" style="width:54px;height:54px;border-radius:16px;border:1px solid rgba(255,255,255,.12);object-fit:cover"/>' : '';
    el('profileBox').innerHTML =
      '<div style="display:flex;gap:10px;align-items:center">' +
        photo +
        '<div>' +
          '<div><b>'+ (u.first_name || '') + ' ' + (u.last_name || '') + '</b></div>' +
          '<div class="muted">@'+ (u.username || '-') + ' | id: '+ (u.id || '-') + '</div>' +
        '</div>' +
      '</div>';

    const list = r.purchases || [];
    const box = el('purchases');
    if(!list.length){
      box.innerHTML = '<i class="muted">Покупок пока нет</i>';
    } else {
      box.innerHTML = list.map(p => {
        return '<div class="saleCard">' +
          '<div style="display:flex;justify-content:space-between;gap:10px;align-items:center">' +
            '<b>'+p.title+'</b>' +
            '<span class="muted">'+p.tsMsk+'</span>' +
          '</div>' +
          '<div class="muted">Price: '+Number(p.priceTon).toFixed(3)+' TON</div>' +
          '<div class="saleBtns">' +
            (p.urlMarket ? '<button class="small lift" data-open="'+p.urlMarket+'">MRKT</button>' : '') +
            (p.urlTelegram ? '<button class="small lift" data-open="'+p.urlTelegram+'">NFT</button>' : '') +
          '</div>' +
        '</div>';
      }).join('');
    }
  }

  async function refreshState(){
    const st = await api('/api/state');
    el('status').textContent =
      'MRKT_AUTH: ' + (st.api.mrktAuthSet?'✅':'❌') +
      (st.api.mrktLastFail ? ' | last fail: '+st.api.mrktLastFail : '');

    el('gift').value = st.user.filters.giftLabel || st.user.filters.gift || '';
    selectedGiftValue = st.user.filters.gift || '';
    el('model').value = st.user.filters.model || '';
    el('backdrop').value = st.user.filters.backdrop || '';
    el('number').value = st.user.filters.numberPrefix || '';

    // subs list minimal (controls + autobuy settings)
    const subsList = st.user.subscriptions || [];
    const box = el('subsList');
    if(!subsList.length){
      box.innerHTML = '<i class="muted">Подписок нет</i>';
    } else {
      box.innerHTML = subsList.map(s => {
        const giftLine = s.filters.gift ? ('Gift: '+s.filters.gift) : 'Gift: any';
        const numLine = s.filters.numberPrefix ? ('Number: #'+s.filters.numberPrefix+'*') : '';
        return '<div class="saleCard">' +
          '<div style="display:flex;justify-content:space-between;gap:10px;align-items:center">' +
            '<b>#'+s.num+' '+(s.enabled?'ON':'OFF')+'</b>' +
            '<button class="small lift" data-act="subDetails" data-id="'+s.id+'">Детали</button>' +
          '</div>' +
          '<div class="muted">'+giftLine+'</div>' +
          (s.filters.model?'<div class="muted">Model: '+s.filters.model+'</div>':'<div class="muted">Model: any</div>') +
          (s.filters.backdrop?'<div class="muted">Backdrop: '+s.filters.backdrop+'</div>':'<div class="muted">Backdrop: any</div>') +
          (numLine?'<div class="muted">'+numLine+'</div>':'') +
          '<div class="saleBtns">' +
            '<button class="small lift" data-act="subToggle" data-id="'+s.id+'">'+(s.enabled?'Отключить':'Включить')+'</button>' +
            '<button class="small lift" data-act="subDel" data-id="'+s.id+'">Удалить</button>' +
            '<button class="small lift" data-act="subNotifyMax" data-id="'+s.id+'">Max уведомл.</button>' +
            '<button class="small lift" data-act="subAutoToggle" data-id="'+s.id+'">'+(s.autoBuyEnabled?'AutoBuy: ON':'AutoBuy: OFF')+'</button>' +
            '<button class="small lift" data-act="subAutoMax" data-id="'+s.id+'">Max AutoBuy</button>' +
          '</div>' +
          '<div class="muted">Notify max: '+(s.maxNotifyTon==null?'∞':s.maxNotifyTon)+' TON</div>' +
          '<div class="muted">AutoBuy max: '+(s.maxAutoBuyTon==null?'-':s.maxAutoBuyTon)+' TON</div>' +
        '</div>';
      }).join('');
    }

    if(st.api.isAdmin){
      el('adminTabBtn').style.display='inline-block';
      el('tokMask').value = st.api.mrktAuthMask || '';
    }
  }

  async function refreshMarketData(){
    const [lots, offer] = await Promise.all([
      api('/api/mrkt/lots'),
      api('/api/mrkt/offers_current'),
    ]);
    renderLots(lots);
    renderOffersLine(offer);
  }

  el('apply').onclick = safe(async () => {
    await patchFilters({
      gift: selectedGiftValue,
      giftLabel: el('gift').value.trim(),
      model: el('model').value.trim(),
      backdrop: el('backdrop').value.trim(),
      numberPrefix: el('number').value.trim(),
    });
    await refreshState();
    await refreshMarketData();
  });

  el('refresh').onclick = safe(async () => {
    await refreshMarketData();
  });

  el('historyBtn').onclick = safe(async () => {
    openSheet('История продаж', 'По текущим фильтрам');
    const r = await api('/api/mrkt/history_current');
    renderSalesToSheet(r);
  });

  // subs buttons
  el('subCreate').onclick = safe(async () => { await api('/api/sub/create',{method:'POST'}); await refreshState(); });
  el('subRefresh').onclick = safe(async () => { await refreshState(); });
  el('subCheckNow').onclick = safe(async () => {
    const r = await api('/api/sub/checknow',{method:'POST'});
    alert('Готово: subs='+r.processedSubs+', уведомлений='+r.floorNotifs);
  });

  document.body.addEventListener('click', async (e) => {
    const btn = e.target.closest('button[data-act]');
    if(!btn) return;

    const act = btn.dataset.act;
    const id = btn.dataset.id;

    hideErr();
    try{
      if(act==='subToggle') await api('/api/sub/toggle',{method:'POST',body:JSON.stringify({id})});
      if(act==='subDel') await api('/api/sub/delete',{method:'POST',body:JSON.stringify({id})});
      if(act==='subNotifyMax'){
        const v = prompt('Max TON для уведомлений (пусто = без лимита):', '');
        const val = (v==null || v.trim()==='') ? null : Number(v);
        await api('/api/sub/set_notify_max',{method:'POST',body:JSON.stringify({id, maxNotifyTon: val})});
      }
      if(act==='subAutoToggle'){
        await api('/api/sub/toggle_autobuy',{method:'POST',body:JSON.stringify({id})});
      }
      if(act==='subAutoMax'){
        const v = prompt('Max TON для AutoBuy (обязательно число):', '5');
        if(v==null) return;
        await api('/api/sub/set_autobuy_max',{method:'POST',body:JSON.stringify({id, maxAutoBuyTon: Number(v)})});
      }
      if(act==='subDetails'){
        openSheet('Детали подписки', 'История + Max offer');
        const r = await api('/api/sub/details',{method:'POST',body:JSON.stringify({id})});
        const offerTxt = (r.offers?.maxOfferTon!=null) ? (r.offers.maxOfferTon.toFixed(3)+' TON') : 'нет данных';
        el('sheetTop').innerHTML =
          '<div class="offerPill">Max offer: '+offerTxt+'</div>' +
          '<div style="margin-top:10px"><b>Оценка цены продажи:</b> '+(r.history?.median!=null?r.history.median.toFixed(3)+' TON':'нет данных')+'</div>';
        renderSalesToSheet(r.history || {ok:true, median:null, count:0, lastSales:[]});
      }

      await refreshState();
    }catch(err){ showErr(err.message||String(err)); }
  });

  // profile refresh on open
  document.querySelectorAll('.tabbtn').forEach(b => {
    b.addEventListener('click', async () => {
      if (b.dataset.tab === 'profile') {
        try { await refreshProfile(); } catch(e){ showErr(e.message||String(e)); }
      }
    });
  });

  // admin save token
  el('tokSave')?.addEventListener('click', safe(async () => {
    const t = el('tokNew').value.trim();
    if(!t) return showErr('Вставь токен.');
    await api('/api/admin/mrkt_auth',{method:'POST',body:JSON.stringify({token:t})});
    el('tokNew').value='';
    await refreshState();
  }));

  // initial
  refreshState().then(refreshMarketData).catch(e=>showErr(e.message||String(e)));
  if (initialTab === 'profile') refreshProfile().catch(()=>{});
})();`;

// ===================== Web server + API =====================
function startWebServer() {
  const app = express();
  app.disable('x-powered-by');
  app.use(express.json({ limit: '1mb' }));

  app.get('/health', (req, res) => res.status(200).send('ok'));
  app.get('/', (req, res) => { res.setHeader('Content-Type', 'text/html; charset=utf-8'); res.send(WEBAPP_HTML); });
  app.get('/app.js', (req, res) => { res.setHeader('Content-Type', 'application/javascript; charset=utf-8'); res.send(WEBAPP_JS); });

  function auth(req, res, next) {
    const initData = String(req.headers['x-tg-init-data'] || '');
    const v = verifyTelegramWebAppInitData(initData);
    if (!v.ok) return res.status(401).json({ ok: false, reason: v.reason });
    req.userId = v.userId;
    req.tgUser = v.user || null;
    next();
  }

  const isAdmin = (userId) => ADMIN_USER_ID && Number(userId) === Number(ADMIN_USER_ID);

  // image proxies
  app.get('/img/gift', async (req, res) => {
    const name = String(req.query.name || '').trim();
    if (!name) return res.status(400).send('no name');

    const url = fragmentGiftRemoteUrl(name);
    const r = await fetchWithTimeout(url, { method: 'GET' }, 8000).catch(() => null);
    if (!r || !r.ok) return res.status(404).send('not found');

    res.setHeader('Content-Type', r.headers.get('content-type') || 'image/jpeg');
    res.setHeader('Cache-Control', 'public, max-age=86400');
    Readable.fromWeb(r.body).pipe(res);
  });

  app.get('/img/cdn', async (req, res) => {
    const key = String(req.query.key || '').trim();
    if (!key) return res.status(400).send('no key');

    const url = joinUrl(MRKT_CDN_BASE, key);
    const r = await fetchWithTimeout(url, { method: 'GET', headers: { Accept: 'image/*' } }, 8000).catch(() => null);
    if (!r || !r.ok) return res.status(404).send('not found');

    res.setHeader('Content-Type', r.headers.get('content-type') || 'image/webp');
    res.setHeader('Cache-Control', 'public, max-age=86400');
    Readable.fromWeb(r.body).pipe(res);
  });

  async function buildSubThumb(sub) {
    const gift = sub?.filters?.gift;
    let backdropHex = null;

    if (gift && sub?.filters?.backdrop) {
      const backs = await mrktGetBackdropsForGift(gift);
      const found = backs.find((b) => norm(b.name) === norm(sub.filters.backdrop));
      backdropHex = found?.centerHex || null;
    }

    if (gift && sub?.filters?.model) {
      const models = await mrktGetModelsForGift(gift);
      const found = models.find((m) => norm(m.name) === norm(sub.filters.model));
      if (found?.thumbKey) return { thumbUrl: `/img/cdn?key=${encodeURIComponent(found.thumbKey)}`, backdropHex };
    }

    if (gift) {
      const cols = await mrktGetCollections();
      const c = cols.find((x) => x.name === gift);
      if (c?.thumbKey) return { thumbUrl: `/img/cdn?key=${encodeURIComponent(c.thumbKey)}`, backdropHex };
    }

    return { thumbUrl: null, backdropHex };
  }

  // state
  app.get('/api/state', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    const mask = MRKT_AUTH_RUNTIME ? MRKT_AUTH_RUNTIME.slice(0, 4) + '…' + MRKT_AUTH_RUNTIME.slice(-4) : '';
    const lastFail =
      mrktAuthState.lastFailAt
        ? `HTTP ${mrktAuthState.lastFailCode}${mrktAuthState.lastFailBody ? ' ' + mrktAuthState.lastFailBody : ''}`
        : null;

    const subs = Array.isArray(u.subscriptions) ? u.subscriptions : [];
    const enriched = [];
    for (const s of subs) {
      const extra = await buildSubThumb(s).catch(() => ({ thumbUrl: null, backdropHex: null }));
      enriched.push({ ...s, ...extra });
    }

    res.json({
      ok: true,
      api: {
        mrktAuthSet: !!MRKT_AUTH_RUNTIME,
        isAdmin: isAdmin(req.userId),
        mrktAuthMask: mask,
        mrktLastFail: lastFail,
      },
      user: {
        enabled: !!u.enabled,
        filters: u.filters,
        subscriptions: enriched,
      },
    });
  });

  // profile
  app.get('/api/profile', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    const purchases = (u.purchases || []).map((p) => ({ ...p, tsMsk: formatMsk(p.ts) }));
    res.json({ ok: true, user: req.tgUser, purchases });
  });

  // collections
  app.get('/api/mrkt/collections', auth, async (req, res) => {
    const q = String(req.query.q || '').trim().toLowerCase();
    const all = await mrktGetCollections();
    const filtered = all
      .filter((x) => !q || x.title.toLowerCase().includes(q) || x.name.toLowerCase().includes(q))
      .slice(0, WEBAPP_SUGGEST_LIMIT);

    const mapLabel = {};
    const items = filtered.map((x) => {
      mapLabel[x.name] = x.title || x.name;
      const imgUrl = x.thumbKey ? `/img/cdn?key=${encodeURIComponent(x.thumbKey)}` : null;
      const floorTon = x.floorNano != null ? tonFromNano(x.floorNano) : null;
      return { label: x.title || x.name, value: x.name, imgUrl, sub: floorTon != null ? `floor: ${floorTon.toFixed(3)} TON` : null, badge: null };
    });

    res.json({ ok: true, items, mapLabel });
  });

  // suggest
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
          imgUrl: m.thumbKey ? `/img/cdn?key=${encodeURIComponent(m.thumbKey)}` : null,
          sub: m.floorNano != null && tonFromNano(m.floorNano) != null ? `min: ${tonFromNano(m.floorNano).toFixed(3)} TON` : null,
          badge: m.rarityPerMille != null ? formatPctFromPermille(m.rarityPerMille) : null,
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
          colorHex: b.centerHex || null,
          sub: null,
          badge: b.rarityPerMille != null ? formatPctFromPermille(b.rarityPerMille) : null,
        }));
      return res.json({ ok: true, items });
    }

    res.status(400).json({ ok: false, reason: 'BAD_KIND' });
  });

  // patch filters
  app.post('/api/state/patch', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    const b = req.body || {};
    if (b.filters && typeof b.filters === 'object') {
      const giftSelected = String(b.filters.gift || '').trim();
      const giftLabel = String(b.filters.giftLabel || '').trim();

      let resolved = giftSelected;
      if (!resolved && giftLabel) {
        const cols = await mrktGetCollections();
        const found = cols.find((c) => norm(c.title) === norm(giftLabel) || norm(c.name) === norm(giftLabel));
        resolved = found ? found.name : giftLabel;
      }

      u.filters.gift = resolved || '';
      u.filters.giftLabel = giftLabel || resolved || '';
      u.filters.model = String(b.filters.model || '').trim();
      u.filters.backdrop = String(b.filters.backdrop || '').trim();
      u.filters.numberPrefix = cleanDigitsPrefix(b.filters.numberPrefix || '');
    }

    scheduleSave();
    res.json({ ok: true });
  });

  // lots
  app.get('/api/mrkt/lots', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);

    const r = await mrktSearchLotsByFilters({
      gift: u.filters.gift || '',
      model: u.filters.model || '',
      backdrop: u.filters.backdrop || '',
      numberPrefix: u.filters.numberPrefix || '',
    }, WEBAPP_LOTS_PAGES);

    if (!r.ok) return res.json({ ok: false, reason: r.reason, lots: [] });

    // backdrop colors only when gift selected
    let backMap = new Map();
    if (u.filters.gift) {
      const backs = await mrktGetBackdropsForGift(u.filters.gift);
      backMap = new Map(backs.map((b) => [norm(b.name), b]));
    }

    const lots = (r.gifts || [])
      .slice(0, WEBAPP_LOTS_LIMIT)
      .map((x) => {
        const b = x.backdrop ? backMap.get(norm(x.backdrop)) : null;
        return {
          id: x.id,
          name: x.name,
          number: x.number,
          priceTon: x.priceTon,
          priceNano: x.priceNano,
          urlTelegram: x.urlTelegram,
          urlMarket: x.urlMarket,
          model: x.model,
          backdrop: x.backdrop,
          imgUrl: x.giftName ? `/img/gift?name=${encodeURIComponent(x.giftName)}` : null,
          backdropColorHex: b?.centerHex || null,
        };
      });

    res.json({ ok: true, lots });
  });

  // offers
  app.get('/api/mrkt/offers_current', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    if (!u.filters.gift) return res.json({ ok: true, maxOfferTon: null, count: 0 });

    const r = await mrktOrdersFetch({ gift: u.filters.gift, model: u.filters.model || '', backdrop: u.filters.backdrop || '' });
    if (!r.ok) return res.json({ ok: false, reason: r.reason });

    let maxNano = null;
    for (const o of (r.orders || [])) {
      const n = o?.priceMaxNanoTONs == null ? null : Number(o.priceMaxNanoTONs);
      if (!Number.isFinite(n) || n <= 0) continue;
      if (maxNano == null || n > maxNano) maxNano = n;
    }

    res.json({ ok: true, maxOfferTon: maxNano != null ? maxNano / 1e9 : null, count: (r.orders || []).length });
  });

  // history
  app.get('/api/mrkt/history_current', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    const h = await mrktHistorySales({
      gift: u.filters.gift || '',
      model: u.filters.model || '',
      backdrop: u.filters.backdrop || '',
      numberPrefix: u.filters.numberPrefix || '',
    });
    res.json({ ok: true, median: h.median, count: h.count, lastSales: h.lastSales || [] });
  });

  // buy
  app.post('/api/mrkt/buy', auth, async (req, res) => {
    const id = String(req.body?.id || '').trim();
    const priceNano = Number(req.body?.priceNano);
    if (!id) return res.status(400).json({ ok: false, reason: 'NO_ID' });
    if (!Number.isFinite(priceNano) || priceNano <= 0) return res.status(400).json({ ok: false, reason: 'BAD_PRICE' });

    const r = await mrktBuy({ id, priceNano });
    if (!r.ok) return res.status(502).json({ ok: false, reason: r.reason });

    const g = r.okItem?.userGift || null;
    const title = `${g?.collectionTitle || g?.collectionName || g?.title || 'Gift'}${g?.number != null ? ` #${g.number}` : ''}`;
    const priceTon = priceNano / 1e9;
    const giftName = g?.name || null;
    const urlTelegram = giftName && String(giftName).includes('-') ? `https://t.me/nft/${giftName}` : '';
    const urlMarket = mrktLotUrlFromId(id);

    const u = getOrCreateUser(req.userId);
    pushPurchase(u, { ts: Date.now(), title, priceTon, urlTelegram, urlMarket });
    scheduleSave();

    res.json({ ok: true, title, priceTon });
  });

  // subs endpoints
  app.post('/api/sub/create', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    const r = makeSubFromCurrentFilters(u);
    if (!r.ok) return res.status(400).json({ ok: false, reason: r.reason });

    u.subscriptions.push(r.sub);
    renumberSubs(u);
    await persistNow().catch(() => scheduleSave());
    res.json({ ok: true });
  });

  app.post('/api/sub/toggle', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    const s = findSub(u, req.body?.id);
    if (!s) return res.status(404).json({ ok: false, reason: 'NOT_FOUND' });
    s.enabled = !s.enabled;
    await persistNow().catch(() => scheduleSave());
    res.json({ ok: true });
  });

  app.post('/api/sub/delete', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    const id = req.body?.id;
    u.subscriptions = (u.subscriptions || []).filter((x) => x && x.id !== id);
    renumberSubs(u);
    await persistNow().catch(() => scheduleSave());
    res.json({ ok: true });
  });

  app.post('/api/sub/set_notify_max', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    const s = findSub(u, req.body?.id);
    if (!s) return res.status(404).json({ ok: false, reason: 'NOT_FOUND' });

    const v = req.body?.maxNotifyTon;
    if (v == null) s.maxNotifyTon = null;
    else {
      const num = Number(v);
      if (!Number.isFinite(num) || num <= 0) return res.status(400).json({ ok: false, reason: 'BAD_MAX' });
      s.maxNotifyTon = num;
    }

    await persistNow().catch(() => scheduleSave());
    res.json({ ok: true });
  });

  app.post('/api/sub/toggle_autobuy', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    const s = findSub(u, req.body?.id);
    if (!s) return res.status(404).json({ ok: false, reason: 'NOT_FOUND' });
    s.autoBuyEnabled = !s.autoBuyEnabled;
    await persistNow().catch(() => scheduleSave());
    res.json({ ok: true });
  });

  app.post('/api/sub/set_autobuy_max', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    const s = findSub(u, req.body?.id);
    if (!s) return res.status(404).json({ ok: false, reason: 'NOT_FOUND' });
    const num = Number(req.body?.maxAutoBuyTon);
    if (!Number.isFinite(num) || num <= 0) return res.status(400).json({ ok: false, reason: 'BAD_MAX' });
    s.maxAutoBuyTon = num;
    await persistNow().catch(() => scheduleSave());
    res.json({ ok: true });
  });

  app.post('/api/sub/checknow', auth, async (req, res) => {
    const st = await checkSubscriptionsForAllUsers({ manual: true });
    res.json(st);
  });

  app.post('/api/sub/details', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    const s = findSub(u, String(req.body?.id || ''));
    if (!s) return res.status(404).json({ ok: false, reason: 'NOT_FOUND' });

    const history = await mrktHistorySales({
      gift: s.filters.gift || '',
      model: s.filters.model || '',
      backdrop: s.filters.backdrop || '',
      numberPrefix: s.filters.numberPrefix || '',
    });

    const offersRaw = s.filters.gift
      ? await mrktOrdersFetch({ gift: s.filters.gift, model: s.filters.model || '', backdrop: s.filters.backdrop || '' })
      : { ok: true, orders: [] };

    let maxOfferTon = null;
    if (offersRaw.ok) {
      let maxNano = null;
      for (const o of (offersRaw.orders || [])) {
        const n = o?.priceMaxNanoTONs == null ? null : Number(o.priceMaxNanoTONs);
        if (!Number.isFinite(n) || n <= 0) continue;
        if (maxNano == null || n > maxNano) maxNano = n;
      }
      maxOfferTon = maxNano != null ? maxNano / 1e9 : null;
    }

    res.json({
      ok: true,
      history: { ok: true, median: history.median, count: history.count, lastSales: history.lastSales || [] },
      offers: { ok: offersRaw.ok, maxOfferTon },
    });
  });

  // admin token
  app.post('/api/admin/mrkt_auth', auth, async (req, res) => {
    if (!isAdmin(req.userId)) return res.status(403).json({ ok: false, reason: 'NOT_ADMIN' });
    const t = String(req.body?.token || '').trim();
    if (!t) return res.status(400).json({ ok: false, reason: 'EMPTY_TOKEN' });

    MRKT_AUTH_RUNTIME = t;
    try { await saveMrktAuthToRedis(t); } catch {}

    collectionsCache = { time: 0, items: [] };
    modelsCache.clear();
    backdropsCache.clear();
    historyCache.clear();
    offersCache.clear();

    res.json({ ok: true });
  });

  const port = Number(process.env.PORT || 3000);
  app.listen(port, '0.0.0.0', () => console.log('WebApp listening on', port));
}

startWebServer();

// ===================== intervals =====================
setInterval(() => {
  checkSubscriptionsForAllUsers().catch((e) => console.error('subs error:', e));
}, SUBS_CHECK_INTERVAL_MS);

// ===================== bootstrap =====================
(async () => {
  if (REDIS_URL) {
    await initRedis();
    if (redis) {
      await loadMrktAuthFromRedis();
      await loadState();
      console.log('Redis persistence enabled. key=', REDIS_KEY_STATE);
    }
  } else {
    console.warn('REDIS_URL not set => subscriptions/profile will NOT persist after restart');
  }
  console.log('Bot started. /start');
})();
