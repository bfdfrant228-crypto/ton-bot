/**
 * ton-bot MRKT + WebApp Panel (single-file)
 * v9.5: phone grid fix, square lots, sheet overlays for history/sub-details, offers auto,
 *       backdrop tint behind thumbs, number filter, compact expandable filters
 * version: 2026-03-01-webapp-v9.5
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
const AUTO_BUY_CHECK_INTERVAL_MS = Number(process.env.AUTO_BUY_CHECK_INTERVAL_MS || 2000);

// requests
const MRKT_TIMEOUT_MS = Number(process.env.MRKT_TIMEOUT_MS || 9000);
const MRKT_COUNT = Number(process.env.MRKT_COUNT || 50);
const MRKT_FEED_COUNT = Number(process.env.MRKT_FEED_COUNT || 50);
const MRKT_FEED_THROTTLE_MS = Number(process.env.MRKT_FEED_THROTTLE_MS || 120);

// orders/offers
const MRKT_ORDERS_COUNT = Number(process.env.MRKT_ORDERS_COUNT || 30);

// UI limits
const WEBAPP_LOTS_LIMIT = Number(process.env.WEBAPP_LOTS_LIMIT || 36);
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

// anti-spam buy attempts
const AUTO_BUY_ATTEMPT_TTL_MS = Number(process.env.AUTO_BUY_ATTEMPT_TTL_MS || 30_000);
const AUTO_BUY_NO_FUNDS_PAUSE_MS = Number(process.env.AUTO_BUY_NO_FUNDS_PAUSE_MS || 10 * 60 * 1000);
const MRKT_AUTH_NOTIFY_COOLDOWN_MS = Number(process.env.MRKT_AUTH_NOTIFY_COOLDOWN_MS || 60 * 60 * 1000);

// Redis keys
const REDIS_KEY_MRKT_AUTH = 'mrkt:auth:token';
const REDIS_KEY_STATE = 'bot:state:v9_5';

console.log('v9.5 start', {
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
const subStates = new Map(); // `${userId}:${subId}` -> { floor, emptyStreak, feedLastId, pausedUntil }
let isSubsChecking = false;

const mrktAuthState = {
  lastOkAt: 0,
  lastFailAt: 0,
  lastFailCode: null,
  lastFailBody: null,
  lastNotifiedAt: 0,
};

const autoBuyLocks = new Set(); // userId
const autoBuyRecentAttempts = new Map(); // `${userId}:${giftId}` -> ts

// caches
const CACHE_TTL_MS = 5 * 60_000;
let collectionsCache = { time: 0, items: [] }; // [{name,title,thumbKey,floorNano}]
const modelsCache = new Map(); // gift -> { time, items:[{name, rarityPerMille, thumbKey, floorNano}] }
const backdropsCache = new Map(); // gift -> { time, items:[{name, rarityPerMille, centerHex}] }

// history cache
const historyCache = new Map(); // key -> { time, data }
const HISTORY_CACHE_TTL_MS = 30_000;

// offers cache
const offersCache = new Map(); // key -> { time, data }
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
function toIntOrNull(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return null;
  const i = Math.trunc(n);
  if (i <= 0) return null;
  return i;
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
  const yyyy = get('year');
  const mm = get('month');
  const dd = get('day');
  const hh = get('hour');
  const mi = get('minute');
  const ss = get('second');
  return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss} MSK`;
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

// ===================== Redis (state persistence) =====================
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
    if (t && String(t).trim()) {
      MRKT_AUTH_RUNTIME = String(t).trim();
      console.log('MRKT_AUTH loaded from Redis');
    }
  } catch (e) {
    console.error('loadMrktAuthFromRedis error:', e?.message || e);
  }
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
      filters: u?.filters || { gift: '', giftLabel: '', model: '', backdrop: '', number: null },
      subscriptions: Array.isArray(u?.subscriptions) ? u.subscriptions : [],
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
        number: toIntOrNull(u?.filters?.number),
      },
      subscriptions: Array.isArray(u?.subscriptions) ? u.subscriptions : [],
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
          number: toIntOrNull(s?.filters?.number),
        },
      }));

    renumberSubs(safe);
    users.set(userId, safe);
  }
}

async function loadState() {
  if (!redis) return;
  try {
    const raw = await redis.get(REDIS_KEY_STATE);
    if (!raw) return;
    importState(JSON.parse(raw));
    console.log('Loaded users state from Redis. users=', users.size);
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
    saveState().catch((e) => console.error('saveState error:', e?.message || e));
  }, 400);
}
async function saveState() {
  if (!redis) return;
  await redis.set(REDIS_KEY_STATE, JSON.stringify(exportState()));
}

// ===================== User state =====================
function getOrCreateUser(userId) {
  if (!users.has(userId)) {
    users.set(userId, {
      enabled: true,
      filters: { gift: '', giftLabel: '', model: '', backdrop: '', number: null },
      subscriptions: [],
    });
  }
  return users.get(userId);
}
function findSub(user, id) {
  return (user.subscriptions || []).find((s) => s && s.id === id) || null;
}
function makeSubFromCurrentFilters(user) {
  if (!user.filters.gift) return { ok: false, reason: 'NO_GIFT' };
  if (!Array.isArray(user.subscriptions)) user.subscriptions = [];
  const sub = {
    id: `sub_${Date.now()}_${Math.random().toString(16).slice(2, 6)}`,
    num: user.subscriptions.length + 1,
    enabled: true,
    maxNotifyTon: null,
    autoBuyEnabled: false,
    maxAutoBuyTon: null,
    filters: {
      gift: user.filters.gift,
      model: user.filters.model || '',
      backdrop: user.filters.backdrop || '',
      number: toIntOrNull(user.filters.number),
    },
  };
  return { ok: true, sub };
}

// ===================== MRKT headers + auth notify =====================
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

// ===================== MRKT API =====================
async function mrktGetCollections() {
  const now = nowMs();
  if (collectionsCache.items.length && now - collectionsCache.time < CACHE_TTL_MS) return collectionsCache.items;

  const headers = MRKT_AUTH_RUNTIME
    ? { ...mrktHeaders(), Accept: 'application/json' }
    : { Accept: 'application/json' };

  const res = await fetchWithTimeout(`${MRKT_API_URL}/gifts/collections`, { method: 'GET', headers }, MRKT_TIMEOUT_MS).catch(() => null);

  if (!res) {
    collectionsCache = { time: now, items: [] };
    return [];
  }
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

  const body = { collections: [giftName] };
  const res = await fetchWithTimeout(`${MRKT_API_URL}/gifts/models`, { method: 'POST', headers: mrktHeaders(), body: JSON.stringify(body) }, MRKT_TIMEOUT_MS).catch(() => null);

  if (!res) return [];
  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    await markMrktFail(res.status, txt);
    return [];
  }
  markMrktOk();

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
        thumbKey: it.modelStickerThumbnailKey || null,
        floorNano: it.floorPriceNanoTons ?? null,
      });
    }
  }

  const items = Array.from(map.values());
  modelsCache.set(giftName, { time: nowMs(), items });
  return items;
}

async function mrktGetBackdropsForGift(giftName) {
  if (!giftName) return [];
  const cached = backdropsCache.get(giftName);
  if (cached && nowMs() - cached.time < CACHE_TTL_MS) return cached.items;

  const body = { collections: [giftName] };
  const res = await fetchWithTimeout(`${MRKT_API_URL}/gifts/backdrops`, { method: 'POST', headers: mrktHeaders(), body: JSON.stringify(body) }, MRKT_TIMEOUT_MS).catch(() => null);

  if (!res) return [];
  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    await markMrktFail(res.status, txt);
    return [];
  }
  markMrktOk();

  const data = await res.json().catch(() => null);
  const arr = Array.isArray(data) ? data : [];
  const map = new Map();

  for (const it of arr) {
    const name = it.backdropName || it.name || null;
    if (!name) continue;
    const key = norm(name);
    if (!map.has(key)) {
      map.set(key, {
        name: String(name),
        rarityPerMille: it.rarityPerMille ?? null,
        centerHex: intToHexColor(it.colorsCenterColor ?? it.centerColor ?? null),
      });
    }
  }

  const items = Array.from(map.values());
  backdropsCache.set(giftName, { time: nowMs(), items });
  return items;
}

async function mrktFetchSalingPage({ collectionName, modelName, backdropName, number, cursor }) {
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
    number: number != null ? Number(number) : null, // <-- number filter
    count: MRKT_COUNT,
    cursor: cursor || '',
    query: null,
    promotedFirst: false,
  };

  const res = await fetchWithTimeout(`${MRKT_API_URL}/gifts/saling`, { method: 'POST', headers: mrktHeaders(), body: JSON.stringify(body) }, MRKT_TIMEOUT_MS).catch(() => null);

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

async function mrktSearchLots({ gift, model, backdrop, number }, pagesLimit) {
  if (!gift) return { ok: false, reason: 'NO_GIFT', gifts: [] };

  let cursor = '';
  const out = [];

  for (let page = 0; page < pagesLimit; page++) {
    const r = await mrktFetchSalingPage({
      collectionName: gift,
      modelName: model || null,
      backdropName: backdrop || null,
      number: number != null ? Number(number) : null,
      cursor,
    });
    if (!r.ok) return { ok: false, reason: r.reason, gifts: [] };

    for (const g of r.gifts) {
      const nano =
        g?.salePriceWithoutFee && Number(g.salePriceWithoutFee) > 0
          ? g.salePriceWithoutFee
          : g?.salePrice;

      if (nano == null) continue;

      const priceTon = Number(nano) / 1e9;
      if (!Number.isFinite(priceTon) || priceTon <= 0) continue;

      const baseName = (g.collectionTitle || g.collectionName || g.title || gift).trim();
      const numberVal = g.number ?? null;
      const displayName = numberVal ? `${baseName} #${numberVal}` : baseName;

      const modelName = g.modelTitle || g.modelName || '';
      const backdropName = g.backdropName || '';

      if (model && !sameTrait(modelName, norm(model))) continue;
      if (backdrop && !sameTrait(backdropName, norm(backdrop))) continue;
      if (number != null && numberVal != null && Number(numberVal) !== Number(number)) continue;

      const giftName =
        g.name ||
        giftNameFallbackFromCollectionAndNumber(g.collectionTitle || g.collectionName || g.title, g.number) ||
        null;

      const urlTelegram =
        giftName && String(giftName).includes('-') ? `https://t.me/nft/${giftName}` : 'https://t.me/mrkt';
      const urlMarket = g.id ? mrktLotUrlFromId(g.id) : 'https://t.me/mrkt';

      out.push({
        id: g.id,
        name: displayName,
        giftName,
        priceTon,
        priceNano: Number(nano),
        urlTelegram,
        urlMarket,
        model: modelName || null,
        backdrop: backdropName || null,
        symbol: g.symbolName || null,
        number: numberVal,
        collectionTitle: g.collectionTitle || g.collectionName || g.title || gift,
      });

      if (out.length >= 500) break;
    }

    cursor = r.cursor || '';
    if (!cursor || out.length >= 500) break;
  }

  out.sort((a, b) => a.priceTon - b.priceTon);
  return { ok: true, reason: 'OK', gifts: out };
}

async function mrktFeedFetch({ gift, model, backdrop, number, cursor, count, types }) {
  if (!MRKT_AUTH_RUNTIME) return { ok: false, reason: 'NO_AUTH', items: [], cursor: '' };

  const body = {
    count: Number(count || MRKT_FEED_COUNT),
    cursor: cursor || '',
    collectionNames: gift ? [gift] : [],
    modelNames: model ? [model] : [],
    backdropNames: backdrop ? [backdrop] : [],
    lowToHigh: false,
    maxPrice: null,
    minPrice: null,
    number: number != null ? Number(number) : null, // <-- number filter
    ordering: 'Latest',
    query: null,
    type: Array.isArray(types) ? types : [],
  };

  const res = await fetchWithTimeout(`${MRKT_API_URL}/feed`, { method: 'POST', headers: mrktHeaders(), body: JSON.stringify(body) }, MRKT_TIMEOUT_MS).catch(() => null);

  if (!res) return { ok: false, reason: 'FETCH_ERROR', items: [], cursor: '' };
  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    await markMrktFail(res.status, txt);
    return { ok: false, reason: `HTTP_${res.status}`, items: [], cursor: '' };
  }
  markMrktOk();

  const data = await res.json().catch(() => null);
  return { ok: true, reason: 'OK', items: Array.isArray(data?.items) ? data.items : [], cursor: data?.cursor || '' };
}

async function mrktOrdersFetch({ gift, model, backdrop }) {
  if (!MRKT_AUTH_RUNTIME) return { ok: false, reason: 'NO_AUTH', orders: [] };

  const key = `orders|${gift}|${model || ''}|${backdrop || ''}`;
  const now = nowMs();
  const cached = offersCache.get(key);
  if (cached && now - cached.time < OFFERS_CACHE_TTL_MS) return cached.data;

  const body = {
    collectionNames: gift ? [gift] : [],
    modelNames: model ? [model] : [],
    backdropNames: backdrop ? [backdrop] : [],
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

function isNoFundsError(r) {
  const raw = JSON.stringify(r?.data || r?.text || '').toLowerCase();
  return raw.includes('not enough') || raw.includes('insufficient') || raw.includes('no funds') || raw.includes('low balance') || raw.includes('balance');
}

// ===================== History =====================
async function mrktHistorySales({ gift, model, backdrop, number }) {
  const key = `hist|${gift}|${model || ''}|${backdrop || ''}|${number || ''}`;
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
      gift,
      model: model || null,
      backdrop: backdrop || null,
      number: number != null ? Number(number) : null,
      cursor,
      count: HISTORY_COUNT_PER_PAGE,
      types: ['sale'],
    });
    if (!r.ok) break;
    if (!r.items.length) break;

    for (const it of r.items) {
      const g = it.gift;
      if (!g) continue;

      const amountNano = it.amount ?? g.salePrice ?? g.salePriceWithoutFee ?? null;
      const ton = amountNano != null ? Number(amountNano) / 1e9 : NaN;
      if (!Number.isFinite(ton) || ton <= 0) continue;

      prices.push(ton);

      if (lastSales.length < 35) {
        const giftName = g.name || giftNameFallbackFromCollectionAndNumber(g.collectionTitle || g.collectionName || g.title, g.number) || null;
        const urlTelegram = giftName && String(giftName).includes('-') ? `https://t.me/nft/${giftName}` : null;
        const urlMarket = g.id ? mrktLotUrlFromId(g.id) : null;

        lastSales.push({
          tsMsk: it.date ? formatMsk(it.date) : '',
          priceTon: ton,
          number: g.number ?? null,
          model: g.modelTitle || g.modelName || null,
          backdrop: g.backdropName || null,
          giftName,
          urlTelegram,
          urlMarket,
          imgUrl: giftName ? `/img/gift?name=${encodeURIComponent(giftName)}` : null,
        });
      }

      if (prices.length >= HISTORY_TARGET_SALES) break;
    }

    cursor = r.cursor || '';
    pages++;
    if (!cursor) break;

    if (MRKT_FEED_THROTTLE_MS > 0) await sleep(MRKT_FEED_THROTTLE_MS);
  }

  prices.sort((a, b) => a - b);
  const data = { ok: true, median: median(prices), count: prices.length, lastSales };

  historyCache.set(key, { time: now, data });
  return data;
}

async function mrktHistorySmart({ gift, model, backdrop, number }) {
  let h = await mrktHistorySales({ gift, model, backdrop, number });
  if (h.median != null && h.count >= 10) return { ...h, level: 'exact' };

  // fallback to without number first
  if (number != null) {
    const hn = await mrktHistorySales({ gift, model, backdrop, number: null });
    if (hn.median != null && hn.count >= 10) return { ...hn, level: 'no_number' };
    if (hn.count > h.count) h = { ...hn, level: 'no_number' };
  }

  if (model) {
    const hm = await mrktHistorySales({ gift, model, backdrop: '', number: null });
    if (hm.median != null && hm.count >= 10) return { ...hm, level: 'model' };
    if (hm.count > h.count) h = { ...hm, level: 'model' };
  }

  if (backdrop) {
    const hb = await mrktHistorySales({ gift, model: '', backdrop, number: null });
    if (hb.median != null && hb.count >= 10) return { ...hb, level: 'backdrop' };
    if (hb.count > h.count) h = { ...hb, level: 'backdrop' };
  }

  const hc = await mrktHistorySales({ gift, model: '', backdrop: '', number: null });
  if (hc.count > h.count) h = { ...hc, level: 'collection' };

  return { ...h, level: h.level || 'unknown' };
}

// ===================== Subscriptions notify =====================
async function notifyFloorToUser(userId, sub, lot, prevFloor, newFloor) {
  const title = lot?.name || sub?.filters?.gift || 'Gift';
  const urlTelegram = lot?.urlTelegram || 'https://t.me/mrkt';
  const urlMarket = lot?.urlMarket || 'https://t.me/mrkt';

  const model = lot?.model || sub?.filters?.model || '';
  const backdrop = lot?.backdrop || sub?.filters?.backdrop || '';
  const num = lot?.number != null ? `#${lot.number}` : (sub?.filters?.number ? `#${sub.filters.number}` : '');

  const lines = [];
  lines.push(`Gift: ${title} ${num}`.trim());
  lines.push(`Floor: ${newFloor.toFixed(3)} TON`);
  if (model) lines.push(`Model: ${model}`);
  if (backdrop) lines.push(`Backdrop: ${backdrop}`);
  lines.push(urlTelegram); // keep for preview

  const openSubUrl = WEBAPP_URL ? `${WEBAPP_URL}?tab=subs&subId=${encodeURIComponent(sub.id)}` : urlMarket;

  const reply_markup = {
    inline_keyboard: [[
      { text: 'Открыть подписку', url: openSubUrl },
      { text: 'MRKT', url: urlMarket },
    ]],
  };

  await sendMessageSafe(userId, lines.join('\n'), {
    disable_web_page_preview: false,
    reply_markup,
  });
}

// ===================== Workers =====================
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
        const prev = subStates.get(stateKey) || { floor: null, emptyStreak: 0, feedLastId: null, pausedUntil: 0 };

        const lots = await mrktSearchLots({
          gift: sub.filters.gift,
          model: sub.filters.model,
          backdrop: sub.filters.backdrop,
          number: sub.filters.number,
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
          await notifyFloorToUser(userId, sub, lot, prevFloor, newFloor);
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

async function autoBuyCycle() {
  // оставляем как было (у тебя уже работало), не трогаем сейчас
  return;
}

// ===================== Telegram: menu + status =====================
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
      `• Persist: ${redis ? '✅' : '❌ (без Redis подписки не сохранятся)'}`;
    await sendMessageSafe(msg.chat.id, txt, { reply_markup: MAIN_KEYBOARD });
  }
});

// ===================== WebApp HTML/JS =====================
const WEBAPP_HTML = `<!doctype html><html lang="ru"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"/>
<title>${APP_TITLE}</title>
<style>
:root{
  --bg:#0b0f14;
  --card:#101826;
  --text:#e5e7eb;
  --muted:#9ca3af;
  --border:#223044;
  --input:#0f172a;
  --btn:#182235;
  --btnText:#e5e7eb;
  --danger:#ef4444;
  --accent:#22c55e;
  --accent2:#38bdf8;
}
*{box-sizing:border-box}
body{margin:0;padding:14px;background:var(--bg);color:var(--text);font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial; animation: fadein .18s ease-out}
@keyframes fadein{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:translateY(0)}}
h2{margin:0 0 10px 0}
.card{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:12px;margin:10px 0}
label{display:block;font-size:12px;color:var(--muted);margin-bottom:4px}
input{
  width:100%;
  padding:10px;
  border:1px solid var(--border);
  border-radius:12px;
  background:var(--input);
  color:var(--text);
  outline:none;
}
input.compact{height:38px;padding:8px 10px;font-size:13px}
input.compact.expanded{height:44px;font-size:14px}
button{
  padding:10px 12px;
  border:1px solid var(--border);
  border-radius:12px;
  background:var(--btn);
  color:var(--btnText);
  cursor:pointer;
  transition: transform .08s ease, filter .12s ease, border-color .12s ease;
}
button:hover{filter:brightness(1.08); transform: translateY(-1px);}
button:active{transform: translateY(0);}
.primary{border-color:var(--accent);color:#052e16;background:var(--accent);font-weight:900}
.primary:hover{filter:brightness(1.03)}
.small{padding:8px 10px; border-radius:10px; font-size:13px}
.buy{border-color:rgba(56,189,248,.65); background:rgba(56,189,248,.18); color:#eaf7ff; font-weight:900}
.buy:hover{border-color:rgba(56,189,248,1); background:rgba(56,189,248,.24)}
#err{display:none;border-color:var(--danger);color:#ffd1d1;white-space:pre-wrap;word-break:break-word}
.row{display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end}
.field{position:relative;flex:1 1 220px;min-width:150px}
.tabs{display:flex;gap:8px;flex-wrap:wrap}
.tabbtn{border-radius:999px;padding:8px 12px}
.tabbtn.active{border-color:var(--accent);color:var(--accent)}
.sug{
  border:1px solid var(--border);
  border-radius:14px;
  overflow:auto;
  background:var(--card);
  max-height:360px;
  position:absolute;
  top:calc(100% + 6px);
  left:0; right:0;
  z-index:9999;
  box-shadow: 0 14px 40px rgba(0,0,0,.45);
}
.sug .item{width:100%;text-align:left;border:0;background:transparent;padding:10px;display:flex;gap:10px;align-items:center}
.sug .item:hover{background:rgba(255,255,255,.06)}
.thumb{width:54px;height:54px;border-radius:16px;object-fit:contain;background:rgba(255,255,255,.06);border:1px solid var(--border);flex:0 0 auto}
.badge{padding:4px 8px;border-radius:999px;border:1px solid var(--border);color:var(--muted);font-size:12px;flex:0 0 auto}
.hr{height:1px;background:var(--border);margin:10px 0}
.muted{color:var(--muted);font-size:13px}

/* grid lots: phone 2 cols */
.grid{display:grid;gap:10px;margin-top:10px;grid-template-columns: repeat(auto-fill, minmax(170px, 1fr));}
@media (max-width: 520px){
  .grid{grid-template-columns: repeat(2, minmax(0, 1fr));}
}
.lot{border:1px solid var(--border);border-radius:16px;padding:10px;background:rgba(255,255,255,.02)}
.lot img{width:100%;aspect-ratio: 1 / 1; height:auto; object-fit:cover;background:rgba(255,255,255,.03);border-radius:14px;border:1px solid var(--border)}
.price{font-size:15px;font-weight:950;margin-top:8px}
.chip{display:inline-flex;align-items:center;gap:8px}
.dot{width:12px;height:12px;border-radius:50%;border:1px solid var(--border);display:inline-block}

/* subs thumb with backdrop tint */
.thumbWrap{width:54px;height:54px;border-radius:16px;border:1px solid var(--border);background:rgba(255,255,255,.04);display:flex;align-items:center;justify-content:center;overflow:hidden}
.thumbWrap img{width:54px;height:54px;object-fit:contain}

/* sheet */
.sheetWrap{position:fixed;inset:0;background:rgba(0,0,0,.55);display:none;align-items:flex-end;justify-content:center;z-index:50000;padding:12px}
.sheet{width:min(980px, 96vw);height:min(70vh, 720px);background:var(--card);border:1px solid var(--border);border-radius:18px;padding:12px;box-shadow:0 30px 90px rgba(0,0,0,.5);display:flex;flex-direction:column;gap:10px}
.sheetHeader{display:flex;justify-content:space-between;gap:12px;align-items:center}
.sheetBody{overflow:auto;padding-right:4px}

/* sales */
.saleRow{display:flex;gap:10px;align-items:stretch}
.saleImg{width:88px;height:88px;border-radius:14px;border:1px solid var(--border);object-fit:cover;background:rgba(255,255,255,.03);flex:0 0 auto}
.saleCard{flex:1;border:1px solid var(--border);border-radius:14px;padding:10px;background:rgba(255,255,255,.02)}
.saleBtns{display:flex;gap:8px;margin-top:8px;flex-wrap:wrap}

/* lot modal */
.modalWrap{position:fixed;inset:0;background:rgba(0,0,0,.55);display:none;align-items:flex-end;justify-content:center;z-index:60000;padding:12px}
.modal{width:min(560px, 96vw);background:var(--card);border:1px solid var(--border);border-radius:18px;padding:12px;box-shadow:0 30px 90px rgba(0,0,0,.5)}
.modalHeader{display:flex;justify-content:space-between;gap:12px;align-items:center}
.modalTitle{font-weight:950}
.modalImg{width:100%;aspect-ratio: 16/10; height:auto;object-fit:cover;border-radius:14px;border:1px solid var(--border);background:rgba(255,255,255,.03);margin-top:10px}
@media (min-width: 900px){
  body{padding:18px}
  .card{max-width:1200px}
}
</style></head><body>
<h2>${APP_TITLE}</h2>
<div id="err" class="card"></div>

<div class="tabs">
  <button class="tabbtn active" data-tab="market">Market</button>
  <button class="tabbtn" data-tab="subs">Subscriptions</button>
  <button class="tabbtn" data-tab="admin" id="adminTabBtn" style="display:none">Admin</button>
</div>

<div id="market" class="card">
  <h3 style="margin:0 0 8px 0">Фильтры</h3>

  <div class="row">
    <div class="field">
      <label>Gift</label>
      <input id="gift" class="compact" placeholder="Gift" autocomplete="off"/>
      <div id="giftSug" class="sug" style="display:none"></div>
    </div>
    <div class="field">
      <label>Model</label>
      <input id="model" class="compact" placeholder="Model" autocomplete="off"/>
      <div id="modelSug" class="sug" style="display:none"></div>
    </div>
    <div class="field">
      <label>Backdrop</label>
      <input id="backdrop" class="compact" placeholder="Backdrop" autocomplete="off"/>
      <div id="backdropSug" class="sug" style="display:none"></div>
    </div>
    <div class="field" style="max-width:160px">
      <label>Number</label>
      <input id="number" class="compact" placeholder="№" inputmode="numeric"/>
    </div>
  </div>

  <div class="row" style="margin-top:10px">
    <button id="apply" class="primary">Показать</button>
    <button id="refresh">Обновить</button>
    <button id="historyBtn">История продаж</button>
  </div>

  <div id="status" class="muted" style="margin-top:10px"></div>
  <div id="offersLine" class="muted" style="margin-top:6px"></div>

  <div class="hr"></div>
  <div><b>Лоты</b> <span class="muted">(клик по карточке → купить/открыть)</span></div>
  <div id="lots" class="grid"></div>
</div>

<div id="subs" class="card" style="display:none">
  <h3 style="margin:0 0 8px 0">Подписки</h3>
  <div class="row">
    <button id="subCreate">Создать из текущих фильтров</button>
    <button id="subCheckNow">Проверить сейчас</button>
    <button id="subRefresh">Обновить</button>
  </div>
  <div id="subsList" style="margin-top:10px"></div>
</div>

<div id="admin" class="card" style="display:none">
  <h3 style="margin:0 0 8px 0">Admin</h3>
  <div class="muted">MRKT токен хранится в Redis.</div>
  <div class="row" style="margin-top:10px">
    <div class="field"><label>Текущий токен (маска)</label><input id="tokMask" disabled/></div>
    <div class="field"><label>Новый MRKT_AUTH</label><input id="tokNew" placeholder="вставь токен"/></div>
    <button id="tokSave">Сохранить токен</button>
  </div>
</div>

<!-- Sheet: History / Sub Details -->
<div id="sheetWrap" class="sheetWrap">
  <div class="sheet">
    <div class="sheetHeader">
      <div>
        <div id="sheetTitle" style="font-weight:950"></div>
        <div id="sheetSub" class="muted"></div>
      </div>
      <button id="sheetClose" class="small">✕</button>
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
      <button id="mClose" class="small">✕</button>
    </div>
    <img id="mImg" class="modalImg" src="" alt="gift"/>
    <div class="row" style="margin-top:10px">
      <button id="mBuy" class="buy">Купить</button>
      <button id="mMrkt">Открыть MRKT</button>
      <button id="mNft">Открыть NFT</button>
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
    if(!res.ok) throw new Error('HTTP '+res.status+': '+JSON.stringify(data).slice(0,600));
    return data;
  }

  function setTab(name){
    ['market','subs','admin'].forEach(x => el(x).style.display = (x===name?'block':'none'));
    document.querySelectorAll('.tabbtn').forEach(b => b.classList.toggle('active', b.dataset.tab===name));
  }
  document.querySelectorAll('.tabbtn').forEach(b => b.onclick = () => setTab(b.dataset.tab));

  // open requested tab/subId
  const params = new URLSearchParams(location.search || '');
  const initialTab = params.get('tab');
  const initialSubId = params.get('subId');
  if (initialTab === 'subs') setTab('subs');

  function safe(fn){
    return async () => {
      hideErr();
      try { await fn(); } catch(e){ showErr(e.message || String(e)); }
    };
  }

  // compact expand behavior
  function expandInput(inp){ inp.classList.add('expanded'); }
  function collapseInput(inp){ inp.classList.remove('expanded'); }

  // suggestions
  function hideSug(id){ const b=el(id); b.style.display='none'; b.innerHTML=''; }
  function renderSug(id, items, onPick){
    const b=el(id);
    if(!items||!items.length){ hideSug(id); return; }
    b.innerHTML = items.map(x => {
      const thumb = x.imgUrl
        ? '<img class="thumb" src="'+x.imgUrl+'" referrerpolicy="no-referrer"/>'
        : (x.colorHex ? '<div class="thumb" style="border-radius:50%;background:'+x.colorHex+'"></div>' : '<div class="thumb"></div>');

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
      collapseInput(el('gift'));
    });
  }
  async function showModelSug(){
    const gift = selectedGiftValue || '';
    const q = el('model').value.trim();
    if(!gift){ hideSug('modelSug'); return; }
    const r = await api('/api/mrkt/suggest?kind=model&gift='+encodeURIComponent(gift)+'&q='+encodeURIComponent(q));
    renderSug('modelSug', r.items||[], (v)=>{ el('model').value=v; el('backdrop').value=''; collapseInput(el('model')); });
  }
  async function showBackdropSug(){
    const gift = selectedGiftValue || '';
    const q = el('backdrop').value.trim();
    if(!gift){ hideSug('backdropSug'); return; }
    const r = await api('/api/mrkt/suggest?kind=backdrop&gift='+encodeURIComponent(gift)+'&q='+encodeURIComponent(q));
    renderSug('backdropSug', r.items||[], (v)=>{ el('backdrop').value=v; collapseInput(el('backdrop')); });
  }

  const giftInp = el('gift');
  const modelInp = el('model');
  const backInp = el('backdrop');

  giftInp.addEventListener('focus', safe(async () => { expandInput(giftInp); await showGiftSug(); }));
  giftInp.addEventListener('input', safe(async () => { selectedGiftValue=''; await showGiftSug(); }));
  giftInp.addEventListener('blur', () => setTimeout(()=>collapseInput(giftInp), 120));

  modelInp.addEventListener('focus', safe(async () => { expandInput(modelInp); await showModelSug(); }));
  modelInp.addEventListener('input', safe(showModelSug));
  modelInp.addEventListener('blur', () => setTimeout(()=>collapseInput(modelInp), 120));

  backInp.addEventListener('focus', safe(async () => { expandInput(backInp); await showBackdropSug(); }));
  backInp.addEventListener('input', safe(showBackdropSug));
  backInp.addEventListener('blur', () => setTimeout(()=>collapseInput(backInp), 120));

  document.addEventListener('click', (e) => {
    if(!e.target.closest('#gift,#model,#backdrop,.sug')){
      hideSug('giftSug'); hideSug('modelSug'); hideSug('backdropSug');
    }
  });

  // ===== Lot modal =====
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
    mSub.textContent = (lot.priceTon!=null ? ('Цена: ' + lot.priceTon.toFixed(3) + ' TON') : '') + (lot.number!=null ? (' | #' + lot.number) : '');
    mImg.src = lot.imgUrl || '';
    modalWrap.style.display = 'flex';
  }
  function closeModal(){ modalWrap.style.display = 'none'; modalLot = null; }
  mClose.onclick = closeModal;
  modalWrap.addEventListener('click', (e) => { if (e.target === modalWrap) closeModal(); });

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

  // ===== Sheet (history / sub details) =====
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
  function closeSheet(){ sheetWrap.style.display = 'none'; }
  sheetClose.onclick = closeSheet;
  sheetWrap.addEventListener('click', (e) => { if (e.target === sheetWrap) closeSheet(); });

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
      const img = x.imgUrl ? '<img class="saleImg" src="'+x.imgUrl+'" referrerpolicy="no-referrer" loading="lazy"/>' :
        '<div class="saleImg"></div>';

      const btns =
        '<div class="saleBtns">' +
          (x.urlMarket ? '<button class="small" data-open="'+x.urlMarket+'">MRKT</button>' : '') +
          (x.urlTelegram ? '<button class="small" data-open="'+x.urlTelegram+'">NFT</button>' : '') +
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

  function renderOffersLine(offer){
    if(offer && offer.ok && offer.maxOfferTon != null){
      el('offersLine').textContent = 'Max offer: ' + offer.maxOfferTon.toFixed(3) + ' TON';
    } else if (offer && offer.ok) {
      el('offersLine').textContent = 'Max offer: нет данных';
    } else {
      el('offersLine').textContent = '';
    }
  }

  // open links from sheet
  document.body.addEventListener('click', (e) => {
    const openBtn = e.target.closest('button[data-open]');
    if(!openBtn) return;
    const url = openBtn.getAttribute('data-open');
    if(url) tg?.openTelegramLink ? tg.openTelegramLink(url) : window.open(url,'_blank');
  });

  // ===== Lots rendering =====
  function renderLots(resp){
    const box = el('lots');
    if(resp.ok===false){
      box.innerHTML = '<div style="color:#ef4444"><b>'+ (resp.reason||'error') +'</b></div>';
      return;
    }
    const lots = resp.lots||[];
    if(!lots.length){ box.innerHTML = '<i class="muted">Лотов не найдено</i>'; return; }

    // map id -> lot
    const map = new Map(lots.map(l => [String(l.id), l]));

    box.innerHTML = lots.map(x => {
      const img = x.imgUrl ? '<img src="'+x.imgUrl+'" referrerpolicy="no-referrer" loading="lazy"/>' :
        '<div style="aspect-ratio:1/1;border:1px solid rgba(255,255,255,.10);border-radius:14px"></div>';
      const bd = x.backdropColorHex ? '<span class="chip"><span class="dot" style="background:'+x.backdropColorHex+'"></span><span class="muted">'+x.backdrop+'</span></span>' : '';
      const num = (x.number!=null) ? ('<span class="badge">#'+x.number+'</span>') : '';
      return '<div class="lot" data-lot="1" data-id="'+x.id+'">'+img+
        '<div class="price">'+x.priceTon.toFixed(3)+' TON</div>'+
        '<div style="display:flex;justify-content:space-between;gap:10px;align-items:center"><b style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'+x.name+'</b>'+num+'</div>'+
        (x.model?'<div class="muted">Model: '+x.model+'</div>':'')+
        (bd?'<div style="margin-top:6px">'+bd+'</div>':'')+
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

  async function refreshState(){
    const st = await api('/api/state');
    el('status').textContent =
      'MRKT_AUTH: ' + (st.api.mrktAuthSet?'✅':'❌') +
      (st.api.mrktLastFail ? ' | last fail: '+st.api.mrktLastFail : '');

    el('gift').value = st.user.filters.giftLabel || st.user.filters.gift || '';
    selectedGiftValue = st.user.filters.gift || '';
    el('model').value = st.user.filters.model || '';
    el('backdrop').value = st.user.filters.backdrop || '';
    el('number').value = st.user.filters.number || '';

    const subsList = st.user.subscriptions || [];
    const box = el('subsList');
    if(!subsList.length){
      box.innerHTML = '<i class="muted">Подписок нет</i>';
    } else {
      box.innerHTML = subsList.map(s => {
        const bg = s.backdropHex ? ('background:'+s.backdropHex+';') : 'background:rgba(255,255,255,.04);';
        const img = s.thumbUrl ? '<div class="thumbWrap" style="'+bg+'"><img src="'+s.thumbUrl+'" referrerpolicy="no-referrer"/></div>' : '<div class="thumbWrap" style="'+bg+'"></div>';
        const num = s.filters.number ? ('<div class="muted">Number: #'+s.filters.number+'</div>') : '';
        return '<div class="card" style="margin:8px 0">' +
          '<div style="display:flex;gap:10px;align-items:center">' +
            img +
            '<div style="min-width:0;flex:1">' +
              '<div style="display:flex;justify-content:space-between;gap:10px;align-items:center">' +
                '<b>#'+s.num+' '+(s.enabled?'ON':'OFF')+'</b>' +
                '<button class="small" data-act="subDetails" data-id="'+s.id+'">Детали</button>' +
              '</div>' +
              '<div class="muted" style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap">Gift: '+s.filters.gift+'</div>' +
              '<div class="muted" style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap">Model: '+(s.filters.model||'any')+'</div>' +
              '<div class="muted" style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap">Backdrop: '+(s.filters.backdrop||'any')+'</div>' +
              num +
            '</div>' +
          '</div>' +
          '<div class="hr"></div>' +
          '<div class="row">' +
            '<button class="small" data-act="subToggle" data-id="'+s.id+'">'+(s.enabled?'Отключить':'Включить')+'</button>' +
            '<button class="small" data-act="subDel" data-id="'+s.id+'">Удалить</button>' +
            '<button class="small" data-act="subNotifyMax" data-id="'+s.id+'">Max уведомлений</button>' +
            '<button class="small" data-act="subAutoToggle" data-id="'+s.id+'">'+(s.autoBuyEnabled?'AutoBuy: ON':'AutoBuy: OFF')+'</button>' +
            '<button class="small" data-act="subAutoMax" data-id="'+s.id+'">Max AutoBuy</button>' +
          '</div>' +
          '<div class="muted" style="margin-top:8px">Notify max: '+(s.maxNotifyTon==null?'∞':s.maxNotifyTon)+' TON</div>' +
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
    await api('/api/state/patch', {
      method:'POST',
      body: JSON.stringify({
        filters:{
          gift: selectedGiftValue,
          giftLabel: el('gift').value.trim(),
          model: el('model').value.trim(),
          backdrop: el('backdrop').value.trim(),
          number: el('number').value.trim()
        }
      })
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

  // Subs
  el('subCreate').onclick = safe(async () => { await api('/api/sub/create',{method:'POST'}); await refreshState(); });
  el('subRefresh').onclick = safe(async () => { await refreshState(); });
  el('subCheckNow').onclick = safe(async () => {
    const r = await api('/api/sub/checknow',{method:'POST'});
    alert('Готово: subs='+r.processedSubs+', уведомлений='+r.floorNotifs);
  });

  // delegated clicks for subs
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
        sheetTop.innerHTML =
          '<div><b>Max offer:</b> '+offerTxt+'</div>' +
          '<div style="margin-top:6px"><b>Оценка цены продажи:</b> '+(r.history?.median!=null?r.history.median.toFixed(3)+' TON':'нет данных')+'</div>';
        renderSalesToSheet(r.history || {ok:true, median:null, count:0, lastSales:[]});
      }

      await refreshState();
    }catch(err){ showErr(err.message||String(err)); }
  });

  // admin save token
  el('tokSave').onclick = safe(async () => {
    const t = el('tokNew').value.trim();
    if(!t) return showErr('Вставь токен.');
    await api('/api/admin/mrkt_auth',{method:'POST',body:JSON.stringify({token:t})});
    el('tokNew').value='';
    await refreshState();
  });

  // initial
  refreshState().then(refreshMarketData).catch(e=>showErr(e.message||String(e)));

  // auto open sub details if subId in url
  if(initialTab === 'subs' && initialSubId){
    setTimeout(async () => {
      try{
        openSheet('Детали подписки', 'История + Max offer');
        const r = await api('/api/sub/details',{method:'POST',body:JSON.stringify({id: initialSubId})});
        const offerTxt = (r.offers?.maxOfferTon!=null) ? (r.offers.maxOfferTon.toFixed(3)+' TON') : 'нет данных';
        sheetTop.innerHTML =
          '<div><b>Max offer:</b> '+offerTxt+'</div>' +
          '<div style="margin-top:6px"><b>Оценка цены продажи:</b> '+(r.history?.median!=null?r.history.median.toFixed(3)+' TON':'нет данных')+'</div>';
        renderSalesToSheet(r.history || {ok:true, median:null, count:0, lastSales:[]});
      }catch{}
    }, 450);
  }
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
    next();
  }
  const isAdmin = (userId) => ADMIN_USER_ID && Number(userId) === Number(ADMIN_USER_ID);

  // Proxy: Fragment gift image
  app.get('/img/gift', async (req, res) => {
    const name = String(req.query.name || '').trim();
    if (!name) return res.status(400).send('no name');

    const url = fragmentGiftRemoteUrl(name);
    const r = await fetchWithTimeout(url, { method: 'GET' }, 8000).catch(() => null);
    if (!r || !r.ok) return res.status(404).send('not found');

    const ct = r.headers.get('content-type') || 'image/jpeg';
    res.setHeader('Content-Type', ct);
    res.setHeader('Cache-Control', 'public, max-age=86400');
    Readable.fromWeb(r.body).pipe(res);
  });

  // Proxy: MRKT CDN image
  app.get('/img/cdn', async (req, res) => {
    const key = String(req.query.key || '').trim();
    if (!key) return res.status(400).send('no key');

    const url = joinUrl(MRKT_CDN_BASE, key);
    const r = await fetchWithTimeout(url, { method: 'GET', headers: { Accept: 'image/*' } }, 8000).catch(() => null);
    if (!r || !r.ok) return res.status(404).send('not found');

    const ct = r.headers.get('content-type') || 'image/webp';
    res.setHeader('Content-Type', ct);
    res.setHeader('Cache-Control', 'public, max-age=86400');
    Readable.fromWeb(r.body).pipe(res);
  });

  async function buildSubThumb(sub) {
    const gift = sub?.filters?.gift;
    if (!gift) return { thumbUrl: null, backdropHex: null };

    let backdropHex = null;
    if (sub?.filters?.backdrop) {
      const backs = await mrktGetBackdropsForGift(gift);
      const found = backs.find((b) => norm(b.name) === norm(sub.filters.backdrop));
      backdropHex = found?.centerHex || null;
    }

    if (sub?.filters?.model) {
      const models = await mrktGetModelsForGift(gift);
      const found = models.find((m) => norm(m.name) === norm(sub.filters.model));
      if (found?.thumbKey) return { thumbUrl: `/img/cdn?key=${encodeURIComponent(found.thumbKey)}`, backdropHex };
    }

    const cols = await mrktGetCollections();
    const c = cols.find((x) => x.name === gift);
    if (c?.thumbKey) return { thumbUrl: `/img/cdn?key=${encodeURIComponent(c.thumbKey)}`, backdropHex };
    return { thumbUrl: null, backdropHex };
  }

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
      api: { mrktAuthSet: !!MRKT_AUTH_RUNTIME, isAdmin: isAdmin(req.userId), mrktAuthMask: mask, mrktLastFail: lastFail },
      user: { enabled: !!u.enabled, filters: u.filters, subscriptions: enriched },
    });
  });

  app.get('/api/mrkt/collections', auth, async (req, res) => {
    const q = String(req.query.q || '').trim().toLowerCase();
    const all = await mrktGetCollections();

    if (!all.length) {
      if (!MRKT_AUTH_RUNTIME) return res.status(400).json({ ok: false, reason: 'MRKT_AUTH_NOT_SET', items: [] });
      if (mrktAuthState.lastFailAt) return res.status(502).json({ ok: false, reason: `MRKT_HTTP_${mrktAuthState.lastFailCode}`, items: [] });
      return res.status(502).json({ ok: false, reason: 'MRKT_NO_COLLECTIONS', items: [] });
    }

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
      u.filters.number = toIntOrNull(b.filters.number);
    }

    scheduleSave();
    res.json({ ok: true });
  });

  app.get('/api/mrkt/lots', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    if (!MRKT_AUTH_RUNTIME) return res.json({ ok: false, reason: 'MRKT_AUTH_NOT_SET', lots: [] });
    if (!u.filters.gift) return res.json({ ok: true, lots: [] });

    const r = await mrktSearchLots({
      gift: u.filters.gift,
      model: u.filters.model,
      backdrop: u.filters.backdrop,
      number: u.filters.number,
    }, WEBAPP_LOTS_PAGES);

    if (!r.ok) return res.json({ ok: false, reason: r.reason, lots: [] });

    const backs = await mrktGetBackdropsForGift(u.filters.gift);
    const backMap = new Map(backs.map((b) => [norm(b.name), b]));

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

  app.get('/api/mrkt/history_current', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    if (!u.filters.gift) return res.json({ ok: true, median: null, count: 0, lastSales: [] });

    const h = await mrktHistorySmart({
      gift: u.filters.gift,
      model: u.filters.model || '',
      backdrop: u.filters.backdrop || '',
      number: u.filters.number,
    });

    res.json({ ok: true, median: h.median, count: h.count, lastSales: h.lastSales || [] });
  });

  app.get('/api/mrkt/offers_current', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    if (!u.filters.gift) return res.json({ ok: true, maxOfferTon: null, count: 0 });

    const r = await mrktOrdersFetch({ gift: u.filters.gift, model: u.filters.model || '', backdrop: u.filters.backdrop || '' });
    if (!r.ok) return res.json({ ok: false, reason: r.reason });

    const orders = r.orders || [];
    let maxNano = null;
    for (const o of orders) {
      const v = o?.priceMaxNanoTONs;
      const n = v == null ? null : Number(v);
      if (!Number.isFinite(n) || n <= 0) continue;
      if (maxNano == null || n > maxNano) maxNano = n;
    }

    res.json({ ok: true, maxOfferTon: maxNano != null ? (maxNano / 1e9) : null, count: orders.length });
  });

  app.post('/api/mrkt/buy', auth, async (req, res) => {
    if (!MRKT_AUTH_RUNTIME) return res.status(400).json({ ok: false, reason: 'MRKT_AUTH_NOT_SET' });

    const id = String(req.body?.id || '').trim();
    const priceNano = Number(req.body?.priceNano);
    if (!id) return res.status(400).json({ ok: false, reason: 'NO_ID' });
    if (!Number.isFinite(priceNano) || priceNano <= 0) return res.status(400).json({ ok: false, reason: 'BAD_PRICE' });

    const r = await mrktBuy({ id, priceNano });
    if (!r.ok) return res.status(502).json({ ok: false, reason: r.reason, data: r.data || null, text: r.text || null });

    const g = r.okItem?.userGift || null;
    const title = `${g?.collectionTitle || g?.collectionName || g?.title || 'Gift'}${g?.number != null ? ` #${g.number}` : ''}`;
    const priceTon = Number(priceNano) / 1e9;

    res.json({ ok: true, title, priceTon });
  });

  // ===== subscriptions endpoints =====
  app.post('/api/sub/create', auth, (req, res) => {
    const u = getOrCreateUser(req.userId);
    const r = makeSubFromCurrentFilters(u);
    if (!r.ok) return res.status(400).json({ ok: false, reason: r.reason });
    u.subscriptions.push(r.sub);
    renumberSubs(u);
    scheduleSave();
    res.json({ ok: true });
  });

  app.post('/api/sub/toggle', auth, (req, res) => {
    const u = getOrCreateUser(req.userId);
    const id = req.body?.id;
    const s = findSub(u, id);
    if (!s) return res.status(404).json({ ok: false, reason: 'NOT_FOUND' });
    s.enabled = !s.enabled;
    scheduleSave();
    res.json({ ok: true });
  });

  app.post('/api/sub/delete', auth, (req, res) => {
    const u = getOrCreateUser(req.userId);
    const id = req.body?.id;
    u.subscriptions = (u.subscriptions || []).filter((x) => x && x.id !== id);
    renumberSubs(u);
    scheduleSave();
    res.json({ ok: true });
  });

  app.post('/api/sub/set_notify_max', auth, (req, res) => {
    const u = getOrCreateUser(req.userId);
    const id = req.body?.id;
    const v = req.body?.maxNotifyTon;
    const s = findSub(u, id);
    if (!s) return res.status(404).json({ ok: false, reason: 'NOT_FOUND' });
    if (v == null) { s.maxNotifyTon = null; scheduleSave(); return res.json({ ok: true }); }
    const num = Number(v);
    if (!Number.isFinite(num) || num <= 0) return res.status(400).json({ ok: false, reason: 'BAD_MAX' });
    s.maxNotifyTon = num;
    scheduleSave();
    res.json({ ok: true });
  });

  app.post('/api/sub/toggle_autobuy', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    const id = req.body?.id;
    const s = findSub(u, id);
    if (!s) return res.status(404).json({ ok: false, reason: 'NOT_FOUND' });
    s.autoBuyEnabled = !s.autoBuyEnabled;
    scheduleSave();
    res.json({ ok: true });
  });

  app.post('/api/sub/set_autobuy_max', auth, (req, res) => {
    const u = getOrCreateUser(req.userId);
    const id = req.body?.id;
    const num = Number(req.body?.maxAutoBuyTon);
    const s = findSub(u, id);
    if (!s) return res.status(404).json({ ok: false, reason: 'NOT_FOUND' });
    if (!Number.isFinite(num) || num <= 0) return res.status(400).json({ ok: false, reason: 'BAD_MAX' });
    s.maxAutoBuyTon = num;
    scheduleSave();
    res.json({ ok: true });
  });

  app.post('/api/sub/checknow', auth, async (req, res) => {
    const st = await checkSubscriptionsForAllUsers({ manual: true });
    res.json(st);
  });

  app.post('/api/sub/details', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    const id = String(req.body?.id || '');
    const s = findSub(u, id);
    if (!s) return res.status(404).json({ ok: false, reason: 'NOT_FOUND' });

    const history = await mrktHistorySmart({
      gift: s.filters.gift,
      model: s.filters.model || '',
      backdrop: s.filters.backdrop || '',
      number: s.filters.number,
    });

    const offersRaw = await mrktOrdersFetch({ gift: s.filters.gift, model: s.filters.model || '', backdrop: s.filters.backdrop || '' });

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

  // Admin: update MRKT token
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

// ===================== Workers =====================
setInterval(() => {
  checkSubscriptionsForAllUsers().catch((e) => console.error('subs error:', e));
}, SUBS_CHECK_INTERVAL_MS);

setInterval(() => {
  autoBuyCycle().catch((e) => console.error('autobuy error:', e));
}, AUTO_BUY_CHECK_INTERVAL_MS);

// ===================== Bootstrap =====================
(async () => {
  if (REDIS_URL) {
    await initRedis();
    if (redis) {
      await loadMrktAuthFromRedis();
      await loadState();
    }
  } else {
    console.warn('REDIS_URL not set => subscriptions WILL NOT persist after restart');
  }
  console.log('Bot started. /start');
})();
