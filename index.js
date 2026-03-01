/**
 * MRKT Panel + Bot (single file)
 * v10.4-full (2026-03-01)
 *
 * Fixes:
 * - Serves "/" (no more Cannot GET /)
 * - WebApp + API + Bot in one service
 * - "All gifts" works when Gift is empty (feed listing Price)
 * - AutoBuy works (saling Latest + baseline "only new after ON")
 * - Admin-only debug
 *
 * Node 18+ recommended
 * deps: express, node-telegram-bot-api, (optional) redis
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

// MRKT
const MRKT_API_URL = 'https://api.tgmrkt.io/api/v1';
const MRKT_CDN_BASE = (process.env.MRKT_CDN_BASE || 'https://cdn.tgmrkt.io/').trim();
const FRAGMENT_GIFT_IMG_BASE = (process.env.FRAGMENT_GIFT_IMG_BASE || 'https://nft.fragment.com/gift/').trim();
let MRKT_AUTH_RUNTIME = (process.env.MRKT_AUTH || '').trim() || null;

// intervals
const SUBS_CHECK_INTERVAL_MS = Number(process.env.SUBS_CHECK_INTERVAL_MS || 9000);

// AutoBuy
const AUTO_BUY_GLOBAL = String(process.env.AUTO_BUY_GLOBAL || '1') === '1';
const AUTO_BUY_DRY_RUN = String(process.env.AUTO_BUY_DRY_RUN || '0') === '1';
const AUTO_BUY_CHECK_INTERVAL_MS = Number(process.env.AUTO_BUY_CHECK_INTERVAL_MS || 2000);
const AUTO_BUY_MAX_PER_CHECK = Number(process.env.AUTO_BUY_MAX_PER_CHECK || 1);
const AUTO_BUY_MAX_BUYS_PER_USER_PER_CYCLE = Number(
  process.env.AUTO_BUY_MAX_BUYS_PER_USER_PER_CYCLE || AUTO_BUY_MAX_PER_CHECK || 1
);
const AUTO_BUY_ATTEMPT_TTL_MS = Number(process.env.AUTO_BUY_ATTEMPT_TTL_MS || 30_000);
const AUTO_BUY_NO_FUNDS_PAUSE_MS = Number(process.env.AUTO_BUY_NO_FUNDS_PAUSE_MS || 10 * 60 * 1000);
const AUTO_BUY_DISABLE_AFTER_SUCCESS = String(process.env.AUTO_BUY_DISABLE_AFTER_SUCCESS || '0') === '1';

// requests
const MRKT_TIMEOUT_MS = Number(process.env.MRKT_TIMEOUT_MS || 9000);
const MRKT_COUNT = Number(process.env.MRKT_COUNT || 50);
const MRKT_FEED_COUNT = Number(process.env.MRKT_FEED_COUNT || 60);
const MRKT_THROTTLE_MS = Number(process.env.MRKT_THROTTLE_MS || 120);

// all gifts (feed)
const ALL_GIFTS_FEED_PAGES = Number(process.env.ALL_GIFTS_FEED_PAGES || 10);
const ALL_GIFTS_FEED_PAGES_NUMBER = Number(process.env.ALL_GIFTS_FEED_PAGES_NUMBER || 25);
const ALL_GIFTS_TARGET_UNIQUE = Number(process.env.ALL_GIFTS_TARGET_UNIQUE || 900);

// number mode (gift selected)
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

// redis keys
const REDIS_KEY_MRKT_AUTH = 'mrkt:auth:token';
const REDIS_KEY_STATE = 'bot:state:main';

console.log('v10.4-full start', {
  MODE,
  APP_TITLE,
  WEBAPP_URL: !!WEBAPP_URL,
  ADMIN_USER_ID,
  REDIS: !!REDIS_URL,
  MRKT_AUTH: !!MRKT_AUTH_RUNTIME,
  AUTO_BUY_GLOBAL,
  AUTO_BUY_DRY_RUN,
  AUTO_BUY_DISABLE_AFTER_SUCCESS,
});

// ===================== Telegram bot =====================
const bot = new TelegramBot(token, { polling: true });
const MAIN_KEYBOARD = { keyboard: [[{ text: '📌 Статус' }]], resize_keyboard: true };

// ===================== State =====================
const users = new Map();

// per-sub runtime state
// `${userId}:${subId}` -> { floor, emptyStreak, pausedUntil, baselineReady, seenIds: Set<string> }
const subStates = new Map();

let isSubsChecking = false;
let isAutoBuying = false;

const mrktState = {
  lastOkAt: 0,
  lastFailAt: 0,
  lastFailMsg: null,
};

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

const autoBuyRecentAttempts = new Map(); // `${userId}:${giftId}` -> ts

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

function parseTonInput(x) {
  const s = String(x ?? '').trim();
  if (!s) return NaN;
  const cleaned = s.replace(',', '.').replace(':', '.').replace(/\s+/g, '');
  const v = Number(cleaned);
  return Number.isFinite(v) ? v : NaN;
}

function cleanDigitsPrefix(v, maxLen = 12) {
  return String(v || '').replace(/\D/g, '').slice(0, maxLen);
}

function joinUrl(base, key) {
  const b = String(base || '').endsWith('/') ? String(base) : String(base) + '/';
  const k = String(key || '').startsWith('/') ? String(key).slice(1) : String(key);
  return b + k;
}

function tonFromNano(nano) {
  const x = Number(nano);
  return Number.isFinite(x) ? x / 1e9 : null;
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

// ===================== WebApp auth verify =====================
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
  try {
    user = JSON.parse(params.get('user') || 'null');
  } catch {}
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
  subs.forEach((s, idx) => {
    if (s) s.num = idx + 1;
  });
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
  if (saveTimer) {
    clearTimeout(saveTimer);
    saveTimer = null;
  }
  await saveState();
}

// ===================== User state =====================
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

// ===================== MRKT headers + status =====================
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
  mrktState.lastOkAt = nowMs();
  mrktState.lastFailAt = 0;
  mrktState.lastFailMsg = null;
}
async function markMrktFail(status, bodyText = null) {
  mrktState.lastFailAt = nowMs();
  mrktState.lastFailMsg = extractMrktErrorMessage(bodyText) || (status ? `HTTP ${status}` : 'MRKT error');
}

// ===================== MRKT API =====================
async function mrktGetCollections() {
  const now = nowMs();
  if (collectionsCache.items.length && now - collectionsCache.time < CACHE_TTL_MS) return collectionsCache.items;

  const headers = MRKT_AUTH_RUNTIME ? { ...mrktHeaders(), Accept: 'application/json' } : { Accept: 'application/json' };
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

  const res = await fetchWithTimeout(
    `${MRKT_API_URL}/gifts/models`,
    { method: 'POST', headers: mrktHeaders(), body: JSON.stringify({ collections: [giftName] }) },
    MRKT_TIMEOUT_MS
  ).catch(() => null);

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

  const res = await fetchWithTimeout(
    `${MRKT_API_URL}/gifts/backdrops`,
    { method: 'POST', headers: mrktHeaders(), body: JSON.stringify({ collections: [giftName] }) },
    MRKT_TIMEOUT_MS
  ).catch(() => null);

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
        centerHex: (() => {
          const v = it.colorsCenterColor ?? it.centerColor ?? null;
          const num = Number(v);
          if (!Number.isFinite(num)) return null;
          const hex = (num >>> 0).toString(16).padStart(6, '0');
          return '#' + hex.slice(-6);
        })(),
      });
    }
  }

  const items = Array.from(map.values());
  backdropsCache.set(giftName, { time: nowMs(), items });
  return items;
}

// feed: used only for "All gifts"
async function mrktFeedFetch({ cursor, count, ordering = 'Price', lowToHigh = true }) {
  if (!MRKT_AUTH_RUNTIME) return { ok: false, reason: 'MRKT_AUTH is missing', items: [], cursor: '' };

  const body = {
    req: String(Date.now()),
    count: Number(count || MRKT_FEED_COUNT),
    cursor: cursor || '',
    collectionNames: [],
    modelNames: [],
    backdropNames: [],
    lowToHigh: !!lowToHigh,
    maxPrice: null,
    minPrice: null,
    number: null,
    ordering: ordering || 'Price',
    query: null,
    type: ['listing'],
    types: ['listing'],
  };

  const res = await fetchWithTimeout(`${MRKT_API_URL}/feed`, {
    method: 'POST',
    headers: mrktHeaders(),
    body: JSON.stringify(body),
  }, MRKT_TIMEOUT_MS).catch(() => null);

  if (!res) return { ok: false, reason: 'Feed fetch error', items: [], cursor: '' };
  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    await markMrktFail(res.status, txt);
    return { ok: false, reason: mrktState.lastFailMsg || 'Feed error', items: [], cursor: '' };
  }

  markMrktOk();
  const data = await res.json().catch(() => null);
  return { ok: true, reason: 'OK', items: Array.isArray(data?.items) ? data.items : [], cursor: data?.cursor || '' };
}

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
    method: 'POST',
    headers: mrktHeaders(),
    body: JSON.stringify(body),
  }, MRKT_TIMEOUT_MS).catch(() => null);

  if (!res) return { ok: false, reason: 'Saling fetch error', gifts: [], cursor: '' };
  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    await markMrktFail(res.status, txt);
    return { ok: false, reason: mrktState.lastFailMsg || 'Saling error', gifts: [], cursor: '' };
  }

  markMrktOk();
  const data = await res.json().catch(() => null);
  return { ok: true, reason: 'OK', gifts: Array.isArray(data?.gifts) ? data.gifts : [], cursor: data?.cursor || '' };
}

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
    method: 'POST',
    headers: mrktHeaders(),
    body: JSON.stringify(body),
  }, MRKT_TIMEOUT_MS).catch(() => null);

  if (!res) return { ok: false, reason: 'Orders fetch error', orders: [] };

  const txt = await res.text().catch(() => '');
  let data = null;
  try { data = txt ? JSON.parse(txt) : null; } catch { data = null; }

  if (!res.ok) {
    await markMrktFail(res.status, txt);
    const out = { ok: false, reason: mrktState.lastFailMsg || 'Orders error', orders: [] };
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

async function mrktBuy({ id, priceNano }) {
  if (!MRKT_AUTH_RUNTIME) return { ok: false, reason: 'MRKT_AUTH is missing' };

  const body = { ids: [id], prices: { [id]: priceNano } };

  const res = await fetchWithTimeout(`${MRKT_API_URL}/gifts/buy`, {
    method: 'POST',
    headers: mrktHeaders(),
    body: JSON.stringify(body),
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
  return (
    s.includes('not enough') ||
    s.includes('insufficient') ||
    s.includes('no funds') ||
    s.includes('low balance') ||
    s.includes('balance')
  );
}

// ===================== Mapping lots =====================
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
    collectionName: g.collectionName || g.collectionTitle || g.title || collectionFallback || null,
  };
}

function feedItemToLot(it) {
  const g = it?.gift;
  if (!g?.id) return null;

  // price: prefer gift.salePrice*
  const priceNanoRaw = g.salePriceWithoutFee ?? g.salePrice ?? it.amount ?? null;
  const priceNano = priceNanoRaw == null ? NaN : Number(priceNanoRaw);
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
    collectionName: g.collectionName || g.collectionTitle || g.title || null,
  };
}

// ===================== Search lots =====================
async function mrktSearchLotsByFilters({ gift, model, backdrop, numberPrefix }, pagesLimit) {
  const prefix = cleanDigitsPrefix(numberPrefix || '');

  // no gift => all gifts via feed
  if (!gift) {
    let cursor = '';
    const byId = new Map();

    const maxPages = prefix ? Math.max(ALL_GIFTS_FEED_PAGES_NUMBER, pagesLimit || 1) : Math.max(ALL_GIFTS_FEED_PAGES, pagesLimit || 1);
    const ordering = prefix ? 'Latest' : 'Price';
    const lowToHigh = prefix ? false : true;

    for (let page = 0; page < maxPages; page++) {
      const r = await mrktFeedFetch({ cursor, count: MRKT_FEED_COUNT, ordering, lowToHigh });
      if (!r.ok) return { ok: false, reason: r.reason, gifts: [] };
      if (!r.items.length) break;

      for (const it of r.items) {
        const lot = feedItemToLot(it);
        if (!lot) continue;

        const numStr = lot.number == null ? '' : String(lot.number);
        if (prefix && !numStr.startsWith(prefix)) continue;

        if (model && lot.model && !sameTrait(lot.model, norm(model))) continue;
        if (backdrop && lot.backdrop && !sameTrait(lot.backdrop, norm(backdrop))) continue;

        if (!byId.has(String(lot.id))) byId.set(String(lot.id), lot);
        if (byId.size >= ALL_GIFTS_TARGET_UNIQUE) break;
      }

      cursor = r.cursor || '';
      if (!cursor || byId.size >= ALL_GIFTS_TARGET_UNIQUE) break;
      await sleep(MRKT_THROTTLE_MS);
    }

    const out = Array.from(byId.values());
    out.sort((a, b) => a.priceTon - b.priceTon);
    return { ok: true, reason: 'OK', gifts: out };
  }

  // gift selected => saling
  const NUMBER_MODE = !!prefix;
  const ordering = NUMBER_MODE ? 'Latest' : 'Price';
  const lowToHigh = NUMBER_MODE ? false : true;
  const pagesToScan = NUMBER_MODE ? Math.max(MRKT_PAGES_NUMBER, pagesLimit || 1) : (pagesLimit || 1);

  let cursor = '';
  const out = [];

  for (let page = 0; page < pagesToScan; page++) {
    const r = await mrktFetchSalingPage({
      collectionName: gift,
      modelName: model || null,
      backdropName: backdrop || null,
      cursor,
      ordering,
      lowToHigh,
    });

    if (!r.ok) return { ok: false, reason: r.reason, gifts: [] };

    for (const g of r.gifts) {
      const lot = salingGiftToLot(g, gift);
      if (!lot) continue;

      const numStr = lot.number == null ? '' : String(lot.number);
      if (prefix && !numStr.startsWith(prefix)) continue;

      out.push(lot);
      if (!NUMBER_MODE && out.length >= 500) break;
    }

    cursor = r.cursor || '';
    if (!cursor) break;
    if (!NUMBER_MODE && out.length >= 500) break;
    await sleep(MRKT_THROTTLE_MS);
  }

  out.sort((a, b) => a.priceTon - b.priceTon);
  return { ok: true, reason: 'OK', gifts: out };
}

// ===================== History sales =====================
async function mrktHistorySales({ gift, model, backdrop, numberPrefix }) {
  const prefix = cleanDigitsPrefix(numberPrefix || '');
  const key = `hist|${gift || ''}|${model || ''}|${backdrop || ''}|${prefix}`;

  const now = nowMs();
  const cached = historyCache.get(key);
  if (cached && now - cached.time < HISTORY_CACHE_TTL_MS) return cached.data;

  // feed endpoint depends on token; if it fails we still return empty
  const started = nowMs();
  let cursor = '';
  let pages = 0;
  const prices = [];
  const lastSales = [];

  // We use /feed with type=sale filtered by gift/model/backdrop when gift selected.
  // If gift empty -> history may be heavy; we just return empty.
  if (!gift) {
    const data = { ok: true, median: null, count: 0, lastSales: [] };
    historyCache.set(key, { time: now, data });
    return data;
  }

  while (pages < HISTORY_MAX_PAGES && prices.length < HISTORY_TARGET_SALES) {
    if (nowMs() - started > HISTORY_TIME_BUDGET_MS) break;

    const body = {
      req: String(Date.now()),
      count: Number(HISTORY_COUNT_PER_PAGE),
      cursor: cursor || '',
      collectionNames: [gift],
      modelNames: model ? [model] : [],
      backdropNames: backdrop ? [backdrop] : [],
      lowToHigh: false,
      maxPrice: null,
      minPrice: null,
      number: null,
      ordering: 'Latest',
      query: null,
      type: ['sale'],
      types: ['sale'],
    };

    const res = await fetchWithTimeout(`${MRKT_API_URL}/feed`, {
      method: 'POST',
      headers: mrktHeaders(),
      body: JSON.stringify(body),
    }, MRKT_TIMEOUT_MS).catch(() => null);

    if (!res || !res.ok) break;

    const data = await res.json().catch(() => null);
    const items = Array.isArray(data?.items) ? data.items : [];
    if (!items.length) break;

    for (const it of items) {
      const g = it?.gift;
      if (!g) continue;

      const num = g.number ?? null;
      const numStr = num == null ? '' : String(num);
      if (prefix && !numStr.startsWith(prefix)) continue;

      const amountNano = it.amount ?? g.salePrice ?? g.salePriceWithoutFee ?? null;
      const ton = amountNano != null ? Number(amountNano) / 1e9 : NaN;
      if (!Number.isFinite(ton) || ton <= 0) continue;

      prices.push(ton);

      if (lastSales.length < 35) {
        const titleBase = g.collectionTitle || g.collectionName || g.title || gift;
        const giftName = g.name || giftNameFallbackFromCollectionAndNumber(titleBase, g.number) || null;

        lastSales.push({
          tsMsk: it.date ? formatMsk(it.date) : '',
          priceTon: ton,
          number: g.number ?? null,
          model: g.modelTitle || g.modelName || null,
          backdrop: g.backdropName || null,
          imgUrl: giftName ? `/img/gift?name=${encodeURIComponent(giftName)}` : null,
          urlTelegram: giftName && String(giftName).includes('-') ? `https://t.me/nft/${giftName}` : null,
          urlMarket: g.id ? mrktLotUrlFromId(g.id) : null,
        });
      }

      if (prices.length >= HISTORY_TARGET_SALES) break;
    }

    cursor = data?.cursor || '';
    pages++;
    if (!cursor) break;
    await sleep(MRKT_THROTTLE_MS);
  }

  prices.sort((a, b) => a - b);

  const out = { ok: true, median: median(prices), count: prices.length, lastSales };
  historyCache.set(key, { time: now, data: out });
  return out;
}

// ===================== Subs thumbs =====================
async function getSubThumb({ gift, model, backdrop }) {
  const key = `thumb|${gift || ''}|${model || ''}|${backdrop || ''}`;
  const now = nowMs();
  const cached = thumbsCache.get(key);
  if (cached && now - cached.time < THUMBS_CACHE_TTL_MS) return cached;

  let thumbUrl = null;
  let backdropHex = null;

  if (gift && backdrop) {
    const backs = await mrktGetBackdropsForGift(gift);
    const b = backs.find((x) => norm(x.name) === norm(backdrop));
    backdropHex = b?.centerHex || null;
  }

  if (gift && model) {
    const models = await mrktGetModelsForGift(gift);
    const m = models.find((x) => norm(x.name) === norm(model));
    if (m?.thumbKey) thumbUrl = `/img/cdn?key=${encodeURIComponent(m.thumbKey)}`;
  }

  if (!thumbUrl && gift) {
    const cols = await mrktGetCollections();
    const c = cols.find((x) => x.name === gift);
    if (c?.thumbKey) thumbUrl = `/img/cdn?key=${encodeURIComponent(c.thumbKey)}`;
  }

  const out = { time: now, thumbUrl, backdropHex };
  thumbsCache.set(key, out);
  return out;
}

// ===================== Sub notify worker =====================
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
        const prev = subStates.get(stateKey) || {
          floor: null,
          emptyStreak: 0,
          pausedUntil: 0,
          baselineReady: false,
          seenIds: new Set(),
        };

        // floor from saling Price
        if (!sub.filters.gift) continue;

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

// ===================== AutoBuy worker (saling Latest + baseline) =====================
async function autobuyBaselineForSub(userId, sub) {
  const stateKey = `${userId}:${sub.id}`;
  const prev = subStates.get(stateKey) || {
    floor: null,
    emptyStreak: 0,
    pausedUntil: 0,
    baselineReady: false,
    seenIds: new Set(),
  };

  // baseline: take latest page and mark ids as seen
  const r = await mrktFetchSalingPage({
    collectionName: sub.filters.gift,
    modelName: sub.filters.model || null,
    backdropName: sub.filters.backdrop || null,
    cursor: '',
    ordering: 'Latest',
    lowToHigh: false,
  });

  const seen = prev.seenIds instanceof Set ? prev.seenIds : new Set();
  if (r.ok) {
    for (const g of r.gifts || []) {
      if (g?.id) seen.add(String(g.id));
    }
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
      const eligible = subs.filter(
        (s) => s && s.enabled && s.autoBuyEnabled && s.maxAutoBuyTon != null && s.filters.gift
      );

      autoBuyDebug.eligible += eligible.length;
      if (!eligible.length) continue;

      let buysDone = 0;

      for (const sub of eligible) {
        if (buysDone >= AUTO_BUY_MAX_BUYS_PER_USER_PER_CYCLE) break;

        const maxBuy = Number(sub.maxAutoBuyTon);
        if (!Number.isFinite(maxBuy) || maxBuy <= 0) {
          autoBuyDebug.lastReason = 'bad maxAutoBuyTon';
          continue;
        }

        const stateKey = `${userId}:${sub.id}`;
        const st = subStates.get(stateKey) || {
          floor: null,
          emptyStreak: 0,
          pausedUntil: 0,
          baselineReady: false,
          seenIds: new Set(),
        };

        if (st.pausedUntil && nowMs() < st.pausedUntil) continue;

        // baseline first
        if (!st.baselineReady) {
          await autobuyBaselineForSub(userId, sub);
          autoBuyDebug.lastReason = 'baseline set';
          continue;
        }

        // fetch latest lots
        const r = await mrktFetchSalingPage({
          collectionName: sub.filters.gift,
          modelName: sub.filters.model || null,
          backdropName: sub.filters.backdrop || null,
          cursor: '',
          ordering: 'Latest',
          lowToHigh: false,
        });

        if (!r.ok) {
          autoBuyDebug.lastReason = `saling error: ${r.reason}`;
          continue;
        }

        const gifts = r.gifts || [];
        autoBuyDebug.scanned += gifts.length;

        const seen = st.seenIds instanceof Set ? st.seenIds : new Set();
        const prefix = cleanDigitsPrefix(sub.filters.numberPrefix || '');

        for (const g of gifts) {
          if (!g?.id) continue;
          const id = String(g.id);

          if (seen.has(id)) continue;
          seen.add(id);
          autoBuyDebug.newIds++;

          const lot = salingGiftToLot(g, sub.filters.gift);
          if (!lot) continue;

          if (prefix) {
            const numStr = lot.number == null ? '' : String(lot.number);
            if (!numStr.startsWith(prefix)) continue;
          }

          if (lot.priceTon > maxBuy) continue;

          const attemptKey = `${userId}:${lot.id}`;
          const lastAttempt = autoBuyRecentAttempts.get(attemptKey);
          if (lastAttempt && nowMs() - lastAttempt < AUTO_BUY_ATTEMPT_TTL_MS) continue;
          autoBuyRecentAttempts.set(attemptKey, nowMs());

          autoBuyDebug.candidates++;

          if (AUTO_BUY_DRY_RUN) {
            await sendMessageSafe(
              userId,
              `🤖 AutoBuy (DRY)\nПодписка #${sub.num}\n${lot.name}\nЦена: ${lot.priceTon.toFixed(3)} TON\n${lot.urlTelegram}`,
              { disable_web_page_preview: true }
            );
            autoBuyDebug.buys++;
            buysDone++;
            break;
          }

          const buyRes = await mrktBuy({ id: lot.id, priceNano: lot.priceNano });

          if (buyRes.ok) {
            const u = getOrCreateUser(userId);
            pushPurchase(u, {
              ts: Date.now(),
              title: lot.name,
              priceTon: lot.priceTon,
              urlTelegram: lot.urlTelegram,
              urlMarket: lot.urlMarket,
            });
            scheduleSave();

            await sendMessageSafe(
              userId,
              `✅ AutoBuy\nПодписка #${sub.num}\nКуплено: ${lot.name}\nЦена: ${lot.priceTon.toFixed(3)} TON\n${lot.urlTelegram}`,
              { disable_web_page_preview: true }
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
              autoBuyDebug.lastReason = 'no funds';
              break;
            } else {
              autoBuyDebug.lastReason = `buy error: ${buyRes.reason || 'err'}`;
            }
          }
        }

        subStates.set(stateKey, { ...st, seenIds: seen, baselineReady: true });

        if (MRKT_THROTTLE_MS > 0) await sleep(MRKT_THROTTLE_MS);
      }
    }

    if (!autoBuyDebug.buys && !autoBuyDebug.candidates) {
      autoBuyDebug.lastReason = autoBuyDebug.lastReason || 'no candidates';
    }
  } catch (e) {
    autoBuyDebug.lastReason = `crash: ${String(e?.message || e).slice(0, 160)}`;
    console.error('autoBuyCycle error:', e);
  } finally {
    isAutoBuying = false;
  }
}

// ===================== Telegram commands =====================
bot.onText(/^\/start\b/, async (msg) => {
  getOrCreateUser(msg.from.id);

  // menu button (optional)
  if (WEBAPP_URL) {
    try {
      await fetch(`https://api.telegram.org/bot${token}/setChatMenuButton`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: msg.chat.id,
          menu_button: { type: 'web_app', text: APP_TITLE, web_app: { url: WEBAPP_URL } },
        }),
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
      `• AutoBuy: ${AUTO_BUY_GLOBAL ? 'ON' : 'OFF'}${AUTO_BUY_DRY_RUN ? ' (DRY)' : ''}\n`;
    await sendMessageSafe(msg.chat.id, txt, { reply_markup: MAIN_KEYBOARD });
  }
});

// ===================== WebApp UI (HTML/JS) =====================
const WEBAPP_HTML = `<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"/>
<title>${APP_TITLE}</title>
<style>
:root{
  --bg:#0b0f14; --card:#101826; --text:#e5e7eb; --muted:#9ca3af; --border:#223044;
  --input:#0f172a; --btn:#182235; --btnText:#e5e7eb; --accent:#22c55e; --danger:#ef4444;
}
*{box-sizing:border-box}
body{margin:0;padding:14px;background:var(--bg);color:var(--text);font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial}
h2{margin:0 0 10px 0;font-size:18px}
.card{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:12px;margin:10px 0}
label{display:block;font-size:12px;color:var(--muted);margin-bottom:4px}
.row{display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end}
.field{position:relative;flex:1 1 200px;min-width:140px}
.inpWrap{position:relative}
input{width:100%;padding:10px;border:1px solid var(--border);border-radius:12px;background:var(--input);color:var(--text);outline:none;font-size:13px}
button{padding:10px 12px;border:1px solid var(--border);border-radius:12px;background:var(--btn);color:var(--btnText);cursor:pointer}
.primary{border-color:var(--accent);color:#052e16;background:var(--accent);font-weight:950}
.small{padding:8px 10px;border-radius:10px;font-size:13px}
.xbtn{position:absolute;right:8px;top:50%;transform:translateY(-50%);width:20px;height:20px;border-radius:8px;border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.05);color:rgba(255,255,255,.55);cursor:pointer;display:flex;align-items:center;justify-content:center}
.badge{padding:4px 8px;border-radius:999px;border:1px solid var(--border);color:var(--muted);font-size:12px}
.hr{height:1px;background:var(--border);margin:10px 0}
.muted{color:var(--muted);font-size:12px}
#err{display:none;border-color:var(--danger);color:#ffd1d1;white-space:pre-wrap;word-break:break-word}

.tabs{display:flex;gap:8px;flex-wrap:wrap}
.tabbtn{border-radius:999px;padding:8px 12px;font-size:13px}
.tabbtn.active{border-color:var(--accent);color:var(--accent)}

.sug{border:1px solid var(--border);border-radius:14px;overflow:auto;background:var(--card);max-height:360px;position:absolute;top:calc(100% + 6px);left:0;right:0;z-index:9999;box-shadow:0 14px 40px rgba(0,0,0,.45)}
.sug .item{width:100%;text-align:left;border:0;background:transparent;padding:10px;display:flex;gap:10px;align-items:center}
.sug .item:hover{background:rgba(255,255,255,.06)}
.thumb{width:44px;height:44px;border-radius:14px;object-fit:contain;background:rgba(255,255,255,.06);border:1px solid var(--border);flex:0 0 auto}
.dot{width:14px;height:14px;border-radius:999px;border:1px solid var(--border);display:inline-block}

.grid{display:grid;gap:10px;margin-top:10px;grid-template-columns: repeat(auto-fill, minmax(170px, 1fr))}
@media (max-width: 520px){.grid{grid-template-columns: repeat(2, minmax(0, 1fr));}}
.lot{border:1px solid var(--border);border-radius:16px;padding:10px;background:rgba(255,255,255,.02);cursor:pointer}
.lot img{width:100%;aspect-ratio:1/1;object-fit:cover;border-radius:14px;border:1px solid var(--border)}
.price{font-size:15px;font-weight:950;margin-top:8px}
.ellipsis{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}

/* bottom sheet */
.sheetWrap{position:fixed;inset:0;background:rgba(0,0,0,.55);display:flex;align-items:flex-end;justify-content:center;z-index:50000;padding:12px;opacity:0;visibility:hidden;pointer-events:none;transition:opacity .16s ease,visibility .16s ease}
.sheetWrap.show{opacity:1;visibility:visible;pointer-events:auto}
.sheet{width:min(980px,96vw);height:min(78vh,820px);background:var(--card);border:1px solid var(--border);border-radius:22px 22px 14px 14px;padding:12px;box-shadow:0 30px 90px rgba(0,0,0,.5);display:flex;flex-direction:column;gap:10px}
.handle{width:44px;height:5px;border-radius:999px;background:rgba(255,255,255,.18);align-self:center}
.sheetHeader{display:flex;justify-content:space-between;gap:12px;align-items:flex-start}
.sheetBody{overflow:auto}

.saleRow{display:flex;gap:10px;align-items:stretch;margin-top:10px}
.saleImg{width:78px;height:78px;border-radius:14px;border:1px solid var(--border);object-fit:cover;background:rgba(255,255,255,.03);flex:0 0 auto}
.saleCard{flex:1;border:1px solid var(--border);border-radius:14px;padding:10px;background:rgba(255,255,255,.02)}
.saleBtns{display:flex;gap:8px;margin-top:8px;flex-wrap:wrap}
</style>
</head>
<body>
<h2>${APP_TITLE}</h2>
<div id="err" class="card"></div>

<div class="tabs">
  <button class="tabbtn active" data-tab="market">Market</button>
  <button class="tabbtn" data-tab="subs">Subscriptions</button>
  <button class="tabbtn" data-tab="profile">Profile</button>
  <button class="tabbtn" data-tab="admin" id="adminTabBtn" style="display:none">Admin</button>
</div>

<div id="market" class="card">
  <h3 style="margin:0 0 8px 0;font-size:15px">Фильтры</h3>
  <div class="row">
    <div class="field">
      <label>Gift</label>
      <div class="inpWrap">
        <input id="gift" placeholder="Gift (можно пусто)" autocomplete="off"/>
        <button class="xbtn" data-clear="gift" type="button">×</button>
      </div>
      <div id="giftSug" class="sug" style="display:none"></div>
    </div>
    <div class="field">
      <label>Model</label>
      <div class="inpWrap">
        <input id="model" placeholder="Model" autocomplete="off"/>
        <button class="xbtn" data-clear="model" type="button">×</button>
      </div>
      <div id="modelSug" class="sug" style="display:none"></div>
    </div>
    <div class="field">
      <label>Backdrop</label>
      <div class="inpWrap">
        <input id="backdrop" placeholder="Backdrop" autocomplete="off"/>
        <button class="xbtn" data-clear="backdrop" type="button">×</button>
      </div>
      <div id="backdropSug" class="sug" style="display:none"></div>
    </div>
    <div class="field" style="max-width:160px">
      <label>Number</label>
      <div class="inpWrap">
        <input id="number" placeholder="№" inputmode="numeric"/>
        <button class="xbtn" data-clear="number" type="button">×</button>
      </div>
    </div>
  </div>

  <div class="row" style="margin-top:10px">
    <button id="apply" class="primary">Показать</button>
    <button id="refresh">Обновить</button>
    <button id="historyBtn">История продаж</button>
    <button id="clearAll">Очистить</button>
  </div>

  <div id="status" class="muted" style="margin-top:10px"></div>
  <div class="hr"></div>
  <div><b>Лоты</b> <span class="muted">(клик → детали)</span></div>
  <div id="lots" class="grid"></div>
</div>

<div id="subs" class="card" style="display:none">
  <h3 style="margin:0 0 8px 0;font-size:15px">Подписки</h3>
  <div class="row">
    <button id="subCreate">Создать из текущих фильтров</button>
    <button id="subRefresh">Обновить</button>
  </div>
  <div id="subsList" style="margin-top:10px"></div>
</div>

<div id="profile" class="card" style="display:none">
  <h3 style="margin:0 0 8px 0;font-size:15px">Профиль</h3>
  <div id="profileBox" class="muted">Загрузка...</div>
  <div class="hr"></div>
  <div><b>История покупок</b></div>
  <div id="purchases" style="margin-top:10px;display:flex;flex-direction:column;gap:10px"></div>
</div>

<div id="admin" class="card" style="display:none">
  <h3 style="margin:0 0 8px 0;font-size:15px">Admin</h3>
  <div id="adminStatus" class="muted">Загрузка...</div>
  <div class="hr"></div>
  <div class="muted">MRKT токен хранится в Redis.</div>
  <div class="row" style="margin-top:10px">
    <div class="field"><label>Текущий токен (маска)</label><input id="tokMask" disabled/></div>
    <div class="field"><label>Новый MRKT_AUTH</label><input id="tokNew" placeholder="вставь токен"/></div>
    <button id="tokSave">Сохранить токен</button>
  </div>
</div>

<!-- Bottom Sheet -->
<div id="sheetWrap" class="sheetWrap">
  <div class="sheet">
    <div class="handle"></div>
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

<script src="https://telegram.org/js/telegram-web-app.js"></script>
<script src="/app.js"></script>
</body>
</html>`;

const WEBAPP_JS = `(() => {
  const tg = window.Telegram?.WebApp;
  try { tg?.ready(); tg?.expand?.(); } catch {}

  const initData = tg?.initData || '';
  const el = (id) => document.getElementById(id);

  if (!initData) {
    document.body.innerHTML = '<div style="padding:16px;font-family:system-ui;color:#fff;background:#0b0f14"><h3>Открой панель из Telegram</h3></div>';
    return;
  }

  function showErr(msg){ const box=el('err'); box.style.display='block'; box.textContent=String(msg||''); }
  function hideErr(){ const box=el('err'); box.style.display='none'; box.textContent=''; }

  async function api(path, opts = {}) {
    const res = await fetch(path, { ...opts, headers: { 'Content-Type':'application/json', 'X-Tg-Init-Data': initData, ...(opts.headers||{}) }});
    const txt = await res.text();
    let data=null;
    try{ data = txt?JSON.parse(txt):null; }catch{ data={raw:txt}; }
    if(!res.ok) throw new Error((data && data.reason) ? String(data.reason) : ('HTTP '+res.status));
    return data;
  }

  function setTab(name){
    ['market','subs','profile','admin'].forEach(x => el(x).style.display = (x===name?'block':'none'));
    document.querySelectorAll('.tabbtn').forEach(b => b.classList.toggle('active', b.dataset.tab===name));
  }
  document.querySelectorAll('.tabbtn').forEach(b => b.onclick = () => setTab(b.dataset.tab));

  function safe(fn){ return async () => { hideErr(); try { await fn(); } catch(e){ showErr(e.message || String(e)); } }; }

  function hideSug(id){ const b=el(id); b.style.display='none'; b.innerHTML=''; }
  function renderSug(id, items, onPick){
    const b=el(id);
    if(!items||!items.length){ hideSug(id); return; }
    b.innerHTML = items.map(x => {
      const thumb = x.imgUrl ? '<img class="thumb" src="'+x.imgUrl+'" referrerpolicy="no-referrer"/>' : '<div class="thumb"></div>';
      return '<button type="button" class="item" data-v="'+x.value.replace(/"/g,'&quot;')+'">'+thumb+
        '<div style="min-width:0;flex:1">' +
          '<div class="ellipsis"><b>'+x.label+'</b></div>' +
          (x.sub?'<div class="muted ellipsis">'+x.sub+'</div>':'')+
        '</div></button>';
    }).join('');
    b.style.display='block';
    b.onclick = (e) => { const btn = e.target.closest('button[data-v]'); if(!btn) return; onPick(btn.getAttribute('data-v')); hideSug(id); };
  }

  let selectedGiftValue = '';

  async function showGiftSug(){
    const q = el('gift').value.trim();
    const r = await api('/api/mrkt/collections?q='+encodeURIComponent(q));
    renderSug('giftSug', r.items||[], (v) => { selectedGiftValue=v; el('gift').value=(r.mapLabel||{})[v]||v; el('model').value=''; el('backdrop').value=''; });
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
    const b=el('backdropSug');
    if(!r.items||!r.items.length){ hideSug('backdropSug'); return; }
    b.innerHTML = r.items.map(x => {
      const dot = x.colorHex ? '<span class="dot" style="background:'+x.colorHex+'"></span>' : '<span class="dot"></span>';
      return '<button type="button" class="item" data-v="'+x.value.replace(/"/g,'&quot;')+'">'+
        '<div class="thumb" style="display:flex;align-items:center;justify-content:center">'+dot+'</div>'+
        '<div style="min-width:0;flex:1">' +
          '<div class="ellipsis"><b>'+x.label+'</b></div>' +
          (x.badge?'<div class="muted">'+x.badge+'</div>':'')+
        '</div></button>';
    }).join('');
    b.style.display='block';
    b.onclick = (e) => { const btn = e.target.closest('button[data-v]'); if(!btn) return; el('backdrop').value=btn.getAttribute('data-v'); hideSug('backdropSug'); };
  }

  el('gift').addEventListener('focus', safe(showGiftSug));
  el('gift').addEventListener('input', safe(() => { selectedGiftValue=''; return showGiftSug(); }));
  el('model').addEventListener('focus', safe(showModelSug));
  el('model').addEventListener('input', safe(showModelSug));
  el('backdrop').addEventListener('focus', safe(showBackdropSug));
  el('backdrop').addEventListener('input', safe(showBackdropSug));

  document.addEventListener('click', (e) => {
    if(!e.target.closest('#gift,#model,#backdrop,.sug')){ hideSug('giftSug'); hideSug('modelSug'); hideSug('backdropSug'); }
  });

  async function patchFilters(){
    await api('/api/state/patch',{method:'POST',body:JSON.stringify({filters:{
      gift:selectedGiftValue,
      giftLabel:el('gift').value.trim(),
      model:el('model').value.trim(),
      backdrop:el('backdrop').value.trim(),
      numberPrefix:el('number').value.trim()
    }})});
  }

  document.body.addEventListener('click', (e) => {
    const b = e.target.closest('button[data-clear]');
    if(!b) return;
    const what = b.getAttribute('data-clear');
    (async()=>{
      if(what==='gift'){ selectedGiftValue=''; el('gift').value=''; el('model').value=''; el('backdrop').value=''; el('number').value=''; }
      if(what==='model'){ el('model').value=''; }
      if(what==='backdrop'){ el('backdrop').value=''; }
      if(what==='number'){ el('number').value=''; }
      await patchFilters();
      await refreshState(); await refreshMarketData();
    })().catch(err=>showErr(err.message||String(err)));
  });

  el('clearAll').onclick = safe(async()=>{
    selectedGiftValue=''; el('gift').value=''; el('model').value=''; el('backdrop').value=''; el('number').value='';
    await patchFilters(); await refreshState(); await refreshMarketData();
  });

  // sheet
  const sheetWrap = el('sheetWrap');
  const sheetTitle = el('sheetTitle');
  const sheetSub = el('sheetSub');
  const sheetTop = el('sheetTop');
  const sheetBody = el('sheetBody');
  el('sheetClose').onclick = ()=>sheetWrap.classList.remove('show');
  sheetWrap.addEventListener('click',(e)=>{ if(e.target===sheetWrap) sheetWrap.classList.remove('show'); });

  function openSheet(title, sub){
    sheetTitle.textContent=title||'';
    sheetSub.textContent=sub||'';
    sheetTop.textContent='';
    sheetBody.innerHTML='';
    sheetWrap.classList.add('show');
  }

  function renderHistory(list){
    if(!list||!list.length){ sheetBody.innerHTML='<i class="muted">Нет данных</i>'; return; }
    sheetBody.innerHTML = list.map(x=>{
      const img = x.imgUrl ? '<img class="saleImg" src="'+x.imgUrl+'" referrerpolicy="no-referrer" loading="lazy"/>' : '<div class="saleImg"></div>';
      const btns = '<div class="saleBtns">' +
        (x.urlMarket?'<button class="small" data-open="'+x.urlMarket+'">MRKT</button>':'') +
        (x.urlTelegram?'<button class="small" data-open="'+x.urlTelegram+'">NFT</button>':'') +
      '</div>';
      const n = (x.number!=null)?('<span class="badge">#'+x.number+'</span>'):'';
      return '<div class="saleRow">'+img+
        '<div class="saleCard">'+
          '<div style="display:flex;justify-content:space-between;gap:10px;align-items:center"><b>'+x.priceTon.toFixed(3)+' TON</b><span class="muted">'+(x.tsMsk||'')+'</span></div>'+
          '<div style="margin-top:4px;display:flex;justify-content:space-between;gap:10px;align-items:center"><div class="muted ellipsis">'+(x.model?('Model: '+x.model):'')+'</div>'+n+'</div>'+
          (x.backdrop?'<div class="muted ellipsis">Backdrop: '+x.backdrop+'</div>':'')+
          btns+
        '</div></div>';
    }).join('');
  }

  document.body.addEventListener('click',(e)=>{
    const b=e.target.closest('button[data-open]');
    if(!b) return;
    const url=b.getAttribute('data-open');
    if(url) tg?.openTelegramLink ? tg.openTelegramLink(url) : window.open(url,'_blank');
  });

  function renderLots(resp){
    const box=el('lots');
    if(resp.ok===false){ box.innerHTML='<div style="color:#ef4444"><b>'+resp.reason+'</b></div>'; return; }
    const lots=resp.lots||[];
    if(!lots.length){ box.innerHTML='<i class="muted">Лотов не найдено</i>'; return; }
    const map=new Map(lots.map(l=>[String(l.id),l]));
    box.innerHTML = lots.map(x=>{
      const img = x.imgUrl ? '<img src="'+x.imgUrl+'" referrerpolicy="no-referrer" loading="lazy"/>' : '<div style="aspect-ratio:1/1;border:1px solid rgba(255,255,255,.10);border-radius:14px"></div>';
      const num = (x.number!=null)?('<span class="badge">#'+x.number+'</span>'):'';
      return '<div class="lot" data-id="'+x.id+'">'+img+
        '<div class="price">'+x.priceTon.toFixed(3)+' TON</div>'+
        '<div style="display:flex;justify-content:space-between;gap:10px;align-items:center"><b class="ellipsis">'+x.name+'</b>'+num+'</div>'+
        (x.model?'<div class="muted ellipsis">Model: '+x.model+'</div>':'')+
        (x.backdrop?'<div class="muted ellipsis" style="margin-top:6px">Backdrop: '+x.backdrop+'</div>':'')+
      '</div>';
    }).join('');

    box.querySelectorAll('.lot').forEach(n=>{
      n.onclick = async()=>{
        const lot = map.get(String(n.getAttribute('data-id')));
        if(!lot) return;
        const ok = await api('/api/lot/details',{method:'POST',body:JSON.stringify({id:lot.id})});

        openSheet('Детали лота', lot.name);

        sheetTop.innerHTML =
          '<b>Цена:</b> '+lot.priceTon.toFixed(3)+' TON<br>' +
          (lot.model?('<b>Model:</b> '+lot.model+'<br>'):'') +
          (lot.backdrop?('<b>Backdrop:</b> '+lot.backdrop+'<br>'):'') +
          '<b>Floor exact:</b> '+(ok.floors?.exact?.priceTon!=null?ok.floors.exact.priceTon.toFixed(3)+' TON':'—')+'<br>'+
          '<b>Floor collection:</b> '+(ok.floors?.collection?.priceTon!=null?ok.floors.collection.priceTon.toFixed(3)+' TON':'—')+'<br>'+
          '<b>Max offer exact:</b> '+(ok.offers?.exact?.maxOfferTon!=null?ok.offers.exact.maxOfferTon.toFixed(3)+' TON':'—')+'<br>'+
          '<b>Max offer collection:</b> '+(ok.offers?.collection?.maxOfferTon!=null?ok.offers.collection.maxOfferTon.toFixed(3)+' TON':'—')+'<br>'+
          '<div style="margin-top:8px"><button class="small" data-open="'+lot.urlMarket+'">MRKT</button> '+
          '<button class="small" data-open="'+lot.urlTelegram+'">NFT</button> '+
          '<button id="buyBtn" class="small" style="border-color: var(--accent); background: var(--accent); color:#052e16; font-weight:900">Buy</button></div>';

        document.getElementById('buyBtn').onclick = async()=>{
          const ok2 = window.confirm('Купить?\\n'+lot.name+'\\nЦена: '+lot.priceTon.toFixed(3)+' TON');
          if(!ok2) return;
          const r = await api('/api/mrkt/buy',{method:'POST',body:JSON.stringify({id:lot.id,priceNano:lot.priceNano})});
          alert('Куплено: '+r.title+' за '+r.priceTon.toFixed(3)+' TON');
          sheetWrap.classList.remove('show');
        };

        sheetBody.innerHTML = '<div class="muted" style="margin-top:10px"><b>История продаж (медиана):</b> '+(ok.history?.median!=null?ok.history.median.toFixed(3)+' TON':'нет')+' (n='+(ok.history?.count||0)+')</div>';
        renderHistory(ok.history?.lastSales || []);
      };
    });
  }

  async function refreshProfile(){
    const r = await api('/api/profile');
    const u=r.user||{};
    el('profileBox').textContent = (u.username?('@'+u.username+' '):'') + 'id: '+(u.id||'-');
    const list=r.purchases||[];
    const box=el('purchases');
    box.innerHTML = list.length ? list.map(p=>'<div class="card"><b class="ellipsis">'+p.title+'</b><div class="muted">'+p.tsMsk+' · '+Number(p.priceTon).toFixed(3)+' TON</div></div>').join('') : '<i class="muted">Покупок пока нет</i>';
  }

  async function refreshAdmin(){
    const r = await api('/api/admin/status');
    el('adminStatus').textContent =
      'MRKT last fail: '+(r.mrktLastFailMsg||'-')+'\\n'+
      'AutoBuy: eligible='+r.autoBuy.eligible+
      ', scanned='+r.autoBuy.scanned+
      ', newIds='+r.autoBuy.newIds+
      ', candidates='+r.autoBuy.candidates+
      ', buys='+r.autoBuy.buys+
      '\\nreason: '+(r.autoBuy.lastReason||'-');
    el('tokMask').value = r.mrktAuthMask||'';
  }

  async function refreshState(){
    const st = await api('/api/state');
    el('status').textContent = 'MRKT_AUTH: '+(st.api.mrktAuthSet?'✅':'❌');

    el('gift').value = st.user.filters.giftLabel || st.user.filters.gift || '';
    selectedGiftValue = st.user.filters.gift || '';
    el('model').value = st.user.filters.model || '';
    el('backdrop').value = st.user.filters.backdrop || '';
    el('number').value = st.user.filters.numberPrefix || '';

    const subs=st.user.subscriptions||[];
    const box=el('subsList');
    if(!subs.length){ box.innerHTML='<i class="muted">Подписок нет</i>'; }
    else{
      box.innerHTML = subs.map(s=>{
        const img = s.thumbUrl ? '<img class="thumb" src="'+s.thumbUrl+'" referrerpolicy="no-referrer"/>' : '<div class="thumb"></div>';
        const dot = s.backdropHex ? '<span class="dot" style="background:'+s.backdropHex+'"></span>' : '<span class="dot"></span>';
        return '<div class="card">'+
          '<div style="display:flex;justify-content:space-between;gap:10px;align-items:center">'+
            '<div style="display:flex;gap:10px;align-items:center;min-width:0">'+img+
              '<div style="min-width:0"><b>#'+s.num+' '+(s.enabled?'ON':'OFF')+'</b> '+dot+
              '<div class="muted ellipsis">'+(s.filters.gift||'any')+'</div></div>'+
            '</div>'+
            '<button class="small" data-act="subDetails" data-id="'+s.id+'">Детали</button>'+
          '</div>'+
          '<div class="muted">AutoBuy: '+(s.autoBuyEnabled?'ON':'OFF')+' | Max: '+(s.maxAutoBuyTon==null?'-':s.maxAutoBuyTon)+'</div>'+
          '<div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:8px">'+
            '<button class="small" data-act="subAutoToggle" data-id="'+s.id+'">'+(s.autoBuyEnabled?'AutoBuy OFF':'AutoBuy ON')+'</button>'+
            '<button class="small" data-act="subAutoMax" data-id="'+s.id+'">Max AutoBuy</button>'+
            '<button class="small" data-act="subToggle" data-id="'+s.id+'">'+(s.enabled?'Disable':'Enable')+'</button>'+
            '<button class="small" data-act="subDel" data-id="'+s.id+'">Delete</button>'+
          '</div>'+
        '</div>';
      }).join('');
    }

    if(st.api.isAdmin) el('adminTabBtn').style.display='inline-block';
  }

  async function refreshMarketData(){
    const lots = await api('/api/mrkt/lots');
    renderLots(lots);
  }

  el('apply').onclick = safe(async()=>{ await patchFilters(); await refreshState(); await refreshMarketData(); });
  el('refresh').onclick = safe(async()=>{ await refreshMarketData(); });
  el('historyBtn').onclick = safe(async()=>{
    openSheet('История продаж','по текущим фильтрам');
    const r = await api('/api/mrkt/history_current');
    sheetTop.innerHTML = '<b>Медиана:</b> '+(r.median!=null?r.median.toFixed(3)+' TON':'нет')+' (n='+(r.count||0)+')';
    renderHistory(r.lastSales||[]);
  });

  el('subCreate').onclick = safe(async()=>{ await api('/api/sub/create',{method:'POST'}); await refreshState(); });
  el('subRefresh').onclick = safe(async()=>{ await refreshState(); });

  document.body.addEventListener('click', async (e)=>{
    const btn = e.target.closest('button[data-act]');
    if(!btn) return;
    const act=btn.dataset.act;
    const id=btn.dataset.id;
    try{
      if(act==='subToggle') await api('/api/sub/toggle',{method:'POST',body:JSON.stringify({id})});
      if(act==='subDel') await api('/api/sub/delete',{method:'POST',body:JSON.stringify({id})});
      if(act==='subAutoToggle') await api('/api/sub/toggle_autobuy',{method:'POST',body:JSON.stringify({id})});
      if(act==='subAutoMax'){
        const v = prompt('Max AutoBuy TON (пример: 4.29):', '');
        if(v==null) return;
        await api('/api/sub/set_autobuy_max',{method:'POST',body:JSON.stringify({id, maxAutoBuyTon: v})});
      }
      if(act==='subDetails'){
        openSheet('Детали подписки','');
        const r = await api('/api/sub/details',{method:'POST',body:JSON.stringify({id})});
        sheetTop.innerHTML =
          '<b>Floor exact:</b> '+(r.floors?.exact?.priceTon!=null?r.floors.exact.priceTon.toFixed(3)+' TON':'—')+'<br>'+
          '<b>Floor collection:</b> '+(r.floors?.collection?.priceTon!=null?r.floors.collection.priceTon.toFixed(3)+' TON':'—')+'<br>'+
          '<b>Max offer exact:</b> '+(r.offers?.exact?.maxOfferTon!=null?r.offers.exact.maxOfferTon.toFixed(3)+' TON':'—')+'<br>'+
          '<b>Max offer collection:</b> '+(r.offers?.collection?.maxOfferTon!=null?r.offers.collection.maxOfferTon.toFixed(3)+' TON':'—')+'<br>'+
          '<div style="margin-top:6px"><b>Медиана:</b> '+(r.history?.median!=null?r.history.median.toFixed(3)+' TON':'нет')+' (n='+(r.history?.count||0)+')</div>';
        renderHistory(r.history?.lastSales||[]);
      }
      await refreshState();
    }catch(err){ showErr(err.message||String(err)); }
  });

  document.querySelectorAll('.tabbtn').forEach(b=>{
    b.addEventListener('click', async ()=>{
      if(b.dataset.tab==='profile') await safe(refreshProfile)();
      if(b.dataset.tab==='admin') await safe(refreshAdmin)();
    });
  });

  el('tokSave')?.addEventListener('click', safe(async()=>{
    const t = el('tokNew').value.trim();
    if(!t) return showErr('Вставь токен');
    await api('/api/admin/mrkt_auth',{method:'POST',body:JSON.stringify({token:t})});
    el('tokNew').value='';
    await refreshAdmin();
  }));

  refreshState().then(refreshMarketData).catch(e=>showErr(e.message||String(e)));
})();`;

// ===================== Web server + API =====================
function streamFetchToRes(r, res, fallbackContentType) {
  res.setHeader('Content-Type', r.headers.get('content-type') || fallbackContentType);
  res.setHeader('Cache-Control', 'public, max-age=86400');

  try {
    if (r.body && Readable.fromWeb) {
      Readable.fromWeb(r.body).pipe(res);
      return;
    }
  } catch {}

  r.arrayBuffer()
    .then((ab) => res.end(Buffer.from(ab)))
    .catch(() => res.status(502).end('bad gateway'));
}

function startWebServer() {
  const app = express();
  app.disable('x-powered-by');
  app.use(express.json({ limit: '1mb' }));

  // serve webapp
  app.get('/', (req, res) => {
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.send(WEBAPP_HTML);
  });
  app.get('/app.js', (req, res) => {
    res.setHeader('Content-Type', 'application/javascript; charset=utf-8');
    res.send(WEBAPP_JS);
  });
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

  // images
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

  // state
  app.get('/api/state', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    const mask = MRKT_AUTH_RUNTIME ? MRKT_AUTH_RUNTIME.slice(0, 4) + '…' + MRKT_AUTH_RUNTIME.slice(-4) : '';

    const subs = Array.isArray(u.subscriptions) ? u.subscriptions : [];
    const enriched = [];
    for (const s of subs) {
      const t = await getSubThumb({ gift: s.filters.gift, model: s.filters.model, backdrop: s.filters.backdrop }).catch(() => ({ thumbUrl: null, backdropHex: null }));
      enriched.push({ ...s, thumbUrl: t.thumbUrl, backdropHex: t.backdropHex });
    }

    res.json({
      ok: true,
      api: { mrktAuthSet: !!MRKT_AUTH_RUNTIME, isAdmin: isAdmin(req.userId), mrktAuthMask: mask },
      user: { enabled: !!u.enabled, filters: u.filters, subscriptions: enriched },
    });
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
      u.filters.numberPrefix = cleanDigitsPrefix(b.filters.numberPrefix || '');
    }
    scheduleSave();
    res.json({ ok: true });
  });

  // meta/suggest
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
      return { label: x.title || x.name, value: x.name, imgUrl, sub: floorTon != null ? `floor: ${floorTon.toFixed(3)} TON` : null };
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
          sub: m.floorNano != null ? `min: ${(tonFromNano(m.floorNano)).toFixed(3)} TON` : null,
          badge: m.rarityPerMille != null ? `${(Number(m.rarityPerMille) / 10).toFixed(1)}%` : null,
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
          badge: b.rarityPerMille != null ? `${(Number(b.rarityPerMille) / 10).toFixed(1)}%` : null,
        }));
      return res.json({ ok: true, items });
    }

    res.status(400).json({ ok: false, reason: 'BAD_KIND' });
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

    const lots = (r.gifts || []).slice(0, WEBAPP_LOTS_LIMIT).map((x) => {
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

  // history current
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

  // lot details for sheet (floors + offers + history)
  app.post('/api/lot/details', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);

    const gift = u.filters.gift || '';
    const model = u.filters.model || '';
    const backdrop = u.filters.backdrop || '';
    const numberPrefix = u.filters.numberPrefix || '';

    // floors (exact and collection)
    let exactLot = null;
    let colLot = null;

    if (gift) {
      const exact = await mrktSearchLotsByFilters({ gift, model, backdrop, numberPrefix }, 1);
      const col = await mrktSearchLotsByFilters({ gift, model: '', backdrop: '', numberPrefix: '' }, 1);
      exactLot = exact.ok ? (exact.gifts[0] || null) : null;
      colLot = col.ok ? (col.gifts[0] || null) : null;
    }

    // offers
    let offersExact = { ok: true, maxOfferTon: null };
    let offersCollection = { ok: true, maxOfferTon: null };
    if (gift) {
      const o1 = await mrktOrdersFetch({ gift, model, backdrop });
      offersExact = { ok: o1.ok, maxOfferTon: o1.ok ? maxOfferTonFromOrders(o1.orders) : null };
      const o2 = await mrktOrdersFetch({ gift, model: '', backdrop: '' });
      offersCollection = { ok: o2.ok, maxOfferTon: o2.ok ? maxOfferTonFromOrders(o2.orders) : null };
    }

    // history
    const h = await mrktHistorySales({ gift, model, backdrop, numberPrefix });

    res.json({
      ok: true,
      floors: {
        exact: exactLot ? { priceTon: exactLot.priceTon } : null,
        collection: colLot ? { priceTon: colLot.priceTon } : null,
      },
      offers: { exact: offersExact, collection: offersCollection },
      history: { median: h.median, count: h.count, lastSales: h.lastSales || [] },
    });
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

  // profile
  app.get('/api/profile', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    const purchases = (u.purchases || []).map((p) => ({ ...p, tsMsk: formatMsk(p.ts) }));
    res.json({ ok: true, user: req.tgUser, purchases });
  });

  // subscriptions CRUD
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

  app.post('/api/sub/toggle_autobuy', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    const s = findSub(u, req.body?.id);
    if (!s) return res.status(404).json({ ok: false, reason: 'NOT_FOUND' });

    s.autoBuyEnabled = !s.autoBuyEnabled;

    // reset baseline when toggled ON
    const stateKey = `${req.userId}:${s.id}`;
    const prev = subStates.get(stateKey) || { floor: null, emptyStreak: 0, pausedUntil: 0, baselineReady: false, seenIds: new Set() };
    subStates.set(stateKey, { ...prev, baselineReady: false, seenIds: new Set(), pausedUntil: 0 });

    await persistNow().catch(() => scheduleSave());
    res.json({ ok: true });
  });

  app.post('/api/sub/set_autobuy_max', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    const s = findSub(u, req.body?.id);
    if (!s) return res.status(404).json({ ok: false, reason: 'NOT_FOUND' });

    const v = parseTonInput(req.body?.maxAutoBuyTon);
    if (!Number.isFinite(v) || v <= 0) return res.status(400).json({ ok: false, reason: 'BAD_MAX' });

    s.maxAutoBuyTon = v;
    await persistNow().catch(() => scheduleSave());
    res.json({ ok: true });
  });

  app.post('/api/sub/details', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    const s = findSub(u, String(req.body?.id || ''));
    if (!s) return res.status(404).json({ ok: false, reason: 'NOT_FOUND' });

    const gift = s.filters.gift || '';
    const model = s.filters.model || '';
    const backdrop = s.filters.backdrop || '';
    const numberPrefix = s.filters.numberPrefix || '';

    const floorExact = gift ? await mrktSearchLotsByFilters({ gift, model, backdrop, numberPrefix }, 1) : { ok: true, gifts: [] };
    const floorCollection = gift ? await mrktSearchLotsByFilters({ gift, model: '', backdrop: '', numberPrefix: '' }, 1) : { ok: true, gifts: [] };

    const exactLot = floorExact.ok ? (floorExact.gifts[0] || null) : null;
    const colLot = floorCollection.ok ? (floorCollection.gifts[0] || null) : null;

    const h = await mrktHistorySales({ gift, model, backdrop, numberPrefix });

    let offersExact = { ok: true, maxOfferTon: null };
    let offersCollection = { ok: true, maxOfferTon: null };
    if (gift) {
      const o1 = await mrktOrdersFetch({ gift, model, backdrop });
      offersExact = { ok: o1.ok, maxOfferTon: o1.ok ? maxOfferTonFromOrders(o1.orders) : null };
      const o2 = await mrktOrdersFetch({ gift, model: '', backdrop: '' });
      offersCollection = { ok: o2.ok, maxOfferTon: o2.ok ? maxOfferTonFromOrders(o2.orders) : null };
    }

    res.json({
      ok: true,
      floors: {
        exact: exactLot ? { priceTon: exactLot.priceTon } : null,
        collection: colLot ? { priceTon: colLot.priceTon } : null,
      },
      offers: { exact: offersExact, collection: offersCollection },
      history: { median: h.median, count: h.count, lastSales: h.lastSales || [] },
    });
  });

  // admin status
  app.get('/api/admin/status', auth, async (req, res) => {
    if (!isAdmin(req.userId)) return res.status(403).json({ ok: false, reason: 'NOT_ADMIN' });

    const mask = MRKT_AUTH_RUNTIME ? MRKT_AUTH_RUNTIME.slice(0, 4) + '…' + MRKT_AUTH_RUNTIME.slice(-4) : '';
    res.json({
      ok: true,
      mrktAuthMask: mask,
      mrktLastFailMsg: mrktState.lastFailMsg || null,
      autoBuy: {
        eligible: autoBuyDebug.eligible,
        scanned: autoBuyDebug.scanned,
        newIds: autoBuyDebug.newIds,
        candidates: autoBuyDebug.candidates,
        buys: autoBuyDebug.buys,
        lastReason: autoBuyDebug.lastReason,
      },
    });
  });

  // admin set token
  app.post('/api/admin/mrkt_auth', auth, async (req, res) => {
    if (!isAdmin(req.userId)) return res.status(403).json({ ok: false, reason: 'NOT_ADMIN' });

    const t = String(req.body?.token || '').trim();
    if (!t) return res.status(400).json({ ok: false, reason: 'EMPTY_TOKEN' });

    MRKT_AUTH_RUNTIME = t;
    try { await saveMrktAuthToRedis(t); } catch {}

    collectionsCache = { time: 0, items: [] };
    modelsCache.clear();
    backdropsCache.clear();
    offersCache.clear();
    historyCache.clear();
    thumbsCache.clear();

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

setInterval(() => {
  autoBuyCycle().catch((e) => console.error('autobuy error:', e));
}, AUTO_BUY_CHECK_INTERVAL_MS);

// ===================== bootstrap =====================
(async () => {
  if (REDIS_URL) {
    await initRedis();
    if (redis) {
      await loadMrktAuthFromRedis();
      await loadState();
    }
  } else {
    console.warn('REDIS_URL not set => subscriptions will not persist after restart');
  }

  console.log('Bot started. /start');
})();
