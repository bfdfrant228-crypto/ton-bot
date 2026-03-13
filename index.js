/**
 * v30 (2026-03-13) MRKT panel + Subs + AutoBuy + Admin + Sales history
 * Node 18+
 * deps: express, node-telegram-bot-api, redis(optional)
 */

const TelegramBot = require('node-telegram-bot-api');
const express = require('express');
const crypto = require('crypto');
const { Readable } = require('stream');

// ===================== ENV =====================
const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
if (!TELEGRAM_TOKEN) { console.error('TELEGRAM_TOKEN not set'); process.exit(1); }

const MODE = process.env.MODE || 'real';

const APP_TITLE = String(process.env.APP_TITLE || 'Панель');
const WEBAPP_URL = process.env.WEBAPP_URL || null;

const PUBLIC_URL =
  process.env.PUBLIC_URL ||
  (process.env.RAILWAY_PUBLIC_DOMAIN ? `https://${process.env.RAILWAY_PUBLIC_DOMAIN}` : null);

const WEBAPP_AUTH_MAX_AGE_SEC = Number(process.env.WEBAPP_AUTH_MAX_AGE_SEC || 86400);
const ADMIN_USER_ID = process.env.ADMIN_USER_ID ? Number(process.env.ADMIN_USER_ID) : null;

const REDIS_URL = process.env.REDIS_URL || process.env.REDIS_PRIVATE_URL || null;

// MRKT
const MRKT_API_URL = 'https://api.tgmrkt.io/api/v1';
const MRKT_CDN_BASE = (process.env.MRKT_CDN_BASE || 'https://cdn.tgmrkt.io/').trim();
const FRAGMENT_GIFT_IMG_BASE = (process.env.FRAGMENT_GIFT_IMG_BASE || 'https://nft.fragment.com/gift/').trim();

let MRKT_AUTH_RUNTIME = (process.env.MRKT_AUTH || '').trim() || null;

// throttling / RPS
const MRKT_TIMEOUT_MS = Number(process.env.MRKT_TIMEOUT_MS || 12000);
const MRKT_MIN_GAP_MS = Number(process.env.MRKT_MIN_GAP_MS || 2200);
const MRKT_429_DEFAULT_PAUSE_MS = Number(process.env.MRKT_429_DEFAULT_PAUSE_MS || 4500);
const MRKT_HTTP_MAX_WAIT_MS = Number(process.env.MRKT_HTTP_MAX_WAIT_MS || 900);

// sizes
const MRKT_COUNT = Number(process.env.MRKT_COUNT || 18);
const MRKT_PAGES = Number(process.env.MRKT_PAGES || 1);
const MRKT_ORDERS_COUNT = Number(process.env.MRKT_ORDERS_COUNT || 25);
const MRKT_FEED_COUNT = Number(process.env.MRKT_FEED_COUNT || 50);

// WebApp
const WEBAPP_LOTS_LIMIT = Number(process.env.WEBAPP_LOTS_LIMIT || 24);
const WEBAPP_LOTS_PAGES = Number(process.env.WEBAPP_LOTS_PAGES || 1);
const WEBAPP_SUGGEST_LIMIT = Number(process.env.WEBAPP_SUGGEST_LIMIT || 120);

// global scan
const GLOBAL_SCAN_COLLECTIONS = Number(process.env.GLOBAL_SCAN_COLLECTIONS || 1);
const GLOBAL_SCAN_CACHE_TTL_MS = Number(process.env.GLOBAL_SCAN_CACHE_TTL_MS || 120_000);

// caches
const CACHE_TTL_MS = Number(process.env.CACHE_TTL_MS || 5 * 60_000);
const OFFERS_CACHE_TTL_MS = Number(process.env.OFFERS_CACHE_TTL_MS || 25_000);
const LOTS_CACHE_TTL_MS = Number(process.env.LOTS_CACHE_TTL_MS || 2000);
const DETAILS_CACHE_TTL_MS = Number(process.env.DETAILS_CACHE_TTL_MS || 3500);
const SALES_CACHE_TTL_MS = Number(process.env.SALES_CACHE_TTL_MS || 25_000);

// subs
const SUBS_CHECK_INTERVAL_MS = Number(process.env.SUBS_CHECK_INTERVAL_MS || 15000);
const SUBS_MAX_NOTIFICATIONS_PER_CYCLE = Number(process.env.SUBS_MAX_NOTIFICATIONS_PER_CYCLE || 6);
const SUBS_EMPTY_CONFIRM = Number(process.env.SUBS_EMPTY_CONFIRM || 2);

const SUBS_FEED_TYPES_RAW = String(process.env.SUBS_FEED_TYPES || 'sale,listing,change_price');
const SUBS_FEED_TYPES = new Set(
  SUBS_FEED_TYPES_RAW.split(',').map((s) => s.trim().toLowerCase()).filter(Boolean)
);
const SUBS_FEED_MAX_EVENTS_PER_CYCLE = Number(process.env.SUBS_FEED_MAX_EVENTS_PER_CYCLE || 10);

// Telegram notifications
const SUBS_SEND_PHOTO = String(process.env.SUBS_SEND_PHOTO || '0') === '1';

// AutoBuy
const AUTO_BUY_GLOBAL = String(process.env.AUTO_BUY_GLOBAL || '0') === '1';
const AUTO_BUY_DRY_RUN = String(process.env.AUTO_BUY_DRY_RUN || '1') === '1';
const AUTO_BUY_CHECK_INTERVAL_MS = Number(process.env.AUTO_BUY_CHECK_INTERVAL_MS || 4500);
const AUTO_BUY_MAX_BUYS_PER_USER_PER_CYCLE = Number(process.env.AUTO_BUY_MAX_BUYS_PER_USER_PER_CYCLE || 1);
const AUTO_BUY_ATTEMPT_TTL_MS = Number(process.env.AUTO_BUY_ATTEMPT_TTL_MS || 60_000);
const AUTO_BUY_DISABLE_AFTER_SUCCESS = String(process.env.AUTO_BUY_DISABLE_AFTER_SUCCESS || '0') === '1';

// sales history
const SALES_HISTORY_TARGET = Number(process.env.SALES_HISTORY_TARGET || 18);
const SALES_HISTORY_MAX_PAGES = Number(process.env.SALES_HISTORY_MAX_PAGES || 12);
const SALES_HISTORY_COUNT_PER_PAGE = Number(process.env.SALES_HISTORY_COUNT_PER_PAGE || 50);
const SALES_HISTORY_THROTTLE_MS = Number(process.env.SALES_HISTORY_THROTTLE_MS || 200);
const SALES_HISTORY_TIME_BUDGET_MS = Number(process.env.SALES_HISTORY_TIME_BUDGET_MS || 9000);

// MRKT auth refresh
const MRKT_AUTH_REFRESH_COOLDOWN_MS = Number(process.env.MRKT_AUTH_REFRESH_COOLDOWN_MS || 8000);

// Redis keys
const REDIS_KEY_STATE = 'bot:state:main';
const REDIS_KEY_MRKT_AUTH = 'mrkt:auth:token';
const REDIS_KEY_MRKT_SESSION = 'mrkt:session:admin';

console.log('v30 start', {
  MODE,
  PUBLIC_URL: !!PUBLIC_URL,
  WEBAPP_URL: !!WEBAPP_URL,
  ADMIN_USER_ID,
  REDIS: !!REDIS_URL,
  MRKT_AUTH_SET: !!MRKT_AUTH_RUNTIME,
  MRKT_MIN_GAP_MS,
  SUBS_CHECK_INTERVAL_MS,
  SUBS_FEED_TYPES: Array.from(SUBS_FEED_TYPES).join(','),
  SUBS_SEND_PHOTO,
  AUTO_BUY_GLOBAL,
  AUTO_BUY_DRY_RUN,
});

// ===================== Helpers =====================
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const nowMs = () => Date.now();

function makeReq() { return `${Date.now()}_${Math.random().toString(16).slice(2)}`; }
function norm(s) { return String(s || '').toLowerCase().trim().replace(/\s+/g, ' '); }
function normTraitName(s) {
  return norm(s)
    .replace(/\s*\([^)]*%[^)]*\)\s*/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}
function cleanDigitsPrefix(v, maxLen = 12) { return String(v || '').replace(/\D/g, '').slice(0, maxLen); }

function parseTonInput(x) {
  const s = String(x ?? '').trim();
  if (!s) return NaN;
  const cleaned = s.replace(',', '.').replace(':', '.').replace(/\s+/g, '');
  const v = Number(cleaned);
  return Number.isFinite(v) ? v : NaN;
}

function ensureArray(v) {
  if (!v) return [];
  if (Array.isArray(v)) return v.filter(Boolean).map(String);
  if (typeof v === 'string') {
    const s = v.trim();
    if (!s) return [];
    return s.split(',').map((x) => x.trim()).filter(Boolean);
  }
  return [String(v)];
}
function uniqNorm(arr) {
  const out = [];
  const seen = new Set();
  for (const x of (arr || [])) {
    const k = norm(x);
    if (!k) continue;
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(String(x).trim());
  }
  return out;
}

function joinUrl(base, key) {
  const b = String(base || '').endsWith('/') ? String(base) : String(base) + '/';
  const k = String(key || '').startsWith('/') ? String(key).slice(1) : String(key);
  return b + k;
}

function tonFromNano(nano) { const x = Number(nano); return Number.isFinite(x) ? x / 1e9 : null; }

function maskToken(t) {
  const s = String(t || '').trim();
  if (!s) return '';
  if (s.length <= 10) return s;
  return s.slice(0, 4) + '…' + s.slice(-4);
}

function mrktLotUrlFromId(id) {
  return id ? `https://t.me/mrkt/app?startapp=${String(id).replace(/-/g, '')}` : 'https://t.me/mrkt';
}

function giftNameFallbackFromCollectionAndNumber(collectionTitleOrName, number) {
  const base = String(collectionTitleOrName || '').replace(/\s+/g, '');
  if (!base || number == null) return null;
  return `${base}-${number}`.toLowerCase();
}

function fragmentGiftRemoteUrlFromGiftName(giftName) {
  if (!giftName) return null;
  const slug = String(giftName).trim().toLowerCase();
  return joinUrl(FRAGMENT_GIFT_IMG_BASE, `${encodeURIComponent(slug)}.medium.jpg`);
}

function absoluteUrlMaybe(path) {
  if (!path) return null;
  if (/^https?:\/\//i.test(path)) return path;
  if (PUBLIC_URL) return PUBLIC_URL.replace(/\/+$/, '') + (path.startsWith('/') ? path : '/' + path);
  return null;
}

function extractMrktErrorMessage(txt) {
  const s = String(txt || '').trim();
  if (!s) return null;
  try {
    const j = JSON.parse(s);
    if (typeof j?.error === 'string') return j.error.slice(0, 220);
    if (typeof j?.message === 'string') return j.message.slice(0, 220);
    if (typeof j?.title === 'string') return j.title.slice(0, 220);
    if (j?.errors && typeof j.errors === 'object') {
      const k = Object.keys(j.errors)[0];
      const v = j.errors[k];
      if (Array.isArray(v) && v[0]) return String(v[0]).slice(0, 220);
      if (typeof v === 'string') return v.slice(0, 220);
    }
  } catch {}
  return s.slice(0, 220);
}

function median(sorted) {
  if (!sorted.length) return null;
  const L = sorted.length;
  return L % 2 ? sorted[(L - 1) / 2] : (sorted[L / 2 - 1] + sorted[L / 2]) / 2;
}

function fmtWaitMs(ms) {
  const s = Math.max(1, Math.ceil(Number(ms || 0) / 1000));
  return `${s}s`;
}

function prettyEventType(t) {
  const x = String(t || '').toLowerCase();
  if (x === 'sale') return 'Продажа';
  if (x === 'listing') return 'Выставлен';
  if (x === 'change_price') return 'Изменение цены';
  return x || 'Событие';
}

async function fetchWithTimeout(url, options, timeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try { return await fetch(url, { ...options, signal: controller.signal }); }
  finally { clearTimeout(timer); }
}

function streamFetchToRes(r, res, fallbackContentType) {
  res.setHeader('Content-Type', r.headers.get('content-type') || fallbackContentType);
  res.setHeader('Cache-Control', 'public, max-age=86400');
  try {
    if (r.body && Readable.fromWeb) { Readable.fromWeb(r.body).pipe(res); return; }
  } catch {}
  r.arrayBuffer()
    .then((ab) => res.end(Buffer.from(ab)))
    .catch(() => res.status(502).end('bad gateway'));
}

// ===================== WebApp initData verify =====================
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

  const secretKey = crypto.createHmac('sha256', 'WebAppData').update(TELEGRAM_TOKEN).digest();
  const calcHash = crypto.createHmac('sha256', secretKey).update(dataCheckString).digest('hex');
  if (calcHash !== hash) return { ok: false, reason: 'BAD_HASH' };

  let user = null;
  try { user = JSON.parse(params.get('user') || 'null'); } catch {}
  const userId = user?.id;
  if (!userId) return { ok: false, reason: 'NO_USER' };

  return { ok: true, userId, user };
}
function isAdmin(userId) { return ADMIN_USER_ID && Number(userId) === Number(ADMIN_USER_ID); }

// ===================== Redis =====================
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
async function redisGet(key) {
  if (!redis) return null;
  try { return await redis.get(key); } catch { return null; }
}
async function redisSet(key, val, opts) {
  if (!redis) return;
  try { await redis.set(key, val, opts); } catch {}
}

// ===================== Telegram bot =====================
const bot = new TelegramBot(TELEGRAM_TOKEN, { polling: false });
const MAIN_KEYBOARD = { keyboard: [[{ text: '📌 Статус' }]], resize_keyboard: true };

async function sendMessageSafe(chatId, text, opts) {
  while (true) {
    try { return await bot.sendMessage(chatId, text, opts); }
    catch (e) {
      const retryAfter =
        e?.response?.body?.parameters?.retry_after ??
        e?.response?.parameters?.retry_after ??
        null;
      if (retryAfter) { await sleep((Number(retryAfter) + 1) * 1000); continue; }
      throw e;
    }
  }
}

async function sendPhotoSafe(chatId, photoUrl, caption, reply_markup) {
  while (true) {
    try {
      return await bot.sendPhoto(chatId, photoUrl, { caption, reply_markup, disable_notification: false });
    } catch (e) {
      const retryAfter =
        e?.response?.body?.parameters?.retry_after ??
        e?.response?.parameters?.retry_after ??
        null;
      if (retryAfter) { await sleep((Number(retryAfter) + 1) * 1000); continue; }
      return await sendMessageSafe(chatId, caption, { reply_markup, disable_web_page_preview: false });
    }
  }
}

// ===================== State =====================
const users = new Map();
const subStates = new Map(); // key -> { floor, emptyStreak, feedLastId, autoBuyLastId }
const autoBuyRecentAttempts = new Map();

function normalizeFilters(f) {
  const gifts = uniqNorm(ensureArray(f?.gifts || f?.gift));
  const giftLabels = (f?.giftLabels && typeof f.giftLabels === 'object') ? f.giftLabels : {};
  const models = uniqNorm(ensureArray(f?.models || f?.model));
  const backdrops = uniqNorm(ensureArray(f?.backdrops || f?.backdrop));
  const numberPrefix = cleanDigitsPrefix(f?.numberPrefix || '');

  const safeModels = gifts.length === 1 ? models : [];
  const safeBackdrops = gifts.length === 1 ? backdrops : [];

  const cleanLabels = {};
  for (const g of gifts) cleanLabels[g] = giftLabels[g] || g;

  return { gifts, giftLabels: cleanLabels, models: safeModels, backdrops: safeBackdrops, numberPrefix };
}

function getOrCreateUser(userId) {
  if (!users.has(userId)) {
    users.set(userId, {
      enabled: true,
      chatId: null,
      filters: normalizeFilters({}),
      subscriptions: [],
      purchases: [],
    });
  }
  return users.get(userId);
}
function userChatId(userId) {
  const u = users.get(userId);
  const cid = u?.chatId;
  if (cid && Number.isFinite(Number(cid))) return Number(cid);
  return userId;
}

function renumberSubs(user) {
  const subs = Array.isArray(user.subscriptions) ? user.subscriptions : [];
  subs.forEach((s, idx) => { if (s) s.num = idx + 1; });
}
function findSub(user, id) {
  return (user.subscriptions || []).find((s) => s && s.id === id) || null;
}

function pushPurchase(user, entry) {
  if (!user.purchases) user.purchases = [];
  user.purchases.unshift({
    tsBought: entry.tsBought || Date.now(),
    latencyMs: entry.latencyMs || null,
    title: String(entry.title || ''),
    priceTon: Number(entry.priceTon || 0),
    urlTelegram: String(entry.urlTelegram || ''),
    urlMarket: String(entry.urlMarket || ''),
    lotId: String(entry.lotId || ''),
    giftName: String(entry.giftName || ''),
    thumbKey: String(entry.thumbKey || ''),
    model: String(entry.model || ''),
    backdrop: String(entry.backdrop || ''),
    collection: String(entry.collection || ''),
    number: entry.number == null ? null : Number(entry.number),
  });
  user.purchases = user.purchases.slice(0, 500);
}

function exportState() {
  const out = { users: {} };
  for (const [userId, u] of users.entries()) {
    out.users[String(userId)] = {
      enabled: u?.enabled !== false,
      chatId: u?.chatId ?? null,
      filters: u?.filters,
      subscriptions: Array.isArray(u?.subscriptions) ? u.subscriptions : [],
      purchases: Array.isArray(u?.purchases) ? u.purchases : [],
    };
  }
  return out;
}

function importState(parsed) {
  const objUsers = parsed?.users && typeof parsed.users === 'object' ? parsed.users : {};
  for (const [idStr, u] of Object.entries(objUsers)) {
    const userId = Number(idStr);
    if (!Number.isFinite(userId)) continue;

    const safe = {
      enabled: u?.enabled !== false,
      chatId: u?.chatId ?? null,
      filters: normalizeFilters(u?.filters || {}),
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
        autoBuyAny: !!s.autoBuyAny, // NEW: buy existing cheap lots too
        filters: normalizeFilters(s?.filters || {}),
        ui: (s.ui && typeof s.ui === 'object')
          ? { thumbKey: s.ui.thumbKey || null, swatches: Array.isArray(s.ui.swatches) ? s.ui.swatches : [] }
          : { thumbKey: null, swatches: [] },
      }));
    renumberSubs(safe);

    safe.purchases = (safe.purchases || []).slice(0, 500).map((p) => ({
      ...p,
      priceTon: Number(p.priceTon || 0),
      number: p.number == null ? null : Number(p.number),
    }));

    users.set(userId, safe);
  }
}

let saveTimer = null;
function scheduleSave() {
  if (!redis) return;
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    redisSet(REDIS_KEY_STATE, JSON.stringify(exportState())).catch(() => {});
  }, 250);
}

function makeSubFromCurrentFilters(user) {
  const f = normalizeFilters(user.filters || {});
  if (!f.gifts.length && !f.numberPrefix) return { ok: false, reason: 'NO_GIFT_AND_NO_NUMBER' };

  const sub = {
    id: `sub_${Date.now()}_${Math.random().toString(16).slice(2, 6)}`,
    num: (user.subscriptions || []).length + 1,
    enabled: true,
    maxNotifyTon: null,
    autoBuyEnabled: false,
    maxAutoBuyTon: null,
    autoBuyAny: false,
    filters: JSON.parse(JSON.stringify(f)),
    ui: { thumbKey: null, swatches: [] },
  };
  return { ok: true, sub };
}

// ===================== MRKT auth/session refresh =====================
let mrktSessionRuntime = null;
let isRefreshing = false;
let lastRefreshAt = 0;

const mrktAuthDebug = {
  lastAttemptAt: 0,
  lastOkAt: 0,
  lastStatus: null,
  lastReason: null,
  lastTokenMask: null,
};

async function loadMrktSessionFromRedis() {
  if (!redis) return null;
  const raw = await redisGet(REDIS_KEY_MRKT_SESSION);
  if (!raw) return null;
  try {
    const j = JSON.parse(raw);
    if (j && typeof j.data === 'string' && j.data.includes('hash=')) return j;
  } catch {}
  return null;
}

async function mrktAuthWithSession(session) {
  const url = `${MRKT_API_URL}/auth`;
  const body = { appId: null, data: String(session?.data || ''), photo: session?.photo ?? null };

  const res = await fetchWithTimeout(url, {
    method: 'POST',
    headers: {
      Accept: 'application/json',
      'Content-Type': 'application/json',
      Origin: 'https://cdn.tgmrkt.io',
      Referer: 'https://cdn.tgmrkt.io/',
      'User-Agent': 'Mozilla/5.0',
      ...(MRKT_AUTH_RUNTIME ? { Authorization: MRKT_AUTH_RUNTIME } : {}),
    },
    body: JSON.stringify(body),
  }, MRKT_TIMEOUT_MS).catch(() => null);

  if (!res) return { ok: false, status: 0, reason: 'FETCH_ERROR' };

  const txt = await res.text().catch(() => '');
  let data = null;
  try { data = txt ? JSON.parse(txt) : null; } catch {}

  if (!res.ok) return { ok: false, status: res.status, reason: extractMrktErrorMessage(txt) || `HTTP_${res.status}` };

  const token = data?.token || null;
  if (!token) return { ok: false, status: res.status, reason: 'NO_TOKEN_IN_RESPONSE' };
  return { ok: true, status: res.status, token: String(token) };
}

async function tryRefreshMrktToken(reason = 'auto', { force = false } = {}) {
  const now = nowMs();
  mrktAuthDebug.lastAttemptAt = now;

  if (isRefreshing) return { ok: false, reason: 'LOCKED' };
  if (!force && now - lastRefreshAt < MRKT_AUTH_REFRESH_COOLDOWN_MS) return { ok: false, reason: 'COOLDOWN' };
  lastRefreshAt = now;

  isRefreshing = true;
  try {
    let session = mrktSessionRuntime;
    if (!session) session = await loadMrktSessionFromRedis();
    if (!session) {
      mrktAuthDebug.lastReason = 'NO_SESSION';
      return { ok: false, reason: 'NO_SESSION' };
    }

    const r = await mrktAuthWithSession(session);
    if (!r.ok) {
      mrktAuthDebug.lastStatus = r.status;
      mrktAuthDebug.lastReason = r.reason;
      return { ok: false, reason: r.reason };
    }

    MRKT_AUTH_RUNTIME = r.token;
    mrktAuthDebug.lastOkAt = nowMs();
    mrktAuthDebug.lastStatus = r.status;
    mrktAuthDebug.lastReason = null;
    mrktAuthDebug.lastTokenMask = maskToken(r.token);

    if (redis) await redisSet(REDIS_KEY_MRKT_AUTH, r.token);

    // drop caches
    collectionsCache.time = 0; collectionsCache.items = [];
    modelsCache.clear();
    backdropsCache.clear();
    offersCache.clear();
    lotsCache.clear();
    detailsCache.clear();
    salesCache.clear();
    globalCheapestCache.time = 0;
    globalCheapestCache.lots = [];
    globalCheapestCache.note = null;

    console.log('[MRKT AUTH] refreshed OK reason=', reason, 'token=', maskToken(r.token));
    return { ok: true };
  } finally {
    isRefreshing = false;
  }
}

async function ensureMrktAuth() {
  if (MRKT_AUTH_RUNTIME) return true;
  const r = await tryRefreshMrktToken('NO_AUTH', { force: false });
  return !!r.ok;
}

// ===================== MRKT queue + gate =====================
const mrktState = {
  lastFailMsg: null,
  pauseUntil: 0,
  nextAllowedAt: 0,
};

let mrktQueue = Promise.resolve();

async function mrktRunExclusive(fn) {
  const prev = mrktQueue;
  let release;
  mrktQueue = new Promise((r) => (release = r));
  await prev;
  try { return await fn(); }
  finally { release(); }
}

function setPauseMs(ms) {
  const until = nowMs() + Math.max(1000, ms || MRKT_429_DEFAULT_PAUSE_MS);
  mrktState.pauseUntil = Math.max(mrktState.pauseUntil || 0, until);
}
function setPauseFrom429(res) {
  const ra = res?.headers?.get?.('retry-after');
  const ms = ra ? (Number(ra) * 1000) : MRKT_429_DEFAULT_PAUSE_MS;
  setPauseMs(ms);
}

async function mrktGateCheckOrWait() {
  const now = nowMs();
  const pauseMs = (mrktState.pauseUntil && now < mrktState.pauseUntil) ? (mrktState.pauseUntil - now) : 0;
  const gapMs = (mrktState.nextAllowedAt && now < mrktState.nextAllowedAt) ? (mrktState.nextAllowedAt - now) : 0;
  const waitMs = Math.max(pauseMs, gapMs);

  if (waitMs > MRKT_HTTP_MAX_WAIT_MS) return { ok: false, waitMs };
  if (waitMs > 0) await sleep(waitMs);

  mrktState.nextAllowedAt = nowMs() + MRKT_MIN_GAP_MS;
  return { ok: true, waitMs: 0 };
}

function bodyLooksLikeRpsLimit(txt) {
  const s = String(txt || '').toLowerCase();
  return s.includes('more rps') || (s.includes('rps') && s.includes('support'));
}

function mrktHeadersCommon() {
  return {
    ...(MRKT_AUTH_RUNTIME ? { Authorization: MRKT_AUTH_RUNTIME } : {}),
    Accept: 'application/json',
    Origin: 'https://cdn.tgmrkt.io',
    Referer: 'https://cdn.tgmrkt.io/',
    'User-Agent': 'Mozilla/5.0',
  };
}
function mrktHeadersJson() {
  return { ...mrktHeadersCommon(), 'Content-Type': 'application/json; charset=utf-8' };
}

async function mrktGetJson(path, { retry = true } = {}) {
  return mrktRunExclusive(async () => {
    if (!MRKT_AUTH_RUNTIME) {
      const ok = await ensureMrktAuth();
      if (!ok) { mrktState.lastFailMsg = 'NO_AUTH'; return { ok: false, status: 401, data: null, text: 'NO_AUTH' }; }
    }

    const gate = await mrktGateCheckOrWait();
    if (!gate.ok) { mrktState.lastFailMsg = `RPS_WAIT ${fmtWaitMs(gate.waitMs)}`; return { ok: false, status: 429, data: null, text: 'RPS_WAIT', waitMs: gate.waitMs }; }

    const url = `${MRKT_API_URL}${path}${path.includes('?') ? '&' : '?'}req=${encodeURIComponent(makeReq())}`;
    const res = await fetchWithTimeout(url, { method: 'GET', headers: mrktHeadersCommon() }, MRKT_TIMEOUT_MS).catch(() => null);
    if (!res) { mrktState.lastFailMsg = 'FETCH_ERROR'; return { ok: false, status: 0, data: null, text: 'FETCH_ERROR' }; }

    const txt = await res.text().catch(() => '');
    let data = null;
    try { data = txt ? JSON.parse(txt) : null; } catch {}

    if (!res.ok) {
      if (res.status === 429 || bodyLooksLikeRpsLimit(txt)) {
        setPauseFrom429(res);
        mrktState.lastFailMsg = `HTTP 429 (pause ${fmtWaitMs(mrktState.pauseUntil - nowMs())})`;
        return { ok: false, status: 429, data, text: txt, waitMs: (mrktState.pauseUntil - nowMs()) };
      }

      mrktState.lastFailMsg = extractMrktErrorMessage(txt) || `HTTP ${res.status}`;

      if ((res.status === 401 || res.status === 403) && retry) {
        const rr = await tryRefreshMrktToken(`HTTP_${res.status}:${path}`, { force: false });
        if (rr.ok) return mrktGetJson(path, { retry: false });
      }
      return { ok: false, status: res.status, data, text: txt };
    }

    mrktState.lastFailMsg = null;
    return { ok: true, status: res.status, data, text: txt };
  });
}

async function mrktPostJson(path, bodyObj, { retry = true } = {}) {
  return mrktRunExclusive(async () => {
    if (!MRKT_AUTH_RUNTIME) {
      const ok = await ensureMrktAuth();
      if (!ok) { mrktState.lastFailMsg = 'NO_AUTH'; return { ok: false, status: 401, data: null, text: 'NO_AUTH' }; }
    }

    const gate = await mrktGateCheckOrWait();
    if (!gate.ok) { mrktState.lastFailMsg = `RPS_WAIT ${fmtWaitMs(gate.waitMs)}`; return { ok: false, status: 429, data: null, text: 'RPS_WAIT', waitMs: gate.waitMs }; }

    const reqVal = bodyObj?.req ? String(bodyObj.req) : makeReq();
    const body = { ...(bodyObj || {}), req: reqVal };
    const url = `${MRKT_API_URL}${path}${path.includes('?') ? '&' : '?'}req=${encodeURIComponent(reqVal)}`;

    const res = await fetchWithTimeout(url, { method: 'POST', headers: mrktHeadersJson(), body: JSON.stringify(body) }, MRKT_TIMEOUT_MS).catch(() => null);
    if (!res) { mrktState.lastFailMsg = 'FETCH_ERROR'; return { ok: false, status: 0, data: null, text: 'FETCH_ERROR' }; }

    const txt = await res.text().catch(() => '');
    let data = null;
    try { data = txt ? JSON.parse(txt) : null; } catch {}

    if (!res.ok) {
      if (res.status === 429 || bodyLooksLikeRpsLimit(txt)) {
        setPauseFrom429(res);
        mrktState.lastFailMsg = `HTTP 429 (pause ${fmtWaitMs(mrktState.pauseUntil - nowMs())})`;
        return { ok: false, status: 429, data, text: txt, waitMs: (mrktState.pauseUntil - nowMs()) };
      }

      mrktState.lastFailMsg = extractMrktErrorMessage(txt) || `HTTP ${res.status}`;

      if ((res.status === 401 || res.status === 403) && retry) {
        const rr = await tryRefreshMrktToken(`HTTP_${res.status}:${path}`, { force: false });
        if (rr.ok) return mrktPostJson(path, bodyObj, { retry: false });
      }

      return { ok: false, status: res.status, data, text: txt };
    }

    mrktState.lastFailMsg = null;
    return { ok: true, status: res.status, data, text: txt };
  });
}

// ===================== MRKT caches + business =====================
let collectionsCache = { time: 0, items: [] };
const modelsCache = new Map();
const backdropsCache = new Map();
const offersCache = new Map();
const lotsCache = new Map();
const detailsCache = new Map();
const salesCache = new Map();
const globalCheapestCache = { time: 0, lots: [], note: null };

async function mrktGetCollections() {
  const now = nowMs();
  if (collectionsCache.items.length && now - collectionsCache.time < CACHE_TTL_MS) return collectionsCache.items;

  const r = await mrktGetJson('/gifts/collections');
  if (!r.ok) return collectionsCache.items.length ? collectionsCache.items : [];

  const arr = Array.isArray(r.data) ? r.data : [];
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

  const r = await mrktPostJson('/gifts/models', { collections: [giftName] });
  if (!r.ok) return cached?.items || [];

  const arr = Array.isArray(r.data) ? r.data : [];
  const map = new Map();
  for (const it of arr) {
    const name = it.modelTitle || it.modelName;
    if (!name) continue;
    const key = normTraitName(name);
    if (!map.has(key)) {
      map.set(key, {
        name: String(name),
        thumbKey: it.modelStickerThumbnailKey || null,
        floorNano: it.floorPriceNanoTons ?? null,
        rarityPerMille: it.rarityPerMille ?? null,
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

  const r = await mrktPostJson('/gifts/backdrops', { collections: [giftName] });
  if (!r.ok) return cached?.items || [];

  const arr = Array.isArray(r.data) ? r.data : [];
  const map = new Map();
  for (const it of arr) {
    const name = it.backdropName || it.name || null;
    if (!name) continue;
    const key = normTraitName(name);

    const v = it.backdropColorsCenterColor ?? it.colorsCenterColor ?? it.centerColor ?? null;
    const num = Number(v);
    const hex = Number.isFinite(num) ? ('#' + ((num >>> 0).toString(16).padStart(6, '0')).slice(-6)) : null;

    if (!map.has(key)) map.set(key, { name: String(name), centerHex: hex });
  }
  const items = Array.from(map.values());
  backdropsCache.set(giftName, { time: nowMs(), items });
  return items;
}

async function mrktFetchSalingPage({ collectionNames, modelNames, backdropNames, cursor, ordering, lowToHigh, count, number }) {
  const body = {
    count: Number(count || MRKT_COUNT),
    cursor: cursor || '',
    collectionNames: Array.isArray(collectionNames) ? collectionNames : [],
    modelNames: Array.isArray(modelNames) ? modelNames : [],
    backdropNames: Array.isArray(backdropNames) ? backdropNames : [],
    symbolNames: [],
    ordering: ordering || 'Price',
    lowToHigh: !!lowToHigh,
    maxPrice: null,
    minPrice: null,
    number: number != null ? Number(number) : null,
    query: null,
    promotedFirst: false,
  };

  const r = await mrktPostJson('/gifts/saling', body);
  if (!r.ok) return { ok: false, reason: r.status === 429 ? 'RPS_WAIT' : 'ERROR', waitMs: r.waitMs || 0, gifts: [], cursor: '' };

  const gifts = Array.isArray(r.data?.gifts) ? r.data.gifts : [];
  const nextCursor = r.data?.cursor || r.data?.nextCursor || '';
  return { ok: true, reason: 'OK', gifts, cursor: nextCursor };
}

function salingGiftToLot(g) {
  const nanoA = g?.salePriceWithoutFee ?? null;
  const nanoB = g?.salePrice ?? null;
  const nano = (nanoA != null && Number(nanoA) > 0) ? Number(nanoA) : (nanoB != null ? Number(nanoB) : NaN);

  const priceTon = Number(nano) / 1e9;
  if (!Number.isFinite(priceTon) || priceTon <= 0) return null;

  const numberVal = g.number ?? null;
  const baseName = (g.collectionTitle || g.collectionName || g.title || 'Gift').trim();
  const displayName = numberVal != null ? `${baseName} #${numberVal}` : baseName;

  const giftName = g.name || giftNameFallbackFromCollectionAndNumber(baseName, numberVal) || null;
  const urlTelegram = giftName && String(giftName).includes('-') ? `https://t.me/nft/${giftName}` : 'https://t.me/mrkt';
  const urlMarket = g.id ? mrktLotUrlFromId(g.id) : 'https://t.me/mrkt';

  const thumbKey =
    g.modelStickerThumbnailKey ||
    g.modelStickerKey ||
    g.stickerThumbnailKey ||
    null;

  return {
    id: String(g.id || ''),
    name: displayName,
    giftName,
    collectionName: g.collectionName || g.collectionTitle || baseName,
    priceTon,
    priceNano: Number(nano),
    urlTelegram,
    urlMarket,
    model: g.modelTitle || g.modelName || null,
    backdrop: g.backdropName || null,
    symbol: g.symbolName || null,
    number: numberVal,
    listedAt: g.receivedDate || g.exportDate || null,
    thumbKey,
  };
}

async function mrktSearchLotsByFilters(filters, pagesLimit, { ordering = 'Price', lowToHigh = true, count = MRKT_COUNT } = {}) {
  const f = normalizeFilters(filters || {});
  const prefix = f.numberPrefix || '';
  const pagesToScan = Math.max(1, Number(pagesLimit || 1));
  let cursor = '';
  const out = [];

  const gifts = f.gifts || [];
  const models = gifts.length === 1 ? (f.models || []) : [];
  const backdrops = gifts.length === 1 ? (f.backdrops || []) : [];

  const exactNumber = prefix && prefix.length >= 4 ? Number(prefix) : null;

  for (let page = 0; page < pagesToScan; page++) {
    const r = await mrktFetchSalingPage({
      collectionNames: gifts,
      modelNames: models,
      backdropNames: backdrops,
      cursor,
      ordering,
      lowToHigh,
      count,
      number: exactNumber != null ? exactNumber : null,
    });

    if (!r.ok) return { ok: false, reason: r.reason, waitMs: r.waitMs || 0, gifts: [] };

    for (const g of r.gifts) {
      const lot = salingGiftToLot(g);
      if (!lot) continue;

      if (exactNumber != null) {
        if (lot.number == null || Number(lot.number) !== exactNumber) continue;
      } else if (prefix) {
        const numStr = lot.number == null ? '' : String(lot.number);
        if (!numStr.startsWith(prefix)) continue;
      }

      out.push(lot);
      if (out.length >= 240) break;
    }

    cursor = r.cursor || '';
    if (!cursor || out.length >= 240) break;
  }

  out.sort((a, b) => a.priceTon - b.priceTon);
  return { ok: true, reason: 'OK', gifts: out };
}

async function mrktOrdersFetch({ gift, model, backdrop }) {
  const key = `orders|${gift || ''}|${model || ''}|${backdrop || ''}`;
  const now = nowMs();
  const cached = offersCache.get(key);

  if (cached && now - cached.time < OFFERS_CACHE_TTL_MS) {
    return { ok: true, orders: cached.orders, fromCache: true, note: null, waitMs: 0 };
  }

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

  const r = await mrktPostJson('/orders', body);
  if (!r.ok) {
    if (cached) {
      return { ok: true, orders: cached.orders, fromCache: true, note: `RPS limit, wait ${fmtWaitMs(r.waitMs || 1000)}`, waitMs: r.waitMs || 1000 };
    }
    return { ok: false, orders: [], note: `RPS limit, wait ${fmtWaitMs(r.waitMs || 1000)}`, waitMs: r.waitMs || 1000 };
  }

  const orders = Array.isArray(r.data?.orders) ? r.data.orders : [];
  offersCache.set(key, { time: now, orders });
  return { ok: true, orders, fromCache: false, note: null, waitMs: 0 };
}

function maxOfferTonFromOrders(orders) {
  const getNano = (o) => {
    const candidates = [
      o?.priceMaxNanoTONs,
      o?.priceMaxNanoTons,
      o?.maxPriceNanoTONs,
      o?.maxPriceNanoTons,
      o?.priceNanoTONs,
      o?.priceNanoTons,
      o?.maxPrice,
      o?.price,
    ];
    for (const c of candidates) {
      const n = Number(c);
      if (Number.isFinite(n) && n > 0) return n;
    }
    return null;
  };

  let maxNano = null;
  for (const o of orders || []) {
    const n = getNano(o);
    if (!n) continue;
    if (maxNano == null || n > maxNano) maxNano = n;
  }
  return maxNano != null ? maxNano / 1e9 : null;
}

async function mrktFeedFetch({ collectionNames = [], modelNames = [], backdropNames = [], cursor = '', count = MRKT_FEED_COUNT, ordering = 'Latest', types = [] }) {
  const body = {
    count: Number(count || MRKT_FEED_COUNT),
    cursor: cursor || '',
    collectionNames: Array.isArray(collectionNames) ? collectionNames : [],
    modelNames: Array.isArray(modelNames) ? modelNames : [],
    backdropNames: Array.isArray(backdropNames) ? backdropNames : [],
    lowToHigh: false,
    maxPrice: null,
    minPrice: null,
    number: null,
    ordering: ordering || 'Latest',
    query: null,
    type: Array.isArray(types) ? types : [],
  };

  const r = await mrktPostJson('/feed', body);
  if (!r.ok) return { ok: false, reason: r.status === 429 ? 'RPS_WAIT' : 'FEED_ERROR', waitMs: r.waitMs || 0, items: [], cursor: '' };

  const items = Array.isArray(r.data?.items) ? r.data.items : [];
  const nextCursor = r.data?.cursor || r.data?.nextCursor || '';
  return { ok: true, reason: 'OK', items, cursor: nextCursor };
}

async function mrktFeedSales({ gift, modelNames = [], backdropNames = [] }) {
  const key = `sales|${gift}|m=${(modelNames||[]).join('|')}|b=${(backdropNames||[]).join('|')}`;
  const now = nowMs();
  const cached = salesCache.get(key);
  if (cached && now - cached.time < SALES_CACHE_TTL_MS) return { ...cached.data, cached: true };

  const data = await (async () => {
    if (!gift) return { ok: true, approxPriceTon: null, sales: [], note: 'Выбери 1 gift', waitMs: 0 };

    const started = nowMs();
    let cursor = '';
    let pages = 0;
    const sales = [];
    const prices = [];

    while (pages < SALES_HISTORY_MAX_PAGES && sales.length < SALES_HISTORY_TARGET) {
      if (nowMs() - started > SALES_HISTORY_TIME_BUDGET_MS) break;

      const r = await mrktFeedFetch({
        collectionNames: [gift],
        modelNames: Array.isArray(modelNames) ? modelNames : [],
        backdropNames: Array.isArray(backdropNames) ? backdropNames : [],
        cursor,
        count: SALES_HISTORY_COUNT_PER_PAGE,
        ordering: 'Latest',
        types: ['sale'],
      });

      if (!r.ok) {
        if (cached) return { ...cached.data, note: `RPS limit, wait ${fmtWaitMs(r.waitMs || 1000)}`, waitMs: r.waitMs || 1000 };
        return { ok: true, approxPriceTon: null, sales: [], note: `RPS limit, wait ${fmtWaitMs(r.waitMs || 1000)}`, waitMs: r.waitMs || 1000 };
      }

      if (!r.items.length) break;

      for (const it of r.items) {
        const type = String(it?.type || '').toLowerCase();
        if (type !== 'sale') continue;

        const g = it?.gift;
        if (!g) continue;

        const amountNano = it.amount ?? g.salePriceWithoutFee ?? g.salePrice ?? null;
        const ton = amountNano != null ? Number(amountNano) / 1e9 : NaN;
        if (!Number.isFinite(ton) || ton <= 0) continue;

        const base = (g.collectionTitle || g.collectionName || g.title || gift).trim();
        const number = g.number ?? null;
        const title = number != null ? `${base} #${number}` : base;

        const giftName = g.name || giftNameFallbackFromCollectionAndNumber(base, number) || null;

        const urlTelegram = giftName && String(giftName).includes('-')
          ? `https://t.me/nft/${giftName}`
          : 'https://t.me/mrkt';

        const urlMarket = g.id ? mrktLotUrlFromId(g.id) : 'https://t.me/mrkt';

        const thumbKey =
          g.modelStickerThumbnailKey ||
          g.modelStickerKey ||
          g.stickerThumbnailKey ||
          null;

        const imgUrl = giftName
          ? `/img/gift?name=${encodeURIComponent(giftName)}`
          : (thumbKey ? `/img/cdn?key=${encodeURIComponent(thumbKey)}` : null);

        prices.push(ton);
        sales.push({
          ts: it.date || null,
          priceTon: ton,
          title,
          imgUrl,
          model: g.modelTitle || g.modelName || null,
          backdrop: g.backdropName || null,
          urlTelegram,
          urlMarket,
        });

        if (sales.length >= SALES_HISTORY_TARGET) break;
      }

      cursor = r.cursor || '';
      pages++;
      if (!cursor) break;

      if (SALES_HISTORY_THROTTLE_MS > 0) await sleep(SALES_HISTORY_THROTTLE_MS);
    }

    prices.sort((a, b) => a - b);
    return { ok: true, approxPriceTon: median(prices), sales, note: null, waitMs: 0 };
  })();

  salesCache.set(key, { time: now, data });
  return data;
}

async function mrktGlobalCheapestLotsReal() {
  const now = nowMs();
  if (globalCheapestCache.time && now - globalCheapestCache.time < GLOBAL_SCAN_CACHE_TTL_MS) {
    return { ok: true, lots: globalCheapestCache.lots, note: globalCheapestCache.note };
  }

  const cols = await mrktGetCollections();
  const sorted = cols
    .map((c) => ({ ...c, floorTon: c.floorNano != null ? tonFromNano(c.floorNano) : null }))
    .filter((c) => c.floorTon != null)
    .sort((a, b) => a.floorTon - b.floorTon)
    .slice(0, Math.max(1, GLOBAL_SCAN_COLLECTIONS));

  const lots = [];
  for (const c of sorted) {
    const r = await mrktFetchSalingPage({
      collectionNames: [c.name],
      modelNames: [],
      backdropNames: [],
      cursor: '',
      ordering: 'Price',
      lowToHigh: true,
      count: 3,
      number: null,
    });

    if (!r.ok) {
      if (r.reason === 'RPS_WAIT') {
        const note = `MRKT RPS limit, wait ${fmtWaitMs(r.waitMs || 1000)}`;
        globalCheapestCache.time = now;
        globalCheapestCache.lots = [];
        globalCheapestCache.note = note;
        return { ok: true, lots: [], note };
      }
      continue;
    }

    const lot = r.gifts?.length ? salingGiftToLot(r.gifts[0]) : null;
    if (lot) lots.push(lot);
  }

  lots.sort((a, b) => a.priceTon - b.priceTon);
  globalCheapestCache.time = now;
  globalCheapestCache.lots = lots.slice(0, WEBAPP_LOTS_LIMIT);
  globalCheapestCache.note = 'Глобальный список ограничен (защита от 429).';
  return { ok: true, lots: globalCheapestCache.lots, note: globalCheapestCache.note };
}

// ===================== Notification helpers =====================
function lotImgUrl(lot) {
  if (lot?.giftName) return `/img/gift?name=${encodeURIComponent(lot.giftName)}`;
  if (lot?.thumbKey) return `/img/cdn?key=${encodeURIComponent(lot.thumbKey)}`;
  return null;
}
function mkReplyMarkupOpen(urlMarket, label = 'MRKT') {
  return urlMarket ? { inline_keyboard: [[{ text: label, url: urlMarket }]] } : undefined;
}
async function notifyTextOrPhoto(chatId, imgUrl, text, reply_markup) {
  if (SUBS_SEND_PHOTO && imgUrl) {
    const abs = absoluteUrlMaybe(imgUrl);
    if (abs) return sendPhotoSafe(chatId, abs, text, reply_markup);
  }
  return sendMessageSafe(chatId, text, { disable_web_page_preview: false, reply_markup });
}

// ===================== Subs worker =====================
let isSubsChecking = false;
let isAutoBuying = false;

function subGiftTitle(sub) {
  const f = normalizeFilters(sub?.filters || {});
  if (!f.gifts.length) return '(не выбран)';
  const labels = f.gifts.map((g) => f.giftLabels?.[g] || g);
  return labels.join(', ');
}

async function notifyFloorToUser(userId, sub, lot, newFloor) {
  const chatId = userChatId(userId);

  const lines = [];
  lines.push(`🔔 Подписка #${sub.num}`);
  lines.push(`Тип: Флор`);
  lines.push(`Gift: ${subGiftTitle(sub)}`);
  lines.push(`Цена: ${newFloor.toFixed(3)} TON`);
  if (lot?.model) lines.push(`Model: ${lot.model}`);
  if (lot?.backdrop) lines.push(`Backdrop: ${lot.backdrop}`);
  lines.push(lot?.urlTelegram || 'https://t.me/mrkt');

  await notifyTextOrPhoto(chatId, lotImgUrl(lot), lines.join('\n'), mkReplyMarkupOpen(lot?.urlMarket, 'MRKT'));
}

function feedItemToEvent(it) {
  const type = String(it?.type || '').toLowerCase();
  const g = it?.gift || null;
  if (!g) return null;

  const amountNano =
    it?.amount ??
    it?.value ??
    g?.salePriceWithoutFee ??
    g?.salePrice ??
    null;

  const amountTon = amountNano != null ? Number(amountNano) / 1e9 : null;
  const ton = (amountTon != null && Number.isFinite(amountTon) && amountTon > 0) ? amountTon : null;

  const base = (g.collectionTitle || g.collectionName || g.title || '').trim();
  const number = g.number ?? null;
  const title = base ? (number != null ? `${base} #${number}` : base) : 'Gift';

  const giftName = g.name || giftNameFallbackFromCollectionAndNumber(base, number) || null;

  const urlTelegram = giftName && String(giftName).includes('-')
    ? `https://t.me/nft/${giftName}`
    : 'https://t.me/mrkt';

  const urlMarket = g.id ? mrktLotUrlFromId(g.id) : 'https://t.me/mrkt';

  const thumbKey =
    g.modelStickerThumbnailKey ||
    g.modelStickerKey ||
    g.stickerThumbnailKey ||
    null;

  const imgUrl = giftName
    ? `/img/gift?name=${encodeURIComponent(giftName)}`
    : (thumbKey ? `/img/cdn?key=${encodeURIComponent(thumbKey)}` : null);

  return {
    id: it?.id || null,
    type,
    typeLabel: prettyEventType(type),
    title,
    priceTon: ton,
    model: g.modelTitle || g.modelName || null,
    backdrop: g.backdropName || null,
    imgUrl,
    urlTelegram,
    urlMarket,
    gift: g,
  };
}

async function notifyFeedEvent(userId, sub, ev) {
  const chatId = userChatId(userId);

  const lines = [];
  lines.push(`🔔 Подписка #${sub.num}`);
  lines.push(`Тип: ${ev.typeLabel}`);
  lines.push(`Gift: ${subGiftTitle(sub)}`);
  if (ev.priceTon != null) lines.push(`Цена: ${ev.priceTon.toFixed(3)} TON`);
  if (ev.model) lines.push(`Model: ${ev.model}`);
  if (ev.backdrop) lines.push(`Backdrop: ${ev.backdrop}`);
  lines.push(ev.urlTelegram || 'https://t.me/mrkt');

  await notifyTextOrPhoto(chatId, ev.imgUrl, lines.join('\n'), mkReplyMarkupOpen(ev.urlMarket, 'MRKT'));
}

async function processFeedForSub(userId, sub, stateKey, budget) {
  if (budget <= 0) return 0;

  const f = normalizeFilters(sub.filters || {});
  if (!f.gifts.length) return 0;

  const models = f.gifts.length === 1 ? (f.models || []) : [];
  const backdrops = f.gifts.length === 1 ? (f.backdrops || []) : [];

  const st = subStates.get(stateKey) || { feedLastId: null };

  const r = await mrktFeedFetch({
    collectionNames: f.gifts,
    modelNames: models,
    backdropNames: backdrops,
    cursor: '',
    count: MRKT_FEED_COUNT,
    ordering: 'Latest',
    types: [],
  });

  if (!r.ok) return 0;
  if (!r.items.length) return 0;

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
    if (sent >= budget) break;

    const type = String(it?.type || '').toLowerCase();
    if (!SUBS_FEED_TYPES.has(type)) continue;

    const ev = feedItemToEvent(it);
    if (!ev) continue;

    if (sub.maxNotifyTon != null && ev.priceTon != null) {
      if (Number(ev.priceTon) > Number(sub.maxNotifyTon)) continue;
    }

    await notifyFeedEvent(userId, sub, ev);
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
    let feedBudget = SUBS_FEED_MAX_EVENTS_PER_CYCLE;

    for (const [userId, user] of users.entries()) {
      if (!user.enabled) continue;

      const subs = Array.isArray(user.subscriptions) ? user.subscriptions : [];
      const active = subs.filter((s) => s && s.enabled);
      if (!active.length) continue;

      for (const sub of active) {
        processedSubs++;

        // floor watcher
        if (floorNotifs < SUBS_MAX_NOTIFICATIONS_PER_CYCLE) {
          const sf = normalizeFilters(sub.filters || {});
          if (sf.gifts.length) {
            const stateKeyFloor = `${userId}:${sub.id}:floor`;
            const prev = subStates.get(stateKeyFloor) || { floor: null, emptyStreak: 0 };

            const r = await mrktSearchLotsByFilters(sf, 1, { ordering: 'Price', lowToHigh: true, count: MRKT_COUNT });
            if (r.ok) {
              const lot = r.gifts?.length ? r.gifts[0] : null;
              const newFloor = lot ? lot.priceTon : null;

              let emptyStreak = prev.emptyStreak || 0;

              if (newFloor == null) {
                emptyStreak++;
                if (emptyStreak >= SUBS_EMPTY_CONFIRM) {
                  subStates.set(stateKeyFloor, { ...prev, emptyStreak, floor: null });
                } else {
                  subStates.set(stateKeyFloor, { ...prev, emptyStreak });
                }
              } else {
                emptyStreak = 0;

                const prevFloor = prev.floor;
                const maxNotify = sub.maxNotifyTon != null ? Number(sub.maxNotifyTon) : null;
                const canNotify = (maxNotify == null || newFloor <= maxNotify);

                if (canNotify && (prevFloor == null || Number(prevFloor) !== Number(newFloor))) {
                  await notifyFloorToUser(userId, sub, lot, newFloor);
                  floorNotifs++;
                }

                subStates.set(stateKeyFloor, { ...prev, floor: newFloor, emptyStreak });
              }
            }
          }
        }

        // feed watcher
        if (feedBudget > 0) {
          const stateKeyFeed = `${userId}:${sub.id}:feed`;
          const sent = await processFeedForSub(userId, sub, stateKeyFeed, feedBudget);
          feedBudget -= sent;
          feedNotifs += sent;
        }
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

// ===================== AutoBuy =====================
function isNoFundsError(obj) {
  const s = JSON.stringify(obj?.data || obj || '').toLowerCase();
  return s.includes('not enough') || s.includes('insufficient') || s.includes('balance');
}

async function mrktBuy({ id, priceNano }) {
  const body = { ids: [id], prices: { [id]: priceNano } };
  const r = await mrktPostJson('/gifts/buy', body);
  if (!r.ok) return { ok: false, reason: r.status === 429 ? 'RPS_WAIT' : 'BUY_ERROR', waitMs: r.waitMs || 0, data: r.data };

  const data = r.data;
  const okItem = Array.isArray(data)
    ? data.find((x) => x?.source?.type === 'buy_gift' && x?.userGift?.isMine === true && String(x?.userGift?.id || '') === String(id))
    : null;

  if (!okItem) return { ok: false, reason: 'BUY_NOT_CONFIRMED', data };
  return { ok: true, okItem, data };
}

function extractListingPriceNanoFromGift(g) {
  const nanoA = g?.salePriceWithoutFee ?? null;
  const nanoB = g?.salePrice ?? null;
  const nano = (nanoA != null && Number(nanoA) > 0) ? Number(nanoA) : (nanoB != null ? Number(nanoB) : NaN);
  return Number.isFinite(nano) && nano > 0 ? nano : null;
}

async function tryAutoBuyFromFeed(userId, sub) {
  const maxBuy = Number(sub.maxAutoBuyTon);
  if (!Number.isFinite(maxBuy) || maxBuy <= 0) return { ok: true, bought: false };

  const f = normalizeFilters(sub.filters || {});
  if (!f.gifts.length) return { ok: true, bought: false };

  const models = f.gifts.length === 1 ? (f.models || []) : [];
  const backdrops = f.gifts.length === 1 ? (f.backdrops || []) : [];

  const stateKey = `${userId}:${sub.id}:autobuy`;
  const st = subStates.get(stateKey) || { autoBuyLastId: null };

  const r = await mrktFeedFetch({
    collectionNames: f.gifts,
    modelNames: models,
    backdropNames: backdrops,
    cursor: '',
    count: 40,
    ordering: 'Latest',
    types: ['listing', 'change_price'],
  });

  if (!r.ok) return { ok: true, bought: false, rps: (r.reason === 'RPS_WAIT'), waitMs: r.waitMs || 0 };
  if (!r.items.length) return { ok: true, bought: false };

  const latestId = r.items[0]?.id || null;

  // default safe: first run doesn't buy old history, unless sub.autoBuyAny = true
  if (latestId && !st.autoBuyLastId && !sub.autoBuyAny) {
    subStates.set(stateKey, { ...st, autoBuyLastId: latestId });
    return { ok: true, bought: false };
  }

  const candidates = [];
  for (const it of r.items) {
    if (!it?.id) continue;
    if (!sub.autoBuyAny && st.autoBuyLastId && it.id === st.autoBuyLastId) break;

    const type = String(it?.type || '').toLowerCase();
    if (type !== 'listing' && type !== 'change_price') continue;

    const g = it?.gift;
    if (!g || !g.id) continue;

    const priceNano = extractListingPriceNanoFromGift(g);
    if (!priceNano) continue;

    const priceTon = priceNano / 1e9;
    if (priceTon > maxBuy) continue;

    candidates.push({ it, g, priceNano, priceTon });
  }

  if (latestId) subStates.set(stateKey, { ...st, autoBuyLastId: latestId });
  if (!candidates.length) return { ok: true, bought: false };

  candidates.sort((a, b) => a.priceTon - b.priceTon);
  const pick = candidates[0];

  const lotId = String(pick.g.id);
  const attemptKey = `${userId}:${lotId}`;
  const lastAttempt = autoBuyRecentAttempts.get(attemptKey);
  if (lastAttempt && nowMs() - lastAttempt < AUTO_BUY_ATTEMPT_TTL_MS) return { ok: true, bought: false };
  autoBuyRecentAttempts.set(attemptKey, nowMs());

  if (AUTO_BUY_DRY_RUN) return { ok: true, bought: true, dry: true, priceTon: pick.priceTon };

  const buyRes = await mrktBuy({ id: lotId, priceNano: pick.priceNano });
  if (!buyRes.ok) return { ok: true, bought: false, buyFail: buyRes };

  const base = (pick.g.collectionTitle || pick.g.collectionName || pick.g.title || f.gifts[0]).trim();
  const number = pick.g.number ?? null;
  const title = number != null ? `${base} #${number}` : base;
  const giftName = pick.g.name || giftNameFallbackFromCollectionAndNumber(base, number) || null;
  const urlTelegram = giftName ? `https://t.me/nft/${giftName}` : 'https://t.me/mrkt';
  const urlMarket = pick.g.id ? mrktLotUrlFromId(pick.g.id) : 'https://t.me/mrkt';

  const thumbKey =
    pick.g.modelStickerThumbnailKey ||
    pick.g.modelStickerKey ||
    pick.g.stickerThumbnailKey ||
    null;

  const u = getOrCreateUser(userId);
  pushPurchase(u, {
    tsBought: Date.now(),
    title,
    priceTon: pick.priceTon,
    urlTelegram,
    urlMarket,
    lotId,
    giftName: giftName || '',
    thumbKey: thumbKey || '',
    model: pick.g.modelTitle || pick.g.modelName || '',
    backdrop: pick.g.backdropName || '',
    collection: pick.g.collectionName || base,
    number: number ?? null,
  });
  scheduleSave();

  return { ok: true, bought: true, priceTon: pick.priceTon, urlTelegram, urlMarket };
}

async function autoBuyCycle() {
  if (!AUTO_BUY_GLOBAL) return;
  if (MODE !== 'real') return;
  if (!MRKT_AUTH_RUNTIME) return;
  if (isAutoBuying) return;

  if (mrktState.pauseUntil && nowMs() < mrktState.pauseUntil) return;

  let hasEligible = false;
  for (const [, user] of users.entries()) {
    const subs = Array.isArray(user.subscriptions) ? user.subscriptions : [];
    if (subs.some((s) => s && s.enabled && s.autoBuyEnabled && s.maxAutoBuyTon != null)) { hasEligible = true; break; }
  }
  if (!hasEligible) return;

  isAutoBuying = true;
  try {
    for (const [userId, user] of users.entries()) {
      if (!user.enabled) continue;

      const subs = Array.isArray(user.subscriptions) ? user.subscriptions : [];
      const eligible = subs.filter((s) => s && s.enabled && s.autoBuyEnabled && s.maxAutoBuyTon != null);
      if (!eligible.length) continue;

      let buysDone = 0;
      for (const sub of eligible) {
        if (buysDone >= AUTO_BUY_MAX_BUYS_PER_USER_PER_CYCLE) break;

        const r1 = await tryAutoBuyFromFeed(userId, sub);
        if (r1 && r1.bought) {
          buysDone++;
          if (AUTO_BUY_DRY_RUN) {
            await sendMessageSafe(userChatId(userId), `AutoBuy (DRY) OK: ${Number(r1.priceTon).toFixed(3)} TON`, { disable_web_page_preview: true });
          } else {
            await sendMessageSafe(
              userChatId(userId),
              `✅ AutoBuy OK\nЦена: ${Number(r1.priceTon).toFixed(3)} TON\n${r1.urlTelegram || ''}`,
              { disable_web_page_preview: false, reply_markup: mkReplyMarkupOpen(r1.urlMarket, 'MRKT') }
            );
          }
          if (AUTO_BUY_DISABLE_AFTER_SUCCESS) {
            sub.autoBuyEnabled = false;
            scheduleSave();
          }
          continue;
        }

        // optional: if sub.autoBuyAny -> also scan existing cheapest saling
        if (sub.autoBuyAny) {
          const maxBuy = Number(sub.maxAutoBuyTon);
          if (!Number.isFinite(maxBuy) || maxBuy <= 0) continue;

          const sf = normalizeFilters(sub.filters || {});
          if (!sf.gifts.length) continue;

          const r = await mrktSearchLotsByFilters(sf, 1, { ordering: 'Price', lowToHigh: true, count: 20 });
          if (!r.ok || !r.gifts?.length) continue;

          const candidate = r.gifts.find((x) => x.priceTon <= maxBuy) || null;
          if (!candidate) continue;

          const attemptKey = `${userId}:${candidate.id}`;
          const lastAttempt = autoBuyRecentAttempts.get(attemptKey);
          if (lastAttempt && nowMs() - lastAttempt < AUTO_BUY_ATTEMPT_TTL_MS) continue;
          autoBuyRecentAttempts.set(attemptKey, nowMs());

          if (AUTO_BUY_DRY_RUN) {
            await sendMessageSafe(userChatId(userId), `AutoBuy (DRY) кандидат: ${candidate.priceTon.toFixed(3)} TON\n${candidate.urlTelegram}`, {
              disable_web_page_preview: false,
              reply_markup: mkReplyMarkupOpen(candidate.urlMarket, 'MRKT'),
            });
            buysDone++;
            continue;
          }

          const buyRes = await mrktBuy({ id: candidate.id, priceNano: candidate.priceNano });
          if (buyRes.ok) {
            const u = getOrCreateUser(userId);
            pushPurchase(u, {
              tsBought: Date.now(),
              title: candidate.name,
              priceTon: candidate.priceTon,
              urlTelegram: candidate.urlTelegram,
              urlMarket: candidate.urlMarket,
              lotId: candidate.id,
              giftName: candidate.giftName || '',
              thumbKey: candidate.thumbKey || '',
              model: candidate.model || '',
              backdrop: candidate.backdrop || '',
              collection: candidate.collectionName || '',
              number: candidate.number ?? null,
            });
            scheduleSave();

            await sendMessageSafe(userChatId(userId), `✅ AutoBuy OK\n${candidate.priceTon.toFixed(3)} TON\n${candidate.urlTelegram}`, {
              disable_web_page_preview: false,
              reply_markup: mkReplyMarkupOpen(candidate.urlMarket, 'MRKT'),
            });
            buysDone++;

            if (AUTO_BUY_DISABLE_AFTER_SUCCESS) {
              sub.autoBuyEnabled = false;
              scheduleSave();
            }
          } else {
            if (isNoFundsError(buyRes)) {
              for (const s2 of subs) if (s2) s2.autoBuyEnabled = false;
              scheduleSave();
              await sendMessageSafe(userChatId(userId), `❌ AutoBuy stop: no funds`, { disable_web_page_preview: true });
            }
          }
        }
      }
    }
  } catch (e) {
    console.error('autoBuyCycle error:', e);
  } finally {
    isAutoBuying = false;
  }
}

// ===================== Telegram commands =====================
bot.onText(/^\/start\b/, async (msg) => {
  const u = getOrCreateUser(msg.from.id);
  u.chatId = msg.chat.id;
  scheduleSave();

  if (WEBAPP_URL) {
    try {
      await fetch(`https://api.telegram.org/bot${TELEGRAM_TOKEN}/setChatMenuButton`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          chat_id: msg.chat.id,
          menu_button: { type: 'web_app', text: APP_TITLE, web_app: { url: WEBAPP_URL } },
        }),
      });
    } catch {}
  }

  await sendMessageSafe(msg.chat.id, `Открой меню “${APP_TITLE}” (кнопка рядом со скрепкой).`, { reply_markup: MAIN_KEYBOARD });
});

bot.on('message', async (msg) => {
  const userId = msg.from?.id;
  if (!userId) return;

  const u = getOrCreateUser(userId);
  u.chatId = msg.chat.id;
  scheduleSave();

  if ((msg.text || '') === '📌 Статус') {
    const txt =
      `Status\n` +
      `MRKT_AUTH: ${MRKT_AUTH_RUNTIME ? 'YES' : 'NO'} (${maskToken(MRKT_AUTH_RUNTIME) || '-'})\n` +
      `lastFail: ${mrktState.lastFailMsg || '-'}\n` +
      `pauseUntil: ${mrktState.pauseUntil ? new Date(mrktState.pauseUntil).toLocaleTimeString('ru-RU') : '-'}\n` +
      `Redis: ${redis ? 'YES' : 'NO'}\n` +
      `Subs interval: ${SUBS_CHECK_INTERVAL_MS}ms\n` +
      `AutoBuy: ${AUTO_BUY_GLOBAL ? 'ON' : 'OFF'} ${AUTO_BUY_DRY_RUN ? '(DRY)' : ''}\n`;
    await sendMessageSafe(msg.chat.id, txt, { reply_markup: MAIN_KEYBOARD });
  }
});

// ===================== WebApp UI =====================
const WEBAPP_HTML = `<!doctype html><html lang="ru"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"/>
<title>${APP_TITLE}</title>
<style>
:root{--bg:#0b0f14;--card:#101826;--text:#e5e7eb;--muted:#9ca3af;--border:#223044;--input:#0f172a;--btn:#182235;--accent:#22c55e;--danger:#ef4444}
*{box-sizing:border-box} html,body{background:var(--bg)} body{margin:0;padding:14px;color:var(--text);font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial}
h2{margin:0 0 10px 0;font-size:18px} h3{margin:0 0 8px 0;font-size:15px}
.card{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:12px;margin:10px 0}
.row{display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end}
.field{position:relative;flex:1 1 240px;min-width:160px}
label{display:block;font-size:12px;color:var(--muted);margin-bottom:4px}
input,textarea{width:100%;padding:10px;border:1px solid var(--border);border-radius:12px;background:var(--input);color:var(--text);outline:none;font-size:13px}
button{padding:10px 12px;border:1px solid var(--border);border-radius:12px;background:var(--btn);color:var(--text);cursor:pointer}
.primary{border-color:var(--accent);background:var(--accent);color:#052e16;font-weight:950}
.small{padding:8px 10px;border-radius:10px;font-size:13px}
.tabs{display:flex;gap:8px;flex-wrap:wrap}
.tabbtn{border-radius:999px;padding:8px 12px;font-size:13px}
.tabbtn.active{border-color:var(--accent);color:var(--accent)}
.muted{color:var(--muted);font-size:12px}
#err{display:none;border-color:var(--danger);color:#ffd1d1;white-space:pre-wrap;word-break:break-word}
.grid{display:grid;gap:10px;margin-top:10px;grid-template-columns: repeat(auto-fill, minmax(170px, 1fr))}
@media (max-width: 520px){.grid{grid-template-columns: repeat(2, minmax(0, 1fr));}}
.lot{border:1px solid var(--border);border-radius:16px;padding:10px;background:rgba(255,255,255,.02);cursor:pointer}
.lot img{width:100%;aspect-ratio:1/1;object-fit:cover;border-radius:14px;border:1px solid var(--border);background:rgba(255,255,255,.03)}
.price{font-size:15px;font-weight:950;margin-top:8px}
.ellipsis{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.sug{position:absolute;left:0;right:0;top:calc(100% + 6px);background:var(--card);border:1px solid var(--border);border-radius:14px;max-height:360px;overflow:auto;z-index:9999}
.sug .it{display:flex;gap:10px;align-items:center;padding:10px;border-bottom:1px solid rgba(255,255,255,.06);cursor:pointer}
.sug .it:hover{background:rgba(255,255,255,.06)}
.thumb{width:36px;height:36px;border-radius:12px;border:1px solid var(--border);object-fit:cover;background:rgba(255,255,255,.05)}
.chips{display:flex;gap:6px;flex-wrap:wrap;margin-top:8px}
.chip{display:inline-flex;gap:6px;align-items:center;padding:6px 10px;border:1px solid var(--border);border-radius:999px;background:rgba(255,255,255,.03);font-size:12px}
.chip b{color:var(--accent)}
.chip button{padding:0 6px;border-radius:999px}
.sheetWrap{position:fixed;inset:0;background:rgba(0,0,0,.35);display:flex;align-items:flex-end;justify-content:center;z-index:50000;padding:10px;opacity:0;visibility:hidden;pointer-events:none;transition:opacity .18s ease,visibility .18s ease}
.sheetWrap.show{opacity:1;visibility:visible;pointer-events:auto}
.sheet{width:min(980px,96vw);height:min(82vh,880px);background:var(--card);border:1px solid var(--border);border-radius:22px 22px 14px 14px;padding:12px;box-shadow:0 30px 90px rgba(0,0,0,.5);display:flex;flex-direction:column;gap:10px}
.sheetBody{overflow:auto}
.pillRow{display:flex;gap:8px;flex-wrap:wrap}
.pill{display:inline-flex;gap:6px;align-items:center;padding:7px 10px;border:1px solid var(--border);border-radius:999px;background:rgba(255,255,255,.03);color:var(--text);text-decoration:none;font-size:12px}
.pill b{color:var(--accent)}
.saleCard{border:1px solid var(--border);border-radius:12px;padding:10px;background:rgba(255,255,255,.02);margin-top:8px}
.saleRow{display:flex;gap:10px;align-items:center}
.saleRow img{width:56px;height:56px;border-radius:14px;border:1px solid rgba(255,255,255,.12);object-fit:cover;background:rgba(255,255,255,.03);flex:0 0 auto}
.hr{height:1px;background:var(--border);margin:10px 0}
</style></head><body>
<h2>${APP_TITLE}</h2>
<div id="err" class="card"></div>

<div class="tabs">
  <button class="tabbtn active" data-tab="market">Market</button>
  <button class="tabbtn" data-tab="subs">Subscriptions</button>
  <button class="tabbtn" data-tab="profile">Profile</button>
  <button class="tabbtn" data-tab="admin" id="adminTabBtn" style="display:none">Admin</button>
</div>

<div id="market" class="card">
  <h3>Фильтры</h3>
  <div class="row">
    <div class="field">
      <label>Gifts (мульти)</label>
      <input id="giftInp" placeholder="Нажми и начни вводить..."/>
      <div id="giftSug" class="sug" style="display:none"></div>
      <div id="giftChips" class="chips"></div>
    </div>
    <div class="field">
      <label>Model (только если 1 gift)</label>
      <input id="modelInp" placeholder="Нажми и начни вводить..."/>
      <div id="modelSug" class="sug" style="display:none"></div>
      <div id="modelChips" class="chips"></div>
    </div>
    <div class="field">
      <label>Backdrop (только если 1 gift)</label>
      <input id="backInp" placeholder="Нажми и начни вводить..."/>
      <div id="backSug" class="sug" style="display:none"></div>
      <div id="backChips" class="chips"></div>
    </div>
    <div class="field">
      <label>Number prefix</label>
      <input id="numPrefix" placeholder="например: 4093"/>
    </div>
  </div>
  <div class="row" style="margin-top:10px">
    <button id="save" class="primary">Сохранить</button>
    <button id="reload" class="small">Обновить лоты</button>
    <button id="salesBtn" class="small">История продаж</button>
  </div>
  <div id="note" class="muted" style="margin-top:10px"></div>
  <div id="lots" class="grid"></div>
</div>

<div id="subs" class="card" style="display:none">
  <h3>Подписки</h3>
  <div class="row">
    <button id="subCreate" class="primary">➕ Создать из текущих фильтров</button>
    <button id="subCheck" class="small">🔄 Проверить сейчас</button>
    <button id="subReload" class="small">Обновить список</button>
  </div>
  <div id="subsList" style="margin-top:10px"></div>
</div>

<div id="profile" class="card" style="display:none">
  <h3>Покупки</h3>
  <div id="profileList" class="muted">Загрузка…</div>
</div>

<div id="admin" class="card" style="display:none">
  <h3>Admin</h3>
  <div class="muted">MRKT session payload (JSON из DevTools WebApp MRKT)</div>
  <textarea id="payloadJson" rows="6" placeholder='{"data":"user=...&hash=...","photo":null}'></textarea>
  <div class="row" style="margin-top:10px">
    <button id="sessSave" class="primary">Save session + refresh token</button>
    <button id="tokRefresh" class="small">Refresh token</button>
    <button id="testMrkt" class="small">Test MRKT</button>
  </div>
  <pre id="adminStatus" class="muted" style="white-space:pre-wrap;margin-top:10px"></pre>
</div>

<div class="sheetWrap" id="sheetWrap">
  <div class="sheet">
    <div class="muted" id="sheetTop"></div>
    <div class="row">
      <button id="sheetClose" class="small">Закрыть</button>
      <button id="sheetOpenTg" class="small">Telegram</button>
      <button id="sheetOpenMrkt" class="small">MRKT</button>
      <button id="sheetApply" class="primary">Применить фильтры</button>
    </div>
    <div class="sheetBody" id="sheetBody"></div>
  </div>
</div>

<script src="/app.js"></script>
</body></html>`;

const WEBAPP_JS = `
(function(){
  const tg = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;
  const initData = tg ? (tg.initData || '') : '';

  const el = (id)=>document.getElementById(id);
  const errBox = el('err');

  function showErr(msg){
    errBox.style.display='block';
    errBox.textContent = String(msg||'');
  }
  function clrErr(){ errBox.style.display='none'; errBox.textContent=''; }

  // IMPORTANT: NO_INIT_DATA => open inside Telegram
  if (!tg || !initData) {
    showErr(
      'Открой панель внутри Telegram (WebApp).\\n\\n' +
      'Шаги: открой бота → /start → кнопка меню “${APP_TITLE}”.\\n' +
      'Если открыть в обычном браузере — initData не передаётся.'
    );
    return;
  }

  try { tg.ready(); tg.expand(); } catch(e){}

  async function api(path, opts){
    const r = await fetch(path, {
      method: (opts && opts.method) || 'GET',
      headers: {
        'Content-Type':'application/json',
        'x-tg-init-data': initData,
      },
      body: opts && opts.body ? opts.body : undefined,
    });
    const j = await r.json().catch(()=>({ok:false,reason:'BAD_JSON'}));
    if (!r.ok || j.ok===false) {
      const reason = j && (j.reason || j.error) ? (j.reason || j.error) : ('HTTP '+r.status);
      throw new Error(reason);
    }
    return j;
  }

  function setTab(name){
    for (const btn of document.querySelectorAll('.tabbtn')) {
      btn.classList.toggle('active', btn.dataset.tab===name);
    }
    for (const id of ['market','subs','profile','admin']) {
      el(id).style.display = (id===name)?'block':'none';
    }
  }
  document.querySelectorAll('.tabbtn').forEach(b=>{
    b.addEventListener('click', ()=> setTab(b.dataset.tab));
  });

  // local UI state
  let state = null;
  let currentLots = [];
  let sheetLot = null;

  function uniqAdd(arr, v){
    const t = String(v||'').trim();
    if (!t) return arr;
    const key = t.toLowerCase();
    if (arr.some(x=>String(x).toLowerCase()===key)) return arr;
    arr.push(t);
    return arr;
  }
  function removeAt(arr, idx){ arr.splice(idx,1); return arr; }

  function renderChips(container, arr, kind){
    container.innerHTML = '';
    arr.forEach((v, idx)=>{
      const d = document.createElement('div');
      d.className='chip';
      d.innerHTML = '<b>'+kind+'</b> '+v+' <button class="small" data-x="'+idx+'">×</button>';
      d.querySelector('button').onclick = (e)=>{
        removeAt(arr, Number(e.target.dataset.x));
        rerenderAllChips();
        refreshLots(true).catch(()=>{});
      };
      container.appendChild(d);
    });
  }

  function rerenderAllChips(){
    renderChips(el('giftChips'), ui.gifts, 'Gift');
    renderChips(el('modelChips'), ui.models, 'Model');
    renderChips(el('backChips'), ui.backdrops, 'Backdrop');
  }

  const ui = { gifts: [], models: [], backdrops: [] };

  function sugBoxSet(box, items, onPick){
    box.style.display = items.length ? 'block' : 'none';
    box.innerHTML = '';
    for (const it of items) {
      const row = document.createElement('div');
      row.className='it';
      row.innerHTML =
        (it.imgUrl?('<img class="thumb" src="'+it.imgUrl+'"/>'):'<div class="thumb"></div>') +
        '<div style="flex:1">' +
          '<div>'+it.label+'</div>' +
          (it.sub?('<div class="muted">'+it.sub+'</div>'):'') +
        '</div>';
      row.onclick = ()=> onPick(it);
      box.appendChild(row);
    }
  }

  async function refreshState(){
    state = await api('/api/state');
    const isAdmin = !!(state.api && state.api.isAdmin);
    el('adminTabBtn').style.display = isAdmin ? 'inline-block' : 'none';

    const f = (state.user && state.user.filters) ? state.user.filters : {};
    ui.gifts = (f.gifts||[]).slice();
    ui.models = (f.models||[]).slice();
    ui.backdrops = (f.backdrops||[]).slice();
    el('numPrefix').value = f.numberPrefix || '';
    rerenderAllChips();
  }

  async function saveFilters(){
    const filters = {
      gifts: ui.gifts.slice(),
      models: ui.models.slice(),
      backdrops: ui.backdrops.slice(),
      numberPrefix: el('numPrefix').value.trim(),
      giftLabels: (state && state.user && state.user.filters && state.user.filters.giftLabels) ? state.user.filters.giftLabels : {},
    };
    await api('/api/state/patch', { method:'POST', body: JSON.stringify({ filters }) });
  }

  async function refreshLots(force){
    const r = await api('/api/mrkt/lots' + (force?'?force=1':''));
    currentLots = r.lots || [];
    el('note').textContent = r.note ? r.note : '';
    el('lots').innerHTML = currentLots.map(l=>{
      const img = l.imgUrl ? '<img src="'+l.imgUrl+'"/>' : '<div style="height:140px"></div>';
      return (
        '<div class="lot" data-id="'+l.id+'">' +
          img +
          '<div class="price">'+Number(l.priceTon).toFixed(3)+' TON</div>' +
          '<div class="muted ellipsis">'+(l.name||'')+'</div>' +
          (l.model?('<div class="muted ellipsis">Model: '+l.model+'</div>'):'') +
          (l.backdrop?('<div class="muted ellipsis">Backdrop: '+l.backdrop+'</div>'):'') +
        '</div>'
      );
    }).join('') || '<div class="muted">Нет лотов</div>';
  }

  // suggestions
  let sugTimers = { gift: null, model: null, back: null };

  async function loadGiftSug(q){
    const r = await api('/api/mrkt/collections?q='+encodeURIComponent(q||''));
    const items = r.items || [];
    sugBoxSet(el('giftSug'), items, (it)=>{
      uniqAdd(ui.gifts, it.value);
      // if more than 1 gift => clear models/backdrops
      if (ui.gifts.length !== 1) { ui.models = []; ui.backdrops = []; }
      rerenderAllChips();
      el('giftSug').style.display='none';
      refreshLots(true).catch(()=>{});
    });
  }

  async function loadModelSug(q){
    if (ui.gifts.length !== 1) { el('modelSug').style.display='none'; return; }
    const gift = ui.gifts[0];
    const r = await api('/api/mrkt/suggest?kind=model&gift='+encodeURIComponent(gift)+'&q='+encodeURIComponent(q||''));
    const items = r.items || [];
    sugBoxSet(el('modelSug'), items, (it)=>{
      uniqAdd(ui.models, it.value);
      rerenderAllChips();
      el('modelSug').style.display='none';
      refreshLots(true).catch(()=>{});
    });
  }

  async function loadBackSug(q){
    if (ui.gifts.length !== 1) { el('backSug').style.display='none'; return; }
    const gift = ui.gifts[0];
    const r = await api('/api/mrkt/suggest?kind=backdrop&gift='+encodeURIComponent(gift)+'&q='+encodeURIComponent(q||''));
    const items = (r.items||[]).map(x=>({label:x.label,value:x.value,sub:x.colorHex?('color: '+x.colorHex):null,imgUrl:null}));
    sugBoxSet(el('backSug'), items, (it)=>{
      uniqAdd(ui.backdrops, it.value);
      rerenderAllChips();
      el('backSug').style.display='none';
      refreshLots(true).catch(()=>{});
    });
  }

  function debounce(kind, fn){
    if (sugTimers[kind]) clearTimeout(sugTimers[kind]);
    sugTimers[kind] = setTimeout(()=>fn().catch(e=>showErr(e.message||String(e))), 160);
  }

  el('giftInp').addEventListener('input', ()=> debounce('gift', ()=>loadGiftSug(el('giftInp').value)));
  el('giftInp').addEventListener('focus', ()=> debounce('gift', ()=>loadGiftSug(el('giftInp').value)));
  el('modelInp').addEventListener('input', ()=> debounce('model', ()=>loadModelSug(el('modelInp').value)));
  el('modelInp').addEventListener('focus', ()=> debounce('model', ()=>loadModelSug(el('modelInp').value)));
  el('backInp').addEventListener('input', ()=> debounce('back', ()=>loadBackSug(el('backInp').value)));
  el('backInp').addEventListener('focus', ()=> debounce('back', ()=>loadBackSug(el('backInp').value)));

  document.addEventListener('click', (e)=>{
    if (!e.target.closest('#giftSug') && e.target!==el('giftInp')) el('giftSug').style.display='none';
    if (!e.target.closest('#modelSug') && e.target!==el('modelInp')) el('modelSug').style.display='none';
    if (!e.target.closest('#backSug') && e.target!==el('backInp')) el('backSug').style.display='none';
  });

  // sheet
  function openSheet(lot){
    sheetLot = lot;
    el('sheetWrap').classList.add('show');

    const pills = [];
    pills.push('<a class="pill" href="#" data-pill="gift"><b>Gift</b> '+(lot.collectionName||'')+'</a>');
    if (lot.model) pills.push('<a class="pill" href="#" data-pill="model"><b>Model</b> '+lot.model+'</a>');
    if (lot.backdrop) pills.push('<a class="pill" href="#" data-pill="backdrop"><b>Backdrop</b> '+lot.backdrop+'</a>');

    el('sheetTop').innerHTML =
      '<div><b style="font-size:15px">'+(lot.name||'')+'</b></div>' +
      '<div class="muted">Цена: <b style="color:var(--accent)">'+Number(lot.priceTon).toFixed(3)+' TON</b></div>' +
      '<div class="pillRow">'+pills.join('')+'</div>';

    el('sheetBody').innerHTML =
      '<div class="muted">Offers/History: загрузка…</div>';

    // load details + sales
    (async()=>{
      try{
        const r = await api('/api/lot/details', { method:'POST', body: JSON.stringify({ lot }) });
        const off = r.offers || {};
        let html = '';
        html += '<div class="muted">Max offer (exact): <b style="color:var(--accent)">'+(off.exact!=null?Number(off.exact).toFixed(3):'—')+' TON</b></div>';
        html += '<div class="muted">Max offer (collection): <b style="color:var(--accent)">'+(off.collection!=null?Number(off.collection).toFixed(3):'—')+' TON</b></div>';
        html += '<div class="hr"></div>';
        html += '<button class="small" id="loadSales">Загрузить историю продаж</button>';
        html += '<div id="salesBox" class="muted" style="margin-top:10px">—</div>';
        el('sheetBody').innerHTML = html;

        const btn = document.getElementById('loadSales');
        if (btn) btn.onclick = async()=>{
          const sb = document.getElementById('salesBox');
          sb.textContent = 'Загрузка…';
          try{
            const rr = await api('/api/lot/sales', { method:'POST', body: JSON.stringify({ lot }) });
            const sh = rr.salesHistory || rr;
            const sales = sh.sales || [];
            const med = sh.approxPriceTon!=null ? Number(sh.approxPriceTon).toFixed(3)+' TON' : '—';
            let out = '<div class="muted">Медиана: <b style="color:var(--accent)">'+med+'</b> · n='+(sales.length||0)+'</div>';
            if (!sales.length) out += '<div class="muted" style="margin-top:8px">Нет данных</div>';
            else {
              for (const s of sales.slice(0, 30)) {
                const img = s.imgUrl ? '<img src="'+s.imgUrl+'"/>' : '<div style="width:56px;height:56px"></div>';
                out += '<div class="saleCard"><div class="saleRow">'+img+
                  '<div style="flex:1">' +
                    '<div><b>'+Number(s.priceTon).toFixed(3)+' TON</b></div>' +
                    '<div class="muted">'+(s.ts ? new Date(s.ts).toLocaleString('ru-RU') : '')+'</div>' +
                    '<div class="muted ellipsis">'+(s.title||'')+'</div>' +
                  '</div>' +
                '</div></div>';
              }
            }
            sb.innerHTML = out;
          }catch(e){ sb.textContent = 'Ошибка: '+(e.message||String(e)); }
        };
      }catch(e){
        el('sheetBody').innerHTML = '<div class="muted">Ошибка: '+(e.message||String(e))+'</div>';
      }
    })();

    el('sheetOpenTg').onclick = ()=> window.open(lot.urlTelegram || 'https://t.me/mrkt', '_blank');
    el('sheetOpenMrkt').onclick = ()=> window.open(lot.urlMarket || 'https://t.me/mrkt', '_blank');

    el('sheetApply').onclick = async()=>{
      try{
        ui.gifts = lot.collectionName ? [lot.collectionName] : [];
        ui.models = lot.model ? [lot.model] : [];
        ui.backdrops = lot.backdrop ? [lot.backdrop] : [];
        rerenderAllChips();
        await saveFilters();
        await refreshLots(true);
        closeSheet();
      }catch(e){ showErr(e.message||String(e)); }
    };
  }
  function closeSheet(){ el('sheetWrap').classList.remove('show'); sheetLot = null; }
  el('sheetClose').onclick = closeSheet;
  el('sheetWrap').addEventListener('click', (e)=>{ if(e.target===el('sheetWrap')) closeSheet(); });

  el('lots').addEventListener('click', (e)=>{
    const card = e.target.closest('.lot');
    if (!card) return;
    const id = card.dataset.id;
    const lot = currentLots.find(x=>String(x.id)===String(id));
    if (lot) openSheet(lot);
  });

  // subs
  function subCard(s){
    const f = s.filters || {};
    const gifts = (f.gifts||[]).join(', ') || '(no gifts)';
    const models = (f.models||[]).join(', ');
    const backs = (f.backdrops||[]).join(', ');
    const maxN = (s.maxNotifyTon==null)?'∞':s.maxNotifyTon;
    const maxA = (s.maxAutoBuyTon==null)?'-':s.maxAutoBuyTon;
    const any = s.autoBuyAny ? 'ANY' : 'NEW';

    return (
      '<div class="card" style="margin:10px 0">' +
        '<div><b>#'+s.num+'</b> '+(s.enabled?'ON':'OFF')+'</div>' +
        '<div class="muted">Gifts: '+gifts+'</div>' +
        (models?'<div class="muted">Models: '+models+'</div>':'') +
        (backs?'<div class="muted">Backdrops: '+backs+'</div>':'') +
        (f.numberPrefix?'<div class="muted">Number prefix: '+f.numberPrefix+'</div>':'') +
        '<div class="muted">Notify max: '+maxN+' TON</div>' +
        '<div class="muted">AutoBuy: '+(s.autoBuyEnabled?'ON':'OFF')+' / max: '+maxA+' TON / mode: '+any+'</div>' +
        '<div class="row" style="margin-top:10px">' +
          '<button class="small" data-act="tog" data-id="'+s.id+'">'+(s.enabled?'⏸':'▶️')+'</button>' +
          '<button class="small" data-act="del" data-id="'+s.id+'">🗑</button>' +
          '<button class="small" data-act="nmax" data-id="'+s.id+'">Notify max</button>' +
          '<button class="small" data-act="atog" data-id="'+s.id+'">AutoBuy</button>' +
          '<button class="small" data-act="amax" data-id="'+s.id+'">Auto max</button>' +
          '<button class="small" data-act="any" data-id="'+s.id+'">Mode</button>' +
        '</div>' +
      '</div>'
    );
  }

  async function refreshSubs(){
    await refreshState();
    const subs = (state.user && state.user.subscriptions) ? state.user.subscriptions : [];
    el('subsList').innerHTML = subs.length ? subs.map(subCard).join('') : '<div class="muted">Подписок нет</div>';
  }

  el('subsList').addEventListener('click', (e)=>{
    const btn = e.target.closest('button[data-act]');
    if (!btn) return;
    const act = btn.dataset.act;
    const id = btn.dataset.id;
    (async()=>{
      try{
        if (act==='tog') await api('/api/sub/toggle', {method:'POST', body: JSON.stringify({id})});
        if (act==='del') await api('/api/sub/delete', {method:'POST', body: JSON.stringify({id})});
        if (act==='nmax') {
          const v = prompt('Max Notify TON (пусто = без лимита):','');
          await api('/api/sub/set_notify_max', {method:'POST', body: JSON.stringify({id, maxNotifyTon: v})});
        }
        if (act==='atog') await api('/api/sub/toggle_autobuy', {method:'POST', body: JSON.stringify({id})});
        if (act==='amax') {
          const v = prompt('Max AutoBuy TON:', '');
          await api('/api/sub/set_autobuy_max', {method:'POST', body: JSON.stringify({id, maxAutoBuyTon: v})});
        }
        if (act==='any') await api('/api/sub/toggle_autobuy_any', {method:'POST', body: JSON.stringify({id})});
        await refreshSubs();
      }catch(e){ showErr(e.message||String(e)); }
    })();
  });

  // buttons
  el('save').onclick = async()=>{
    try{
      clrErr();
      // enforce: if gifts != 1 => clear model/back
      if (ui.gifts.length !== 1) { ui.models = []; ui.backdrops = []; rerenderAllChips(); }
      await saveFilters();
      await refreshLots(true);
    }catch(e){ showErr(e.message||String(e)); }
  };
  el('reload').onclick = ()=> refreshLots(true).catch(e=>showErr(e.message||String(e)));

  el('salesBtn').onclick = async()=>{
    try{
      const r = await api('/api/mrkt/sales_by_filters?force=1');
      alert('Медиана: '+(r.approxPriceTon!=null?Number(r.approxPriceTon).toFixed(3)+' TON':'—')+'\\nПродаж: '+(r.sales||[]).length);
    }catch(e){ showErr(e.message||String(e)); }
  };

  el('subCreate').onclick = async()=>{ try{ await api('/api/sub/create',{method:'POST'}); await refreshSubs(); }catch(e){showErr(e.message||String(e));} };
  el('subReload').onclick = async()=>{ try{ await refreshSubs(); }catch(e){showErr(e.message||String(e));} };
  el('subCheck').onclick = async()=>{ try{ const r = await api('/api/sub/check_now',{method:'POST'}); alert('Проверено: '+r.processedSubs+'\\nФлор: '+r.floorNotifs+'\\nСобытий: '+r.feedNotifs); }catch(e){showErr(e.message||String(e));} };

  // profile
  async function refreshProfile(){
    const r = await api('/api/profile');
    const items = r.purchases || [];
    if (!items.length) { el('profileList').textContent = 'Покупок нет'; return; }
    let html = '';
    for (const p of items) {
      html += '<div class="card" style="margin:10px 0">' +
        '<div><b>'+p.title+'</b></div>' +
        '<div class="muted">Цена: '+Number(p.priceTon).toFixed(3)+' TON</div>' +
        (p.boughtMsk?'<div class="muted">'+p.boughtMsk+'</div>':'') +
        (p.urlTelegram?'<div class="muted"><a style="color:var(--accent)" href="'+p.urlTelegram+'" target="_blank">Telegram</a></div>':'') +
      '</div>';
    }
    el('profileList').innerHTML = html;
  }

  // admin
  async function refreshAdmin(){
    const r = await api('/api/admin/status');
    el('adminStatus').textContent = JSON.stringify(r, null, 2);
  }
  el('sessSave').onclick = async()=>{
    try{
      const payloadJson = el('payloadJson').value.trim();
      if (!payloadJson) throw new Error('Вставь payload JSON');
      const r = await api('/api/admin/session', {method:'POST', body: JSON.stringify({payloadJson})});
      alert('OK token='+(r.tokenMask||''));
      await refreshAdmin();
    }catch(e){ showErr(e.message||String(e)); }
  };
  el('tokRefresh').onclick = async()=>{
    try{
      const r = await api('/api/admin/refresh_token', {method:'POST', body: '{}'});
      alert('OK token='+(r.tokenMask||''));
      await refreshAdmin();
    }catch(e){ showErr(e.message||String(e)); }
  };
  el('testMrkt').onclick = async()=>{
    try{
      const r = await api('/api/admin/test_mrkt');
      alert('Collections: '+r.collectionsCount+'\\nToken: '+(r.tokenMask||'-')+'\\nFail: '+(r.lastFail||'-'));
      await refreshAdmin();
    }catch(e){ showErr(e.message||String(e)); }
  };

  async function refreshAll(forceLots){
    await refreshState();
    await refreshLots(!!forceLots);
    await refreshSubs();
    await refreshProfile();
    if (state.api && state.api.isAdmin) { try{ await refreshAdmin(); }catch(e){} }
  }

  refreshAll(true).catch(e=>showErr(e.message||String(e)));
})();`;

// ===================== Express =====================
const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '4mb' }));

app.get('/health', (req, res) => res.status(200).send('ok'));
app.get('/favicon.ico', (req, res) => res.status(204).end());

function auth(req, res, next) {
  const initData = String(req.headers['x-tg-init-data'] || '');
  const v = verifyTelegramWebAppInitData(initData);
  if (!v.ok) return res.status(401).json({ ok: false, reason: v.reason });
  req.userId = v.userId;
  req.tgUser = v.user || null;
  next();
}

// images proxy
app.get('/img/cdn', async (req, res) => {
  const key = String(req.query.key || '').trim();
  if (!key) return res.status(400).send('no key');
  const url = joinUrl(MRKT_CDN_BASE, key);
  const r = await fetchWithTimeout(url, { method: 'GET', headers: { Accept: 'image/*' } }, 8000).catch(() => null);
  if (!r || !r.ok) return res.status(404).send('not found');
  streamFetchToRes(r, res, 'image/webp');
});

app.get('/img/gift', async (req, res) => {
  const name = String(req.query.name || '').trim();
  if (!name) return res.status(400).send('no name');
  const url = fragmentGiftRemoteUrlFromGiftName(name);
  const r = await fetchWithTimeout(url, { method: 'GET' }, 8000).catch(() => null);
  if (!r || !r.ok) return res.status(404).send('not found');
  streamFetchToRes(r, res, 'image/jpeg');
});

// UI
app.get('/', (req, res) => { res.setHeader('Content-Type','text/html; charset=utf-8'); res.send(WEBAPP_HTML); });
app.get('/app.js', (req, res) => { res.setHeader('Content-Type','application/javascript; charset=utf-8'); res.send(WEBAPP_JS); });

// state
app.get('/api/state', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const sess = mrktSessionRuntime || (await loadMrktSessionFromRedis());

  res.json({
    ok: true,
    api: {
      isAdmin: isAdmin(req.userId),
      mrktAuthSet: !!MRKT_AUTH_RUNTIME,
      mrktAuthMask: MRKT_AUTH_RUNTIME ? maskToken(MRKT_AUTH_RUNTIME) : null,
      mrktSessionSaved: !!sess,
      mrktLastFail: mrktState.lastFailMsg || null,
      mrktPauseUntil: mrktState.pauseUntil || 0,
      refresh: { ...mrktAuthDebug },
    },
    user: {
      enabled: !!u.enabled,
      filters: u.filters,
      subscriptions: u.subscriptions || [],
      purchasesCount: (u.purchases || []).length,
    },
    tgUser: req.tgUser || null,
  });
});

app.post('/api/state/patch', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const b = req.body || {};
  if (b.filters && typeof b.filters === 'object') u.filters = normalizeFilters(b.filters);
  scheduleSave();
  res.json({ ok: true });
});

// MRKT collections + suggest
app.get('/api/mrkt/collections', auth, async (req, res) => {
  const q = String(req.query.q || '').trim().toLowerCase();
  const all = await mrktGetCollections();

  const filtered = all
    .filter((x) => !q || x.title.toLowerCase().includes(q) || x.name.toLowerCase().includes(q))
    .slice(0, WEBAPP_SUGGEST_LIMIT);

  const items = filtered.map((x) => {
    const imgUrl = x.thumbKey ? `/img/cdn?key=${encodeURIComponent(x.thumbKey)}` : null;
    const floorTon = x.floorNano != null ? tonFromNano(x.floorNano) : null;
    return {
      label: x.title || x.name,
      value: x.name,
      imgUrl,
      sub: floorTon != null ? `floor: ${floorTon.toFixed(3)} TON` : null,
    };
  });

  res.json({ ok: true, items });
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
      .map((m) => {
        const imgUrl = m.thumbKey ? `/img/cdn?key=${encodeURIComponent(m.thumbKey)}` : null;
        const floor = m.floorNano != null ? tonFromNano(m.floorNano) : null;
        const rarityPct =
          m.rarityPerMille != null && Number.isFinite(Number(m.rarityPerMille))
            ? (Number(m.rarityPerMille) / 10).toFixed(1) + '%'
            : null;
        const sub =
          (floor != null ? `min: ${floor.toFixed(3)} TON` : 'min: —') +
          (rarityPct ? ` · rarity: ${rarityPct}` : '');
        return { label: m.name, value: m.name, imgUrl, sub };
      });
    return res.json({ ok: true, items });
  }

  if (kind === 'backdrop') {
    if (!gift) return res.json({ ok: true, items: [] });
    const backs = await mrktGetBackdropsForGift(gift);
    const items = backs
      .filter((b) => !q || b.name.toLowerCase().includes(q))
      .slice(0, WEBAPP_SUGGEST_LIMIT)
      .map((b) => ({ label: b.name, value: b.name, colorHex: b.centerHex || null }));
    return res.json({ ok: true, items });
  }

  res.status(400).json({ ok: false, reason: 'BAD_KIND' });
});

// lots
app.get('/api/mrkt/lots', auth, async (req, res) => {
  const force = req.query.force != null;
  const u = getOrCreateUser(req.userId);
  const f = normalizeFilters(u.filters || {});
  const cacheKey = `lots|${req.userId}|${JSON.stringify(f)}`;
  const cached = lotsCache.get(cacheKey);
  if (!force && cached && nowMs() - cached.time < LOTS_CACHE_TTL_MS) return res.json(cached.data);

  let lots = [];
  let note = null;

  if (!f.gifts.length) {
    const r = await mrktGlobalCheapestLotsReal();
    note = r.note || null;
    lots = (r.lots || []).slice(0, WEBAPP_LOTS_LIMIT);
  } else {
    const r = await mrktSearchLotsByFilters(f, WEBAPP_LOTS_PAGES || MRKT_PAGES, { ordering: 'Price', lowToHigh: true, count: MRKT_COUNT });
    if (!r.ok) {
      note = `MRKT RPS limit, wait ${fmtWaitMs(r.waitMs || 1000)}`;
      lots = [];
    } else {
      lots = (r.gifts || []).slice(0, WEBAPP_LOTS_LIMIT);
    }
  }

  const mapped = lots.map((lot) => ({
    ...lot,
    imgUrl: lot.giftName ? `/img/gift?name=${encodeURIComponent(lot.giftName)}` : (lot.thumbKey ? `/img/cdn?key=${encodeURIComponent(lot.thumbKey)}` : null),
  }));

  const payload = { ok: true, lots: mapped, note };
  if (!force) lotsCache.set(cacheKey, { time: nowMs(), data: payload });
  res.json(payload);
});

// lot details (offers)
app.post('/api/lot/details', auth, async (req, res) => {
  const lot = req.body?.lot || null;
  const lotId = String(lot?.id || '').trim();
  if (!lotId) return res.status(400).json({ ok: false, reason: 'NO_LOT' });

  const cached = detailsCache.get(lotId);
  if (cached && nowMs() - cached.time < DETAILS_CACHE_TTL_MS) return res.json(cached.data);

  const gift = String(lot?.collectionName || '').trim();
  const model = lot?.model ? String(lot.model) : null;
  const backdrop = lot?.backdrop ? String(lot.backdrop) : null;

  let offerExact = null;
  let offerCollection = null;

  if (gift) {
    const o1 = await mrktOrdersFetch({ gift, model, backdrop });
    if (o1.ok) offerExact = maxOfferTonFromOrders(o1.orders);

    const o2 = await mrktOrdersFetch({ gift, model: null, backdrop: null });
    if (o2.ok) offerCollection = maxOfferTonFromOrders(o2.orders);
  }

  const payload = { ok: true, offers: { exact: offerExact, collection: offerCollection } };
  detailsCache.set(lotId, { time: nowMs(), data: payload });
  res.json(payload);
});

// sales by filters
app.get('/api/mrkt/sales_by_filters', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const f = normalizeFilters(u.filters || {});
  if (f.gifts.length !== 1) {
    return res.json({ ok: true, approxPriceTon: null, sales: [], note: 'Выбери ровно 1 gift.', waitMs: 0 });
  }
  const gift = f.gifts[0];
  const out = await mrktFeedSales({ gift, modelNames: f.models, backdropNames: f.backdrops });
  res.json(out);
});

// lot sales
app.post('/api/lot/sales', auth, async (req, res) => {
  const lot = req.body?.lot || null;
  const gift = String(lot?.collectionName || '').trim();
  const model = lot?.model ? String(lot.model) : null;
  const backdrop = lot?.backdrop ? String(lot.backdrop) : null;

  if (!gift) return res.json({ ok: true, salesHistory: { ok: true, approxPriceTon: null, sales: [], note: 'NO_GIFT', waitMs: 0 } });
  const out = await mrktFeedSales({ gift, modelNames: model ? [model] : [], backdropNames: backdrop ? [backdrop] : [] });
  res.json({ ok: true, salesHistory: out });
});

// Profile
app.get('/api/profile', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const purchases = (u.purchases || []).slice(0, 120).map((p) => ({
    title: p.title,
    lotId: p.lotId || null,
    priceTon: p.priceTon,
    boughtMsk: p.tsBought ? new Date(p.tsBought).toLocaleString('ru-RU', { timeZone: 'Europe/Moscow' }) : null,
    latencyMs: p.latencyMs ?? null,
    urlTelegram: p.urlTelegram || null,
    urlMarket: p.urlMarket || null,
  }));
  res.json({ ok: true, user: req.tgUser, purchases });
});

// ===================== Subs endpoints =====================
app.post('/api/sub/create', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const r = makeSubFromCurrentFilters(u);
  if (!r.ok) return res.status(400).json({ ok: false, reason: r.reason });

  u.subscriptions.push(r.sub);
  renumberSubs(u);
  scheduleSave();

  // immediate notify floor (so user sees it works)
  setTimeout(async () => {
    try {
      const sf = normalizeFilters(r.sub.filters || {});
      const rLots = await mrktSearchLotsByFilters(sf, 1, { ordering: 'Price', lowToHigh: true, count: MRKT_COUNT });
      const lot = (rLots.ok && rLots.gifts && rLots.gifts[0]) ? rLots.gifts[0] : null;
      if (!lot) return;
      await notifyFloorToUser(req.userId, r.sub, lot, lot.priceTon);
    } catch (e) {
      console.error('sub create notify error:', e?.message || e);
    }
  }, 150);

  res.json({ ok: true });
});

app.post('/api/sub/check_now', auth, async (req, res) => {
  const st = await checkSubscriptionsForAllUsers({ manual: true });
  res.json({ ok: true, ...st });
});

app.post('/api/sub/toggle', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const s = findSub(u, String(req.body?.id || ''));
  if (!s) return res.status(404).json({ ok: false, reason: 'NOT_FOUND' });
  s.enabled = !s.enabled;
  scheduleSave();
  res.json({ ok: true });
});

app.post('/api/sub/delete', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const id = String(req.body?.id || '');
  u.subscriptions = (u.subscriptions || []).filter((x) => x && x.id !== id);
  renumberSubs(u);
  scheduleSave();
  res.json({ ok: true });
});

app.post('/api/sub/set_notify_max', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const s = findSub(u, String(req.body?.id || ''));
  if (!s) return res.status(404).json({ ok: false, reason: 'NOT_FOUND' });

  const str = String(req.body?.maxNotifyTon ?? '').trim();
  if (!str) { s.maxNotifyTon = null; scheduleSave(); return res.json({ ok: true }); }

  const v = parseTonInput(str);
  if (!Number.isFinite(v) || v <= 0) return res.status(400).json({ ok: false, reason: 'BAD_MAX' });

  s.maxNotifyTon = v;
  scheduleSave();
  res.json({ ok: true });
});

app.post('/api/sub/toggle_autobuy', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const s = findSub(u, String(req.body?.id || ''));
  if (!s) return res.status(404).json({ ok: false, reason: 'NOT_FOUND' });
  s.autoBuyEnabled = !s.autoBuyEnabled;
  scheduleSave();
  res.json({ ok: true });
});

app.post('/api/sub/set_autobuy_max', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const s = findSub(u, String(req.body?.id || ''));
  if (!s) return res.status(404).json({ ok: false, reason: 'NOT_FOUND' });

  const v = parseTonInput(req.body?.maxAutoBuyTon);
  if (!Number.isFinite(v) || v <= 0) return res.status(400).json({ ok: false, reason: 'BAD_MAX' });

  s.maxAutoBuyTon = v;
  scheduleSave();
  res.json({ ok: true });
});

app.post('/api/sub/toggle_autobuy_any', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const s = findSub(u, String(req.body?.id || ''));
  if (!s) return res.status(404).json({ ok: false, reason: 'NOT_FOUND' });
  s.autoBuyAny = !s.autoBuyAny;
  scheduleSave();
  res.json({ ok: true, autoBuyAny: s.autoBuyAny });
});

// ===================== Admin endpoints =====================
function tryParseJsonMaybe(s) {
  if (typeof s !== 'string') return null;
  const t = s.trim();
  if (!t) return null;
  if (!(t.startsWith('{') || t.startsWith('['))) return null;
  try { return JSON.parse(t); } catch { return null; }
}

app.get('/api/admin/status', auth, async (req, res) => {
  if (!isAdmin(req.userId)) return res.status(403).json({ ok: false, reason: 'NOT_ADMIN' });
  const sess = mrktSessionRuntime || (await loadMrktSessionFromRedis());
  res.json({
    ok: true,
    mrktAuthSet: !!MRKT_AUTH_RUNTIME,
    mrktAuthMask: MRKT_AUTH_RUNTIME ? maskToken(MRKT_AUTH_RUNTIME) : null,
    mrktSessionSaved: !!sess,
    lastFail: mrktState.lastFailMsg || null,
    pauseUntil: mrktState.pauseUntil || 0,
    refresh: { ...mrktAuthDebug },
  });
});

app.post('/api/admin/session', auth, async (req, res) => {
  if (!isAdmin(req.userId)) return res.status(403).json({ ok: false, reason: 'NOT_ADMIN' });

  const payloadJson = req.body?.payloadJson;
  const j = tryParseJsonMaybe(String(payloadJson || ''));
  if (!j || typeof j !== 'object') return res.status(400).json({ ok: false, reason: 'BAD_PAYLOAD_JSON' });

  const data = typeof j.data === 'string' ? j.data : null;
  const photo = (j.photo === undefined) ? null : j.photo;

  if (!data || !String(data).includes('hash=')) return res.status(400).json({ ok: false, reason: 'BAD_DATA_NO_HASH' });

  const sess = { data: String(data), photo };
  mrktSessionRuntime = sess;
  if (redis) await redisSet(REDIS_KEY_MRKT_SESSION, JSON.stringify(sess), { EX: WEBAPP_AUTH_MAX_AGE_SEC });

  const rr = await tryRefreshMrktToken('admin_save_session', { force: true });
  if (!rr.ok) return res.status(400).json({ ok: false, reason: rr.reason, refresh: { ...mrktAuthDebug } });

  res.json({ ok: true, tokenMask: maskToken(MRKT_AUTH_RUNTIME), refresh: { ...mrktAuthDebug } });
});

app.post('/api/admin/refresh_token', auth, async (req, res) => {
  if (!isAdmin(req.userId)) return res.status(403).json({ ok: false, reason: 'NOT_ADMIN' });
  const rr = await tryRefreshMrktToken('admin_manual_refresh', { force: true });
  if (!rr.ok) return res.status(400).json({ ok: false, reason: rr.reason, refresh: { ...mrktAuthDebug } });
  res.json({ ok: true, tokenMask: maskToken(MRKT_AUTH_RUNTIME), refresh: { ...mrktAuthDebug } });
});

app.get('/api/admin/test_mrkt', auth, async (req, res) => {
  if (!isAdmin(req.userId)) return res.status(403).json({ ok: false, reason: 'NOT_ADMIN' });
  const cols = await mrktGetCollections();
  res.json({
    ok: true,
    collectionsCount: cols.length,
    tokenMask: MRKT_AUTH_RUNTIME ? maskToken(MRKT_AUTH_RUNTIME) : null,
    lastFail: mrktState.lastFailMsg || null,
    refresh: { ...mrktAuthDebug },
  });
});

// ===================== webhook/polling =====================
if (PUBLIC_URL) {
  const hookUrl = `${PUBLIC_URL.replace(/\/+$/, '')}/telegram-webhook`;
  bot.setWebHook(hookUrl)
    .then(() => console.log('Webhook set:', hookUrl))
    .catch((e) => console.error('setWebHook error:', e?.message || e));
  app.post('/telegram-webhook', (req, res) => { bot.processUpdate(req.body); res.sendStatus(200); });
} else {
  bot.deleteWebHook({ drop_pending_updates: true }).catch(() => {});
  bot.startPolling({ interval: 300, autoStart: true });
  console.log('Polling started (PUBLIC_URL not set)');
}

// ===================== start server + intervals + bootstrap =====================
const PORT = Number(process.env.PORT || 3000);
app.listen(PORT, '0.0.0.0', () => console.log('HTTP listening on', PORT));

setInterval(() => {
  checkSubscriptionsForAllUsers().catch((e) => console.error('subs error:', e));
}, SUBS_CHECK_INTERVAL_MS);

setInterval(() => {
  autoBuyCycle().catch((e) => console.error('autobuy error:', e));
}, AUTO_BUY_CHECK_INTERVAL_MS);

(async () => {
  if (REDIS_URL) {
    await initRedis();
    if (redis) {
      const t = await redisGet(REDIS_KEY_MRKT_AUTH);
      if (t && String(t).trim()) MRKT_AUTH_RUNTIME = String(t).trim();

      const rawSess = await redisGet(REDIS_KEY_MRKT_SESSION);
      if (rawSess) {
        try { mrktSessionRuntime = JSON.parse(rawSess); } catch {}
      }

      const keys = [REDIS_KEY_STATE, 'bot:state:v8','bot:state:v7','bot:state:v6','bot:state:v5','bot:state:v4','bot:state:v3','bot:state:v2','bot:state:v1'];
      for (const k of keys) {
        const raw = await redisGet(k);
        if (raw) {
          try { importState(JSON.parse(raw)); console.log('Loaded state from', k, 'users=', users.size); break; } catch {}
        }
      }
    }
  } else {
    console.warn('REDIS_URL not set => state/session/token won’t persist');
  }
  console.log('Bot started. /start');
})();
