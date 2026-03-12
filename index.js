/**
 * v29 (2026-03-12)
 * - No AutoBuy RPS chat spam (AUTO_BUY_RPS_NOTIFY=0 by default)
 * - Lot sheet: NO sales history; Gift/Model/Backdrop are clickable text links
 * - Offers: no sticky RPS; returns last ok + waitMs; UI auto-retry
 * - AutoBuy faster: uses feed listing/change_price first, fallback to saling
 * - AutoBuy purchases saved to Profile
 *
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
const MRKT_HTTP_MAX_WAIT_MS = Number(process.env.MRKT_HTTP_MAX_WAIT_MS || 800);

// sizes
const MRKT_COUNT = Number(process.env.MRKT_COUNT || 12);
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
const SUMMARY_CACHE_TTL_MS = Number(process.env.SUMMARY_CACHE_TTL_MS || 5000);

// subs
const SUBS_CHECK_INTERVAL_MS = Number(process.env.SUBS_CHECK_INTERVAL_MS || 15000);
const SUBS_MAX_NOTIFICATIONS_PER_CYCLE = Number(process.env.SUBS_MAX_NOTIFICATIONS_PER_CYCLE || 6);
const SUBS_EMPTY_CONFIRM = Number(process.env.SUBS_EMPTY_CONFIRM || 2);
const SUBS_FEED_TYPES_RAW = String(process.env.SUBS_FEED_TYPES || 'sale,listing,change_price');
const SUBS_FEED_TYPES = new Set(SUBS_FEED_TYPES_RAW.split(',').map((s) => s.trim().toLowerCase()).filter(Boolean));
const SUBS_FEED_MAX_EVENTS_PER_CYCLE = Number(process.env.SUBS_FEED_MAX_EVENTS_PER_CYCLE || 8);

// Telegram: photo notifications (OFF by default)
const SUBS_SEND_PHOTO = String(process.env.SUBS_SEND_PHOTO || '0') === '1';

// AutoBuy
const AUTO_BUY_GLOBAL = String(process.env.AUTO_BUY_GLOBAL || '0') === '1';
const AUTO_BUY_DRY_RUN = String(process.env.AUTO_BUY_DRY_RUN || '1') === '1';
const AUTO_BUY_CHECK_INTERVAL_MS = Number(process.env.AUTO_BUY_CHECK_INTERVAL_MS || 4500);
const AUTO_BUY_MAX_BUYS_PER_USER_PER_CYCLE = Number(process.env.AUTO_BUY_MAX_BUYS_PER_USER_PER_CYCLE || 1);
const AUTO_BUY_ATTEMPT_TTL_MS = Number(process.env.AUTO_BUY_ATTEMPT_TTL_MS || 60_000);
const AUTO_BUY_DISABLE_AFTER_SUCCESS = String(process.env.AUTO_BUY_DISABLE_AFTER_SUCCESS || '0') === '1';

// IMPORTANT: stop chat spam about RPS
const AUTO_BUY_RPS_NOTIFY = String(process.env.AUTO_BUY_RPS_NOTIFY || '0') === '1';
const AUTO_BUY_RPS_NOTIFY_COOLDOWN_MS = Number(process.env.AUTO_BUY_RPS_NOTIFY_COOLDOWN_MS || 5 * 60_000);

// sales history (MRKT feed sale)
const SALES_HISTORY_TARGET = Number(process.env.SALES_HISTORY_TARGET || 18);
const SALES_HISTORY_MAX_PAGES = Number(process.env.SALES_HISTORY_MAX_PAGES || 12);
const SALES_HISTORY_COUNT_PER_PAGE = Number(process.env.SALES_HISTORY_COUNT_PER_PAGE || 50);
const SALES_HISTORY_THROTTLE_MS = Number(process.env.SALES_HISTORY_THROTTLE_MS || 200);
const SALES_HISTORY_TIME_BUDGET_MS = Number(process.env.SALES_HISTORY_TIME_BUDGET_MS || 9000);

// MRKT auth refresh
const MRKT_AUTH_REFRESH_COOLDOWN_MS = Number(process.env.MRKT_AUTH_REFRESH_COOLDOWN_MS || 8000);

// manual buy button in WebApp
const MANUAL_BUY_ENABLED = String(process.env.MANUAL_BUY_ENABLED || '0') === '1';

// Redis keys
const REDIS_KEY_STATE = 'bot:state:main';
const REDIS_KEY_MRKT_AUTH = 'mrkt:auth:token';
const REDIS_KEY_MRKT_SESSION = 'mrkt:session:admin';

console.log('v29 start', {
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
  AUTO_BUY_RPS_NOTIFY,
  MANUAL_BUY_ENABLED,
});

// ===================== Helpers =====================
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const nowMs = () => Date.now();

function makeReq() { return `${Date.now()}_${Math.random().toString(16).slice(2)}`; }
function norm(s) { return String(s || '').toLowerCase().trim().replace(/\s+/g, ' '); }
function normTraitName(s) {
  return norm(s).replace(/\s*\([^)]*%[^)]*\)\s*/g, ' ').replace(/\s+/g, ' ').trim();
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
  r.arrayBuffer().then((ab) => res.end(Buffer.from(ab))).catch(() => res.status(502).end('bad gateway'));
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
    collectionsCache.time = 0;
    collectionsCache.items = [];
    modelsCache.clear();
    backdropsCache.clear();
    offersCache.clear();
    lotsCache.clear();
    detailsCache.clear();
    salesCache.clear();
    summaryCache.clear();
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

function bodyLooksLikeRpsLimit(txt) {
  const s = String(txt || '').toLowerCase();
  return s.includes('more rps') || (s.includes('rps') && s.includes('support'));
}

async function mrktGetJson(path, { retry = true } = {}) {
  return mrktRunExclusive(async () => {
    if (!MRKT_AUTH_RUNTIME) {
      const ok = await ensureMrktAuth();
      if (!ok) return { ok: false, status: 401, data: null, text: 'NO_AUTH' };
    }

    const gate = await mrktGateCheckOrWait();
    if (!gate.ok) return { ok: false, status: 429, data: null, text: 'RPS_WAIT', waitMs: gate.waitMs };

    const url = `${MRKT_API_URL}${path}${path.includes('?') ? '&' : '?'}req=${encodeURIComponent(makeReq())}`;
    const res = await fetchWithTimeout(url, { method: 'GET', headers: mrktHeadersCommon() }, MRKT_TIMEOUT_MS).catch(() => null);
    if (!res) return { ok: false, status: 0, data: null, text: 'FETCH_ERROR' };

    const txt = await res.text().catch(() => '');
    let data = null;
    try { data = txt ? JSON.parse(txt) : null; } catch {}

    if (!res.ok) {
      if (res.status === 429 || bodyLooksLikeRpsLimit(txt)) {
        setPauseFrom429(res);
        return { ok: false, status: 429, data, text: txt, waitMs: (mrktState.pauseUntil - nowMs()) };
      }

      if ((res.status === 401 || res.status === 403) && retry) {
        const rr = await tryRefreshMrktToken(`HTTP_${res.status}:${path}`, { force: false });
        if (rr.ok) return mrktGetJson(path, { retry: false });
      }
      return { ok: false, status: res.status, data, text: txt };
    }

    return { ok: true, status: res.status, data, text: txt };
  });
}

async function mrktPostJson(path, bodyObj, { retry = true } = {}) {
  return mrktRunExclusive(async () => {
    if (!MRKT_AUTH_RUNTIME) {
      const ok = await ensureMrktAuth();
      if (!ok) return { ok: false, status: 401, data: null, text: 'NO_AUTH' };
    }

    const gate = await mrktGateCheckOrWait();
    if (!gate.ok) return { ok: false, status: 429, data: null, text: 'RPS_WAIT', waitMs: gate.waitMs };

    const reqVal = bodyObj?.req ? String(bodyObj.req) : makeReq();
    const body = { ...(bodyObj || {}), req: reqVal };
    const url = `${MRKT_API_URL}${path}${path.includes('?') ? '&' : '?'}req=${encodeURIComponent(reqVal)}`;

    const res = await fetchWithTimeout(url, { method: 'POST', headers: mrktHeadersJson(), body: JSON.stringify(body) }, MRKT_TIMEOUT_MS).catch(() => null);
    if (!res) return { ok: false, status: 0, data: null, text: 'FETCH_ERROR' };

    const txt = await res.text().catch(() => '');
    let data = null;
    try { data = txt ? JSON.parse(txt) : null; } catch {}

    if (!res.ok) {
      if (res.status === 429 || bodyLooksLikeRpsLimit(txt)) {
        setPauseFrom429(res);
        return { ok: false, status: 429, data, text: txt, waitMs: (mrktState.pauseUntil - nowMs()) };
      }

      if ((res.status === 401 || res.status === 403) && retry) {
        const rr = await tryRefreshMrktToken(`HTTP_${res.status}:${path}`, { force: false });
        if (rr.ok) return mrktPostJson(path, bodyObj, { retry: false });
      }

      return { ok: false, status: res.status, data, text: txt };
    }

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
const summaryCache = new Map();
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
    maxPrice: null, minPrice: null,
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
      if (out.length >= 200) break;
    }

    cursor = r.cursor || '';
    if (!cursor || out.length >= 200) break;
  }

  out.sort((a, b) => a.priceTon - b.priceTon);
  return { ok: true, reason: 'OK', gifts: out };
}

async function mrktOrdersFetch({ gift, model, backdrop }) {
  const key = `orders|${gift || ''}|${model || ''}|${backdrop || ''}`;
  const now = nowMs();
  const cached = offersCache.get(key);

  if (cached && now - cached.time < OFFERS_CACHE_TTL_MS) return { ok: true, orders: cached.orders, fromCache: true, note: null, waitMs: 0 };

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
    types: Array.isArray(types) ? types : [],
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
        sales.push({ ts: it.date || null, priceTon: ton, title, imgUrl, model: g.modelTitle || g.modelName || null, backdrop: g.backdropName || null, urlMarket, urlTelegram });
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

// ===================== Subs helpers =====================
async function buildSubUi(sub) {
  try {
    const f = normalizeFilters(sub.filters || {});
    const gift = f.gifts.length === 1 ? f.gifts[0] : null;
    if (!gift) return { thumbKey: null, swatches: [] };

    let thumbKey = null;

    if (f.models.length === 1) {
      const models = await mrktGetModelsForGift(gift);
      const m = models.find((x) => normTraitName(x.name) === normTraitName(f.models[0]));
      if (m?.thumbKey) thumbKey = m.thumbKey;
    }

    if (!thumbKey) {
      const cols = await mrktGetCollections();
      const c = cols.find((x) => x.name === gift);
      if (c?.thumbKey) thumbKey = c.thumbKey;
    }

    let swatches = [];
    if (f.backdrops.length) {
      const backs = await mrktGetBackdropsForGift(gift);
      const map = new Map(backs.map((b) => [normTraitName(b.name), b.centerHex]));
      swatches = f.backdrops.map((b) => map.get(normTraitName(b)) || null).filter(Boolean).slice(0, 8);
    }

    return { thumbKey: thumbKey || null, swatches };
  } catch {
    return { thumbKey: null, swatches: [] };
  }
}

function subGiftTitle(sub) {
  const f = normalizeFilters(sub?.filters || {});
  if (!f.gifts.length) return '(не выбран)';
  const labels = f.gifts.map((g) => f.giftLabels?.[g] || g);
  return labels.join(', ');
}

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

  const reply_markup = mkReplyMarkupOpen(lot?.urlMarket, 'MRKT');
  await notifyTextOrPhoto(chatId, lotImgUrl(lot), lines.join('\n'), reply_markup);
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

  const reply_markup = mkReplyMarkupOpen(ev.urlMarket, 'MRKT');
  await notifyTextOrPhoto(chatId, ev.imgUrl, lines.join('\n'), reply_markup);
}

// ===================== Subs worker =====================
let isSubsChecking = false;
let isAutoBuying = false;

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
    types: [], // all
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
  if (isAutoBuying && !manual) return { processedSubs: 0, floorNotifs: 0, feedNotifs: 0 };

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
const autoBuyLastRpsNotify = new Map(); // userId -> ts

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
    count: 30,
    ordering: 'Latest',
    types: ['listing', 'change_price'],
  });

  if (!r.ok) {
    if (r.reason === 'RPS_WAIT') return { ok: true, bought: false, rps: true, waitMs: r.waitMs || 0 };
    return { ok: true, bought: false };
  }
  if (!r.items.length) return { ok: true, bought: false };

  const latestId = r.items[0]?.id || null;
  if (latestId && !st.autoBuyLastId) {
    subStates.set(stateKey, { ...st, autoBuyLastId: latestId });
    return { ok: true, bought: false };
  }

  const candidates = [];
  for (const it of r.items) {
    if (!it?.id) continue;
    if (st.autoBuyLastId && it.id === st.autoBuyLastId) break;

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

  // if MRKT is paused - don't spam attempts
  if (mrktState.pauseUntil && nowMs() < mrktState.pauseUntil) return;

  // if no eligible subs at all - do nothing
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

        if (r1?.rps && AUTO_BUY_RPS_NOTIFY) {
          const last = autoBuyLastRpsNotify.get(userId) || 0;
          if (nowMs() - last > AUTO_BUY_RPS_NOTIFY_COOLDOWN_MS) {
            autoBuyLastRpsNotify.set(userId, nowMs());
            await sendMessageSafe(userChatId(userId), `⚠️ AutoBuy: RPS limit, wait ${fmtWaitMs(r1.waitMs || 1000)}`, { disable_web_page_preview: true });
          }
        }

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

  await sendMessageSafe(msg.chat.id, `Открой меню “${APP_TITLE}”`, { reply_markup: MAIN_KEYBOARD });
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
      `pauseUntil: ${mrktState.pauseUntil ? new Date(mrktState.pauseUntil).toLocaleTimeString('ru-RU') : '-'}\n` +
      `Redis: ${redis ? 'YES' : 'NO'}\n` +
      `Subs interval: ${SUBS_CHECK_INTERVAL_MS}ms\n` +
      `AutoBuy: ${AUTO_BUY_GLOBAL ? 'ON' : 'OFF'} ${AUTO_BUY_DRY_RUN ? '(DRY)' : ''}\n`;
    await sendMessageSafe(msg.chat.id, txt, { reply_markup: MAIN_KEYBOARD });
  }
});

// ===================== WebApp HTML/JS =====================
const WEBAPP_HTML = `<!doctype html>
<html lang="ru"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"/>
<title>${APP_TITLE}</title>
<style>
:root{--bg:#0b0f14;--card:#101826;--text:#e5e7eb;--muted:#9ca3af;--border:#223044;--input:#0f172a;--btn:#182235;--accent:#22c55e;--danger:#ef4444}
*{box-sizing:border-box}
html,body{background:var(--bg);overscroll-behavior:none}
body{margin:0;padding:14px;color:var(--text);font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial}
h2{margin:0 0 10px 0;font-size:18px}
.card{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:12px;margin:10px 0}
label{display:block;font-size:12px;color:var(--muted);margin-bottom:4px}
.row{display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end}
.field{position:relative;flex:1 1 200px;min-width:140px}
.inpWrap{position:relative}
input,textarea{width:100%;padding:10px;border:1px solid var(--border);border-radius:12px;background:var(--input);color:var(--text);outline:none;font-size:13px}
button{padding:10px 12px;border:1px solid var(--border);border-radius:12px;background:var(--btn);color:var(--text);cursor:pointer}
.primary{border-color:var(--accent);background:var(--accent);color:#052e16;font-weight:950}
.small{padding:8px 10px;border-radius:10px;font-size:13px}
.xbtn{position:absolute;right:8px;top:50%;transform:translateY(-50%);width:20px;height:20px;border-radius:8px;border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.05);color:rgba(255,255,255,.65);cursor:pointer;display:flex;align-items:center;justify-content:center}
.hr{height:1px;background:var(--border);margin:10px 0}
.muted{color:var(--muted);font-size:12px}
#err{display:none;border-color:var(--danger);color:#ffd1d1;white-space:pre-wrap;word-break:break-word}
.tabs{display:flex;gap:8px;flex-wrap:wrap}
.tabbtn{border-radius:999px;padding:8px 12px;font-size:13px}
.tabbtn.active{border-color:var(--accent);color:var(--accent)}
.tlink{color:var(--accent);text-decoration:underline;cursor:pointer}
.tlink:active{opacity:.7}

.sug{border:1px solid var(--border);border-radius:14px;overflow:auto;background:var(--card);max-height:380px;position:absolute;top:calc(100% + 6px);left:0;right:0;z-index:1000000;box-shadow:0 14px 40px rgba(0,0,0,.45);-webkit-overflow-scrolling:touch}
.field.open{z-index:999999}
.sugHead{display:flex;justify-content:space-between;align-items:center;padding:8px 10px;border-bottom:1px solid var(--border);position:sticky;top:0;background:var(--card);z-index:2}
.sug .item{width:100%;text-align:left;border:0;background:transparent;padding:10px;display:flex;gap:10px;align-items:flex-start}
.sug .item:hover{background:rgba(255,255,255,.06)}
.thumb{width:44px;height:44px;border-radius:14px;object-fit:cover;background:rgba(255,255,255,.06);border:1px solid var(--border);flex:0 0 auto}
.thumb.color{background:transparent;display:flex;align-items:center;justify-content:center}
.colorFill{width:100%;height:100%;border-radius:14px;border:1px solid rgba(255,255,255,.12)}
.grid{display:grid;gap:10px;margin-top:10px;grid-template-columns: repeat(auto-fill, minmax(170px, 1fr))}
@media (max-width: 520px){.grid{grid-template-columns: repeat(2, minmax(0, 1fr));}}
.lot{border:1px solid var(--border);border-radius:16px;padding:10px;background:rgba(255,255,255,.02);cursor:pointer}
.lot img{width:100%;aspect-ratio:1/1;object-fit:cover;border-radius:14px;border:1px solid var(--border);background:rgba(255,255,255,.03)}
.price{font-size:15px;font-weight:950;margin-top:8px}
.ellipsis{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.badge{padding:4px 8px;border-radius:999px;border:1px solid var(--border);color:var(--muted);font-size:12px}
.loaderLine{display:none;align-items:center;gap:10px;color:var(--muted);font-size:12px;margin-top:10px}
.spinner{width:16px;height:16px;border-radius:999px;border:3px solid rgba(255,255,255,.2);border-top-color:var(--accent);animation:spin .8s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}

/* summary */
.sumBox{border:1px solid var(--border);border-radius:14px;padding:10px;background:rgba(255,255,255,.02);display:flex;flex-wrap:wrap;gap:10px;margin-top:10px}
.sumPill{padding:6px 10px;border:1px solid var(--border);border-radius:999px;font-size:12px;color:var(--muted)}

/* sheet */
.sheetWrap{position:fixed;inset:0;background:rgba(0,0,0,.35);display:flex;align-items:flex-end;justify-content:center;z-index:50000;padding:10px;opacity:0;visibility:hidden;pointer-events:none;transition:opacity .18s ease,visibility .18s ease}
.sheetWrap.show{opacity:1;visibility:visible;pointer-events:auto}
.sheet{width:min(980px,96vw);height:min(82vh,880px);background:var(--card);border:1px solid var(--border);border-radius:22px 22px 14px 14px;padding:12px;box-shadow:0 30px 90px rgba(0,0,0,.5);display:flex;flex-direction:column;gap:10px;transform:translateY(20px);transition:transform .18s ease}
.sheetWrap.show .sheet{transform:translateY(0)}
.handle{width:44px;height:5px;border-radius:999px;background:rgba(255,255,255,.18);align-self:center}
.sheetHeader{display:flex;justify-content:space-between;gap:12px;align-items:flex-start}
.sheetBody{overflow:auto;-webkit-overflow-scrolling:touch}
.sheetImg{width:100%;max-height:230px;object-fit:contain;border-radius:14px;border:1px solid var(--border);background:rgba(255,255,255,.03)}
.saleRow{display:flex;gap:10px;align-items:center}
.saleRow img{width:60px;height:60px;border-radius:14px;border:1px solid rgba(255,255,255,.12);object-fit:cover;background:rgba(255,255,255,.03);flex:0 0 auto}
.saleCard{border:1px solid var(--border);border-radius:12px;padding:10px;background:rgba(255,255,255,.02)}
.purchRow{display:flex;gap:10px;align-items:center}
.purchRow img{width:52px;height:52px;border-radius:14px;border:1px solid var(--border);object-fit:cover;background:rgba(255,255,255,.03);flex:0 0 auto}
.swRow{display:flex;gap:6px;flex-wrap:wrap;margin-top:6px}
.dot{width:14px;height:14px;border-radius:999px;border:1px solid rgba(255,255,255,.18)}

/* desktop scrollbar */
*::-webkit-scrollbar{width:10px;height:10px}
*::-webkit-scrollbar-track{background:rgba(255,255,255,.06)}
*::-webkit-scrollbar-thumb{background:rgba(255,255,255,.18);border-radius:999px}
*::-webkit-scrollbar-thumb:hover{background:rgba(255,255,255,.26)}
</style></head>
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
  <h3 style="margin:0 0 8px 0;font-size:15px">Поиск</h3>

  <div class="row">
    <div class="field" id="giftField">
      <label>Gift (можно несколько)</label>
      <div class="inpWrap">
        <input id="gift" placeholder="Нажми чтобы выбрать" autocomplete="off"/>
        <button class="xbtn" data-clear="gift" type="button">×</button>
      </div>
      <div id="giftSug" class="sug" style="display:none"></div>
    </div>

    <div class="field" id="modelField">
      <label>Model (мульти, только если 1 gift)</label>
      <div class="inpWrap">
        <input id="model" placeholder="Нажми чтобы выбрать" autocomplete="off"/>
        <button class="xbtn" data-clear="model" type="button">×</button>
      </div>
      <div id="modelSug" class="sug" style="display:none"></div>
    </div>

    <div class="field" id="backdropField">
      <label>Backdrop (мульти, только если 1 gift)</label>
      <div class="inpWrap">
        <input id="backdrop" placeholder="Нажми чтобы выбрать" autocomplete="off"/>
        <button class="xbtn" data-clear="backdrop" type="button">×</button>
      </div>
      <div id="backdropSug" class="sug" style="display:none"></div>
    </div>

    <div class="field" style="max-width:160px">
      <label>Number prefix</label>
      <div class="inpWrap">
        <input id="number" placeholder="№" inputmode="numeric"/>
        <button class="xbtn" data-clear="number" type="button">×</button>
      </div>
    </div>
  </div>

  <div class="row" style="margin-top:10px">
    <button id="apply" class="primary">Показать</button>
    <button id="refresh">Обновить</button>
    <button id="salesByFilters" class="small">История продаж</button>
  </div>

  <div id="summary" class="sumBox" style="display:none"></div>

  <div id="lotsLoading" class="loaderLine"><div class="spinner"></div><div>Загрузка…</div></div>
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
    <button id="subRebuildUi" class="small">Обновить картинки</button>
    <button id="subCheckNow" class="small">Проверить сейчас</button>
  </div>
  <div id="subsLoading" class="loaderLine"><div class="spinner"></div><div>Загрузка…</div></div>
  <div id="subsList" style="margin-top:10px"></div>
</div>

<div id="profile" class="card" style="display:none">
  <h3 style="margin:0 0 8px 0;font-size:15px">Профиль</h3>
  <div class="row" style="align-items:center">
    <img id="pfp" class="thumb" style="display:none"/>
    <div id="profileBox" class="muted">Загрузка...</div>
  </div>
  <div id="profileLoading" class="loaderLine"><div class="spinner"></div><div>Загрузка…</div></div>
  <div class="hr"></div>
  <div><b>История покупок</b></div>
  <div id="purchases" style="margin-top:10px;display:flex;flex-direction:column;gap:10px"></div>
</div>

<div id="admin" class="card" style="display:none">
  <h3 style="margin:0 0 8px 0;font-size:15px">Admin</h3>
  <div id="adminStatus" class="muted">Загрузка...</div>
  <div class="hr"></div>
  <div class="muted">Вставь <b>полный JSON Payload</b> из Network MRKT <code>/api/v1/auth</code> (POST): поля <code>data</code> и <code>photo</code>. Затем Save → refresh токена.</div>
  <textarea id="payloadJson" rows="6" placeholder='{"appId":null,"data":"...","photo":...}' style="margin-top:10px"></textarea>
  <div class="row" style="margin-top:10px">
    <button id="sessSave">Save session + refresh</button>
    <button id="tokRefresh">Refresh token</button>
    <button id="testMrkt">Test MRKT</button>
  </div>
</div>

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

    <img id="sheetImg" class="sheetImg" style="display:none" />
    <div id="sheetTop" class="muted"></div>
    <div class="row" id="sheetBtns"></div>

    <div class="hr"></div>
    <div id="sheetBody" class="sheetBody"></div>
  </div>
</div>

<script src="https://telegram.org/js/telegram-web-app.js"></script>
<script src="/app.js"></script>
</body></html>`;

const WEBAPP_JS = `(() => {
  const tg = window.Telegram?.WebApp;
  try { tg?.ready(); tg?.expand?.(); } catch {}

  const initData = tg?.initData || '';
  const el = (id) => document.getElementById(id);

  if (!initData) {
    document.body.innerHTML = '<div style="padding:16px;font-family:system-ui;color:#fff;background:#0b0f14"><h3>Открой панель из Telegram</h3></div>';
    return;
  }

  const lotsLoading = el('lotsLoading');
  const subsLoading = el('subsLoading');
  const profileLoading = el('profileLoading');

  function setLoading(which, on){
    if(which==='lots') lotsLoading.style.display = on ? 'flex' : 'none';
    if(which==='subs') subsLoading.style.display = on ? 'flex' : 'none';
    if(which==='profile') profileLoading.style.display = on ? 'flex' : 'none';
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

  function openTg(url){
    if (!url) return;
    try { if (tg?.openTelegramLink) return tg.openTelegramLink(url); } catch {}
    window.open(url, '_blank');
  }

  function setTab(name){
    ['market','subs','profile','admin'].forEach(x => el(x).style.display = (x===name?'block':'none'));
    document.querySelectorAll('.tabbtn').forEach(b => b.classList.toggle('active', b.dataset.tab===name));
  }
  document.querySelectorAll('.tabbtn').forEach(b => b.onclick = async () => {
    setTab(b.dataset.tab);
    if (b.dataset.tab === 'profile') await refreshProfile().catch(()=>{});
    if (b.dataset.tab === 'admin') await refreshAdmin().catch(()=>{});
  });

  // selection
  let sel = { gifts: [], giftLabels: {}, models: [], backdrops: [], numberPrefix: '' };

  function giftsInputText(){
    if (!sel.gifts.length) return '';
    return sel.gifts.map(v => sel.giftLabels[v] || v).join(', ');
  }
  function listInputText(arr){ return (arr || []).join(', '); }
  function isSelectedNorm(arr, v){
    const k = String(v||'').toLowerCase().trim();
    return (arr || []).some(x => String(x).toLowerCase().trim() === k);
  }
  function toggleIn(arr, v){
    const k = String(v||'').toLowerCase().trim();
    const out = [];
    let removed = false;
    for (const x of arr || []) {
      if (String(x).toLowerCase().trim() === k) { removed = true; continue; }
      out.push(x);
    }
    if (!removed) out.push(v);
    return out;
  }

  async function patchFilters(){
    await api('/api/state/patch',{method:'POST',body:JSON.stringify({filters:{
      gifts: sel.gifts,
      giftLabels: sel.giftLabels,
      models: sel.models,
      backdrops: sel.backdrops,
      numberPrefix: el('number').value.trim()
    }})});
  }

  function wrap(which, fn){
    return async () => {
      hideErr();
      setLoading(which, true);
      try { await fn(); }
      catch(e){ showErr(e.message || String(e)); }
      finally { setLoading(which, false); }
    };
  }

  const timers = { gift: null, model: null, backdrop: null };
  function debounce(kind, fn, ms=220){
    clearTimeout(timers[kind]);
    timers[kind] = setTimeout(fn, ms);
  }

  function scheduleRetryIfWait(resp, fn){
    const waitMs = resp && Number(resp.waitMs || 0);
    if (!waitMs || waitMs < 500 || waitMs > 20000) return;
    setTimeout(() => { fn().catch(()=>{}); }, waitMs + 150);
  }

  // dropdowns
  function openField(fieldId){
    ['giftField','modelField','backdropField'].forEach(id => el(id)?.classList.remove('open'));
    el(fieldId)?.classList.add('open');
  }
  function closeAllFields(){
    ['giftField','modelField','backdropField'].forEach(id => el(id)?.classList.remove('open'));
  }
  function hideSug(id){ const b=el(id); b.style.display='none'; b.innerHTML=''; }

  function renderSug(id, title, items, isSelected, onToggle){
    const b=el(id);
    const head =
      '<div class="sugHead">' +
        '<b>'+title+'</b>' +
        '<span class="muted">тап = добавить/убрать</span>' +
      '</div>';

    b.innerHTML = head + (items||[]).map(x => {
      const selMark = isSelected(x.value) ? '✅ ' : '';
      const thumb = x.imgUrl
        ? '<img class="thumb" src="'+x.imgUrl+'" referrerpolicy="no-referrer"/>'
        : (x.colorHex
            ? '<div class="thumb color"><div class="colorFill" style="background:'+x.colorHex+'"></div></div>'
            : '<div class="thumb"></div>'
          );
      const sub = x.sub ? '<div class="muted" style="white-space:normal;line-height:1.15;margin-top:2px">'+x.sub+'</div>' : '';
      return '<button type="button" class="item" data-v="'+String(x.value).replace(/"/g,'&quot;')+'">'+thumb+
        '<div style="min-width:0;flex:1">' +
          '<div style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap"><b>'+selMark+x.label+'</b></div>' +
          sub +
        '</div></button>';
    }).join('');

    b.style.display='block';
    b.onpointerdown = (e) => { e.stopPropagation(); };
    b.onclick = (e) => {
      const btn = e.target.closest('button[data-v]');
      if(!btn) return;
      onToggle(btn.getAttribute('data-v'));
    };
  }

  function giftQuery(){
    const raw = el('gift').value.trim();
    const selected = giftsInputText().trim();
    if (raw === selected) return '';
    return raw;
  }
  function modelQuery(){
    const raw = el('model').value.trim();
    const selected = listInputText(sel.models).trim();
    if (raw === selected) return '';
    return raw;
  }
  function backdropQuery(){
    const raw = el('backdrop').value.trim();
    const selected = listInputText(sel.backdrops).trim();
    if (raw === selected) return '';
    return raw;
  }

  async function showGiftSug(){
    openField('giftField');
    const q = giftQuery();
    const r = await api('/api/mrkt/collections?q='+encodeURIComponent(q));
    const items = r.items || [];
    const rerender = () => renderSug('giftSug', 'Gift', items,
      (v)=> isSelectedNorm(sel.gifts, v),
      (v)=>{
        const next = toggleIn(sel.gifts, v);
        if (next.length !== 1) { sel.models = []; sel.backdrops = []; }
        sel.gifts = next;
        sel.giftLabels[v] = (r.mapLabel||{})[v] || v;
        el('gift').value = giftsInputText();
        el('model').value = listInputText(sel.models);
        el('backdrop').value = listInputText(sel.backdrops);
        rerender();
      }
    );
    rerender();
    scheduleRetryIfWait(r, showGiftSug);
  }

  async function showModelSug(){
    openField('modelField');
    if (sel.gifts.length !== 1) { hideSug('modelSug'); return; }
    const gift = sel.gifts[0];
    const q = modelQuery();
    const r = await api('/api/mrkt/suggest?kind=model&gift='+encodeURIComponent(gift)+'&q='+encodeURIComponent(q));
    const items = r.items || [];
    const rerender = () => renderSug('modelSug', 'Model', items,
      (v)=> isSelectedNorm(sel.models, v),
      (v)=>{
        sel.models = toggleIn(sel.models, v);
        el('model').value = listInputText(sel.models);
        rerender();
      }
    );
    rerender();
    scheduleRetryIfWait(r, showModelSug);
  }

  async function showBackdropSug(){
    openField('backdropField');
    if (sel.gifts.length !== 1) { hideSug('backdropSug'); return; }
    const gift = sel.gifts[0];
    const q = backdropQuery();
    const r = await api('/api/mrkt/suggest?kind=backdrop&gift='+encodeURIComponent(gift)+'&q='+encodeURIComponent(q));
    const items = r.items || [];
    const rerender = () => renderSug('backdropSug', 'Backdrop', items,
      (v)=> isSelectedNorm(sel.backdrops, v),
      (v)=>{
        sel.backdrops = toggleIn(sel.backdrops, v);
        el('backdrop').value = listInputText(sel.backdrops);
        rerender();
      }
    );
    rerender();
    scheduleRetryIfWait(r, showBackdropSug);
  }

  el('gift').addEventListener('focus', wrap('lots', ()=>showGiftSug()));
  el('gift').addEventListener('click', wrap('lots', ()=>showGiftSug()));
  el('gift').addEventListener('input', ()=> debounce('gift', ()=>wrap('lots', ()=>showGiftSug())(), 220));

  el('model').addEventListener('focus', wrap('lots', ()=>showModelSug()));
  el('model').addEventListener('click', wrap('lots', ()=>showModelSug()));
  el('model').addEventListener('input', ()=> debounce('model', ()=>wrap('lots', ()=>showModelSug())(), 220));

  el('backdrop').addEventListener('focus', wrap('lots', ()=>showBackdropSug()));
  el('backdrop').addEventListener('click', wrap('lots', ()=>showBackdropSug()));
  el('backdrop').addEventListener('input', ()=> debounce('backdrop', ()=>wrap('lots', ()=>showBackdropSug())(), 220));

  document.addEventListener('pointerdown', (e) => {
    if(!e.target.closest('#gift,#model,#backdrop,.sug')) {
      hideSug('giftSug'); hideSug('modelSug'); hideSug('backdropSug');
      closeAllFields();
    }
  });

  // X clear
  document.body.addEventListener('click', (e) => {
    const b = e.target.closest('button[data-clear]');
    if(!b) return;
    const what = b.getAttribute('data-clear');
    wrap('lots', async()=>{
      if(what==='gift'){ sel.gifts=[]; sel.giftLabels={}; sel.models=[]; sel.backdrops=[]; el('gift').value=''; el('model').value=''; el('backdrop').value=''; }
      if(what==='model'){ sel.models=[]; el('model').value=''; }
      if(what==='backdrop'){ sel.backdrops=[]; el('backdrop').value=''; }
      if(what==='number'){ el('number').value=''; }
      await patchFilters();
      await refreshAll(true);
    })();
  });

  // sheet
  const sheetWrap = el('sheetWrap');
  const sheetTitle = el('sheetTitle');
  const sheetSub = el('sheetSub');
  const sheetTop = el('sheetTop');
  const sheetBtns = el('sheetBtns');
  const sheetBody = el('sheetBody');
  const sheetImg = el('sheetImg');

  el('sheetClose').onclick = ()=>sheetWrap.classList.remove('show');
  sheetWrap.addEventListener('click',(e)=>{ if(e.target===sheetWrap) sheetWrap.classList.remove('show'); });

  function openSheet(title, sub){
    sheetTitle.textContent=title||'';
    sheetSub.textContent=sub||'';
    sheetTop.innerHTML='';
    sheetBtns.innerHTML='';
    sheetBody.innerHTML='';
    sheetImg.style.display='none';
    sheetImg.src='';
    sheetWrap.classList.add('show');
  }

  function renderLots(resp){
    const box=el('lots');
    el('status').textContent = resp.note || '';

    const lots=resp.lots||[];
    if(!lots.length){ box.innerHTML='<i class="muted">Лотов не найдено</i>'; return; }

    box.innerHTML = lots.map(x=>{
      const img = x.imgUrl ? '<img src="'+x.imgUrl+'" referrerpolicy="no-referrer" loading="lazy"/>' : '';
      const num = (x.number!=null)?('<span class="badge">#'+x.number+'</span>'):'';
      return '<div class="lot" data-id="'+x.id+'">'+
        (img || '<div style="aspect-ratio:1/1;border:1px solid rgba(255,255,255,.10);border-radius:14px"></div>')+
        '<div class="price">'+x.priceTon.toFixed(3)+' TON</div>'+
        '<div style="display:flex;justify-content:space-between;gap:10px;align-items:center"><b class="ellipsis">'+x.name+'</b>'+num+'</div>'+
        (x.model?'<div class="muted ellipsis">Model: '+x.model+'</div>':'')+
        (x.backdrop?'<div class="muted ellipsis" style="margin-top:6px">Backdrop: '+x.backdrop+'</div>':'')+
      '</div>';
    }).join('');

    const map = new Map(lots.map(l => [String(l.id), l]));
    box.querySelectorAll('.lot').forEach(node=>{
      node.onclick = wrap('lots', async()=>{
        const id = node.getAttribute('data-id');
        const lot = map.get(String(id));
        if(!lot) return;

        const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
        openSheet('Лот', lot.name);

        if (lot.imgUrl) { sheetImg.style.display='block'; sheetImg.src = lot.imgUrl; sheetImg.referrerPolicy='no-referrer'; }

        // clickable text lines
        sheetTop.innerHTML =
          '<div><b>Цена:</b> '+lot.priceTon.toFixed(3)+' TON</div>' +
          '<div>Gift: <span class="tlink" data-set="gift">'+esc(lot.collectionName)+'</span></div>' +
          (lot.model ? ('<div>Model: <span class="tlink" data-set="model">'+esc(lot.model)+'</span></div>') : '') +
          (lot.backdrop ? ('<div>Backdrop: <span class="tlink" data-set="backdrop">'+esc(lot.backdrop)+'</span></div>') : '');

        sheetTop.querySelectorAll('.tlink').forEach((x) => {
          x.onclick = async () => {
            const kind = x.getAttribute('data-set');
            sel.gifts = [lot.collectionName];
            sel.giftLabels = { [lot.collectionName]: lot.collectionName };

            if (kind === 'gift') { sel.models = []; sel.backdrops = []; }
            else if (kind === 'model') { sel.models = lot.model ? [lot.model] : []; sel.backdrops = []; }
            else if (kind === 'backdrop') { sel.models = lot.model ? [lot.model] : []; sel.backdrops = lot.backdrop ? [lot.backdrop] : []; }

            el('number').value = '';
            await patchFilters();
            await refreshAll(true);
            sheetWrap.classList.remove('show');
          };
        });

        // buttons only MRKT/NFT/(BUY)
        sheetBtns.innerHTML = '';
        const mkBtn = (label, action, strong=false) => {
          const b = document.createElement('button');
          b.className = 'small';
          b.textContent = label;
          if (strong) {
            b.style.borderColor='var(--accent)';
            b.style.background='var(--accent)';
            b.style.color='#052e16';
            b.style.fontWeight='900';
          }
          b.onclick = action;
          sheetBtns.appendChild(b);
        };

        mkBtn('MRKT', ()=>openTg(lot.urlMarket));
        mkBtn('NFT', ()=>openTg(lot.urlTelegram));

        if (lot.canBuy) {
          mkBtn('BUY', async()=>{
            const ok = confirm('Купить за ' + lot.priceTon.toFixed(3) + ' TON?');
            if (!ok) return;
            try {
              setLoading('lots', true);
              const r = await api('/api/mrkt/buy', { method:'POST', body: JSON.stringify({ id: lot.id, priceNano: lot.priceNano, lot }) });
              alert('OK: ' + r.priceTon.toFixed(3) + ' TON');
            } catch (e) {
              alert('FAIL: ' + (e.message || e));
            } finally {
              setLoading('lots', false);
            }
          }, true);
        }

        // NO sales history in lot sheet
        sheetBody.innerHTML = '<div class="muted">Нажми Gift / Model / Backdrop, чтобы применить фильтр.</div>';

        // offers only
        const det = await api('/api/lot/details?force=1', { method:'POST', body: JSON.stringify({ lot }) });

        const buildOffersTop = (offers) => {
          const o = offers || {};
          return (
            '<div class="muted">Max offer (exact): <b>'+(o.exact!=null?o.exact.toFixed(3):'—')+' TON</b></div>' +
            '<div class="muted">Max offer (collection): <b>'+(o.collection!=null?o.collection.toFixed(3):'—')+' TON</b></div>'
          );
        };

        sheetTop.innerHTML = buildOffersTop(det.offers) + sheetTop.innerHTML;

        if (det.waitMs) scheduleRetryIfWait(det, async()=> {
          const det2 = await api('/api/lot/details?force=1', { method:'POST', body: JSON.stringify({ lot }) });
          if (det2.waitMs) return;
          sheetTop.innerHTML = buildOffersTop(det2.offers) + sheetTop.innerHTML;
        });
      });
    });
  }

  function renderSummary(sum){
    const box = el('summary');
    if (!sum) { box.style.display='none'; box.innerHTML=''; return; }
    box.style.display='flex';
    const parts = [];
    if (sum.note) parts.push('<div class="muted" style="width:100%">'+sum.note+'</div>');
    parts.push('<div class="sumPill">Max offer (exact): <b>'+(sum.offers?.exact!=null?sum.offers.exact.toFixed(3):'—')+' TON</b></div>');
    parts.push('<div class="sumPill">Max offer (collection): <b>'+(sum.offers?.collection!=null?sum.offers.collection.toFixed(3):'—')+' TON</b></div>');
    box.innerHTML = parts.join('');
  }

  async function refreshState(){
    const st = await api('/api/state');

    sel.gifts = st.user.filters.gifts || [];
    sel.giftLabels = st.user.filters.giftLabels || {};
    sel.models = st.user.filters.models || [];
    sel.backdrops = st.user.filters.backdrops || [];
    el('number').value = st.user.filters.numberPrefix || '';

    el('gift').value = giftsInputText();
    el('model').value = (sel.models||[]).join(', ');
    el('backdrop').value = (sel.backdrops||[]).join(', ');

    if(st.api.isAdmin) el('adminTabBtn').style.display='inline-block';

    const subs=st.user.subscriptions||[];
    const box=el('subsList');
    if(!subs.length){ box.innerHTML='<i class="muted">Подписок нет</i>'; }
    else{
      box.innerHTML = subs.map(s=>{
        const thumb = s.ui?.thumbKey ? '<img class="thumb" src="/img/cdn?key='+encodeURIComponent(s.ui.thumbKey)+'" referrerpolicy="no-referrer"/>' : '<div class="thumb"></div>';
        const sw = (s.ui?.swatches||[]).map(h => '<span class="dot" style="background:'+h+'"></span>').join('');
        const gifts = (s.filters.gifts||[]).slice(0,2).join(', ');
        const more = (s.filters.gifts||[]).length>2 ? ' +' + ((s.filters.gifts||[]).length-2) : '';
        const notifyTxt = (s.maxNotifyTon==null)?'∞':String(s.maxNotifyTon);
        const buyTxt = (s.maxAutoBuyTon==null)?'-':String(s.maxAutoBuyTon);

        return '<div class="card">'+
          '<div style="display:flex;gap:10px;align-items:center">'+thumb+
            '<div style="min-width:0;flex:1">'+
              '<div style="display:flex;justify-content:space-between;gap:10px;align-items:center">'+
                '<b>#'+s.num+' '+(s.enabled?'ON':'OFF')+'</b>'+
                '<button class="small" data-act="subInfo" data-id="'+s.id+'">Info</button>'+
              '</div>'+
              '<div class="muted ellipsis">'+gifts+more+'</div>'+
              (sw?('<div class="swRow">'+sw+'</div>'):'')+
              '<div class="muted" style="margin-top:6px">Notify max: '+notifyTxt+' TON</div>'+
              '<div class="muted">AutoBuy: '+(s.autoBuyEnabled?'ON':'OFF')+' | Max: '+buyTxt+' TON</div>'+
            '</div>'+
          '</div>'+
          '<div class="row" style="margin-top:8px">'+
            '<button class="small" data-act="subNotifyMax" data-id="'+s.id+'">Max Notify</button>'+
            '<button class="small" data-act="subAutoMax" data-id="'+s.id+'">Max AutoBuy</button>'+
            '<button class="small" data-act="subAutoToggle" data-id="'+s.id+'">'+(s.autoBuyEnabled?'AutoBuy OFF':'AutoBuy ON')+'</button>'+
            '<button class="small" data-act="subToggle" data-id="'+s.id+'">'+(s.enabled?'Disable':'Enable')+'</button>'+
            '<button class="small" data-act="subDel" data-id="'+s.id+'">Delete</button>'+
          '</div>'+
        '</div>';
      }).join('');
    }
  }

  async function refreshMarketData(force=false){
    const lots = await api('/api/mrkt/lots'+(force?'?force=1':''));
    renderLots(lots);
    scheduleRetryIfWait(lots, ()=>refreshMarketData(force));
  }

  async function refreshSummary(force=false){
    const sum = await api('/api/mrkt/summary_by_filters'+(force?'?force=1':''));
    renderSummary(sum);
    scheduleRetryIfWait(sum, ()=>refreshSummary(force));
  }

  async function refreshAll(force=false){
    await refreshState();
    await Promise.all([refreshMarketData(force), refreshSummary(force)]);
  }

  async function refreshProfile(){
    setLoading('profile', true);
    try{
      const r = await api('/api/profile');
      const u=r.user||{};
      el('profileBox').textContent = (u.username?('@'+u.username+' '):'') + 'id: '+(u.id||'-');

      const pfp = el('pfp');
      if (u.photo_url) { pfp.style.display='block'; pfp.src=u.photo_url; pfp.referrerPolicy='no-referrer'; }
      else { pfp.style.display='none'; pfp.src=''; }

      const list=r.purchases||[];
      const box=el('purchases');
      box.innerHTML = list.length
        ? list.map(p=>{
            const img = p.imgUrl ? '<img src="'+p.imgUrl+'" referrerpolicy="no-referrer" loading="lazy" style="width:52px;height:52px;border-radius:14px;border:1px solid rgba(255,255,255,.12);object-fit:cover"/>' : '<div class="thumb"></div>';
            const links =
              (p.urlMarket ? '<button class="small" data-open="'+p.urlMarket+'">MRKT</button>' : '') +
              (p.urlTelegram ? '<button class="small" data-open="'+p.urlTelegram+'">NFT</button>' : '');
            return '<div class="card">'+
              '<div class="purchRow">'+img+
                '<div style="min-width:0;flex:1">'+
                  '<div class="ellipsis"><b>'+p.title+'</b></div>'+
                  (p.model?'<div class="muted ellipsis">Model: '+p.model+'</div>':'')+
                  (p.backdrop?'<div class="muted ellipsis">Backdrop: '+p.backdrop+'</div>':'')+
                  (p.number!=null?'<div class="muted">Number: '+p.number+'</div>':'')+
                  '<div class="muted">Buy: '+(p.boughtMsk||'-')+'</div>'+
                  '<div class="muted">Price: '+Number(p.priceTon).toFixed(3)+' TON</div>'+
                '</div>'+
              '</div>'+
              '<div class="row" style="margin-top:8px">'+links+'</div>'+
            '</div>';
          }).join('')
        : '<i class="muted">Покупок пока нет</i>';

      box.querySelectorAll('button[data-open]').forEach(btn=>{
        btn.onclick = () => openTg(btn.getAttribute('data-open'));
      });

    } finally {
      setLoading('profile', false);
    }
  }

  async function refreshAdmin(){
    const r = await api('/api/admin/status');
    el('adminStatus').textContent =
      'MRKT_AUTH: '+(r.mrktAuthSet?'YES':'NO')+'\\n'+
      'token: '+(r.mrktAuthMask||'-')+'\\n'+
      'session saved: '+(r.mrktSessionSaved?'YES':'NO')+'\\n'+
      'pauseUntil: '+(r.pauseUntil ? new Date(r.pauseUntil).toLocaleTimeString('ru-RU') : '-')+'\\n'+
      'lastFail: '+(r.mrktLastFail||'-');
  }

  // Buttons
  el('apply').onclick = wrap('lots', async()=>{ await patchFilters(); await refreshAll(true); });
  el('refresh').onclick = wrap('lots', async()=>{ await refreshAll(true); });

  el('salesByFilters').onclick = wrap('lots', async()=>{
    // sales history in separate screen (sheet)
    const r = await api('/api/mrkt/sales_by_filters?force=1');
    alert('Median: ' + (r.approxPriceTon!=null ? r.approxPriceTon.toFixed(3)+' TON' : 'нет данных'));
  });

  // Subs buttons
  el('subCreate').onclick = wrap('subs', async()=>{ await api('/api/sub/create',{method:'POST'}); await refreshState(); });
  el('subRefresh').onclick = wrap('subs', async()=>{ await refreshState(); });
  el('subRebuildUi').onclick = wrap('subs', async()=>{ await api('/api/sub/rebuild_ui',{method:'POST'}); await refreshState(); });
  el('subCheckNow').onclick = wrap('subs', async()=>{
    const r = await api('/api/sub/check_now',{method:'POST'});
    alert('Проверено: '+r.processedSubs+'\\nФлор: '+r.floorNotifs+'\\nСобытий: '+r.feedNotifs);
  });

  // Subs actions
  document.body.addEventListener('click', (e) => {
    const btn = e.target.closest('button[data-act]');
    if(!btn) return;
    const act=btn.dataset.act;
    const id=btn.dataset.id;

    wrap('subs', async()=>{
      if(act==='subToggle') await api('/api/sub/toggle',{method:'POST',body:JSON.stringify({id})});
      if(act==='subDel') await api('/api/sub/delete',{method:'POST',body:JSON.stringify({id})});

      if(act==='subNotifyMax'){
        const v = prompt('Max Notify TON (пусто = без лимита):', '');
        if (v == null) return;
        await api('/api/sub/set_notify_max',{method:'POST',body:JSON.stringify({id, maxNotifyTon: v})});
      }

      if(act==='subAutoToggle') await api('/api/sub/toggle_autobuy',{method:'POST',body:JSON.stringify({id})});

      if(act==='subAutoMax'){
        const v = prompt('Max AutoBuy TON:', '');
        if(v==null) return;
        await api('/api/sub/set_autobuy_max',{method:'POST',body:JSON.stringify({id, maxAutoBuyTon: v})});
      }

      await refreshState();
    })();
  });

  // Admin actions
  el('sessSave')?.addEventListener('click', wrap('subs', async()=>{
    const payloadJson = el('payloadJson').value.trim();
    if(!payloadJson) throw new Error('Вставь payload JSON');
    const r = await api('/api/admin/session',{method:'POST',body:JSON.stringify({payloadJson})});
    alert('OK. token='+(r.tokenMask||''));
    await refreshAdmin();
  }));

  el('tokRefresh')?.addEventListener('click', wrap('subs', async()=>{
    const r = await api('/api/admin/refresh_token',{method:'POST',body:'{}'});
    alert('Refreshed token='+(r.tokenMask||''));
    await refreshAdmin();
  }));

  el('testMrkt')?.addEventListener('click', wrap('subs', async()=>{
    const r = await api('/api/admin/test_mrkt');
    alert('Collections: '+r.collectionsCount+'\\nToken: '+(r.tokenMask||'-'));
    await refreshAdmin();
  }));

  // initial load
  wrap('lots', async()=>{ await refreshAll(true); })();
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

// proxy images
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
      mrktPauseUntil: mrktState.pauseUntil || 0,
      refresh: { ...mrktAuthDebug },
      ui: { manualBuyEnabled: MANUAL_BUY_ENABLED },
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
  if (b.filters && typeof b.filters === 'object') {
    u.filters = normalizeFilters(b.filters);
  }
  scheduleSave();
  res.json({ ok: true });
});

// collections/suggest
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

  const waitMs = Math.max(0, (mrktState.pauseUntil || 0) - nowMs());
  res.json({ ok: true, items, mapLabel, waitMs: waitMs > 0 ? waitMs : 0 });
});

app.get('/api/mrkt/suggest', auth, async (req, res) => {
  const kind = String(req.query.kind || '');
  const gift = String(req.query.gift || '').trim();
  const q = String(req.query.q || '').trim().toLowerCase();

  if (kind === 'model') {
    if (!gift) return res.json({ ok: true, items: [], waitMs: 0 });
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

    const waitMs = Math.max(0, (mrktState.pauseUntil || 0) - nowMs());
    return res.json({ ok: true, items, waitMs: waitMs > 0 ? waitMs : 0 });
  }

  if (kind === 'backdrop') {
    if (!gift) return res.json({ ok: true, items: [], waitMs: 0 });
    const backs = await mrktGetBackdropsForGift(gift);
    const items = backs
      .filter((b) => !q || b.name.toLowerCase().includes(q))
      .slice(0, WEBAPP_SUGGEST_LIMIT)
      .map((b) => ({ label: b.name, value: b.name, colorHex: b.centerHex || '#000000' }));

    const waitMs = Math.max(0, (mrktState.pauseUntil || 0) - nowMs());
    return res.json({ ok: true, items, waitMs: waitMs > 0 ? waitMs : 0 });
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
  let waitMs = 0;

  if (!f.gifts.length) {
    const r = await mrktGlobalCheapestLotsReal();
    note = r.note || null;
    lots = (r.lots || []).slice(0, WEBAPP_LOTS_LIMIT);
  } else {
    const r = await mrktSearchLotsByFilters(f, WEBAPP_LOTS_PAGES || MRKT_PAGES, { ordering: 'Price', lowToHigh: true, count: MRKT_COUNT });
    if (!r.ok) {
      note = `MRKT RPS limit, wait ${fmtWaitMs(r.waitMs || 1000)}`;
      waitMs = r.waitMs || 1000;
      lots = [];
    } else {
      lots = (r.gifts || []).slice(0, WEBAPP_LOTS_LIMIT);
    }
  }

  const mapped = lots.map((lot) => ({
    ...lot,
    imgUrl: lot.giftName ? `/img/gift?name=${encodeURIComponent(lot.giftName)}` : (lot.thumbKey ? `/img/cdn?key=${encodeURIComponent(lot.thumbKey)}` : null),
    canBuy: MANUAL_BUY_ENABLED,
  }));

  const payload = { ok: true, lots: mapped, note, waitMs };
  if (!force) lotsCache.set(cacheKey, { time: nowMs(), data: payload });
  res.json(payload);
});

// summary offers only
app.get('/api/mrkt/summary_by_filters', auth, async (req, res) => {
  const force = req.query.force != null;
  const u = getOrCreateUser(req.userId);
  const f = normalizeFilters(u.filters || {});
  const cacheKey = `sum|${req.userId}|${JSON.stringify(f)}`;

  const cached = summaryCache.get(cacheKey);
  if (!force && cached && nowMs() - cached.time < SUMMARY_CACHE_TTL_MS) return res.json(cached.data);

  if (f.gifts.length !== 1) {
    const payload = { ok: true, offers: { exact: null, collection: null }, note: f.gifts.length ? 'Сводка работает для 1 gift' : 'Выбери gift', waitMs: 0 };
    if (!force) summaryCache.set(cacheKey, { time: nowMs(), data: payload });
    return res.json(payload);
  }

  const gift = f.gifts[0];
  const model = f.models.length === 1 ? f.models[0] : null;
  const backdrop = f.backdrops.length === 1 ? f.backdrops[0] : null;

  const o1 = await mrktOrdersFetch({ gift, model, backdrop });
  const o2 = await mrktOrdersFetch({ gift, model: null, backdrop: null });

  // if RPS and cached exists -> return cached with wait
  if ((o1.waitMs || o2.waitMs) && cached) {
    return res.json({ ...cached.data, note: o1.note || o2.note || cached.data.note, waitMs: Math.max(o1.waitMs || 0, o2.waitMs || 0) });
  }

  if (!o1.ok || !o2.ok) {
    return res.json({
      ok: true,
      offers: {
        exact: o1.ok ? maxOfferTonFromOrders(o1.orders) : null,
        collection: o2.ok ? maxOfferTonFromOrders(o2.orders) : null,
      },
      note: o1.note || o2.note || 'RPS limit',
      waitMs: Math.max(o1.waitMs || 0, o2.waitMs || 0),
    });
  }

  const payload = {
    ok: true,
    offers: {
      exact: maxOfferTonFromOrders(o1.orders),
      collection: maxOfferTonFromOrders(o2.orders),
    },
    note: null,
    waitMs: 0,
  };

  if (!force) summaryCache.set(cacheKey, { time: nowMs(), data: payload });
  res.json(payload);
});

// lot details: offers only
app.post('/api/lot/details', auth, async (req, res) => {
  const force = req.query.force != null;
  const lot = req.body?.lot || null;
  const lotId = String(lot?.id || '').trim();
  if (!lotId) return res.status(400).json({ ok: false, reason: 'NO_LOT' });

  const cached = detailsCache.get(lotId);
  if (!force && cached && nowMs() - cached.time < DETAILS_CACHE_TTL_MS) return res.json(cached.data);

  const gift = String(lot?.collectionName || '').trim();
  const model = lot?.model ? String(lot.model) : null;
  const backdrop = lot?.backdrop ? String(lot.backdrop) : null;

  let offerExact = null;
  let offerCollection = null;
  let note = null;
  let waitMs = 0;

  if (gift) {
    const o1 = await mrktOrdersFetch({ gift, model, backdrop });
    const o2 = await mrktOrdersFetch({ gift, model: null, backdrop: null });

    offerExact = o1.ok ? maxOfferTonFromOrders(o1.orders) : null;
    offerCollection = o2.ok ? maxOfferTonFromOrders(o2.orders) : null;

    note = o1.note || o2.note || null;
    waitMs = Math.max(o1.waitMs || 0, o2.waitMs || 0);

    if (waitMs && cached) {
      return res.json({ ...cached.data, note, waitMs });
    }
  }

  const payload = { ok: true, offers: { exact: offerExact, collection: offerCollection }, note, waitMs };
  if (!force) detailsCache.set(lotId, { time: nowMs(), data: payload });
  res.json(payload);
});

// MRKT sales by filters (still exists; WebApp uses it)
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

// Manual buy
app.post('/api/mrkt/buy', auth, async (req, res) => {
  if (!MANUAL_BUY_ENABLED) return res.status(403).json({ ok: false, reason: 'MANUAL_BUY_DISABLED' });

  const id = String(req.body?.id || '').trim();
  const priceNano = Number(req.body?.priceNano);
  const lot = req.body?.lot || null;

  if (!id) return res.status(400).json({ ok: false, reason: 'NO_ID' });
  if (!Number.isFinite(priceNano) || priceNano <= 0) return res.status(400).json({ ok: false, reason: 'BAD_PRICE' });

  const r = await mrktBuy({ id, priceNano });
  if (!r.ok) {
    if (r.reason === 'RPS_WAIT') return res.status(429).json({ ok: false, reason: `RPS limit, wait ${fmtWaitMs(r.waitMs || 1000)}` });
    return res.status(502).json({ ok: false, reason: r.reason });
  }

  const u = getOrCreateUser(req.userId);
  pushPurchase(u, {
    tsBought: Date.now(),
    title: lot?.name || 'Gift',
    priceTon: priceNano / 1e9,
    urlTelegram: lot?.urlTelegram || '',
    urlMarket: lot?.urlMarket || mrktLotUrlFromId(id),
    lotId: id,
    giftName: lot?.giftName || '',
    thumbKey: lot?.thumbKey || '',
    model: lot?.model || '',
    backdrop: lot?.backdrop || '',
    collection: lot?.collectionName || '',
    number: lot?.number ?? null,
  });
  scheduleSave();

  res.json({ ok: true, title: lot?.name || 'Gift', priceTon: priceNano / 1e9 });
});

// Profile purchases
app.get('/api/profile', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const purchases = (u.purchases || []).slice(0, 120).map((p) => ({
    title: p.title,
    lotId: p.lotId || null,
    priceTon: p.priceTon,
    boughtMsk: p.tsBought ? new Date(p.tsBought).toLocaleString('ru-RU', { timeZone: 'Europe/Moscow' }) : null,
    imgUrl: p.giftName ? `/img/gift?name=${encodeURIComponent(p.giftName)}` : (p.thumbKey ? `/img/cdn?key=${encodeURIComponent(p.thumbKey)}` : null),
    urlTelegram: p.urlTelegram || null,
    urlMarket: p.urlMarket || null,
    model: p.model || null,
    backdrop: p.backdrop || null,
    number: p.number ?? null,
  }));
  res.json({ ok: true, user: req.tgUser, purchases });
});

// ===================== Subs endpoints =====================
app.post('/api/sub/create', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const r = makeSubFromCurrentFilters(u);
  if (!r.ok) return res.status(400).json({ ok: false, reason: r.reason });

  r.sub.ui = await buildSubUi(r.sub);

  u.subscriptions.push(r.sub);
  renumberSubs(u);
  scheduleSave();

  // immediate floor notify
  setTimeout(async () => {
    try {
      const sf = normalizeFilters(r.sub.filters || {});
      const rLots = await mrktSearchLotsByFilters(sf, 1, { ordering: 'Price', lowToHigh: true, count: MRKT_COUNT });
      const lot = (rLots.ok && rLots.gifts && rLots.gifts[0]) ? rLots.gifts[0] : null;
      if (!lot) return;
      await notifyFloorToUser(req.userId, r.sub, lot, lot.priceTon);
    } catch {}
  }, 120);

  res.json({ ok: true });
});

app.post('/api/sub/rebuild_ui', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const subs = Array.isArray(u.subscriptions) ? u.subscriptions : [];
  for (const s of subs) {
    if (!s) continue;
    s.ui = await buildSubUi(s);
  }
  scheduleSave();
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
    mrktLastFail: null,
    pauseUntil: mrktState.pauseUntil || 0,
  });
});

app.post('/api/admin/session', auth, async (req, res) => {
  if (!isAdmin(req.userId)) return res.status(403).json({ ok: false, reason: 'NOT_ADMIN' });

  const payloadJson = req.body?.payloadJson;
  const j = tryParseJsonMaybe(payloadJson);
  if (!j || typeof j !== 'object') return res.status(400).json({ ok: false, reason: 'BAD_PAYLOAD_JSON' });

  const data = typeof j.data === 'string' ? j.data : null;
  const photo = (j.photo === undefined) ? null : j.photo;

  if (!data || !String(data).includes('hash=')) return res.status(400).json({ ok: false, reason: 'BAD_DATA_NO_HASH' });

  const sess = { data: String(data), photo };
  mrktSessionRuntime = sess;
  if (redis) await redisSet(REDIS_KEY_MRKT_SESSION, JSON.stringify(sess), { EX: WEBAPP_AUTH_MAX_AGE_SEC });

  const rr = await tryRefreshMrktToken('admin_save_session', { force: true });
  if (!rr.ok) return res.status(400).json({ ok: false, reason: rr.reason });

  res.json({ ok: true, tokenMask: maskToken(MRKT_AUTH_RUNTIME) });
});

app.post('/api/admin/refresh_token', auth, async (req, res) => {
  if (!isAdmin(req.userId)) return res.status(403).json({ ok: false, reason: 'NOT_ADMIN' });
  const rr = await tryRefreshMrktToken('admin_manual_refresh', { force: true });
  if (!rr.ok) return res.status(400).json({ ok: false, reason: rr.reason });
  res.json({ ok: true, tokenMask: maskToken(MRKT_AUTH_RUNTIME) });
});

app.get('/api/admin/test_mrkt', auth, async (req, res) => {
  if (!isAdmin(req.userId)) return res.status(403).json({ ok: false, reason: 'NOT_ADMIN' });
  const cols = await mrktGetCollections();
  res.json({ ok: true, collectionsCount: cols.length, tokenMask: MRKT_AUTH_RUNTIME ? maskToken(MRKT_AUTH_RUNTIME) : null });
});

// ===================== webhook/polling =====================
if (PUBLIC_URL) {
  const hookUrl = `${PUBLIC_URL.replace(/\/+$/, '')}/telegram-webhook`;
  bot.setWebHook(hookUrl).then(() => console.log('Webhook set:', hookUrl)).catch((e) => console.error('setWebHook error:', e?.message || e));
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

      const sessRaw = await redisGet(REDIS_KEY_MRKT_SESSION);
      if (sessRaw) { try { mrktSessionRuntime = JSON.parse(sessRaw); } catch {} }

      const keys = [
        REDIS_KEY_STATE,
        'bot:state:v8','bot:state:v7','bot:state:v6','bot:state:v5','bot:state:v4','bot:state:v3','bot:state:v2','bot:state:v1',
      ];
      for (const k of keys) {
        const raw = await redisGet(k);
        if (raw) { try { importState(JSON.parse(raw)); console.log('Loaded state from', k, 'users=', users.size); break; } catch {} }
      }
    }
  } else {
    console.warn('REDIS_URL not set => state/session/token won’t persist');
  }
  console.log('Bot started. /start');
})();
