/**
 * v31-fixed
 * Node 18+
 * deps: express, node-telegram-bot-api, redis(optional)
 */

const TelegramBot = require('node-telegram-bot-api');
const express = require('express');
const crypto = require('crypto');
const { Readable } = require('stream');

// ===================== ENV =====================
const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
if (!TELEGRAM_TOKEN) {
  console.error('TELEGRAM_TOKEN not set');
  process.exit(1);
}

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
// Если в Railway выставлено слишком большое значение — ограничиваем до 800ms
const MRKT_MIN_GAP_MS = Math.min(Number(process.env.MRKT_MIN_GAP_MS || 400), 800);
const MRKT_429_DEFAULT_PAUSE_MS = Number(process.env.MRKT_429_DEFAULT_PAUSE_MS || 4500);
const MRKT_HTTP_MAX_WAIT_MS = Number(process.env.MRKT_HTTP_MAX_WAIT_MS || 2500);

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
const CACHE_TTL_MS = Number(process.env.CACHE_TTL_MS || 10 * 60_000);
const OFFERS_CACHE_TTL_MS = Number(process.env.OFFERS_CACHE_TTL_MS || 25_000);
const LOTS_CACHE_TTL_MS = Number(process.env.LOTS_CACHE_TTL_MS || 8000);
const DETAILS_CACHE_TTL_MS = Number(process.env.DETAILS_CACHE_TTL_MS || 8000);
const SALES_CACHE_TTL_MS = Number(process.env.SALES_CACHE_TTL_MS || 25_000);
const SUMMARY_CACHE_TTL_MS = Number(process.env.SUMMARY_CACHE_TTL_MS || 5000);

// subs
const SUBS_CHECK_INTERVAL_MS = Number(process.env.SUBS_CHECK_INTERVAL_MS || 8000);
const SUBS_MAX_NOTIFICATIONS_PER_CYCLE = Number(process.env.SUBS_MAX_NOTIFICATIONS_PER_CYCLE || 20);
const SUBS_EMPTY_CONFIRM = Number(process.env.SUBS_EMPTY_CONFIRM || 2);
// change_price невалидный тип для MRKT /feed API — убираем из дефолта
const SUBS_FEED_TYPES_RAW = String(process.env.SUBS_FEED_TYPES || 'sale,listing');
const SUBS_FEED_TYPES = new Set(
  SUBS_FEED_TYPES_RAW.split(',').map((s) => s.trim().toLowerCase()).filter(Boolean)
);
const SUBS_FEED_MAX_EVENTS_PER_CYCLE = Number(process.env.SUBS_FEED_MAX_EVENTS_PER_CYCLE || 20);
// пауза между подписками чтобы не получать 429
const SUBS_BETWEEN_PAUSE_MS = Number(process.env.SUBS_BETWEEN_PAUSE_MS || 200);

// Telegram notifications
const SUBS_SEND_PHOTO = String(process.env.SUBS_SEND_PHOTO || '0') === '1';

// AutoBuy
const AUTO_BUY_GLOBAL = String(process.env.AUTO_BUY_GLOBAL || '0') === '1';
const AUTO_BUY_DRY_RUN = String(process.env.AUTO_BUY_DRY_RUN || '1') === '1';
const AUTO_BUY_CHECK_INTERVAL_MS = Number(process.env.AUTO_BUY_CHECK_INTERVAL_MS || 3000);
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

// manual buy button
const MANUAL_BUY_ENABLED = String(process.env.MANUAL_BUY_ENABLED || '0') === '1';

// Redis keys
const REDIS_KEY_STATE = 'bot:state:main';
const REDIS_KEY_MRKT_AUTH = 'mrkt:auth:token';
const REDIS_KEY_MRKT_SESSION = 'mrkt:session:admin';

console.log('v31-fixed start', {
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
  MANUAL_BUY_ENABLED,
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
function cleanDigitsPrefix(v, maxLen = 12) {
  return String(v || '').replace(/\D/g, '').slice(0, maxLen);
}
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
function tonFromNano(nano) {
  const x = Number(nano);
  return Number.isFinite(x) ? x / 1e9 : null;
}
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
  if (x === 'listing') return 'Листинг';
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
    if (r.body && Readable.fromWeb) {
      Readable.fromWeb(r.body).pipe(res);
      return;
    }
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
function isAdmin(userId) {
  return ADMIN_USER_ID && Number(userId) === Number(ADMIN_USER_ID);
}

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
const MAIN_KEYBOARD = {
  keyboard: [[{ text: 'Статус' }]],
  resize_keyboard: true,
};

async function sendMessageSafe(chatId, text, opts) {
  while (true) {
    try { return await bot.sendMessage(chatId, text, opts); }
    catch (e) {
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
async function sendPhotoSafe(chatId, photoUrl, caption, reply_markup) {
  while (true) {
    try {
      return await bot.sendPhoto(chatId, photoUrl, {
        caption,
        reply_markup,
        disable_notification: false,
      });
    } catch (e) {
      const retryAfter =
        e?.response?.body?.parameters?.retry_after ??
        e?.response?.parameters?.retry_after ??
        null;
      if (retryAfter) {
        await sleep((Number(retryAfter) + 1) * 1000);
        continue;
      }
      return await sendMessageSafe(chatId, caption, {
        reply_markup,
        disable_web_page_preview: false,
      });
    }
  }
}

// ===================== State =====================
const users = new Map();
const subStates = new Map();
const autoBuyRecentAttempts = new Map();
const recentSubCreates = new Map();

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
  return Number(userId);
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
    tsListed: entry.tsListed || null,
    tsFound: entry.tsFound || null,
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
        autoBuyAny: !!s.autoBuyAny,
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
  const body = {
    appId: null,
    data: String(session?.data || ''),
    photo: session?.photo ?? null,
  };

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

  if (!res) return { ok: false, status: 0, reason: 'FETCH_ERROR', text: '' };

  const txt = await res.text().catch(() => '');
  let data = null;
  try { data = txt ? JSON.parse(txt) : null; } catch {}

  if (!res.ok) {
    return {
      ok: false,
      status: res.status,
      reason: extractMrktErrorMessage(txt) || `HTTP_${res.status}`,
      text: txt,
    };
  }

  const token = data?.token || null;
  if (!token) return { ok: false, status: res.status, reason: 'NO_TOKEN_IN_RESPONSE', text: txt };

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
      return { ok: false, reason: r.reason, status: r.status };
    }

    MRKT_AUTH_RUNTIME = r.token;
    mrktAuthDebug.lastOkAt = nowMs();
    mrktAuthDebug.lastStatus = r.status;
    mrktAuthDebug.lastReason = null;
    mrktAuthDebug.lastTokenMask = maskToken(r.token);

    if (redis) await redisSet(REDIS_KEY_MRKT_AUTH, r.token);

    collectionsCache = { time: 0, items: [] };
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
// Отдельный state для веб-запросов (от пользователя через WebApp)
const mrktState = {
  lastOkAt: 0,
  lastFailAt: 0,
  lastFailMsg: null,
  lastFailStatus: null,
  lastFailEndpoint: null,
  pauseUntil: 0,
  nextAllowedAt: 0,
};
// Отдельный state для фоновых запросов (подписки, AutoBuy)
// Полностью изолирован от веб-запросов — 429 в фоне не блокирует WebApp и наоборот
const bgState = {
  pauseUntil: 0,
  nextAllowedAt: 0,
  lastFailMsg: null,
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
function markMrktOk() {
  mrktState.lastOkAt = nowMs();
  mrktState.lastFailAt = 0;
  mrktState.lastFailMsg = null;
  mrktState.lastFailStatus = null;
  mrktState.lastFailEndpoint = null;
}
function markMrktFail(endpoint, status, txt) {
  mrktState.lastFailAt = nowMs();
  mrktState.lastFailEndpoint = endpoint || null;
  mrktState.lastFailStatus = status || null;
  mrktState.lastFailMsg = extractMrktErrorMessage(txt) || (status ? `HTTP ${status}` : 'MRKT error');
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
      if (!ok) {
        markMrktFail(path, 401, 'NO_AUTH');
        return { ok: false, status: 401, data: null, text: 'NO_AUTH' };
      }
    }

    const gate = await mrktGateCheckOrWait();
    if (!gate.ok) return { ok: false, status: 429, data: null, text: 'RPS_WAIT', waitMs: gate.waitMs };

    const url = `${MRKT_API_URL}${path}${path.includes('?') ? '&' : '?'}req=${encodeURIComponent(makeReq())}`;
    const res = await fetchWithTimeout(url, { method: 'GET', headers: mrktHeadersCommon() }, MRKT_TIMEOUT_MS).catch(() => null);
    if (!res) {
      markMrktFail(path, 0, 'FETCH_ERROR');
      return { ok: false, status: 0, data: null, text: 'FETCH_ERROR' };
    }

    const txt = await res.text().catch(() => '');
    let data = null;
    try { data = txt ? JSON.parse(txt) : null; } catch {}

    if (!res.ok) {
      if (res.status === 429 || bodyLooksLikeRpsLimit(txt)) {
        setPauseFrom429(res);
        return { ok: false, status: 429, data, text: txt, waitMs: (mrktState.pauseUntil - nowMs()) };
      }

      markMrktFail(path, res.status, txt);

      if ((res.status === 401 || res.status === 403) && retry) {
        const rr = await tryRefreshMrktToken(`HTTP_${res.status}:${path}`, { force: false });
        if (rr.ok) return mrktGetJson(path, { retry: false });
      }

      return { ok: false, status: res.status, data, text: txt };
    }

    markMrktOk();
    return { ok: true, status: res.status, data, text: txt };
  });
}

async function mrktPostJson(path, bodyObj, { retry = true } = {}) {
  return mrktRunExclusive(async () => {
    if (!MRKT_AUTH_RUNTIME) {
      const ok = await ensureMrktAuth();
      if (!ok) {
        markMrktFail(path, 401, 'NO_AUTH');
        return { ok: false, status: 401, data: null, text: 'NO_AUTH' };
      }
    }

    const gate = await mrktGateCheckOrWait();
    if (!gate.ok) return { ok: false, status: 429, data: null, text: 'RPS_WAIT', waitMs: gate.waitMs };

    const reqVal = bodyObj?.req ? String(bodyObj.req) : makeReq();
    const body = { ...(bodyObj || {}), req: reqVal };
    const url = `${MRKT_API_URL}${path}${path.includes('?') ? '&' : '?'}req=${encodeURIComponent(reqVal)}`;

    const res = await fetchWithTimeout(url, {
      method: 'POST',
      headers: mrktHeadersJson(),
      body: JSON.stringify(body),
    }, MRKT_TIMEOUT_MS).catch(() => null);

    if (!res) {
      markMrktFail(path, 0, 'FETCH_ERROR');
      return { ok: false, status: 0, data: null, text: 'FETCH_ERROR' };
    }

    const txt = await res.text().catch(() => '');
    let data = null;
    try { data = txt ? JSON.parse(txt) : null; } catch {}

    if (!res.ok) {
      if (res.status === 429 || bodyLooksLikeRpsLimit(txt)) {
        setPauseFrom429(res);
        return { ok: false, status: 429, data, text: txt, waitMs: (mrktState.pauseUntil - nowMs()) };
      }

      markMrktFail(path, res.status, txt);

      if ((res.status === 401 || res.status === 403) && retry) {
        const rr = await tryRefreshMrktToken(`HTTP_${res.status}:${path}`, { force: false });
        if (rr.ok) return mrktPostJson(path, bodyObj, { retry: false });
      }

      return { ok: false, status: res.status, data, text: txt };
    }

    markMrktOk();
    return { ok: true, status: res.status, data, text: txt };
  });
}

// ===================== MRKT business =====================
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

  // Пробуем оба варианта запроса — с collectionNames и collections
  let r = await mrktPostJson('/gifts/models', { collectionNames: [giftName], collections: [giftName] });
  if (!r.ok) {
    // Fallback — только collections
    r = await mrktPostJson('/gifts/models', { collections: [giftName] });
  }
  if (!r.ok) {
    console.warn('[MODELS] Не удалось загрузить модели для', giftName, 'status=', r.status);
    return cached?.items || [];
  }

  // MRKT может вернуть массив напрямую или объект с полем models/items
  let arr = [];
  if (Array.isArray(r.data)) {
    arr = r.data;
  } else if (r.data && Array.isArray(r.data.models)) {
    arr = r.data.models;
  } else if (r.data && Array.isArray(r.data.items)) {
    arr = r.data.items;
  }

  const map = new Map();
  for (const it of arr) {
    const name = it.modelTitle || it.modelName || it.title || it.name;
    if (!name) continue;
    const key = normTraitName(name);
    if (!map.has(key)) {
      map.set(key, {
        name: String(name),
        thumbKey: it.modelStickerThumbnailKey || it.stickerThumbnailKey || null,
        floorNano: it.floorPriceNanoTons ?? it.floorPrice ?? null,
        rarityPerMille: it.rarityPerMille ?? null,
      });
    }
  }
  const items = Array.from(map.values());
  if (items.length > 0) {
    modelsCache.set(giftName, { time: nowMs(), items });
  }
  console.log('[MODELS]', giftName, '→', items.length, 'моделей');
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
    const hex = Number.isFinite(num)
      ? ('#' + ((num >>> 0).toString(16).padStart(6, '0')).slice(-6))
      : null;

    if (!map.has(key)) map.set(key, { name: String(name), centerHex: hex });
  }

  const items = Array.from(map.values());
  backdropsCache.set(giftName, { time: nowMs(), items });
  return items;
}

function buildSalingBody({
  count = 20,
  cursor = '',
  collectionNames = [],
  modelNames = [],
  backdropNames = [],
  ordering = 'Price',
  lowToHigh = true,
  number = null,
} = {}) {
  return {
    count,
    cursor,
    collectionNames,
    modelNames,
    backdropNames,
    symbolNames: [],
    ordering,
    lowToHigh,
    maxPrice: null,
    minPrice: null,
    number: number != null ? Number(number) : null,
    query: null,
    promotedFirst: false,
  };
}

async function mrktFetchSalingPage({ collectionNames, modelNames, backdropNames, cursor, ordering, lowToHigh, count, number }) {
  const body = buildSalingBody({
    count: Number(count || MRKT_COUNT),
    cursor: cursor || '',
    collectionNames: Array.isArray(collectionNames) ? collectionNames : [],
    modelNames: Array.isArray(modelNames) ? modelNames : [],
    backdropNames: Array.isArray(backdropNames) ? backdropNames : [],
    ordering: ordering || 'Price',
    lowToHigh: !!lowToHigh,
    number: number != null ? Number(number) : null,
  });

  const r = await mrktPostJson('/gifts/saling', body);
  if (!r.ok) {
    return {
      ok: false,
      reason: r.status === 429 ? 'RPS_WAIT' : 'ERROR',
      waitMs: r.waitMs || 0,
      gifts: [],
      cursor: '',
    };
  }

  const gifts = Array.isArray(r.data?.gifts) ? r.data.gifts : [];
  const nextCursor = r.data?.cursor || r.data?.nextCursor || '';
  return { ok: true, reason: 'OK', gifts, cursor: nextCursor };
}

function salingGiftToLot(g) {
  const nanoA = g?.salePriceWithoutFee ?? null;
  const nanoB = g?.salePrice ?? null;
  const nano = (nanoA != null && Number(nanoA) > 0)
    ? Number(nanoA)
    : (nanoB != null ? Number(nanoB) : NaN);

  const priceTon = Number(nano) / 1e9;
  if (!Number.isFinite(priceTon) || priceTon <= 0) return null;

  const numberVal = g.number ?? null;
  const baseName = (g.collectionTitle || g.collectionName || g.title || 'Gift').trim();
  const displayName = numberVal != null ? `${baseName} #${numberVal}` : baseName;

  const giftName = g.name || giftNameFallbackFromCollectionAndNumber(baseName, numberVal) || null;
  const urlTelegram = giftName && String(giftName).includes('-')
    ? `https://t.me/nft/${giftName}`
    : 'https://t.me/mrkt';
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
  if (cached && now - cached.time < OFFERS_CACHE_TTL_MS) return { ...cached.data, cached: true };

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
    if (cached) return { ...cached.data, cached: true, note: `RPS limit, wait ${fmtWaitMs(r.waitMs || 1000)}`, waitMs: r.waitMs || 1000 };
    return { ok: false, orders: [], reason: r.status === 429 ? 'RPS_WAIT' : 'ORDERS_ERROR', waitMs: r.waitMs || 0 };
  }

  const out = {
    ok: true,
    orders: Array.isArray(r.data?.orders) ? r.data.orders : [],
    reason: 'OK',
    waitMs: 0,
  };
  offersCache.set(key, { time: now, data: out });
  return out;
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

// MRKT API принимает только типы: sale, listing (change_price — невалидный тип!)
const MRKT_VALID_FEED_TYPES = new Set(['sale', 'listing']);

async function mrktFeedFetch({ collectionNames = [], modelNames = [], backdropNames = [], cursor = '', count = MRKT_FEED_COUNT, ordering = 'Latest', types = [] }) {
  // type: [] = все события (sale + listing). Передаём только валидные типы.
  const validTypes = (Array.isArray(types) ? types : []).filter(t => MRKT_VALID_FEED_TYPES.has(String(t).toLowerCase()));
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
    type: validTypes,
  };

  const r = await mrktPostJson('/feed', body);
  if (!r.ok) {
    if (r.status === 429) return { ok: false, reason: 'RPS_WAIT', waitMs: r.waitMs || 0, items: [], cursor: '' };
    return { ok: false, reason: mrktState.lastFailMsg || 'FEED_ERROR', waitMs: 0, items: [], cursor: '' };
  }

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
        if (r.reason === 'RPS_WAIT') {
          if (cached) return { ...cached.data, note: `RPS limit, wait ${fmtWaitMs(r.waitMs || 1000)}`, waitMs: r.waitMs || 1000 };
          return { ok: true, approxPriceTon: null, sales: [], note: `RPS limit, wait ${fmtWaitMs(r.waitMs || 1000)}`, waitMs: r.waitMs || 1000 };
        }
        break;
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
          giftName,
          imgUrl,
          model: g.modelTitle || g.modelName || null,
          backdrop: g.backdropName || null,
          urlMarket,
          urlTelegram,
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

// ===================== Sub UI =====================
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
  return sendMessageSafe(chatId, text, {
    disable_web_page_preview: false,
    reply_markup,
  });
}

// ===================== Subscription notifications =====================
async function notifyFloorToUser(userId, sub, lot, newFloor, prevFloor) {
  const chatId = userChatId(userId);
  const now = new Date();
  const timeStr = now.toLocaleTimeString('ru-RU', { timeZone: 'Europe/Moscow', hour12: false }) +
    '.' + String(now.getMilliseconds()).padStart(3, '0');
  console.log(`[NOTIFY FLOOR] userId=${userId} chatId=${chatId} sub=#${sub.num} floor=${newFloor} time=${timeStr}`);

  const isFirst = prevFloor == null;
  const went = !isFirst ? (newFloor < prevFloor ? '📉' : newFloor > prevFloor ? '📈' : '➡️') : '🆕';

  const lines = [];
  lines.push(`🔔 Подписка #${sub.num} — Флор ${went}`);
  lines.push(`Gift: ${subGiftTitle(sub)}`);
  lines.push(`Цена: ${newFloor.toFixed(3)} TON${!isFirst ? ` (было ${Number(prevFloor).toFixed(3)})` : ''}`);
  if (lot?.model) lines.push(`Model: ${lot.model}`);
  if (lot?.backdrop) lines.push(`Backdrop: ${lot.backdrop}`);
  if (lot?.number != null) lines.push(`#${lot.number}`);
  lines.push(`⏱ ${timeStr} MSK`);
  lines.push('');
  lines.push(lot?.urlTelegram || 'https://t.me/mrkt');

  const reply_markup = {
    inline_keyboard: [[
      ...(lot?.urlTelegram ? [{ text: '🎁 NFT', url: lot.urlTelegram }] : []),
      ...(lot?.urlMarket ? [{ text: '🛍 MRKT', url: lot.urlMarket }] : []),
    ]]
  };
  await notifyTextOrPhoto(chatId, null, lines.join('\n'), reply_markup);
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
    giftName,
    imgUrl,
    urlTelegram,
    urlMarket,
    gift: g,
    raw: it,
  };
}

async function notifyFeedEvent(userId, sub, ev) {
  const chatId = userChatId(userId);
  const now = new Date();
  const timeStr = now.toLocaleTimeString('ru-RU', { timeZone: 'Europe/Moscow', hour12: false }) +
    '.' + String(now.getMilliseconds()).padStart(3, '0');
  console.log(`[NOTIFY FEED] userId=${userId} chatId=${chatId} sub=#${sub.num} type=${ev.type} price=${ev.priceTon} time=${timeStr}`);

  const typeIcon = ev.type === 'sale' ? '💰' : ev.type === 'listing' ? '🏷' : '📌';
  const typeLabel = ev.type === 'sale' ? 'Продажа' : ev.type === 'listing' ? 'Листинг' : 'Событие';

  const lines = [];
  lines.push(`🔔 Подписка #${sub.num} — ${typeIcon} ${typeLabel}`);
  lines.push(`Gift: ${subGiftTitle(sub)}`);
  if (ev.priceTon != null) lines.push(`Цена: ${ev.priceTon.toFixed(3)} TON`);
  if (ev.model) lines.push(`Model: ${ev.model}`);
  if (ev.backdrop) lines.push(`Backdrop: ${ev.backdrop}`);
  if (ev.title) lines.push(ev.title);
  lines.push(`⏱ ${timeStr} MSK`);
  lines.push('');
  lines.push(ev.urlTelegram || 'https://t.me/mrkt');

  const reply_markup = {
    inline_keyboard: [[
      ...(ev.urlTelegram ? [{ text: '🎁 NFT', url: ev.urlTelegram }] : []),
      ...(ev.urlMarket ? [{ text: '🛍 MRKT', url: ev.urlMarket }] : []),
    ]]
  };
  await notifyTextOrPhoto(chatId, null, lines.join('\n'), reply_markup);
}

// ===================== Subs worker =====================
let isSubsChecking = false;

async function processFeedForSub(userId, sub, stateKey, budget) {
  if (budget <= 0) return 0;

  const f = normalizeFilters(sub.filters || {});
  if (!f.gifts.length) return 0;

  const models = f.gifts.length === 1 ? (f.models || []) : [];
  const backdrops = f.gifts.length === 1 ? (f.backdrops || []) : [];

  // Загружаем из Redis если в памяти нет (переживает рестарт)
  let st = subStates.get(stateKey);
  if (!st) {
    const saved = await redisGet(`sub:feed:${stateKey}`).catch(() => null);
    if (saved) { try { st = JSON.parse(saved); } catch { st = { feedLastId: null }; } }
    else { st = { feedLastId: null }; }
    subStates.set(stateKey, st);
  }

  const r = await mrktFeedFetchBackground({
    collectionNames: f.gifts,
    modelNames: models,
    backdropNames: backdrops,
    cursor: '',
    count: 50,
    types: [], // [] = все события (sale + listing). MRKT не принимает change_price
  });

  console.log(`[FEED] Sub #${sub.num} userId=${userId}: feedFetch ok=${r.ok} items=${r.items?.length} reason=${r.reason||''}`);

  if (!r.ok) {
    console.error(`[FEED] Sub #${sub.num}: ошибка feedFetch reason=${r.reason}`);
    return 0;
  }
  if (!r.items.length) {
    console.log(`[FEED] Sub #${sub.num}: нет событий в фиде`);
    return 0;
  }

  const latestId = r.items[0]?.id || null;
  if (!latestId) return 0;

  if (!st.feedLastId) {
    const newSt = { ...st, feedLastId: latestId };
    subStates.set(stateKey, newSt);
    if (redis) redisSet(`sub:feed:${stateKey}`, JSON.stringify(newSt), { EX: 86400 * 7 }).catch(() => {});
    console.log(`[FEED] Sub #${sub.num} userId=${userId}: первый запуск, запомнили lastId=${latestId}`);
    return 0;
  }

  if (latestId === st.feedLastId) {
    // Нет новых событий
    return 0;
  }

  const newItems = [];
  for (const it of r.items) {
    if (!it?.id) continue;
    if (it.id === st.feedLastId) break;
    newItems.push(it);
  }

  if (!newItems.length) {
    const newSt = { ...st, feedLastId: latestId };
    subStates.set(stateKey, newSt);
    if (redis) redisSet(`sub:feed:${stateKey}`, JSON.stringify(newSt), { EX: 86400 * 7 }).catch(() => {});
    return 0;
  }

  console.log(`[FEED] Sub #${sub.num} userId=${userId}: найдено новых событий: ${newItems.length}`);

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

    try {
      await notifyFeedEvent(userId, sub, ev);
      sent++;
    } catch (e) {
      console.error(`[FEED] Ошибка отправки уведомления userId=${userId}:`, e?.message || e);
    }
  }

  const newSt2 = { ...st, feedLastId: latestId };
  subStates.set(stateKey, newSt2);
  if (redis) redisSet(`sub:feed:${stateKey}`, JSON.stringify(newSt2), { EX: 86400 * 7 }).catch(() => {});
  return sent;
}

// Отдельная функция для запросов в фоне (подписки/autobuy)
// НЕ использует mrktRunExclusive — работает параллельно с веб-запросами
// но с большими паузами чтобы не получать 429
async function mrktPostJsonBackground(path, bodyObj) {
  if (!MRKT_AUTH_RUNTIME) return { ok: false, status: 401, data: null, text: 'NO_AUTH' };

  // Используем ОТДЕЛЬНЫЙ bgState — изолирован от веб-запросов
  const now = nowMs();
  const pauseMs = (bgState.pauseUntil && now < bgState.pauseUntil) ? (bgState.pauseUntil - now) : 0;
  const gapMs = (bgState.nextAllowedAt && now < bgState.nextAllowedAt) ? (bgState.nextAllowedAt - now) : 0;
  const waitMs0 = Math.max(pauseMs, gapMs);

  if (pauseMs > 20000) {
    console.log(`[BG] Пауза слишком долгая (${Math.round(pauseMs/1000)}s), пропускаем запрос`);
    return { ok: false, status: 429, data: null, text: 'RPS_WAIT', waitMs: pauseMs };
  }
  if (waitMs0 > 0) await sleep(waitMs0);

  // Минимальный gap между фоновыми запросами — 500ms
  bgState.nextAllowedAt = nowMs() + 350;

  const reqVal = makeReq();
  const body = { ...(bodyObj || {}), req: reqVal };
  const url = `${MRKT_API_URL}${path}?req=${encodeURIComponent(reqVal)}`;

  const res = await fetchWithTimeout(url, {
    method: 'POST',
    headers: mrktHeadersJson(),
    body: JSON.stringify(body),
  }, MRKT_TIMEOUT_MS).catch(() => null);

  if (!res) return { ok: false, status: 0, data: null, text: 'FETCH_ERROR' };

  const txt = await res.text().catch(() => '');
  let data = null;
  try { data = txt ? JSON.parse(txt) : null; } catch {}

  if (!res.ok) {
    if (res.status === 429 || bodyLooksLikeRpsLimit(txt)) {
      // Ставим паузу ТОЛЬКО в bgState — не трогаем веб-запросы
      const ra = res.headers?.get?.('retry-after');
      const pauseDur = ra ? (Number(ra) * 1000) : 8000;
      bgState.pauseUntil = nowMs() + pauseDur;
      bgState.lastFailMsg = 'RPS_WAIT';
      console.log(`[BG] 429 на ${path}, пауза ${Math.round(pauseDur/1000)}s (изолировано от веб)`);
      return { ok: false, status: 429, data, text: txt, waitMs: pauseDur };
    }
    if (res.status === 401 || res.status === 403) {
      await tryRefreshMrktToken(`BG_${res.status}`, { force: false });
    }
    bgState.lastFailMsg = `HTTP_${res.status}`;
    return { ok: false, status: res.status, data, text: txt };
  }

  bgState.lastFailMsg = null;
  return { ok: true, status: res.status, data, text: txt };
}

// (константа перенесена выше)

async function mrktFeedFetchBackground({ collectionNames = [], modelNames = [], backdropNames = [], cursor = '', count = 50, types = [] }) {
  // type: [] = все события. Передаём только валидные типы (sale, listing).
  const validTypes = (Array.isArray(types) ? types : []).filter(t => MRKT_VALID_FEED_TYPES.has(String(t).toLowerCase()));

  const reqVal = makeReq();
  const body = {
    count: Number(count),
    cursor: cursor || '',
    collectionNames: Array.isArray(collectionNames) ? collectionNames : [],
    modelNames: Array.isArray(modelNames) ? modelNames : [],
    backdropNames: Array.isArray(backdropNames) ? backdropNames : [],
    lowToHigh: false,
    maxPrice: null,
    minPrice: null,
    number: null,
    ordering: 'Latest',
    query: null,
    type: validTypes,
  };

  const r = await mrktPostJsonBackground('/feed', body);
  if (!r.ok) {
    // Логируем реальный ответ от MRKT чтобы понять в чём проблема
    console.log(`[BG FEED ERROR] status=${r.status} text=${String(r.text || '').slice(0, 200)}`);
    if (r.status === 429 || r.text === 'RPS_WAIT') {
      return { ok: false, reason: 'RPS_WAIT', waitMs: r.waitMs || 8000, items: [], cursor: '' };
    }
    if (r.status === 401 || r.status === 403) {
      // Токен протух — пробуем обновить
      console.log('[BG FEED] 401/403 — пробуем обновить токен');
      await tryRefreshMrktToken('bg_feed_401', { force: true });
      return { ok: false, reason: 'AUTH_ERROR', waitMs: 0, items: [], cursor: '' };
    }
    return { ok: false, reason: 'FEED_ERROR', waitMs: 0, items: [], cursor: '' };
  }

  const items = Array.isArray(r.data?.items) ? r.data.items : [];
  const nextCursor = r.data?.cursor || r.data?.nextCursor || '';
  console.log(`[BG FEED OK] items=${items.length} cursor=${!!nextCursor}`);
  return { ok: true, reason: 'OK', items, cursor: nextCursor };
}

async function mrktSearchLotsBackground(collectionNames, modelNames, backdropNames, count = 10) {
  const body = {
    count: Number(count),
    cursor: '',
    collectionNames: Array.isArray(collectionNames) ? collectionNames : [],
    modelNames: Array.isArray(modelNames) ? modelNames : [],
    backdropNames: Array.isArray(backdropNames) ? backdropNames : [],
    ordering: 'Price',
    lowToHigh: true,
    maxPrice: null,
    minPrice: null,
    number: null,
    query: null,
    promotedFirst: false,
  };

  const r = await mrktPostJsonBackground('/gifts/saling', body);
  if (!r.ok) return { ok: false, reason: r.status === 429 ? 'RPS_WAIT' : 'ERROR', waitMs: r.waitMs || 0, gifts: [] };

  const gifts = Array.isArray(r.data?.gifts) ? r.data.gifts : [];
  return { ok: true, gifts };
}

async function checkSubscriptionsForAllUsers({ manual = false } = {}) {
  if (MODE !== 'real') return { processedSubs: 0, floorNotifs: 0, feedNotifs: 0 };
  if (isSubsChecking && !manual) return { processedSubs: 0, floorNotifs: 0, feedNotifs: 0 };

  const totalActive = Array.from(users.values()).reduce((acc, u) =>
    acc + (u.subscriptions || []).filter(s => s && s.enabled).length, 0);
  console.log(`[SUBS CYCLE] users=${users.size} activeSubs=${totalActive} auth=${!!MRKT_AUTH_RUNTIME}`);

  isSubsChecking = true;
  try {
    let processedSubs = 0;
    let floorNotifs = 0;
    let feedNotifs = 0;

    for (const [userId, user] of users.entries()) {
      if (!user.enabled) continue;
      const subs = Array.isArray(user.subscriptions) ? user.subscriptions : [];
      const active = subs.filter((s) => s && s.enabled);
      if (!active.length) continue;

      // === ШАГ 1: Группируем подписки по gift и делаем ОДИН запрос на каждый gift ===
      // Вместо 8 запросов feed делаем 4-5 (по количеству уникальных gifts)
      const giftFeedCache = new Map(); // giftKey -> { items, ok }

      const getGiftKey = (sub) => {
        const f = normalizeFilters(sub.filters || {});
        return f.gifts.slice().sort().join('|') + '::' +
          (f.gifts.length === 1 ? f.models.slice().sort().join('|') : '') + '::' +
          (f.gifts.length === 1 ? f.backdrops.slice().sort().join('|') : '');
      };

      // Собираем уникальные комбинации
      const uniqueKeys = new Map();
      for (const sub of active) {
        const key = getGiftKey(sub);
        if (!uniqueKeys.has(key)) {
          const f = normalizeFilters(sub.filters || {});
          uniqueKeys.set(key, {
            collectionNames: f.gifts,
            modelNames: f.gifts.length === 1 ? f.models : [],
            backdropNames: f.gifts.length === 1 ? f.backdrops : [],
          });
        }
      }

      // Делаем feed запросы для каждой уникальной комбинации
      console.log(`[SUBS] userId=${userId}: ${active.length} подписок, ${uniqueKeys.size} уникальных gift-групп`);

      // Пауза если был 429
      if (bgState.pauseUntil > nowMs()) {
        const waitFor = Math.min(bgState.pauseUntil - nowMs() + 100, 12000);
        console.log(`[SUBS] 429 пауза ${Math.round(waitFor/1000)}s`);
        await sleep(waitFor);
      }

      for (const [key, params] of uniqueKeys.entries()) {
        const r = await mrktFeedFetchBackground({
          collectionNames: params.collectionNames,
          modelNames: params.modelNames,
          backdropNames: params.backdropNames,
          cursor: '',
          count: 50,
          types: [],
        });
        giftFeedCache.set(key, r);
        // Минимальная пауза между уникальными gift запросами
        if (uniqueKeys.size > 1) await sleep(200);
      }

      // === ШАГ 2: Обрабатываем каждую подписку используя кэшированный feed ===
      // AutoBuy тоже здесь — использует тот же кэш, без лишних запросов
      let autoBuysDone = 0;

      for (const sub of active) {
        processedSubs++;
        const key = getGiftKey(sub);
        const feedResult = giftFeedCache.get(key) || { ok: false, items: [], reason: 'NO_CACHE' };

        // Feed уведомления — из кэша, без доп запросов
        if (feedResult.ok && feedResult.items.length > 0) {
          const stateKeyFeed = `${userId}:${sub.id}:feed`;
          try {
            let st = subStates.get(stateKeyFeed);
            if (!st) {
              const saved = await redisGet(`sub:feed:${stateKeyFeed}`).catch(() => null);
              st = saved ? (JSON.parse(saved) || { feedLastId: null }) : { feedLastId: null };
              subStates.set(stateKeyFeed, st);
            }

            const latestId = feedResult.items[0]?.id || null;
            if (latestId && !st.feedLastId) {
              // Первый запуск — запоминаем позицию
              const newSt = { feedLastId: latestId };
              subStates.set(stateKeyFeed, newSt);
              if (redis) redisSet(`sub:feed:${stateKeyFeed}`, JSON.stringify(newSt), { EX: 86400 * 7 }).catch(() => {});
            } else if (latestId && latestId !== st.feedLastId) {
              // Есть новые события
              const newItems = [];
              for (const it of feedResult.items) {
                if (!it?.id) continue;
                if (it.id === st.feedLastId) break;
                newItems.push(it);
              }

              if (newItems.length > 0) {
                console.log(`[FEED] Sub #${sub.num}: найдено новых событий: ${newItems.length}`);
                newItems.reverse();
                let sent = 0;
                for (const it of newItems) {
                  if (sent >= 5) break;
                  const type = String(it?.type || '').toLowerCase();
                  if (!SUBS_FEED_TYPES.has(type)) continue;
                  const ev = feedItemToEvent(it);
                  if (!ev) continue;
                  if (sub.maxNotifyTon != null && ev.priceTon != null && Number(ev.priceTon) > Number(sub.maxNotifyTon)) continue;
                  try { await notifyFeedEvent(userId, sub, ev); sent++; feedNotifs++; } catch {}
                }
              }

              const newSt2 = { feedLastId: latestId };
              subStates.set(stateKeyFeed, newSt2);
              if (redis) redisSet(`sub:feed:${stateKeyFeed}`, JSON.stringify(newSt2), { EX: 86400 * 7 }).catch(() => {});
            }
          } catch (e) {
            console.error(`[SUBS] Feed ошибка sub #${sub.num}:`, e?.message);
          }
        }

        // === AutoBuy — используем тот же feedResult, без нового запроса ===
        if (AUTO_BUY_GLOBAL && sub.autoBuyEnabled && sub.maxAutoBuyTon != null &&
            autoBuysDone < AUTO_BUY_MAX_BUYS_PER_USER_PER_CYCLE && feedResult.ok && feedResult.items.length > 0) {
          try {
            const maxBuy = Number(sub.maxAutoBuyTon);
            if (Number.isFinite(maxBuy) && maxBuy > 0) {
              const stateKey = `${userId}:${sub.id}:autobuy`;
              let st = subStates.get(stateKey);
              if (!st) {
                const saved = await redisGet(`sub:autobuy:${stateKey}`).catch(() => null);
                st = saved ? (JSON.parse(saved) || { autoBuyLastId: null }) : { autoBuyLastId: null };
                subStates.set(stateKey, st);
              }

              const latestId = feedResult.items[0]?.id || null;

              if (latestId && !st.autoBuyLastId) {
                // Первый запуск — запоминаем позицию
                const newSt = { autoBuyLastId: latestId };
                subStates.set(stateKey, newSt);
                if (redis) redisSet(`sub:autobuy:${stateKey}`, JSON.stringify(newSt), { EX: 86400*7 }).catch(()=>{});
              } else {
                // Ищем кандидатов
                const candidates = [];
                for (const it of feedResult.items) {
                  if (!it?.id) continue;
                  if (st.autoBuyLastId && it.id === st.autoBuyLastId) break;
                  const type = String(it?.type || '').toLowerCase();
                  if (type !== 'listing') continue;
                  const g = it?.gift;
                  if (!g || !g.id) continue;
                  const priceNano = extractListingPriceNanoFromGift(g);
                  if (!priceNano) continue;
                  const priceTon = priceNano / 1e9;
                  if (priceTon > maxBuy) continue;
                  candidates.push({ it, g, priceNano, priceTon });
                }

                if (latestId) {
                  const newSt = { autoBuyLastId: latestId };
                  subStates.set(stateKey, newSt);
                  if (redis) redisSet(`sub:autobuy:${stateKey}`, JSON.stringify(newSt), { EX: 86400*7 }).catch(()=>{});
                }

                if (candidates.length > 0) {
                  candidates.sort((a, b) => a.priceTon - b.priceTon);
                  const pick = candidates[0];
                  const lotId = String(pick.g.id);
                  const attemptKey = `${userId}:${lotId}`;
                  const lastAttempt = autoBuyRecentAttempts.get(attemptKey);

                  if (!lastAttempt || nowMs() - lastAttempt > AUTO_BUY_ATTEMPT_TTL_MS) {
                    autoBuyRecentAttempts.set(attemptKey, nowMs());
                    const tsFound = Date.now();
                    console.log(`[AUTOBUY] Sub #${sub.num}: кандидат ${pick.priceTon} TON lotId=${lotId}`);

                    if (!AUTO_BUY_DRY_RUN) {
                      const buyRes = await mrktBuy({ id: lotId, priceNano: pick.priceNano });
                      if (buyRes.ok) {
                        autoBuysDone++;
                        const tsBought = Date.now();
                        const listedRaw = pick.it?.date ? Date.parse(pick.it.date) : NaN;
                        const tsListed = Number.isFinite(listedRaw) ? listedRaw : null;
                        const latencyMs = tsListed != null ? Math.max(0, tsBought - tsListed) : null;
                        const base = (pick.g.collectionTitle || pick.g.collectionName || '').trim();
                        const number = pick.g.number ?? null;
                        const title = number != null ? `${base} #${number}` : base;
                        const giftName = pick.g.name || null;
                        const urlTelegram = giftName ? `https://t.me/nft/${giftName}` : 'https://t.me/mrkt';
                        const urlMarket = pick.g.id ? mrktLotUrlFromId(pick.g.id) : 'https://t.me/mrkt';
                        const thumbKey = pick.g.modelStickerThumbnailKey || pick.g.modelStickerKey || null;
                        const uBuy = getOrCreateUser(userId);
                        pushPurchase(uBuy, { tsListed, tsFound, tsBought, latencyMs, title, priceTon: pick.priceTon, urlTelegram, urlMarket, lotId, giftName: giftName||'', thumbKey: thumbKey||'', model: pick.g.modelTitle||pick.g.modelName||'', backdrop: pick.g.backdropName||'', collection: pick.g.collectionName||base, number: number??null });
                        scheduleSave();
                        const nowMsk = new Date().toLocaleString('ru-RU', { timeZone: 'Europe/Moscow', hour:'2-digit', minute:'2-digit', second:'2-digit', fractionalSecondDigits:3 });
                        let msg = `✅ AutoBuy OK ⏱${nowMsk} MSK\nЦена: ${pick.priceTon.toFixed(3)} TON\n`;
                        if (latencyMs != null) msg += `Latency: ${(latencyMs/1000).toFixed(2)}s\n`;
                        msg += `${title}\n${urlTelegram}`;
                        await sendMessageSafe(userChatId(userId), msg, { disable_web_page_preview: false, reply_markup: mkReplyMarkupOpen(urlMarket, 'MRKT') });
                        if (AUTO_BUY_DISABLE_AFTER_SUCCESS) { sub.autoBuyEnabled = false; scheduleSave(); }
                      } else if (buyRes.noFunds) {
                        for (const s2 of active) if (s2) s2.autoBuyEnabled = false;
                        scheduleSave();
                        await sendMessageSafe(userChatId(userId), `⚠️ AutoBuy стоп: нет средств\nПопытка: ${pick.priceTon.toFixed(3)} TON`, { disable_web_page_preview: true });
                      }
                    } else {
                      autoBuysDone++;
                      await sendMessageSafe(userChatId(userId), `🔍 DRY: ${pick.priceTon.toFixed(3)} TON`, { disable_web_page_preview: true });
                    }
                  }
                }
              }
            }
          } catch (e) { console.error(`[AUTOBUY] ошибка sub #${sub.num}:`, e?.message); }
        }

        // === ШАГ 3: Флор — тоже кэшируем по gift ===
        const sf = normalizeFilters(sub.filters || {});
        if (!sf.gifts.length) continue;

        const floorKey = sf.gifts.slice().sort().join('|');
        if (!giftFeedCache.has('floor:' + floorKey)) {
          const rLots = await mrktSearchLotsBackground(
            sf.gifts,
            sf.gifts.length === 1 ? sf.models : [],
            sf.gifts.length === 1 ? sf.backdrops : [],
            5
          );
          giftFeedCache.set('floor:' + floorKey, rLots);
          if (uniqueKeys.size > 1) await sleep(400);
        }

        const rLots = giftFeedCache.get('floor:' + floorKey);
        if (!rLots?.ok) continue;

        const lotsSorted = (rLots.gifts || []).map(g => salingGiftToLot(g)).filter(Boolean).sort((a,b) => a.priceTon - b.priceTon);
        const lot = lotsSorted[0] || null;
        const newFloor = lot ? lot.priceTon : null;

        const stateKeyFloor = `${userId}:${sub.id}:floor`;
        let prev = subStates.get(stateKeyFloor);
        if (!prev) {
          const saved = await redisGet(`sub:floor:${stateKeyFloor}`).catch(() => null);
          prev = saved ? (JSON.parse(saved) || { floor: null, emptyStreak: 0 }) : { floor: null, emptyStreak: 0 };
          subStates.set(stateKeyFloor, prev);
        }

        if (newFloor != null) {
          const maxNotify = sub.maxNotifyTon != null ? Number(sub.maxNotifyTon) : null;
          const canNotify = maxNotify == null || newFloor <= maxNotify;
          if (canNotify && (prev.floor == null || Math.abs(Number(prev.floor) - newFloor) > 0.0001)) {
            try { await notifyFloorToUser(userId, sub, lot, newFloor, prev.floor); floorNotifs++; } catch {}
          }
          const newSt = { floor: newFloor, emptyStreak: 0 };
          subStates.set(stateKeyFloor, newSt);
          if (redis) redisSet(`sub:floor:${stateKeyFloor}`, JSON.stringify(newSt), { EX: 86400 * 7 }).catch(() => {});
        }
      }
    }

    console.log(`[SUBS] Итого: processedSubs=${processedSubs} floorNotifs=${floorNotifs} feedNotifs=${feedNotifs}`);
    return { processedSubs, floorNotifs, feedNotifs };
  } catch (e) {
    console.error('subs error:', e);
    return { processedSubs: 0, floorNotifs: 0, feedNotifs: 0 };
  } finally {
    isSubsChecking = false;
  }
}

// ===================== AutoBuy =====================
let isAutoBuying = false;

function isNoFundsError(obj) {
  const s = JSON.stringify(obj?.data || obj || '').toLowerCase();
  const t = String(obj?.text || obj?.reason || '').toLowerCase();
  return s.includes('not enough') || s.includes('insufficient') || s.includes('balance') ||
    t.includes('not enough') || t.includes('insufficient') || t.includes('balance') ||
    s.includes('funds') || t.includes('funds');
}

async function mrktBuy({ id, priceNano }) {
  const body = { ids: [id], prices: { [id]: priceNano } };
  const r = await mrktPostJson('/gifts/buy', body);

  // Проверяем ошибку недостатка средств ДО всего остального
  if (!r.ok) {
    const noFunds = isNoFundsError({ data: r.data, text: r.text });
    if (noFunds) return { ok: false, reason: 'NO_FUNDS', noFunds: true, data: r.data };
    return { ok: false, reason: r.status === 429 ? 'RPS_WAIT' : (mrktState.lastFailMsg || 'BUY_ERROR'), waitMs: r.waitMs || 0, data: r.data };
  }

  const data = r.data;

  // Проверяем нет ли ошибки средств в теле успешного ответа
  if (isNoFundsError({ data })) {
    return { ok: false, reason: 'NO_FUNDS', noFunds: true, data };
  }

  let okItem = null;
  if (Array.isArray(data)) {
    okItem = data.find((x) =>
      (x?.source?.type === 'buy_gift' && x?.userGift?.isMine === true && String(x?.userGift?.id || '') === String(id)) ||
      (x?.type === 'buy_gift' && String(x?.gift?.id || x?.id || '') === String(id)) ||
      (x?.status === 'success' && String(x?.id || '') === String(id))
    );
  } else if (data && typeof data === 'object') {
    if (!data.error && !data.message) okItem = data;
  }

  if (!okItem) {
    console.warn('[BUY] Не нашли подтверждение в ответе, data=', JSON.stringify(data).slice(0, 300));
    // Пустой массив или объект без подтверждения = покупка НЕ прошла
    if (Array.isArray(data) && data.length === 0) {
      return { ok: false, reason: 'BUY_NOT_CONFIRMED_EMPTY', data };
    }
    // Объект без ошибки но и без подтверждения = тоже не прошла
    return { ok: false, reason: 'BUY_NOT_CONFIRMED', data };
  }
  return { ok: true, okItem, data };
}

function extractListingPriceNanoFromGift(g) {
  const nanoA = g?.salePriceWithoutFee ?? null;
  const nanoB = g?.salePrice ?? null;
  const nano = (nanoA != null && Number(nanoA) > 0) ? Number(nanoA) : (nanoB != null ? Number(nanoB) : NaN);
  return Number.isFinite(nano) && nano > 0 ? nano : null;
}

async function tryAutoBuyFromFeed(userId, sub, feedCache = new Map()) {
  const maxBuy = Number(sub.maxAutoBuyTon);
  if (!Number.isFinite(maxBuy) || maxBuy <= 0) return { ok: true, bought: false };

  const f = normalizeFilters(sub.filters || {});
  if (!f.gifts.length) return { ok: true, bought: false };

  const models = f.gifts.length === 1 ? (f.models || []) : [];
  const backdrops = f.gifts.length === 1 ? (f.backdrops || []) : [];

  const stateKey = `${userId}:${sub.id}:autobuy`;
  let st = subStates.get(stateKey);
  if (!st) {
    const saved = await redisGet(`sub:autobuy:${stateKey}`).catch(() => null);
    if (saved) { try { st = JSON.parse(saved); } catch { st = { autoBuyLastId: null }; } }
    else { st = { autoBuyLastId: null }; }
    subStates.set(stateKey, st);
  }

  // Используем кэш feed с TTL — не спамим MRKT одинаковыми запросами
  const feedCacheKey = `${f.gifts.join(',')}|${models.join(',')}|${backdrops.join(',')}`;
  const cached = feedCache.get(feedCacheKey);
  let r;
  if (cached && (nowMs() - cached.time) < AUTO_BUY_FEED_CACHE_TTL) {
    r = cached.data;
  } else {
    r = await mrktFeedFetchBackground({
      collectionNames: f.gifts,
      modelNames: models,
      backdropNames: backdrops,
      cursor: '',
      count: 50,
      types: [], // пустой = все события (sale + listing)
    });
    feedCache.set(feedCacheKey, { time: nowMs(), data: r });
  }

  if (!r.ok) {
    if (r.reason === 'RPS_WAIT') return { ok: true, bought: false, rps: true, waitMs: r.waitMs || 0 };
    return { ok: true, bought: false };
  }
  if (!r.items.length) return { ok: true, bought: false };

  const latestId = r.items[0]?.id || null;

  // ИСПРАВЛЕНИЕ: при первом запуске в режиме "Новые" — запоминаем позицию
  if (latestId && !st.autoBuyLastId && !sub.autoBuyAny) {
    const newSt = { ...st, autoBuyLastId: latestId };
    subStates.set(stateKey, newSt);
    if (redis) redisSet(`sub:autobuy:${stateKey}`, JSON.stringify(newSt), { EX: 86400 * 7 }).catch(() => {});
    console.log(`[AUTOBUY] Sub #${sub.num} userId=${userId}: первый запуск (Новые), запомнили lastId=${latestId}`);
    return { ok: true, bought: false };
  }

  const candidates = [];
  for (const it of r.items) {
    if (!it?.id) continue;
    // В режиме "Новые" — только новые события после lastId
    if (!sub.autoBuyAny && st.autoBuyLastId && it.id === st.autoBuyLastId) break;

    const type = String(it?.type || '').toLowerCase();
    if (type !== 'listing' && type !== 'sale') continue;

    const g = it?.gift;
    if (!g || !g.id) continue;

    const priceNano = extractListingPriceNanoFromGift(g);
    if (!priceNano) continue;

    const priceTon = priceNano / 1e9;
    if (priceTon > maxBuy) continue;

    candidates.push({ it, g, priceNano, priceTon });
  }

  if (latestId) {
    const newSt = { ...st, autoBuyLastId: latestId };
    subStates.set(stateKey, newSt);
    if (redis) redisSet(`sub:autobuy:${stateKey}`, JSON.stringify(newSt), { EX: 86400 * 7 }).catch(() => {});
  }
  if (!candidates.length) return { ok: true, bought: false };

  candidates.sort((a, b) => a.priceTon - b.priceTon);
  const pick = candidates[0];

  const lotId = String(pick.g.id);
  const attemptKey = `${userId}:${lotId}`;
  const lastAttempt = autoBuyRecentAttempts.get(attemptKey);
  if (lastAttempt && nowMs() - lastAttempt < AUTO_BUY_ATTEMPT_TTL_MS) {
    return { ok: true, bought: false };
  }
  autoBuyRecentAttempts.set(attemptKey, nowMs());

  const tsFound = Date.now();

  console.log(`[AUTOBUY] Sub #${sub.num} userId=${userId}: кандидат lotId=${lotId} price=${pick.priceTon} TON DRY_RUN=${AUTO_BUY_DRY_RUN}`);

  if (AUTO_BUY_DRY_RUN) {
    return { ok: true, bought: true, dry: true, priceTon: pick.priceTon, tsFound, lotId };
  }

  const buyRes = await mrktBuy({ id: lotId, priceNano: pick.priceNano });
  if (!buyRes.ok) {
    console.error(`[AUTOBUY] Покупка не удалась: ${buyRes.reason}`);
    if (buyRes.noFunds) {
      return { ok: true, bought: false, noFunds: true, priceTon: pick.priceTon };
    }
    return { ok: true, bought: false, buyFail: buyRes };
  }

  const tsBought = Date.now();
  const listedRaw = pick.it?.date ? Date.parse(pick.it.date) : NaN;
  const tsListed = Number.isFinite(listedRaw) ? listedRaw : null;
  const latencyMs = tsListed != null ? Math.max(0, tsBought - tsListed) : null;

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
    tsListed,
    tsFound,
    tsBought,
    latencyMs,
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

  console.log(`[AUTOBUY] Куплено! lotId=${lotId} price=${pick.priceTon} TON latency=${latencyMs}ms`);

  return {
    ok: true,
    bought: true,
    priceTon: pick.priceTon,
    urlTelegram,
    urlMarket,
    latencyMs,
    title,
  };
}

// AutoBuy теперь встроен в checkSubscriptionsForAllUsers — отдельного цикла нет!
// Это исключает дублирование feed запросов
async function autoBuyCycle() {
  // AutoBuy теперь выполняется внутри checkSubscriptionsForAllUsers
  // Этот цикл больше не делает отдельных запросов к MRKT
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
          menu_button: {
            type: 'web_app',
            text: APP_TITLE,
            web_app: { url: WEBAPP_URL },
          },
        }),
      });
    } catch {}
  }

  await sendMessageSafe(msg.chat.id, `Открой меню "${APP_TITLE}"`, { reply_markup: MAIN_KEYBOARD });

  if (WEBAPP_URL) {
    await sendMessageSafe(msg.chat.id, 'Если меню не видно — открой через кнопку ниже:', {
      reply_markup: {
        inline_keyboard: [[
          { text: `Открыть ${APP_TITLE}`, web_app: { url: WEBAPP_URL } }
        ]]
      }
    }).catch(() => {});
  }
});

bot.on('message', async (msg) => {
  const userId = msg.from?.id;
  if (!userId) return;

  const u = getOrCreateUser(userId);
  u.chatId = msg.chat.id;
  scheduleSave();

  if ((msg.text || '') === 'Статус') {
    const now = new Date().toLocaleString('ru-RU', { timeZone: 'Europe/Moscow' });
    const txt =
      `Статус (${now})\n` +
      `MRKT Auth: ${MRKT_AUTH_RUNTIME ? 'OK (' + maskToken(MRKT_AUTH_RUNTIME) + ')' : 'НЕТ'}\n` +
      `Ошибка: ${mrktState.lastFailMsg || '-'}\n` +
      `Пауза до: ${mrktState.pauseUntil ? new Date(mrktState.pauseUntil).toLocaleTimeString('ru-RU') : '-'}\n` +
      `Redis: ${redis ? 'OK' : 'НЕТ'}\n` +
      `MODE: ${MODE}\n` +
      `Подписки: ${SUBS_CHECK_INTERVAL_MS}ms\n` +
      `Feed типы: ${Array.from(SUBS_FEED_TYPES).join(', ')}\n` +
      `AutoBuy: ${AUTO_BUY_GLOBAL ? 'ON' : 'OFF'} ${AUTO_BUY_DRY_RUN ? '(DRY RUN — не покупает!)' : '(реальный)'}\n`;
    await sendMessageSafe(msg.chat.id, txt, { reply_markup: MAIN_KEYBOARD });
  }
});

// ===================== WebApp HTML =====================
const WEBAPP_HTML = `<!doctype html>
<html lang="ru"><head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"/>
<title>${APP_TITLE}</title>
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<style>
:root{
  --bg:#080d12;
  --bg2:#0d1520;
  --card:#0f1923;
  --card2:#131f2e;
  --text:#e2e8f0;
  --muted:#64748b;
  --muted2:#94a3b8;
  --border:#1e2d3d;
  --border2:#243347;
  --input:#0a1520;
  --accent:#3b82f6;
  --accent2:#60a5fa;
  --green:#22c55e;
  --green2:#16a34a;
  --red:#ef4444;
  --yellow:#f59e0b;
  --purple:#8b5cf6;
}
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
html,body{background:var(--bg);overscroll-behavior:none;min-height:100vh}
body{margin:0;padding:0;color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;font-size:14px;line-height:1.5}

/* App header */
.appHeader{
  background:var(--bg2);
  border-bottom:1px solid var(--border);
  padding:12px 16px 0;
  position:sticky;top:0;z-index:100;
  backdrop-filter:blur(12px);
  -webkit-backdrop-filter:blur(12px);
}
.appHeaderTop{display:flex;align-items:center;justify-content:space-between;margin-bottom:10px}
.appTitle{font-size:17px;font-weight:700;letter-spacing:-.3px}
.statusDot{width:8px;height:8px;border-radius:50%;background:var(--muted);flex-shrink:0}
.statusDot.green{background:var(--green)}
.statusDot.red{background:var(--red)}

/* Tabs */
.bottomNav{
  position:fixed;bottom:0;left:0;right:0;z-index:500;
  background:rgba(13,17,23,0.95);
  border-top:1px solid var(--border);
  display:flex;align-items:stretch;
  padding-bottom:env(safe-area-inset-bottom,0px);
  backdrop-filter:blur(20px);-webkit-backdrop-filter:blur(20px);
}
.tabbtn{
  flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;
  gap:2px;padding:8px 4px 10px;
  border:none;background:transparent;color:var(--muted);
  cursor:pointer;font-size:10px;font-weight:500;letter-spacing:.2px;
  transition:color .15s;min-width:0;
}
.tabbtn .tabIcon{width:22px;height:22px;transition:transform .15s,filter .15s;flex-shrink:0;}
.tabbtn.active{color:var(--accent2);}
.tabbtn.active .tabIcon{transform:scale(1.1);filter:drop-shadow(0 0 5px var(--accent));}
.tabbtn:hover:not(.active){color:var(--text)}

/* Main content */
.content{padding:12px 14px 90px;max-width:720px;margin:0 auto}

/* Cards */
.card{
  background:var(--card);
  border:1px solid var(--border);
  border-radius:16px;
  padding:14px;
  margin-bottom:12px;
}
.card2{
  background:var(--card2);
  border:1px solid var(--border2);
  border-radius:12px;
  padding:12px;
}

/* Section title */
.sectionTitle{font-size:13px;font-weight:600;color:var(--muted2);text-transform:uppercase;letter-spacing:.5px;margin:0 0 10px 0}

/* Error */
#err{display:none;background:rgba(239,68,68,.1);border:1px solid rgba(239,68,68,.3);border-radius:12px;padding:10px 12px;color:#fca5a5;font-size:13px;margin-bottom:10px;white-space:pre-wrap;word-break:break-word}

/* Inputs */
label{display:block;font-size:11px;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.4px;margin-bottom:5px}
.inpWrap{position:relative}
input,textarea{
  width:100%;padding:10px 34px 10px 12px;
  border:1px solid var(--border2);
  border-radius:10px;
  background:var(--input);
  color:var(--text);
  outline:none;font-size:14px;
  transition:border-color .15s;
  -webkit-appearance:none;
}
input:focus,textarea:focus{border-color:var(--accent)}
textarea{padding:10px 12px;resize:vertical;min-height:80px}
.xbtn{
  position:absolute;right:8px;top:50%;transform:translateY(-50%);
  width:22px;height:22px;border-radius:6px;
  border:none;background:rgba(255,255,255,.06);
  color:var(--muted2);cursor:pointer;
  display:flex;align-items:center;justify-content:center;
  font-size:14px;line-height:1;transition:background .15s;
}
.xbtn:hover{background:rgba(255,255,255,.12)}

/* Buttons */
button{cursor:pointer;border:1px solid var(--border2);border-radius:10px;background:var(--card2);color:var(--text);padding:9px 14px;font-size:13px;font-weight:500;transition:background .15s,border-color .15s}
button:hover{background:rgba(255,255,255,.06)}
.btn-primary{background:var(--accent);border-color:var(--accent);color:#fff;font-weight:600}
.btn-primary:hover{background:var(--accent2);border-color:var(--accent2)}
.btn-green{background:var(--green2);border-color:var(--green);color:#fff;font-weight:600}
.btn-green:hover{background:var(--green)}
.btn-sm{padding:7px 11px;font-size:12px;border-radius:8px}
.btn-danger{border-color:rgba(239,68,68,.4);color:var(--red)}
.btn-danger:hover{background:rgba(239,68,68,.1)}
.row{display:flex;gap:8px;flex-wrap:wrap;align-items:flex-end}
.field{flex:1 1 200px;min-width:140px;position:relative}
.field.open{z-index:999999}

/* Chips */
.chips{display:flex;flex-wrap:wrap;gap:5px;margin-top:8px}
.chip{
  display:inline-flex;align-items:center;gap:5px;
  padding:4px 10px;border-radius:999px;
  background:rgba(59,130,246,.15);border:1px solid rgba(59,130,246,.3);
  color:var(--accent2);font-size:12px;font-weight:500;
}

/* Suggest dropdown */
.sug{
  position:fixed;
  background:var(--card);border:1px solid var(--border2);
  border-radius:14px;overflow:auto;max-height:50vh;
  z-index:99999;box-shadow:0 20px 60px rgba(0,0,0,.75);
  -webkit-overflow-scrolling:touch;
  left:14px;right:14px;
  width:auto;
}
.sugHead{
  display:flex;justify-content:space-between;align-items:center;
  padding:9px 12px;border-bottom:1px solid var(--border);
  position:sticky;top:0;background:var(--card);z-index:2;
  font-size:12px;font-weight:600;color:var(--muted2);
}
.sug .item{
  width:100%;text-align:left;border:0;background:transparent;
  padding:9px 12px;display:flex;gap:10px;align-items:center;
  border-radius:0;transition:background .1s;
}
.sug .item:hover{background:rgba(255,255,255,.04)}
.sug .item.sel{background:rgba(59,130,246,.08)}
.thumb{
  width:40px;height:40px;border-radius:10px;
  object-fit:cover;background:rgba(255,255,255,.05);
  border:1px solid var(--border);flex-shrink:0;
}
.thumb.color{display:flex;align-items:center;justify-content:center;overflow:hidden}
.colorFill{width:100%;height:100%;border-radius:9px;border:1px solid rgba(255,255,255,.1)}

/* Lots grid */
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:10px;margin-top:10px}
@media(max-width:440px){.grid{grid-template-columns:repeat(2,1fr)}}
.lot{
  background:var(--card2);border:1px solid var(--border);
  border-radius:14px;padding:10px;cursor:pointer;
  transition:border-color .15s,transform .1s;
  position:relative;overflow:hidden;
}
.lot:hover{border-color:var(--border2);transform:translateY(-1px)}
.lot:active{transform:translateY(0)}
.lotImg{width:100%;aspect-ratio:1/1;object-fit:cover;border-radius:10px;border:1px solid var(--border);background:rgba(255,255,255,.03);display:block}
.lotImgPlaceholder{width:100%;aspect-ratio:1/1;border-radius:10px;border:1px solid var(--border);background:linear-gradient(135deg,rgba(59,130,246,.08),rgba(139,92,246,.08));display:flex;align-items:center;justify-content:center;color:var(--border2)}
.lotPrice{font-size:15px;font-weight:700;margin-top:8px;color:var(--text)}
.lotName{font-size:12px;color:var(--muted2);margin-top:2px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.lotMeta{font-size:11px;color:var(--muted);margin-top:1px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}

/* Badge */
.badge{display:inline-flex;align-items:center;padding:3px 8px;border-radius:6px;font-size:11px;font-weight:600;letter-spacing:.2px}
.badge-green{background:rgba(34,197,94,.12);color:var(--green);border:1px solid rgba(34,197,94,.2)}
.badge-blue{background:rgba(59,130,246,.12);color:var(--accent2);border:1px solid rgba(59,130,246,.2)}
.badge-yellow{background:rgba(245,158,11,.12);color:var(--yellow);border:1px solid rgba(245,158,11,.2)}
.badge-muted{background:rgba(100,116,139,.1);color:var(--muted2);border:1px solid rgba(100,116,139,.15)}

/* Loader */
.loaderLine{display:none;align-items:center;gap:8px;color:var(--muted);font-size:13px;padding:8px 0}
.spinner{width:16px;height:16px;border-radius:50%;border:2px solid rgba(255,255,255,.1);border-top-color:var(--accent);animation:spin .7s linear infinite;flex-shrink:0}
@keyframes spin{to{transform:rotate(360deg)}}

/* Summary bar */
.sumBar{display:flex;flex-wrap:wrap;gap:8px;padding:10px 0 4px}
.sumPill{
  padding:5px 10px;border:1px solid var(--border2);border-radius:8px;
  font-size:12px;color:var(--muted2);background:var(--card2);
}
.sumPill b{color:var(--text)}

/* Sheet */
.sheetOverlay{
  position:fixed;inset:0;background:rgba(0,0,0,.5);
  display:flex;align-items:flex-end;justify-content:center;
  z-index:50000;padding:0;
  opacity:0;visibility:hidden;pointer-events:none;
  transition:opacity .2s ease,visibility .2s ease;
  backdrop-filter:blur(4px);-webkit-backdrop-filter:blur(4px);
}
.sheetOverlay.show{opacity:1;visibility:visible;pointer-events:auto}
.sheet{
  width:100%;max-width:680px;
  max-height:88vh;
  background:var(--bg2);
  border:1px solid var(--border2);
  border-radius:22px 22px 0 0;
  display:flex;flex-direction:column;
  box-shadow:0 -20px 80px rgba(0,0,0,.6);
  transform:translateY(40px);
  transition:transform .2s cubic-bezier(.32,1.2,.5,1);
}
.sheetOverlay.show .sheet{transform:translateY(0)}
.sheetHandle{width:36px;height:4px;border-radius:2px;background:rgba(255,255,255,.15);margin:10px auto 0}
.sheetHead{padding:12px 16px;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:flex-start;gap:10px}
.sheetTitle{font-size:16px;font-weight:700}
.sheetSub{font-size:13px;color:var(--muted2);margin-top:2px}
.sheetScroll{overflow-y:auto;-webkit-overflow-scrolling:touch;flex:1;padding:14px 16px}
.sheetImg{width:100%;max-height:200px;object-fit:contain;border-radius:14px;border:1px solid var(--border);background:rgba(255,255,255,.02);margin-bottom:12px}
.sheetMeta{display:flex;flex-direction:column;gap:5px;margin-bottom:12px}
.sheetMetaRow{display:flex;justify-content:space-between;align-items:center;font-size:13px}
.sheetMetaRow .key{color:var(--muted)}
.sheetMetaRow .val{font-weight:600;cursor:pointer;color:var(--accent2)}
.sheetMetaRow .val:hover{text-decoration:underline}
.sheetBtns{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px}
.sheetDivider{height:1px;background:var(--border);margin:12px 0}

/* Sale rows */
.saleRow{display:flex;gap:10px;align-items:center;padding:8px 0;border-bottom:1px solid var(--border)}
.saleRow:last-child{border-bottom:none}
.saleThumb{width:48px;height:48px;border-radius:10px;object-fit:cover;background:rgba(255,255,255,.04);border:1px solid var(--border);flex-shrink:0}

/* Purchase rows */
.purchCard{
  background:var(--card2);border:1px solid var(--border);
  border-radius:14px;padding:12px;
  display:flex;gap:12px;align-items:flex-start;
}
.purchThumb{width:56px;height:56px;border-radius:12px;object-fit:cover;background:rgba(255,255,255,.04);border:1px solid var(--border);flex-shrink:0}
.purchInfo{flex:1;min-width:0}

/* Sub cards */
.subCard{
  background:var(--card2);border:1px solid var(--border);
  border-radius:16px;padding:14px;margin-bottom:10px;
}
.subCardHead{display:flex;gap:12px;align-items:flex-start}
.subImg{width:52px;height:52px;border-radius:12px;object-fit:cover;background:rgba(255,255,255,.04);border:1px solid var(--border);flex-shrink:0}
.subImgPlaceholder{width:52px;height:52px;border-radius:12px;background:linear-gradient(135deg,rgba(59,130,246,.1),rgba(139,92,246,.1));border:1px solid var(--border);flex-shrink:0;display:flex;align-items:center;justify-content:center;color:var(--border2)}
.subInfo{flex:1;min-width:0}
.subTitle{font-size:14px;font-weight:700;margin-bottom:3px}
.subMeta{font-size:12px;color:var(--muted2);margin-bottom:2px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.subActions{display:flex;align-items:center;gap:6px}
.iconBtn{
  width:32px;height:32px;border-radius:8px;
  border:1px solid var(--border2);background:var(--card);
  display:flex;align-items:center;justify-content:center;
  cursor:pointer;font-size:15px;color:var(--muted2);
  transition:background .15s,color .15s;padding:0;
}
.iconBtn:hover{background:rgba(255,255,255,.08);color:var(--text)}
.iconBtn.danger:hover{background:rgba(239,68,68,.12);color:var(--red);border-color:rgba(239,68,68,.3)}
.subSwatches{display:flex;gap:4px;flex-wrap:wrap;margin-top:6px}
.swatch{width:12px;height:12px;border-radius:50%;border:1px solid rgba(255,255,255,.15)}
.subActions2{display:flex;gap:6px;flex-wrap:wrap;margin-top:10px;padding-top:10px;border-top:1px solid var(--border)}

/* Link buttons */
.linkBtns{display:flex;gap:6px;margin-top:8px}
.linkBtn{
  padding:5px 12px;border-radius:8px;
  border:1px solid var(--border2);background:var(--card);
  color:var(--muted2);font-size:12px;font-weight:500;
  text-decoration:none;display:inline-flex;align-items:center;
  transition:background .15s,color .15s;
}
.linkBtn:hover{background:rgba(255,255,255,.07);color:var(--text)}

/* Admin */
.adminRow{display:flex;justify-content:space-between;align-items:center;padding:8px 0;border-bottom:1px solid var(--border);font-size:13px}
.adminRow:last-child{border-bottom:none}
.adminLabel{color:var(--muted)}
.adminVal{font-weight:600;font-family:monospace;font-size:12px}

/* Status line */
.statusLine{font-size:12px;color:var(--muted);padding:4px 0}

/* Scrollbar */
*::-webkit-scrollbar{width:6px;height:6px}
*::-webkit-scrollbar-track{background:transparent}
*::-webkit-scrollbar-thumb{background:rgba(255,255,255,.1);border-radius:3px}
*::-webkit-scrollbar-thumb:hover{background:rgba(255,255,255,.2)}

/* Divider */
.hr{height:1px;background:var(--border);margin:10px 0}

/* Toggle */
.toggle{
  position:relative;width:38px;height:21px;flex-shrink:0;
}
.toggle input{opacity:0;width:0;height:0;position:absolute}
.toggleSlider{
  position:absolute;inset:0;border-radius:21px;
  background:rgba(255,255,255,.1);border:1px solid var(--border2);
  cursor:pointer;transition:background .2s;
}
.toggleSlider:before{
  content:'';position:absolute;left:2px;top:2px;
  width:15px;height:15px;border-radius:50%;
  background:#fff;transition:transform .2s;
}
.toggle input:checked + .toggleSlider{background:var(--green);border-color:var(--green2)}
.toggle input:checked + .toggleSlider:before{transform:translateX(17px)}

/* Empty state */
.emptyState{display:flex;flex-direction:column;align-items:center;justify-content:center;text-align:center;padding:40px 20px;color:var(--muted);gap:8px}
.emptyIcon{font-size:36px;opacity:.35;line-height:1}
.emptyState div{font-size:13px;line-height:1.4}

/* Sub preview image size fix */
.subImg{width:52px;height:52px;border-radius:12px;object-fit:cover;background:rgba(255,255,255,.04);border:1px solid var(--border);flex-shrink:0}
.subImgPlaceholder{width:52px;height:52px;border-radius:12px;background:linear-gradient(135deg,rgba(59,130,246,.1),rgba(139,92,246,.1));border:1px solid var(--border);flex-shrink:0;display:flex;align-items:center;justify-content:center;color:var(--border2);font-size:22px}
</style>
</head>
<body>

<div class="appHeader">
  <div class="appHeaderTop">
    <div class="appTitle">${APP_TITLE}</div>
    <div style="display:flex;align-items:center;gap:8px">
      <div id="authDot" class="statusDot" title="MRKT Auth"></div>
    </div>
  </div>
</div>

<div class="content">
  <div id="err"></div>

  <!-- MARKET TAB -->
  <div id="market">
    <div class="card">
      <div class="sectionTitle">Фильтры</div>
      <div class="row">
        <div class="field" id="giftField">
          <label>Gift</label>
          <div class="inpWrap">
            <input id="gift" placeholder="Выбрать коллекцию..." autocomplete="off"/>
            <button class="xbtn" data-clear="gift" type="button">×</button>
          </div>
          <div id="giftSug" class="sug" style="display:none"></div>
        </div>
        <div class="field" id="modelField">
          <label>Model <span style="color:var(--muted);font-weight:400">(1 gift)</span></label>
          <div class="inpWrap">
            <input id="model" placeholder="Выбрать модель..." autocomplete="off"/>
            <button class="xbtn" data-clear="model" type="button">×</button>
          </div>
          <div id="modelSug" class="sug" style="display:none"></div>
        </div>
        <div class="field" id="backdropField">
          <label>Backdrop <span style="color:var(--muted);font-weight:400">(1 gift)</span></label>
          <div class="inpWrap">
            <input id="backdrop" placeholder="Выбрать фон..." autocomplete="off"/>
            <button class="xbtn" data-clear="backdrop" type="button">×</button>
          </div>
          <div id="backdropSug" class="sug" style="display:none"></div>
        </div>
        <div class="field" style="max-width:130px;flex:0 0 130px">
          <label>Number #</label>
          <div class="inpWrap">
            <input id="number" placeholder="Префикс" inputmode="numeric"/>
            <button class="xbtn" data-clear="number" type="button">×</button>
          </div>
        </div>
      </div>

      <div class="row" style="margin-top:10px">
        <button id="apply" class="btn-primary">Найти лоты</button>
        <button id="refresh">Обновить</button>
        <button id="salesByFilters" class="btn-sm">История продаж</button>
      </div>
    </div>

    <div id="summary" class="sumBar" style="display:none"></div>
    <div id="lotsLoading" class="loaderLine"><div class="spinner"></div><div>Загрузка лотов...</div></div>
    <div id="status" class="statusLine"></div>
    <div id="lots" class="grid"></div>
  </div>

  <!-- SUBS TAB -->
  <div id="subs" style="display:none">
    <div class="card">
      <div class="sectionTitle">Подписки</div>
      <div class="row">
        <button id="subCreate" class="btn-green">+ Создать из фильтров</button>
        <button id="subRefresh">Обновить</button>
        <button id="subRebuildUi" class="btn-sm">Обновить картинки</button>
        <button id="subCheckNow" class="btn-sm">Проверить сейчас</button>
      </div>
    </div>
    <div id="subsLoading" class="loaderLine"><div class="spinner"></div><div>Загрузка...</div></div>
    <div id="subsList"></div>
  </div>

  <!-- PROFILE TAB -->
  <div id="profile" style="display:none">
    <div class="card">
      <div style="display:flex;align-items:center;gap:12px">
        <img id="pfp" style="width:48px;height:48px;border-radius:50%;object-fit:cover;border:2px solid var(--border2);display:none"/>
        <div>
          <div id="profileName" style="font-size:15px;font-weight:700">Загрузка...</div>
          <div id="profileMeta" class="statusLine"></div>
        </div>
      </div>
    </div>
    <div id="profileLoading" class="loaderLine"><div class="spinner"></div><div>Загрузка покупок...</div></div>
    <div id="profileStats" style="display:none" class="card">
      <div class="sectionTitle">Статистика</div>
      <div id="statsContent" style="display:flex;gap:8px;flex-wrap:wrap"></div>
    </div>
    <div style="font-size:13px;font-weight:600;color:var(--muted2);margin:4px 0 8px">История покупок</div>
    <div id="purchases" style="display:flex;flex-direction:column;gap:8px"></div>
  </div>

  <!-- ADMIN TAB -->
  <div id="admin" style="display:none">
    <div class="card">
      <div class="sectionTitle">Статус системы</div>
      <div id="adminRows"></div>
    </div>
    <div class="card">
      <div class="sectionTitle">MRKT Session Payload</div>
      <div style="font-size:12px;color:var(--muted);margin-bottom:8px">Вставь JSON из Network вкладки MRKT — поля <code style="background:rgba(255,255,255,.05);padding:1px 5px;border-radius:4px">data</code> и <code style="background:rgba(255,255,255,.05);padding:1px 5px;border-radius:4px">photo</code></div>
      <textarea id="payloadJson" rows="5" placeholder='{"appId":null,"data":"query_id=...&hash=...","photo":null}'></textarea>
      <div class="row" style="margin-top:10px">
        <button id="sessSave" class="btn-primary">Сохранить + обновить токен</button>
        <button id="tokRefresh">Обновить токен</button>
        <button id="testMrkt" class="btn-sm">Test API</button>
      </div>
    </div>
  </div>
</div>

<nav class="bottomNav">
  <button class="tabbtn active" data-tab="market">
    <svg class="tabIcon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="9" cy="21" r="1"/><circle cx="20" cy="21" r="1"/><path d="M1 1h4l2.68 13.39a2 2 0 0 0 2 1.61h9.72a2 2 0 0 0 2-1.61L23 6H6"/></svg>
    Market
  </button>
  <button class="tabbtn" data-tab="subs">
    <svg class="tabIcon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg>
    Подписки
  </button>
  <button class="tabbtn" data-tab="profile">
    <svg class="tabIcon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>
    Профиль
  </button>
  <button class="tabbtn" data-tab="admin" id="adminTabBtn" style="display:none">
    <svg class="tabIcon" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><path d="M19.07 4.93a10 10 0 0 1 0 14.14M4.93 4.93a10 10 0 0 0 0 14.14"/></svg>
    Admin
  </button>
</nav>

<!-- BOTTOM SHEET -->
<div id="sheetOverlay" class="sheetOverlay">
  <div class="sheet">
    <div class="sheetHandle"></div>
    <div class="sheetHead">
      <div style="flex:1;min-width:0">
        <div id="sheetTitle" class="sheetTitle"></div>
        <div id="sheetSub" class="sheetSub"></div>
      </div>
      <button id="sheetClose" class="btn-sm" style="flex-shrink:0">✕</button>
    </div>
    <div class="sheetScroll" id="sheetScroll">
      <img id="sheetImg" class="sheetImg" style="display:none"/>
      <div id="sheetMeta" class="sheetMeta"></div>
      <div id="sheetBtns" class="sheetBtns"></div>
      <div id="sheetBody"></div>
    </div>
  </div>
</div>

<script src="/app.js"></script>
</body></html>`;

// ===================== WebApp JS =====================
const WEBAPP_JS = `(() => {
  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const el = id => document.getElementById(id);

  function setLoading(which, on) {
    const ids = { lots: 'lotsLoading', subs: 'subsLoading', profile: 'profileLoading' };
    const node = ids[which] ? el(ids[which]) : null;
    if (node) node.style.display = on ? 'flex' : 'none';
  }
  function showErr(msg) {
    const b = el('err');
    if (!b) return;
    b.style.display = 'block';
    b.textContent = String(msg || '');
  }
  function hideErr() {
    const b = el('err');
    if (!b) return;
    b.style.display = 'none';
    b.textContent = '';
  }

  function parseInitDataFromLocation() {
    try {
      for (const raw of [location.hash, location.search]) {
        const s = (raw || '').replace(/^[#?]/, '');
        if (!s) continue;
        const p = new URLSearchParams(s);
        const v = p.get('tgWebAppData') || p.get('tgWebAppInitData') || p.get('initData') || '';
        if (v) return v;
      }
    } catch {}
    return '';
  }

  async function getTelegramContext() {
    for (let i = 0; i < 30; i++) {
      const tg = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;
      const initData = tg && tg.initData ? tg.initData : parseInitDataFromLocation();
      if (tg && initData) {
        try { tg.ready(); tg.expand(); } catch {}
        return { tg, initData };
      }
      await sleep(100);
    }
    const tg = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;
    const initData = tg && tg.initData ? tg.initData : parseInitDataFromLocation();
    if (tg) { try { tg.ready(); tg.expand(); } catch {} }
    return { tg, initData };
  }

  (async () => {
    const ctx = await getTelegramContext();
    const tg = ctx.tg;
    const initData = ctx.initData || '';

    if (!initData) {
      document.body.innerHTML =
        '<div style="padding:20px;font-family:system-ui;color:#e2e8f0;background:#080d12;min-height:100vh">' +
          '<div style="max-width:400px;margin:0 auto">' +
            '<div style="font-size:18px;font-weight:700;margin-bottom:12px">Нет доступа</div>' +
            '<div style="color:#64748b;line-height:1.6;font-size:14px">' +
              'Открой панель через Telegram: /start → кнопка меню.<br><br>' +
              'Хост: <b style="color:#94a3b8">' + (location.host || '-') + '</b><br>' +
              'Platform: ' + (tg ? (tg.platform || '-') : 'нет объекта Telegram') +
            '</div>' +
          '</div>' +
        '</div>';
      return;
    }

    async function api(path, opts) {
      opts = opts || {};
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
      if (!res.ok) throw new Error((data && data.reason) ? String(data.reason) : ('HTTP ' + res.status));
      return data;
    }

    function openTg(url) {
      if (!url) return;
      try { if (tg && tg.openTelegramLink) return tg.openTelegramLink(url); } catch {}
      window.open(url, '_blank');
    }

    function setTab(name) {
      ['market', 'subs', 'profile', 'admin'].forEach(x => {
        const node = el(x);
        if (node) node.style.display = x === name ? 'block' : 'none';
      });
      document.querySelectorAll('.tabbtn').forEach(b =>
        b.classList.toggle('active', b.dataset.tab === name)
      );
    }

    document.querySelectorAll('.tabbtn').forEach(b => b.onclick = async () => {
      hideSug('giftSug'); hideSug('modelSug'); hideSug('backdropSug');
      setTab(b.dataset.tab);
      if (b.dataset.tab === 'profile') await refreshProfile().catch(() => {});
      if (b.dataset.tab === 'admin') await refreshAdmin().catch(() => {});
      if (b.dataset.tab === 'subs') await refreshSubs().catch(() => {});
    });

    let sel = { gifts: [], giftLabels: {}, models: [], backdrops: [], numberPrefix: '' };
    let currentLots = [];
    let currentState = null;

    function giftsInputText() {
      if (!sel.gifts.length) return '';
      return sel.gifts.map(v => sel.giftLabels[v] || v).join(', ');
    }
    function listInputText(arr) { return (arr || []).join(', '); }
    function isSelectedNorm(arr, v) {
      const k = String(v || '').toLowerCase().trim();
      return (arr || []).some(x => String(x).toLowerCase().trim() === k);
    }
    function toggleIn(arr, v) {
      const k = String(v || '').toLowerCase().trim();
      const out = [];
      let removed = false;
      for (const x of (arr || [])) {
        if (String(x).toLowerCase().trim() === k) { removed = true; continue; }
        out.push(x);
      }
      if (!removed) out.push(v);
      return out;
    }

    async function patchFilters() {
      await api('/api/state/patch', {
        method: 'POST',
        body: JSON.stringify({
          filters: {
            gifts: sel.gifts,
            giftLabels: sel.giftLabels,
            models: sel.models,
            backdrops: sel.backdrops,
            numberPrefix: el('number').value.trim()
          }
        })
      });
    }

    function wrap(which, fn) {
      return async () => {
        hideErr();
        setLoading(which, true);
        try { await fn(); }
        catch (e) { showErr(e.message || String(e)); }
        finally { setLoading(which, false); }
      };
    }

    const timers = {};
    function debounce(kind, fn, ms) {
      clearTimeout(timers[kind]);
      timers[kind] = setTimeout(fn, ms || 220);
    }

    function scheduleRetryIfWait(resp, fn) {
      const waitMs = resp && Number(resp.waitMs || 0);
      if (!waitMs || waitMs < 500 || waitMs > 20000) return;
      setTimeout(() => { fn().catch(() => {}); }, waitMs + 150);
    }

    function openField(fieldId) {
      ['giftField', 'modelField', 'backdropField'].forEach(id => el(id) && el(id).classList.remove('open'));
      if (el(fieldId)) el(fieldId).classList.add('open');
    }
    function positionSug(sugId, fieldId) {
      const sug = el(sugId);
      const field = el(fieldId);
      if (!sug || !field) return;
      const rect = field.getBoundingClientRect();
      const navH = 64;
      const spaceBelow = window.innerHeight - rect.bottom - navH - 8;
      const spaceAbove = rect.top - 56;
      // Ширина по полю, но не выходить за экран
      const left = Math.max(8, rect.left);
      const right = Math.max(8, window.innerWidth - rect.right);
      sug.style.left = left + 'px';
      sug.style.right = right + 'px';
      sug.style.width = 'auto';
      if (spaceBelow >= 160 || spaceBelow >= spaceAbove) {
        sug.style.top = (rect.bottom + 4) + 'px';
        sug.style.bottom = 'auto';
        sug.style.maxHeight = Math.max(140, Math.min(spaceBelow, window.innerHeight * 0.45)) + 'px';
      } else {
        sug.style.top = 'auto';
        sug.style.bottom = (window.innerHeight - rect.top + 4) + 'px';
        sug.style.maxHeight = Math.max(140, Math.min(spaceAbove, window.innerHeight * 0.45)) + 'px';
      }
    }
    function hideSug(id) {
      const b = el(id);
      if (!b) return;
      b.style.display = 'none';
      b.innerHTML = '';
    }

    function renderSug(id, title, items, isSelected, onToggle) {
      const b = el(id);
      if (!b) return;
      let html = '<div class="sugHead"><span>' + title + '</span><span style="font-weight:400;font-size:11px">нажми чтобы выбрать</span></div>';
      html += (items || []).map(x => {
        const selected = isSelected(x.value);
        let thumb = '';
        if (x.imgUrl) {
          thumb = '<img class="thumb" src="' + x.imgUrl + '" referrerpolicy="no-referrer" loading="lazy"/>';
        } else if (x.colorHex) {
          thumb = '<div class="thumb color"><div class="colorFill" style="background:' + x.colorHex + '"></div></div>';
        } else {
          thumb = '<div class="thumb"></div>';
        }
        const mark = selected ? '<span style="color:var(--green)">✓ </span>' : '';
        const sub = x.sub ? '<div style="font-size:11px;color:var(--muted);margin-top:1px">' + x.sub + '</div>' : '';
        return '<button type="button" class="item' + (selected ? ' sel' : '') + '" data-v="' + String(x.value).replace(/"/g, '&quot;') + '">' +
          thumb +
          '<div style="min-width:0;flex:1">' +
            '<div style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:13px;font-weight:' + (selected ? '700' : '500') + '">' + mark + (x.label || x.value) + '</div>' +
            sub +
          '</div></button>';
      }).join('');

      b.innerHTML = html;
      b.style.display = 'block';
      b.onpointerdown = e => e.stopPropagation();
      b.onclick = e => {
        const btn = e.target.closest('button[data-v]');
        if (!btn) return;
        onToggle(btn.getAttribute('data-v'));
      };
    }

    async function showGiftSug() {
      openField('giftField');
      const q = (() => {
        const raw = (el('gift').value || '').trim();
        const sel2 = giftsInputText().trim();
        return raw === sel2 ? '' : raw;
      })();
      const r = await api('/api/mrkt/collections?q=' + encodeURIComponent(q));
      const items = r.items || [];
      const mapLabel = r.mapLabel || {};
      const rerender = () => {
        renderSug('giftSug', 'Gift', items,
          v => isSelectedNorm(sel.gifts, v),
          v => {
            const next = toggleIn(sel.gifts, v);
            if (next.length !== 1) { sel.models = []; sel.backdrops = []; }
            sel.gifts = next;
            sel.giftLabels[v] = mapLabel[v] || v;
            el('gift').value = giftsInputText();
            el('model').value = listInputText(sel.models);
            el('backdrop').value = listInputText(sel.backdrops);
            rerender();
          }
        );
        positionSug('giftSug', 'giftField');
      };
      rerender();
    }

    async function showModelSug() {
      openField('modelField');
      if (sel.gifts.length !== 1) { hideSug('modelSug'); return; }
      const gift = sel.gifts[0];
      const q = (() => {
        const raw = (el('model').value || '').trim();
        return raw === listInputText(sel.models).trim() ? '' : raw;
      })();
      const r = await api('/api/mrkt/suggest?kind=model&gift=' + encodeURIComponent(gift) + '&q=' + encodeURIComponent(q));
      const items = r.items || [];
      const rerender = () => {
        renderSug('modelSug', 'Model', items,
          v => isSelectedNorm(sel.models, v),
          v => {
            sel.models = toggleIn(sel.models, v);
            el('model').value = listInputText(sel.models);
            rerender();
          }
        );
        positionSug('modelSug', 'modelField');
      };
      rerender();
    }

    async function showBackdropSug() {
      openField('backdropField');
      if (sel.gifts.length !== 1) { hideSug('backdropSug'); return; }
      const gift = sel.gifts[0];
      const q = (() => {
        const raw = (el('backdrop').value || '').trim();
        return raw === listInputText(sel.backdrops).trim() ? '' : raw;
      })();
      const r = await api('/api/mrkt/suggest?kind=backdrop&gift=' + encodeURIComponent(gift) + '&q=' + encodeURIComponent(q));
      const items = r.items || [];
      const rerender = () => {
        renderSug('backdropSug', 'Backdrop', items,
          v => isSelectedNorm(sel.backdrops, v),
          v => {
            sel.backdrops = toggleIn(sel.backdrops, v);
            el('backdrop').value = listInputText(sel.backdrops);
            rerender();
          }
        );
        positionSug('backdropSug', 'backdropField');
      };
      rerender();
    }

    document.addEventListener('click', e => {
      if (!e.target.closest('#giftSug') && !e.target.closest('#giftField')) hideSug('giftSug');
      if (!e.target.closest('#modelSug') && !e.target.closest('#modelField')) hideSug('modelSug');
      if (!e.target.closest('#backdropSug') && !e.target.closest('#backdropField')) hideSug('backdropSug');
    });
    // Закрывать dropdown при скролле и при открытии sheet
    window.addEventListener('scroll', () => { hideSug('giftSug'); hideSug('modelSug'); hideSug('backdropSug'); }, { passive: true });
    function hideAllSug() { hideSug('giftSug'); hideSug('modelSug'); hideSug('backdropSug'); }

    el('gift').addEventListener('focus', () => debounce('gift', () => showGiftSug().catch(e => showErr(e.message || String(e)))));
    el('gift').addEventListener('input', () => debounce('gift', () => showGiftSug().catch(e => showErr(e.message || String(e)))));
    el('model').addEventListener('focus', () => debounce('model', () => showModelSug().catch(e => showErr(e.message || String(e)))));
    el('model').addEventListener('input', () => debounce('model', () => showModelSug().catch(e => showErr(e.message || String(e)))));
    el('backdrop').addEventListener('focus', () => debounce('backdrop', () => showBackdropSug().catch(e => showErr(e.message || String(e)))));
    el('backdrop').addEventListener('input', () => debounce('backdrop', () => showBackdropSug().catch(e => showErr(e.message || String(e)))));

    document.querySelectorAll('[data-clear]').forEach(btn => {
      btn.onclick = () => {
        const what = btn.dataset.clear;
        if (what === 'gift') { sel.gifts = []; sel.models = []; sel.backdrops = []; sel.giftLabels = {}; el('gift').value = ''; el('model').value = ''; el('backdrop').value = ''; }
        if (what === 'model') { sel.models = []; el('model').value = ''; }
        if (what === 'backdrop') { sel.backdrops = []; el('backdrop').value = ''; }
        if (what === 'number') el('number').value = '';
      };
    });

    async function refreshState() {
      const s = await api('/api/state');
      currentState = s;
      const isAdm = !!(s.api && s.api.isAdmin);
      el('adminTabBtn').style.display = isAdm ? 'inline-block' : 'none';

      const dot = el('authDot');
      if (dot) {
        dot.className = 'statusDot ' + (s.api && s.api.mrktAuthSet ? 'green' : 'red');
        dot.title = s.api && s.api.mrktAuthSet ? ('Auth OK: ' + (s.api.mrktAuthMask || '')) : 'Auth MISSING';
      }

      const f = s.user && s.user.filters ? s.user.filters : {};
      sel.gifts = (f.gifts || []).slice();
      sel.giftLabels = Object.assign({}, f.giftLabels || {});
      sel.models = (f.models || []).slice();
      sel.backdrops = (f.backdrops || []).slice();

      el('gift').value = giftsInputText();
      el('model').value = listInputText(sel.models);
      el('backdrop').value = listInputText(sel.backdrops);
      el('number').value = f.numberPrefix || '';
      return s;
    }

    function lotCard(lot) {
      const imgHtml = lot.imgUrl
        ? '<img class="lotImg" src="' + lot.imgUrl + '" referrerpolicy="no-referrer" loading="lazy"/>'
        : '<div class="lotImgPlaceholder"><svg width="32" height="32" fill="none" viewBox="0 0 24 24"><rect width="24" height="24" rx="6" fill="rgba(255,255,255,.04)"/><path d="M12 8a4 4 0 100 8 4 4 0 000-8z" fill="rgba(255,255,255,.15)"/></svg></div>';
      return '<div class="lot" data-id="' + lot.id + '">' +
        imgHtml +
        '<div class="lotPrice">' + Number(lot.priceTon).toFixed(3) + ' TON</div>' +
        '<div class="lotName">' + (lot.name || '') + '</div>' +
        (lot.model ? '<div class="lotMeta">Model: ' + lot.model + '</div>' : '') +
        (lot.backdrop ? '<div class="lotMeta">Backdrop: ' + lot.backdrop + '</div>' : '') +
      '</div>';
    }

    async function refreshSummary(force) {
      try {
        const r = await api('/api/mrkt/summary_by_filters' + (force ? '?force=1' : ''));
        const box = el('summary');
        const offers = r.offers || {};
        if (!r.note && offers.exact == null && offers.collection == null) {
          box.style.display = 'none'; box.innerHTML = ''; return;
        }
        box.style.display = 'flex';
        box.innerHTML =
          (offers.exact != null ? '<div class="sumPill">Макс. оффер (точный): <b>' + Number(offers.exact).toFixed(3) + ' TON</b></div>' : '') +
          (offers.collection != null ? '<div class="sumPill">Макс. оффер (коллекция): <b>' + Number(offers.collection).toFixed(3) + ' TON</b></div>' : '') +
          (r.note ? '<div class="sumPill" style="color:var(--muted)">' + r.note + '</div>' : '');
      } catch {}
    }

    async function refreshLots(force) {
      const r = await api('/api/mrkt/lots' + (force ? '?force=1' : ''));
      currentLots = r.lots || [];
      const statusEl = el('status');
      if (statusEl) statusEl.textContent = r.note || '';
      const lotsEl = el('lots');
      if (lotsEl) {
        lotsEl.innerHTML = currentLots.length
          ? currentLots.map(lotCard).join('')
          : '<div class="emptyState"><div class="emptyIcon">◇</div><div>Лоты не найдены</div></div>';
      }
    }

    async function refreshProfile() {
      setLoading('profile', true);
      try {
        const r = await api('/api/profile');
        const user = r.user || null;
        const nameEl = el('profileName');
        const metaEl = el('profileMeta');
        const pfp = el('pfp');

        if (user) {
          const fn = user.first_name || '';
          const un = user.username ? ('@' + user.username) : '';
          if (nameEl) nameEl.textContent = (fn + ' ' + un).trim() || 'Пользователь';
          if (user.photo_url && pfp) { pfp.src = user.photo_url; pfp.style.display = 'block'; }
          else if (pfp) pfp.style.display = 'none';
        } else {
          if (nameEl) nameEl.textContent = 'Пользователь';
          if (pfp) pfp.style.display = 'none';
        }

        const items = r.purchases || [];
        const statsEl = el('profileStats');
        const statsContent = el('statsContent');
        if (items.length && statsEl && statsContent) {
          statsEl.style.display = 'block';
          const totalTon = items.reduce((s, p) => s + Number(p.priceTon || 0), 0);
          const withLatency = items.filter(p => p.latencyMs != null);
          const avgLatency = withLatency.length ? withLatency.reduce((s, p) => s + p.latencyMs, 0) / withLatency.length : null;
          statsContent.innerHTML =
            '<div class="badge badge-blue">Покупок: ' + items.length + '</div>' +
            '<div class="badge badge-muted">Потрачено: ' + totalTon.toFixed(3) + ' TON</div>' +
            (avgLatency != null ? '<div class="badge badge-green">Avg latency: ' + (avgLatency / 1000).toFixed(2) + 's</div>' : '');
        } else if (statsEl) statsEl.style.display = 'none';

        if (metaEl) metaEl.textContent = items.length + ' покупок';

        const list = el('purchases');
        if (!list) return;
        if (!items.length) {
          list.innerHTML = '<div class="emptyState"><div class="emptyIcon">◻</div><div>Покупок нет</div></div>';
          return;
        }

        list.innerHTML = items.map(p => {
          const imgHtml = p.imgUrl
            ? '<img class="purchThumb" src="' + p.imgUrl + '" referrerpolicy="no-referrer" loading="lazy"/>'
            : '<div class="purchThumb" style="display:flex;align-items:center;justify-content:center;color:var(--border2)">◻</div>';

          const latColor = p.latencyMs == null ? '' :
            p.latencyMs < 2000 ? 'color:var(--green)' :
            p.latencyMs < 10000 ? 'color:var(--yellow)' : 'color:var(--muted2)';

          const meta = [];
          if (p.model) meta.push(p.model);
          if (p.backdrop) meta.push(p.backdrop);
          if (p.collection && p.number != null) meta.push('#' + p.number);

          return '<div class="purchCard">' +
            imgHtml +
            '<div class="purchInfo">' +
              '<div style="font-weight:700;font-size:14px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + (p.title || 'Gift') + '</div>' +
              '<div style="font-size:15px;font-weight:700;color:var(--accent2);margin-top:2px">' + Number(p.priceTon || 0).toFixed(3) + ' TON</div>' +
              (p.boughtMsk ? '<div style="font-size:11px;color:var(--muted);margin-top:2px">' + p.boughtMsk + '</div>' : '') +
              (p.latencyMs != null ? '<div style="font-size:11px;margin-top:1px;' + latColor + '">Latency: ' + (p.latencyMs / 1000).toFixed(2) + 's</div>' : '') +
              (meta.length ? '<div style="font-size:11px;color:var(--muted);margin-top:2px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + meta.join(' · ') + '</div>' : '') +
              '<div class="linkBtns">' +
                (p.urlMarket ? '<a class="linkBtn" href="' + p.urlMarket + '" target="_blank">MRKT</a>' : '') +
                (p.urlTelegram ? '<a class="linkBtn" href="' + p.urlTelegram + '" target="_blank">NFT</a>' : '') +
              '</div>' +
            '</div>' +
          '</div>';
        }).join('');
      } finally {
        setLoading('profile', false);
      }
    }

    async function refreshSubs() {
      setLoading('subs', true);
      try {
        const s = await api('/api/state');
        currentState = s;
        const list = el('subsList');
        if (!list) return;
        const subs = (s.user && s.user.subscriptions) ? s.user.subscriptions : [];

        if (!subs.length) {
          list.innerHTML = '<div class="emptyState"><div class="emptyIcon">◇</div><div>Нет подписок</div><div style="font-size:12px;margin-top:4px">Выбери фильтры на Market и нажми "+ Создать"</div></div>';
          return;
        }

        list.innerHTML = subs.map(sub => {
          const f = sub.filters || {};
          const gifts = (f.gifts || []).map(v => (f.giftLabels && f.giftLabels[v]) ? f.giftLabels[v] : v).join(', ') || '—';
          const models = (f.models || []).join(', ');
          const backs = (f.backdrops || []).join(', ');
          const notifyMax = sub.maxNotifyTon == null ? '∞' : sub.maxNotifyTon + ' TON';
          const autoMax = sub.maxAutoBuyTon == null ? '—' : sub.maxAutoBuyTon + ' TON';
          const imgHtml = sub.ui && sub.ui.thumbKey
            ? '<img class="subImg" src="/img/cdn?key=' + encodeURIComponent(sub.ui.thumbKey) + '" referrerpolicy="no-referrer" loading="lazy"/>'
            : '<div class="subImgPlaceholder">◇</div>';

          const swatches = Array.isArray(sub.ui && sub.ui.swatches ? sub.ui.swatches : [])
            ? (sub.ui.swatches || []).map(c => '<span class="swatch" style="background:' + c + '"></span>').join('')
            : '';

          const statusBadge = sub.enabled
            ? '<span class="badge badge-green">Активна</span>'
            : '<span class="badge badge-muted">Пауза</span>';

          const autoBuyBadge = sub.autoBuyEnabled
            ? '<span class="badge badge-yellow" style="margin-left:4px">AutoBuy</span>'
            : '';

          return '<div class="subCard">' +
            '<div class="subCardHead">' +
              imgHtml +
              '<div class="subInfo">' +
                '<div class="subTitle">#' + sub.num + ' ' + statusBadge + autoBuyBadge + '</div>' +
                '<div class="subMeta">Gift: ' + gifts + '</div>' +
                (models ? '<div class="subMeta">Model: ' + models + '</div>' : '') +
                (backs ? '<div class="subMeta">Backdrop: ' + backs + '</div>' : '') +
                (f.numberPrefix ? '<div class="subMeta">Number: ' + f.numberPrefix + '</div>' : '') +
                (swatches ? '<div class="subSwatches">' + swatches + '</div>' : '') +
                '<div style="display:flex;gap:6px;flex-wrap:wrap;margin-top:6px">' +
                  '<span style="font-size:11px;color:var(--muted)">Notify max: <b style="color:var(--text)">' + notifyMax + '</b></span>' +
                  (sub.autoBuyEnabled ? '<span style="font-size:11px;color:var(--muted)">AutoBuy max: <b style="color:var(--yellow)">' + autoMax + '</b></span>' : '') +
                '</div>' +
              '</div>' +
              '<div class="subActions">' +
                '<button class="iconBtn" title="' + (sub.enabled ? 'Пауза' : 'Включить') + '" data-sub-act="toggle" data-id="' + sub.id + '">' +
                  (sub.enabled
                    ? '<svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><rect x="5" y="4" width="4" height="16" rx="1"/><rect x="15" y="4" width="4" height="16" rx="1"/></svg>'
                    : '<svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor"><path d="M8 5.14v14l11-7-11-7z"/></svg>') +
                '</button>' +
                '<button class="iconBtn danger" title="Удалить" data-sub-act="delete" data-id="' + sub.id + '">✕</button>' +
              '</div>' +
            '</div>' +
            '<div class="subActions2">' +
              '<button class="btn-sm" data-sub-act="nmax" data-id="' + sub.id + '">Notify max</button>' +
              '<button class="btn-sm' + (sub.autoBuyEnabled ? ' btn-green' : '') + '" data-sub-act="ab" data-id="' + sub.id + '">AutoBuy ' + (sub.autoBuyEnabled ? 'ON' : 'OFF') + '</button>' +
              (sub.autoBuyEnabled ? '<button class="btn-sm" data-sub-act="amax" data-id="' + sub.id + '">Auto max</button>' : '') +
            '</div>' +
          '</div>';
        }).join('');
      } finally {
        setLoading('subs', false);
      }
    }

    async function refreshAdmin() {
      try {
        const r = await api('/api/admin/status');
        const rows = el('adminRows');
        if (!rows) return;
        const fmt = v => v == null ? '—' : String(v);
        const boolBadge = v => v
          ? '<span class="badge badge-green">YES</span>'
          : '<span class="badge badge-muted">NO</span>';

        rows.innerHTML = [
          ['MRKT Auth', boolBadge(r.mrktAuthSet) + (r.mrktAuthMask ? ' <span style="font-family:monospace;font-size:11px;color:var(--muted2)">' + r.mrktAuthMask + '</span>' : '')],
          ['Session saved', boolBadge(r.mrktSessionSaved)],
          ['Last fail', r.mrktLastFail ? '<span style="color:var(--red);font-size:12px">' + r.mrktLastFail + '</span>' : '<span class="badge badge-green">OK</span>'],
          ['Pause until', r.pauseUntil && r.pauseUntil > Date.now() ? new Date(r.pauseUntil).toLocaleTimeString('ru-RU') : '—'],
          ['Last refresh', r.refresh && r.refresh.lastOkAt ? new Date(r.refresh.lastOkAt).toLocaleString('ru-RU') : '—'],
        ].map(([k, v]) =>
          '<div class="adminRow"><span class="adminLabel">' + k + '</span><span class="adminVal">' + v + '</span></div>'
        ).join('');
      } catch (e) {
        const rows = el('adminRows');
        if (rows) rows.innerHTML = '<div style="color:var(--red);font-size:13px">' + (e.message || String(e)) + '</div>';
      }
    }

    // Sheet
    let sheetIsOpen = false;
    function openSheet() {
      sheetIsOpen = true;
      hideAllSug(); // закрываем все dropdown когда открываем sheet
      el('sheetOverlay').classList.add('show');
    }
    function closeSheet() {
      sheetIsOpen = false;
      el('sheetOverlay').classList.remove('show');
      setTimeout(() => {
        ['sheetTitle','sheetSub','sheetMeta','sheetBtns','sheetBody'].forEach(id => { if(el(id)) el(id).innerHTML = ''; });
        if (el('sheetImg')) { el('sheetImg').style.display = 'none'; el('sheetImg').src = ''; }
      }, 220);
    }
    el('sheetClose').onclick = closeSheet;
    el('sheetOverlay').addEventListener('click', e => { if (e.target === el('sheetOverlay')) closeSheet(); });

    function renderSalesList(r) {
      const sales = r.sales || [];
      if (!sales.length) {
        el('sheetBody').innerHTML = '<div class="emptyState" style="padding:20px 0"><div class="emptyIcon">📭</div><div>Нет данных о продажах</div></div>';
        return;
      }
      const rows = sales.map((s, idx) => {
        const imgHtml = s.imgUrl
          ? '<img class="saleThumb" src="' + s.imgUrl + '" referrerpolicy="no-referrer" loading="lazy"/>'
          : '<div class="saleThumb" style="background:rgba(255,255,255,.04)"></div>';
        return '<div class="saleRow" data-idx="' + idx + '">' +
          imgHtml +
          '<div style="flex:1;min-width:0">' +
            '<div style="font-weight:700;font-size:14px">' + Number(s.priceTon).toFixed(3) + ' TON</div>' +
            (s.ts ? '<div style="font-size:11px;color:var(--muted)">' + new Date(s.ts).toLocaleString('ru-RU') + '</div>' : '') +
            '<div style="font-size:12px;color:var(--muted2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap">' + (s.title || '') + '</div>' +
            (s.model || s.backdrop ? '<div style="font-size:11px;color:var(--muted)">' + [s.model, s.backdrop].filter(Boolean).join(' · ') + '</div>' : '') +
          '</div>' +
          '<div style="display:flex;flex-direction:column;gap:4px;flex-shrink:0">' +
            (s.urlMarket ? '<button class="linkBtn sale-mrkt-btn" data-url="' + s.urlMarket + '">MRKT</button>' : '') +
            (s.urlTelegram ? '<button class="linkBtn sale-tg-btn" data-url="' + s.urlTelegram + '">NFT</button>' : '') +
          '</div>' +
        '</div>';
      }).join('');
      el('sheetBody').innerHTML = rows;
      el('sheetBody').querySelectorAll('.sale-tg-btn').forEach(btn => {
        btn.onclick = () => openTg(btn.getAttribute('data-url'));
      });
      el('sheetBody').querySelectorAll('.sale-mrkt-btn').forEach(btn => {
        btn.onclick = () => openTg(btn.getAttribute('data-url'));
      });
    }

    async function openLotSheet(lot) {
      openSheet();
      el('sheetTitle').textContent = lot.name || 'Gift';
      el('sheetSub').textContent = lot.priceTon != null ? Number(lot.priceTon).toFixed(3) + ' TON' : '';

      if (lot.imgUrl) {
        el('sheetImg').src = lot.imgUrl;
        el('sheetImg').style.display = 'block';
      }

      // Кликабельные фильтры
      const metaItems = [];
      if (lot.collectionName) metaItems.push(['Gift', lot.collectionName, () => {
        sel.gifts = [lot.collectionName]; sel.giftLabels = { [lot.collectionName]: lot.collectionName };
        sel.models = []; sel.backdrops = [];
        el('gift').value = giftsInputText(); el('model').value = ''; el('backdrop').value = '';
        closeSheet(); setTab('market');
        wrap('lots', async () => { await patchFilters(); await refreshSummary(true); await refreshLots(true); })();
      }]);
      if (lot.model) metaItems.push(['Model', lot.model, () => {
        if (sel.gifts.length !== 1 || sel.gifts[0] !== lot.collectionName) {
          sel.gifts = [lot.collectionName]; sel.giftLabels = { [lot.collectionName]: lot.collectionName };
        }
        sel.models = [lot.model]; el('gift').value = giftsInputText(); el('model').value = lot.model; el('backdrop').value = '';
        closeSheet(); setTab('market');
        wrap('lots', async () => { await patchFilters(); await refreshSummary(true); await refreshLots(true); })();
      }]);
      if (lot.backdrop) metaItems.push(['Backdrop', lot.backdrop, () => {
        if (sel.gifts.length !== 1 || sel.gifts[0] !== lot.collectionName) {
          sel.gifts = [lot.collectionName]; sel.giftLabels = { [lot.collectionName]: lot.collectionName };
        }
        sel.backdrops = [lot.backdrop]; el('gift').value = giftsInputText(); el('backdrop').value = lot.backdrop;
        closeSheet(); setTab('market');
        wrap('lots', async () => { await patchFilters(); await refreshSummary(true); await refreshLots(true); })();
      }]);
      if (lot.number != null) metaItems.push(['Number', '#' + lot.number, null]);

      el('sheetMeta').innerHTML = metaItems.map(([k, v, fn]) =>
        '<div class="sheetMetaRow">' +
          '<span class="key">' + k + '</span>' +
          '<span class="val" style="' + (fn ? 'cursor:pointer' : 'cursor:default;color:var(--text)') + '">' + v + '</span>' +
        '</div>'
      ).join('');

      metaItems.forEach(([k, v, fn], i) => {
        if (!fn) return;
        const spans = el('sheetMeta').querySelectorAll('.val');
        if (spans[i]) spans[i].onclick = fn;
      });

      el('sheetBtns').innerHTML =
        '<button class="btn-sm" id="sBtn_tg">Telegram NFT</button>' +
        '<button class="btn-sm" id="sBtn_mrkt">MRKT</button>' +
        '<button class="btn-sm" id="sBtn_apply">Применить фильтры</button>' +
        '<button class="btn-sm" id="sBtn_hist">История продаж</button>';

      el('sBtn_tg').onclick = () => openTg(lot.urlTelegram || 'https://t.me/mrkt');
      el('sBtn_mrkt').onclick = () => openTg(lot.urlMarket || 'https://t.me/mrkt');
      el('sBtn_apply').onclick = async () => {
        sel.gifts = lot.collectionName ? [lot.collectionName] : [];
        sel.giftLabels = lot.collectionName ? { [lot.collectionName]: lot.collectionName } : {};
        sel.models = lot.model ? [lot.model] : [];
        sel.backdrops = lot.backdrop ? [lot.backdrop] : [];
        el('gift').value = giftsInputText(); el('model').value = listInputText(sel.models); el('backdrop').value = listInputText(sel.backdrops); el('number').value = '';
        await patchFilters(); await refreshSummary(true); await refreshLots(true);
        closeSheet();
      };
      el('sBtn_hist').onclick = async () => {
        el('sheetBody').innerHTML = '<div class="loaderLine" style="display:flex"><div class="spinner"></div><div>Загрузка истории...</div></div>';
        try {
          const s = await api('/api/lot/sales', { method: 'POST', body: JSON.stringify({ lot }) });
          const hist = s.salesHistory || { approxPriceTon: null, sales: [] };
          el('sheetSub').textContent =
            (lot.priceTon != null ? Number(lot.priceTon).toFixed(3) + ' TON' : '') +
            (hist.approxPriceTon != null ? ' · медиана ' + Number(hist.approxPriceTon).toFixed(3) + ' TON' : '');
          renderSalesList(hist);
        } catch (e) {
          el('sheetBody').innerHTML = '<div style="color:var(--red);font-size:13px">' + (e.message || String(e)) + '</div>';
        }
      };

      // Загрузить offers сразу
      el('sheetBody').innerHTML = '<div class="statusLine">Загрузка офферов...</div>';
      try {
        const d = await api('/api/lot/details', { method: 'POST', body: JSON.stringify({ lot }) });
        const off = d.offers || {};
        el('sheetBody').innerHTML = [
          off.exact != null ? '<div class="sumPill" style="margin-bottom:4px">Макс. оффер (точный): <b>' + Number(off.exact).toFixed(3) + ' TON</b></div>' : '',
          off.collection != null ? '<div class="sumPill">Макс. оффер (коллекция): <b>' + Number(off.collection).toFixed(3) + ' TON</b></div>' : '',
          (!off.exact && !off.collection) ? '<div class="statusLine">Офферов нет</div>' : '',
        ].join('');
      } catch {
        el('sheetBody').innerHTML = '';
      }
    }

    async function openSalesByFilters() {
      openSheet();
      el('sheetTitle').textContent = 'История продаж';
      el('sheetSub').textContent = 'По текущим фильтрам';
      el('sheetImg').style.display = 'none';
      el('sheetMeta').innerHTML = '';
      el('sheetBtns').innerHTML = '<button class="btn-sm" id="salesCloseBtn">Закрыть</button>';
      el('sheetBody').innerHTML = '<div class="loaderLine" style="display:flex"><div class="spinner"></div><div>Загрузка истории продаж...</div></div>';
      const closeBtn = el('salesCloseBtn');
      if (closeBtn) closeBtn.onclick = closeSheet;
      try {
        const r = await api('/api/mrkt/sales_by_filters?force=1');
        if (r.approxPriceTon != null) {
          el('sheetSub').textContent = 'Медиана: ' + Number(r.approxPriceTon).toFixed(3) + ' TON';
        } else {
          el('sheetSub').textContent = 'По текущим фильтрам';
        }
        renderSalesList(r);
      } catch (e) {
        el('sheetBody').innerHTML = '<div style="color:var(--red);font-size:13px">Ошибка: ' + (e.message || String(e)) + '</div>';
      }
    }

    el('lots').addEventListener('click', e => {
      const card = e.target.closest('.lot');
      if (!card) return;
      const id = card.getAttribute('data-id');
      const lot = currentLots.find(x => String(x.id) === String(id));
      if (lot) openLotSheet(lot).catch(err => showErr(err.message || String(err)));
    });

    el('apply').onclick = wrap('lots', async () => {
      if (sel.gifts.length !== 1) { sel.models = []; sel.backdrops = []; el('model').value = ''; el('backdrop').value = ''; }
      hideSug('giftSug'); hideSug('modelSug'); hideSug('backdropSug');
      await patchFilters();
      await Promise.all([refreshSummary(true).catch(()=>{}), refreshLots(true)]);
    });
    el('refresh').onclick = wrap('lots', async () => {
      await Promise.all([refreshSummary(true).catch(()=>{}), refreshLots(true)]);
    });
    el('salesByFilters').onclick = async (e) => { e.stopPropagation(); hideErr(); await openSalesByFilters(); };

    let subCreateBusy = false;
    el('subCreate').onclick = wrap('subs', async () => {
      if (subCreateBusy) return;
      subCreateBusy = true;
      try {
        await api('/api/sub/create', { method: 'POST' });
        await refreshSubs();
      } finally { subCreateBusy = false; }
    });
    el('subRefresh').onclick = wrap('subs', async () => { await refreshSubs(); });
    el('subRebuildUi').onclick = wrap('subs', async () => { await api('/api/sub/rebuild_ui', { method: 'POST' }); await refreshSubs(); });
    el('subCheckNow').onclick = wrap('subs', async () => {
      const r = await api('/api/sub/check_now', { method: 'POST' });
      alert('Подписок проверено: ' + r.processedSubs + '\\nФлор уведомлений: ' + r.floorNotifs + '\\nFeed событий: ' + r.feedNotifs);
    });

    el('subsList').addEventListener('click', e => {
      const btn = e.target.closest('button[data-sub-act]');
      if (!btn) return;
      const act = btn.getAttribute('data-sub-act');
      const id = btn.getAttribute('data-id');
      wrap('subs', async () => {
        if (act === 'toggle') await api('/api/sub/toggle', { method: 'POST', body: JSON.stringify({ id }) });
        if (act === 'delete') { if (!confirm('Удалить подписку?')) return; await api('/api/sub/delete', { method: 'POST', body: JSON.stringify({ id }) }); }
        if (act === 'nmax') {
          const v = prompt('Max Notify TON (пусто = без лимита):');
          if (v == null) return;
          await api('/api/sub/set_notify_max', { method: 'POST', body: JSON.stringify({ id, maxNotifyTon: v }) });
        }
        if (act === 'ab') await api('/api/sub/toggle_autobuy', { method: 'POST', body: JSON.stringify({ id }) });
        if (act === 'amax') {
          const v = prompt('Max AutoBuy TON:');
          if (v == null) return;
          await api('/api/sub/set_autobuy_max', { method: 'POST', body: JSON.stringify({ id, maxAutoBuyTon: v }) });
        }
        // mode button removed — always "Новые" mode
        await refreshSubs();
      })();
    });

    el('sessSave').onclick = wrap('admin', async () => {
      const payloadJson = el('payloadJson').value.trim();
      if (!payloadJson) throw new Error('Вставь payload JSON');
      const r = await api('/api/admin/session', { method: 'POST', body: JSON.stringify({ payloadJson }) });
      alert('Сохранено. Token: ' + (r.tokenMask || ''));
      await refreshAdmin();
    });
    el('tokRefresh').onclick = wrap('admin', async () => {
      const r = await api('/api/admin/refresh_token', { method: 'POST', body: '{}' });
      alert('Token обновлён: ' + (r.tokenMask || ''));
      await refreshAdmin();
    });
    el('testMrkt').onclick = wrap('admin', async () => {
      const r = await api('/api/admin/test_mrkt');
      alert('Коллекций: ' + r.collectionsCount + '\\nToken: ' + (r.tokenMask || '—') + '\\nОшибка: ' + (r.lastFail || 'нет'));
      await refreshAdmin();
    });

    async function refreshAll(forceLots) {
      hideErr();
      await refreshState();
      // Параллельная загрузка — быстрее
      await Promise.all([
        refreshSummary(forceLots).catch(() => {}),
        refreshLots(forceLots).catch(e => showErr(e.message || String(e))),
      ]);
    }

    await wrap('lots', async () => { await refreshAll(true); })();
  })();
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

// ===================== Images =====================
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

// ===================== UI =====================
app.get('/', (req, res) => {
  res.setHeader('Content-Type', 'text/html; charset=utf-8');
  res.send(WEBAPP_HTML);
});
app.get('/app.js', (req, res) => {
  res.setHeader('Content-Type', 'application/javascript; charset=utf-8');
  res.send(WEBAPP_JS);
});

// ===================== State =====================
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

// ===================== MRKT collections/suggest =====================
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
    return {
      label: x.title || x.name,
      value: x.name,
      imgUrl,
      sub: floorTon != null ? `флор: ${floorTon.toFixed(3)} TON` : null,
    };
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
        const rarityPct = m.rarityPerMille != null && Number.isFinite(Number(m.rarityPerMille))
          ? (Number(m.rarityPerMille) / 10).toFixed(1) + '%' : null;
        const sub = (floor != null ? `от ${floor.toFixed(3)} TON` : '') + (rarityPct ? ` · редкость: ${rarityPct}` : '');
        return { label: m.name, value: m.name, imgUrl, sub: sub || null };
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
      .map((b) => ({ label: b.name, value: b.name, colorHex: b.centerHex || '#444444' }));
    const waitMs = Math.max(0, (mrktState.pauseUntil || 0) - nowMs());
    return res.json({ ok: true, items, waitMs: waitMs > 0 ? waitMs : 0 });
  }

  res.status(400).json({ ok: false, reason: 'BAD_KIND' });
});

// ===================== Lots + Summary =====================
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
    const r = await mrktSearchLotsByFilters(f, WEBAPP_LOTS_PAGES || MRKT_PAGES, {
      ordering: 'Price',
      lowToHigh: true,
      count: MRKT_COUNT,
    });
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
    imgUrl: lot.giftName
      ? `/img/gift?name=${encodeURIComponent(lot.giftName)}`
      : (lot.thumbKey ? `/img/cdn?key=${encodeURIComponent(lot.thumbKey)}` : null),
    canBuy: MANUAL_BUY_ENABLED,
  }));

  const payload = { ok: true, lots: mapped, note, waitMs };
  // Сохраняем кэш всегда (даже при force) чтобы повторные запросы были быстрыми
  lotsCache.set(cacheKey, { time: nowMs(), data: payload });
  res.json(payload);
});

app.get('/api/mrkt/summary_by_filters', auth, async (req, res) => {
  const force = req.query.force != null;
  const u = getOrCreateUser(req.userId);
  const f = normalizeFilters(u.filters || {});
  const cacheKey = `sum|${req.userId}|${JSON.stringify(f)}`;

  const cached = summaryCache.get(cacheKey);
  if (!force && cached && nowMs() - cached.time < SUMMARY_CACHE_TTL_MS) return res.json(cached.data);

  if (f.gifts.length !== 1) {
    const payload = {
      ok: true,
      offers: { exact: null, collection: null },
      note: f.gifts.length ? 'Сводка работает для 1 gift' : null,
      waitMs: 0,
    };
    if (!force) summaryCache.set(cacheKey, { time: nowMs(), data: payload });
    return res.json(payload);
  }

  const gift = f.gifts[0];
  const model = f.models.length === 1 ? f.models[0] : null;
  const backdrop = f.backdrops.length === 1 ? f.backdrops[0] : null;

  const o1 = await mrktOrdersFetch({ gift, model, backdrop });
  if (!o1.ok && o1.reason === 'RPS_WAIT') {
    return res.json({ ok: true, offers: { exact: null, collection: null }, note: `RPS limit, wait ${fmtWaitMs(o1.waitMs || 1000)}`, waitMs: o1.waitMs || 1000 });
  }

  const o2 = await mrktOrdersFetch({ gift, model: null, backdrop: null });
  if (!o2.ok && o2.reason === 'RPS_WAIT') {
    return res.json({ ok: true, offers: { exact: null, collection: null }, note: `RPS limit, wait ${fmtWaitMs(o2.waitMs || 1000)}`, waitMs: o2.waitMs || 1000 });
  }

  const payload = {
    ok: true,
    offers: {
      exact: o1.ok ? maxOfferTonFromOrders(o1.orders) : null,
      collection: o2.ok ? maxOfferTonFromOrders(o2.orders) : null,
    },
    note: null,
    waitMs: 0,
  };

  if (!force) summaryCache.set(cacheKey, { time: nowMs(), data: payload });
  res.json(payload);
});

// ===================== Lot details + sales =====================
app.post('/api/lot/details', auth, async (req, res) => {
  const force = req.query.force != null;
  const lot = req.body && req.body.lot ? req.body.lot : null;
  const lotId = String((lot && lot.id) || '').trim();
  if (!lotId) return res.status(400).json({ ok: false, reason: 'NO_LOT' });

  const cached = detailsCache.get(lotId);
  if (!force && cached && nowMs() - cached.time < DETAILS_CACHE_TTL_MS) return res.json(cached.data);

  const gift = String((lot && lot.collectionName) || '').trim();
  const model = lot && lot.model ? String(lot.model) : null;
  const backdrop = lot && lot.backdrop ? String(lot.backdrop) : null;

  let offerExact = null;
  let offerCollection = null;
  let waitMs = 0;

  if (gift) {
    const o1 = await mrktOrdersFetch({ gift, model, backdrop });
    if (!o1.ok && o1.reason === 'RPS_WAIT') waitMs = o1.waitMs || 1000;
    else if (o1.ok) offerExact = maxOfferTonFromOrders(o1.orders);

    const o2 = await mrktOrdersFetch({ gift, model: null, backdrop: null });
    if (!waitMs && !o2.ok && o2.reason === 'RPS_WAIT') waitMs = o2.waitMs || 1000;
    else if (o2.ok) offerCollection = maxOfferTonFromOrders(o2.orders);
  }

  const payload = { ok: true, offers: { exact: offerExact, collection: offerCollection }, waitMs };
  if (!force) detailsCache.set(lotId, { time: nowMs(), data: payload });
  res.json(payload);
});

app.post('/api/lot/sales', auth, async (req, res) => {
  const lot = req.body && req.body.lot ? req.body.lot : null;
  const gift = String((lot && lot.collectionName) || '').trim();
  const model = lot && lot.model ? String(lot.model) : null;
  const backdrop = lot && lot.backdrop ? String(lot.backdrop) : null;

  if (!gift) {
    return res.json({ ok: true, salesHistory: { ok: true, approxPriceTon: null, sales: [], note: 'NO_GIFT', waitMs: 0 } });
  }

  const out = await mrktFeedSales({ gift, modelNames: model ? [model] : [], backdropNames: backdrop ? [backdrop] : [] });
  res.json({ ok: true, salesHistory: out });
});

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

// ===================== Manual buy =====================
app.post('/api/mrkt/buy', auth, async (req, res) => {
  if (!MANUAL_BUY_ENABLED) return res.status(403).json({ ok: false, reason: 'MANUAL_BUY_DISABLED' });

  const id = String((req.body && req.body.id) || '').trim();
  const priceNano = Number(req.body && req.body.priceNano);
  const lot = (req.body && req.body.lot) || null;

  if (!id) return res.status(400).json({ ok: false, reason: 'NO_ID' });
  if (!Number.isFinite(priceNano) || priceNano <= 0) return res.status(400).json({ ok: false, reason: 'BAD_PRICE' });

  const foundAt = Date.now();
  const buyStart = Date.now();
  const r = await mrktBuy({ id, priceNano });
  const boughtAt = Date.now();
  const latencyMs = boughtAt - buyStart;

  if (!r.ok) {
    if (r.reason === 'RPS_WAIT') return res.status(429).json({ ok: false, reason: `RPS limit, wait ${fmtWaitMs(r.waitMs || 1000)}` });
    return res.status(502).json({ ok: false, reason: r.reason });
  }

  const u = getOrCreateUser(req.userId);
  pushPurchase(u, {
    tsFound: foundAt,
    tsBought: boughtAt,
    latencyMs,
    title: (lot && lot.name) || 'Gift',
    priceTon: priceNano / 1e9,
    urlTelegram: (lot && lot.urlTelegram) || '',
    urlMarket: (lot && lot.urlMarket) || mrktLotUrlFromId(id),
    lotId: id,
    giftName: (lot && lot.giftName) || '',
    thumbKey: (lot && lot.thumbKey) || '',
    model: (lot && lot.model) || '',
    backdrop: (lot && lot.backdrop) || '',
    collection: (lot && lot.collectionName) || '',
    number: (lot && lot.number) != null ? lot.number : null,
  });
  scheduleSave();

  res.json({ ok: true, title: (lot && lot.name) || 'Gift', priceTon: priceNano / 1e9 });
});

// ===================== Profile =====================
app.get('/api/profile', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);

  const purchases = (u.purchases || []).slice(0, 120).map((p) => ({
    title: p.title,
    lotId: p.lotId || null,
    priceTon: p.priceTon,
    boughtMsk: p.tsBought ? new Date(p.tsBought).toLocaleString('ru-RU', { timeZone: 'Europe/Moscow' }) : null,
    latencyMs: p.latencyMs != null ? p.latencyMs : null,
    imgUrl: p.giftName
      ? `/img/gift?name=${encodeURIComponent(p.giftName)}`
      : (p.thumbKey ? `/img/cdn?key=${encodeURIComponent(p.thumbKey)}` : null),
    urlTelegram: p.urlTelegram || null,
    urlMarket: p.urlMarket || null,
    model: p.model || null,
    backdrop: p.backdrop || null,
    collection: p.collection || null,
    number: p.number != null ? p.number : null,
  }));

  res.json({ ok: true, user: req.tgUser, purchases });
});

// ===================== Subs endpoints =====================
app.post('/api/sub/create', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);

  const r = makeSubFromCurrentFilters(u);
  if (!r.ok) return res.status(400).json({ ok: false, reason: r.reason });

  const sig = JSON.stringify(normalizeFilters(r.sub.filters || {}));
  const exists = (u.subscriptions || []).find((x) => x && JSON.stringify(normalizeFilters(x.filters || {})) === sig);
  if (exists) return res.json({ ok: true, alreadyExists: true });

  r.sub.ui = await buildSubUi(r.sub);
  u.subscriptions.push(r.sub);
  renumberSubs(u);
  scheduleSave();

  // Стартовое уведомление о флоре
  setTimeout(async () => {
    try {
      const sf = normalizeFilters(r.sub.filters || {});
      const rLots = await mrktSearchLotsByFilters(sf, 1, { ordering: 'Price', lowToHigh: true, count: MRKT_COUNT });
      const lot = (rLots.ok && rLots.gifts && rLots.gifts[0]) ? rLots.gifts[0] : null;
      if (!lot) return;
      await notifyFloorToUser(req.userId, r.sub, lot, lot.priceTon);
      console.log(`[SUB CREATE] Стартовое уведомление отправлено userId=${req.userId} sub=#${r.sub.num}`);
    } catch (e) {
      console.error('sub create notify error:', e && e.message || e);
    }
  }, 300);

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
  const s = findSub(u, String((req.body && req.body.id) || ''));
  if (!s) return res.status(404).json({ ok: false, reason: 'NOT_FOUND' });
  s.enabled = !s.enabled;
  scheduleSave();
  res.json({ ok: true });
});

app.post('/api/sub/delete', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const id = String((req.body && req.body.id) || '');
  u.subscriptions = (u.subscriptions || []).filter((x) => x && x.id !== id);
  renumberSubs(u);
  scheduleSave();
  res.json({ ok: true });
});

app.post('/api/sub/set_notify_max', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const s = findSub(u, String((req.body && req.body.id) || ''));
  if (!s) return res.status(404).json({ ok: false, reason: 'NOT_FOUND' });
  const str = String(req.body && req.body.maxNotifyTon != null ? req.body.maxNotifyTon : '').trim();
  if (!str) { s.maxNotifyTon = null; scheduleSave(); return res.json({ ok: true }); }
  const v = parseTonInput(str);
  if (!Number.isFinite(v) || v <= 0) return res.status(400).json({ ok: false, reason: 'BAD_MAX' });
  s.maxNotifyTon = v;
  scheduleSave();
  res.json({ ok: true });
});

app.post('/api/sub/toggle_autobuy', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const s = findSub(u, String((req.body && req.body.id) || ''));
  if (!s) return res.status(404).json({ ok: false, reason: 'NOT_FOUND' });
  s.autoBuyEnabled = !s.autoBuyEnabled;
  scheduleSave();
  res.json({ ok: true });
});

app.post('/api/sub/set_autobuy_max', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const s = findSub(u, String((req.body && req.body.id) || ''));
  if (!s) return res.status(404).json({ ok: false, reason: 'NOT_FOUND' });
  const v = parseTonInput(req.body && req.body.maxAutoBuyTon);
  if (!Number.isFinite(v) || v <= 0) return res.status(400).json({ ok: false, reason: 'BAD_MAX' });
  s.maxAutoBuyTon = v;
  scheduleSave();
  res.json({ ok: true });
});

app.post('/api/sub/toggle_autobuy_any', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const s = findSub(u, String((req.body && req.body.id) || ''));
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
    mrktLastFail: mrktState.lastFailMsg || null,
    pauseUntil: mrktState.pauseUntil || 0,
    refresh: { ...mrktAuthDebug },
  });
});

app.post('/api/admin/session', auth, async (req, res) => {
  if (!isAdmin(req.userId)) return res.status(403).json({ ok: false, reason: 'NOT_ADMIN' });

  const payloadJson = req.body && req.body.payloadJson;
  let data = null;
  let photo = null;

  if (typeof payloadJson === 'string' && payloadJson.trim()) {
    const j = tryParseJsonMaybe(payloadJson);
    if (!j || typeof j !== 'object') return res.status(400).json({ ok: false, reason: 'BAD_PAYLOAD_JSON' });
    data = typeof j.data === 'string' ? j.data : null;
    photo = j.photo === undefined ? null : j.photo;
  } else {
    data = req.body && typeof req.body.data === 'string' ? req.body.data : null;
    const rawPhoto = req.body && req.body.photo;
    if (rawPhoto == null || rawPhoto === '') photo = null;
    else if (typeof rawPhoto === 'string') {
      const pj = tryParseJsonMaybe(rawPhoto);
      photo = pj != null ? pj : rawPhoto;
    } else photo = rawPhoto;
  }

  if (!data || !String(data).includes('hash=')) return res.status(400).json({ ok: false, reason: 'BAD_DATA_NO_HASH' });

  const sess = { data: String(data), photo };
  mrktSessionRuntime = sess;
  if (redis) await redisSet(REDIS_KEY_MRKT_SESSION, JSON.stringify(sess), { EX: WEBAPP_AUTH_MAX_AGE_SEC });

  const rr = await tryRefreshMrktToken('admin_save_session', { force: true });
  if (!rr.ok) {
    return res.status(400).json({
      ok: false,
      reason: `SESSION_SAVED_BUT_REFRESH_FAILED:${rr.reason}`,
      refresh: { ...mrktAuthDebug },
      lastFail: mrktState.lastFailMsg || null,
    });
  }

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
    .catch((e) => console.error('setWebHook error:', e && e.message || e));

  app.post('/telegram-webhook', (req, res) => {
    bot.processUpdate(req.body);
    res.sendStatus(200);
  });
} else {
  bot.deleteWebHook({ drop_pending_updates: true }).catch(() => {});
  bot.startPolling({ interval: 300, autoStart: true });
  console.log('Polling started (PUBLIC_URL not set)');
}

// ===================== start server =====================
const PORT = Number(process.env.PORT || 3000);
app.listen(PORT, '0.0.0.0', () => console.log('HTTP listening on', PORT));

(async () => {
  if (REDIS_URL) {
    await initRedis();
    if (redis) {
      const t = await redisGet(REDIS_KEY_MRKT_AUTH);
      if (t && String(t).trim()) {
        MRKT_AUTH_RUNTIME = String(t).trim();
        console.log('Loaded MRKT token from Redis:', maskToken(MRKT_AUTH_RUNTIME));
      }

      const rawSess = await redisGet(REDIS_KEY_MRKT_SESSION);
      if (rawSess) {
        try { mrktSessionRuntime = JSON.parse(rawSess); console.log('Loaded MRKT session from Redis'); } catch {}
      }

      const keys = [
        REDIS_KEY_STATE,
        'bot:state:v8', 'bot:state:v7', 'bot:state:v6',
        'bot:state:v5', 'bot:state:v4', 'bot:state:v3',
        'bot:state:v2', 'bot:state:v1',
      ];
      for (const k of keys) {
        const raw = await redisGet(k);
        if (raw) {
          try {
            importState(JSON.parse(raw));
            console.log('Loaded state from', k, 'users=', users.size);
            break;
          } catch {}
        }
      }
    }
  } else {
    console.warn('REDIS_URL not set => state/session/token won\'t persist');
  }

  console.log('Bot ready. MODE=' + MODE + ' AUTO_BUY_GLOBAL=' + AUTO_BUY_GLOBAL + ' AUTO_BUY_DRY_RUN=' + AUTO_BUY_DRY_RUN);
  console.log('Users loaded:', users.size, '| Subs total:', Array.from(users.values()).reduce((a,u)=>a+(u.subscriptions?.length||0),0));

  // Запускаем интервалы ТОЛЬКО ПОСЛЕ загрузки Redis и state
  // Это критично — иначе первые циклы работают с пустым users Map
  // Один цикл на всё — подписки + AutoBuy вместе (нет дублирования запросов)
  setInterval(() => {
    checkSubscriptionsForAllUsers().catch((e) => console.error('subs interval error:', e));
  }, SUBS_CHECK_INTERVAL_MS);

  // Первый запуск через 5 секунд после старта
  setTimeout(() => {
    console.log('[BOOT] Первый запуск checkSubscriptions (+ AutoBuy)...');
    checkSubscriptionsForAllUsers({ manual: true }).catch((e) => console.error('subs boot error:', e));
  }, 5000);
})();
