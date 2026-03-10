/**
 * v19-full-fast (2026-03-10)
 * MRKT Panel + Bot (single file)
 *
 * Fixes:
 * - /api/state is FAST: no MRKT network calls inside (no more "10 min queue")
 * - Admin tab stable: paste FULL /api/v1/auth payload JSON -> store session (photo as OBJECT) -> refresh token
 * - Auto refresh MRKT_AUTH on 401/403 using saved session
 * - Debounced dropdown requests, X clear buttons
 * - Market: removed "recent purchases" block (purchases only in Profile)
 * - Subs notify: no extra gift line, only MRKT button, only t.me/nft link (preview ON)
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

// throttling
const MRKT_TIMEOUT_MS = Number(process.env.MRKT_TIMEOUT_MS || 12000);
const MRKT_MIN_GAP_MS = Number(process.env.MRKT_MIN_GAP_MS || 350);
const MRKT_429_DEFAULT_PAUSE_MS = Number(process.env.MRKT_429_DEFAULT_PAUSE_MS || 4500);

// limits
const MRKT_COUNT = Number(process.env.MRKT_COUNT || 25);
const MRKT_PAGES = Number(process.env.MRKT_PAGES || 2);
const MRKT_ORDERS_COUNT = Number(process.env.MRKT_ORDERS_COUNT || 30);

const WEBAPP_LOTS_LIMIT = Number(process.env.WEBAPP_LOTS_LIMIT || 40);
const WEBAPP_LOTS_PAGES = Number(process.env.WEBAPP_LOTS_PAGES || 1);
const WEBAPP_SUGGEST_LIMIT = Number(process.env.WEBAPP_SUGGEST_LIMIT || 120);

const GLOBAL_SCAN_COLLECTIONS = Number(process.env.GLOBAL_SCAN_COLLECTIONS || 6);
const GLOBAL_SCAN_CACHE_TTL_MS = Number(process.env.GLOBAL_SCAN_CACHE_TTL_MS || 60_000);

// Subs
const SUBS_CHECK_INTERVAL_MS = Number(process.env.SUBS_CHECK_INTERVAL_MS || 15000);
const SUBS_MAX_NOTIFICATIONS_PER_CYCLE = Number(process.env.SUBS_MAX_NOTIFICATIONS_PER_CYCLE || 6);
const SUBS_EMPTY_CONFIRM = Number(process.env.SUBS_EMPTY_CONFIRM || 2);

// AutoBuy
const AUTO_BUY_GLOBAL = String(process.env.AUTO_BUY_GLOBAL || '0') === '1';
const AUTO_BUY_DRY_RUN = String(process.env.AUTO_BUY_DRY_RUN || '1') === '1';
const AUTO_BUY_CHECK_INTERVAL_MS = Number(process.env.AUTO_BUY_CHECK_INTERVAL_MS || 6000);
const AUTO_BUY_MAX_BUYS_PER_USER_PER_CYCLE = Number(process.env.AUTO_BUY_MAX_BUYS_PER_USER_PER_CYCLE || 1);
const AUTO_BUY_ATTEMPT_TTL_MS = Number(process.env.AUTO_BUY_ATTEMPT_TTL_MS || 60_000);
const AUTO_BUY_DISABLE_AFTER_SUCCESS = String(process.env.AUTO_BUY_DISABLE_AFTER_SUCCESS || '0') === '1';

// Sales history
const SALES_HISTORY_TARGET = Number(process.env.SALES_HISTORY_TARGET || 24);
const SALES_HISTORY_MAX_PAGES = Number(process.env.SALES_HISTORY_MAX_PAGES || 30);
const SALES_HISTORY_COUNT_PER_PAGE = Number(process.env.SALES_HISTORY_COUNT_PER_PAGE || 50);
const SALES_HISTORY_THROTTLE_MS = Number(process.env.SALES_HISTORY_THROTTLE_MS || 180);
const SALES_HISTORY_TIME_BUDGET_MS = Number(process.env.SALES_HISTORY_TIME_BUDGET_MS || 15000);

const MRKT_AUTH_REFRESH_COOLDOWN_MS = Number(process.env.MRKT_AUTH_REFRESH_COOLDOWN_MS || 8000);

// Redis keys
const REDIS_KEY_STATE = 'bot:state:main';
const REDIS_KEY_MRKT_AUTH = 'mrkt:auth:token';
const REDIS_KEY_MRKT_SESSION = 'mrkt:session:admin'; // { data:string, photo:any }

console.log('v19-full-fast start', {
  MODE,
  WEBAPP_URL: !!WEBAPP_URL,
  PUBLIC_URL: !!PUBLIC_URL,
  ADMIN_USER_ID,
  REDIS: !!REDIS_URL,
  MRKT_AUTH_SET: !!MRKT_AUTH_RUNTIME,
  MRKT_MIN_GAP_MS,
  AUTO_BUY_GLOBAL,
  AUTO_BUY_DRY_RUN,
});

// ===================== Telegram bot =====================
const bot = new TelegramBot(TELEGRAM_TOKEN, { polling: false });
const MAIN_KEYBOARD = { keyboard: [[{ text: '📌 Статус' }]], resize_keyboard: true };

// ===================== Helpers =====================
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const nowMs = () => Date.now();

function makeReq() { return `${Date.now()}_${Math.random().toString(16).slice(2)}`; }
function norm(s) { return String(s || '').toLowerCase().trim().replace(/\s+/g, ' '); }
function normTraitName(s) { return norm(s).replace(/\s*\([^)]*%[^)]*\)\s*/g, ' ').replace(/\s+/g, ' ').trim(); }
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
function mrktLotUrlFromId(id) { return id ? `https://t.me/mrkt/app?startapp=${String(id).replace(/-/g, '')}` : 'https://t.me/mrkt'; }

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

function maskToken(t) {
  const s = String(t || '').trim();
  if (!s) return '';
  if (s.length <= 10) return s;
  return s.slice(0, 4) + '…' + s.slice(-4);
}

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

function isAdmin(userId) { return ADMIN_USER_ID && Number(userId) === Number(ADMIN_USER_ID); }

// ===================== WebApp initData verify (our panel) =====================
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

// ===================== State =====================
const users = new Map();
const subStates = new Map();
const autoBuyRecentAttempts = new Map();

function normalizeFilters(f) {
  const gifts = uniqNorm(ensureArray(f?.gifts || f?.gift));
  const giftLabels = (f?.giftLabels && typeof f.giftLabels === 'object') ? f.giftLabels : {};
  const models = uniqNorm(ensureArray(f?.models || f?.model));
  const backdrops = uniqNorm(ensureArray(f?.backdrops || f?.backdrop));
  const numberPrefix = cleanDigitsPrefix(f?.numberPrefix || '');

  const safeModels = gifts.length === 1 ? models : [];
  const safeBackdrops = gifts.length === 1 ? backdrops : [];

  return { gifts, giftLabels, models: safeModels, backdrops: safeBackdrops, numberPrefix };
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
    ui: { thumbKey: null, swatches: [] }, // IMPORTANT: store UI here, /api/state won't call MRKT
  };
  return { ok: true, sub };
}

function pushPurchase(user, entry) {
  if (!user.purchases) user.purchases = [];
  user.purchases.unshift({
    tsListed: entry.tsListed || null,
    tsFound: entry.tsFound || null,
    tsBought: entry.tsBought || entry.ts || Date.now(),
    latencyMs: entry.latencyMs || null,
    title: String(entry.title || ''),
    priceTon: Number(entry.priceTon || 0),
    urlTelegram: String(entry.urlTelegram || ''),
    urlMarket: String(entry.urlMarket || ''),
    lotId: String(entry.lotId || ''),
    giftName: String(entry.giftName || ''),
    model: String(entry.model || ''),
    backdrop: String(entry.backdrop || ''),
    collection: String(entry.collection || ''),
  });
  user.purchases = user.purchases.slice(0, 300);
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

    safe.purchases = (safe.purchases || []).slice(0, 300).map((p) => ({
      ...p,
      priceTon: Number(p.priceTon || 0),
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

// ===================== MRKT: auth/session refresh =====================
let mrktSessionRuntime = null; // {data, photo} photo can be object/string/null
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
async function saveMrktSessionToRedis(sess) {
  if (!redis) return;
  await redisSet(REDIS_KEY_MRKT_SESSION, JSON.stringify(sess), { EX: WEBAPP_AUTH_MAX_AGE_SEC });
}

async function mrktAuthWithSession(session) {
  const url = `${MRKT_API_URL}/auth`;
  const body = {
    appId: null,
    data: String(session?.data || ''),
    photo: session?.photo ?? null, // IMPORTANT: keep as object/null/string (NO String(photo))
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

  if (!res.ok) return { ok: false, status: res.status, reason: extractMrktErrorMessage(txt) || `HTTP_${res.status}`, text: txt };

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

    // drop MRKT caches
    collectionsCache = { time: 0, items: [] };
    modelsCache.clear();
    backdropsCache.clear();
    offersCache.clear();
    lotsCache.clear();
    detailsCache.clear();
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

// ===================== MRKT: queue + throttle wrappers =====================
const mrktState = {
  lastOkAt: 0,
  lastFailAt: 0,
  lastFailMsg: null,
  lastFailStatus: null,
  lastFailEndpoint: null,
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

async function mrktWaitGate() {
  const now = nowMs();
  if (mrktState.pauseUntil && now < mrktState.pauseUntil) {
    await sleep(mrktState.pauseUntil - now);
  }
  const now2 = nowMs();
  const wait = (mrktState.nextAllowedAt || 0) - now2;
  if (wait > 0) await sleep(wait);
  mrktState.nextAllowedAt = nowMs() + MRKT_MIN_GAP_MS;
}

function setPauseFrom429(res) {
  const ra = res?.headers?.get?.('retry-after');
  const ms = ra ? (Number(ra) * 1000) : MRKT_429_DEFAULT_PAUSE_MS;
  const until = nowMs() + (Number.isFinite(ms) && ms > 0 ? ms : MRKT_429_DEFAULT_PAUSE_MS);
  mrktState.pauseUntil = Math.max(mrktState.pauseUntil || 0, until);
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
      if (!ok) { markMrktFail(path, 401, 'NO_AUTH'); return { ok: false, status: 401, data: null, text: 'NO_AUTH' }; }
    }

    await mrktWaitGate();
    const url = `${MRKT_API_URL}${path}${path.includes('?') ? '&' : '?'}req=${encodeURIComponent(makeReq())}`;

    const res = await fetchWithTimeout(url, { method: 'GET', headers: mrktHeadersCommon() }, MRKT_TIMEOUT_MS).catch(() => null);
    if (!res) { markMrktFail(path, 0, 'FETCH_ERROR'); return { ok: false, status: 0, data: null, text: 'FETCH_ERROR' }; }

    const txt = await res.text().catch(() => '');
    let data = null;
    try { data = txt ? JSON.parse(txt) : null; } catch {}

    if (!res.ok) {
      if (res.status === 429) setPauseFrom429(res);
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
      if (!ok) { markMrktFail(path, 401, 'NO_AUTH'); return { ok: false, status: 401, data: null, text: 'NO_AUTH' }; }
    }

    await mrktWaitGate();
    const reqVal = bodyObj?.req ? String(bodyObj.req) : makeReq();
    const body = { ...(bodyObj || {}), req: reqVal };
    const url = `${MRKT_API_URL}${path}${path.includes('?') ? '&' : '?'}req=${encodeURIComponent(reqVal)}`;

    const res = await fetchWithTimeout(url, { method: 'POST', headers: mrktHeadersJson(), body: JSON.stringify(body) }, MRKT_TIMEOUT_MS).catch(() => null);
    if (!res) { markMrktFail(path, 0, 'FETCH_ERROR'); return { ok: false, status: 0, data: null, text: 'FETCH_ERROR' }; }

    const txt = await res.text().catch(() => '');
    let data = null;
    try { data = txt ? JSON.parse(txt) : null; } catch {}

    if (!res.ok) {
      if (res.status === 429) setPauseFrom429(res);
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

// ===================== MRKT API + caches =====================
let collectionsCache = { time: 0, items: [] };
const modelsCache = new Map();
const backdropsCache = new Map();
const offersCache = new Map();
const thumbsCache = new Map();
const lotsCache = new Map();
const detailsCache = new Map();

const globalCheapestCache = { time: 0, lots: [], note: null };

async function mrktGetCollections() {
  const now = nowMs();
  if (collectionsCache.items.length && now - collectionsCache.time < CACHE_TTL_MS) return collectionsCache.items;

  const r = await mrktGetJson('/gifts/collections');
  if (!r.ok) { collectionsCache = { time: now, items: [] }; return []; }

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
  if (!r.ok) return [];

  const arr = Array.isArray(r.data) ? r.data : [];
  const map = new Map();

  for (const it of arr) {
    const name = it.modelTitle || it.modelName;
    if (!name) continue;
    const key = normTraitName(name);
    if (!map.has(key)) map.set(key, { name: String(name), thumbKey: it.modelStickerThumbnailKey || null, floorNano: it.floorPriceNanoTons ?? null });
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
  if (!r.ok) return [];

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

function buildSalingBody({ count = 20, cursor = '', collectionNames = [], modelNames = [], backdropNames = [], ordering = 'Price', lowToHigh = true } = {}) {
  return {
    count, cursor, collectionNames, modelNames, backdropNames,
    symbolNames: [],
    ordering, lowToHigh,
    maxPrice: null, minPrice: null, number: null, query: null,
    promotedFirst: false,
  };
}

async function mrktFetchSalingPage({ collectionNames, modelNames, backdropNames, cursor, ordering, lowToHigh, count }) {
  const body = buildSalingBody({
    count: Number(count || MRKT_COUNT),
    cursor: cursor || '',
    collectionNames: Array.isArray(collectionNames) ? collectionNames : [],
    modelNames: Array.isArray(modelNames) ? modelNames : [],
    backdropNames: Array.isArray(backdropNames) ? backdropNames : [],
    ordering: ordering || 'Price',
    lowToHigh: !!lowToHigh,
  });

  const r = await mrktPostJson('/gifts/saling', body);
  if (!r.ok) return { ok: false, reason: mrktState.lastFailMsg || `HTTP_${r.status}`, gifts: [], cursor: '' };

  const gifts = Array.isArray(r.data?.gifts) ? r.data.gifts : [];
  const nextCursor = r.data?.cursor || '';
  return { ok: true, reason: 'OK', gifts, cursor: nextCursor };
}

function salingGiftToLot(g) {
  const nano = (g?.salePriceWithoutFee && Number(g.salePriceWithoutFee) > 0) ? g.salePriceWithoutFee : g?.salePrice;
  const priceTon = Number(nano) / 1e9;
  if (!Number.isFinite(priceTon) || priceTon <= 0) return null;

  const numberVal = g.number ?? null;
  const baseName = (g.collectionTitle || g.collectionName || g.title || 'Gift').trim();
  const displayName = numberVal != null ? `${baseName} #${numberVal}` : baseName;

  const giftName = g.name || giftNameFallbackFromCollectionAndNumber(baseName, numberVal) || null;
  const urlTelegram = giftName && String(giftName).includes('-') ? `https://t.me/nft/${giftName}` : 'https://t.me/mrkt';
  const urlMarket = g.id ? mrktLotUrlFromId(g.id) : 'https://t.me/mrkt';

  const listedAt = g.receivedDate || g.exportDate || null;

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
    listedAt,
  };
}

async function mrktSearchLotsByFilters(filters, pagesLimit) {
  const f = normalizeFilters(filters || {});
  const prefix = f.numberPrefix;
  const pagesToScan = Math.max(1, Number(pagesLimit || 1));
  let cursor = '';
  const out = [];

  const gifts = f.gifts || [];
  const models = gifts.length === 1 ? (f.models || []) : [];
  const backdrops = gifts.length === 1 ? (f.backdrops || []) : [];

  for (let page = 0; page < pagesToScan; page++) {
    const r = await mrktFetchSalingPage({
      collectionNames: gifts,
      modelNames: models,
      backdropNames: backdrops,
      cursor,
      ordering: prefix ? 'Latest' : 'Price',
      lowToHigh: prefix ? false : true,
      count: MRKT_COUNT,
    });
    if (!r.ok) return { ok: false, reason: r.reason, gifts: [] };

    for (const g of r.gifts) {
      const lot = salingGiftToLot(g);
      if (!lot) continue;

      if (prefix) {
        const numStr = lot.number == null ? '' : String(lot.number);
        if (!numStr.startsWith(prefix)) continue;
      }

      out.push(lot);
      if (out.length >= 500) break;
    }

    cursor = r.cursor || '';
    if (!cursor || out.length >= 500) break;
  }

  out.sort((a, b) => a.priceTon - b.priceTon);
  return { ok: true, reason: 'OK', gifts: out };
}

async function mrktOrdersFetch({ gift, model, backdrop }) {
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

  const r = await mrktPostJson('/orders', body);
  const out = r.ok ? { ok: true, orders: Array.isArray(r.data?.orders) ? r.data.orders : [] } : { ok: false, orders: [] };
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

async function mrktFeedSales({ gift, modelNames = [], backdropNames = [] }) {
  if (!gift) return { ok: true, approxPriceTon: null, sales: [], note: 'no gift' };

  const wantModels = new Set((modelNames || []).map(normTraitName).filter(Boolean));
  const wantBacks = new Set((backdropNames || []).map(normTraitName).filter(Boolean));

  const started = nowMs();
  let cursor = '';
  let pages = 0;
  const sales = [];
  const prices = [];

  while (pages < SALES_HISTORY_MAX_PAGES && sales.length < SALES_HISTORY_TARGET) {
    if (nowMs() - started > SALES_HISTORY_TIME_BUDGET_MS) break;

    const body = {
      count: Number(SALES_HISTORY_COUNT_PER_PAGE),
      cursor: cursor || '',
      collectionNames: [gift],
      modelNames: Array.isArray(modelNames) ? modelNames : [],
      backdropNames: Array.isArray(backdropNames) ? backdropNames : [],
      lowToHigh: false,
      maxPrice: null,
      minPrice: null,
      number: null,
      ordering: 'Latest',
      query: null,
      type: ['sale'],
      types: ['sale'],
    };

    const r = await mrktPostJson('/feed', body);
    if (!r.ok) break;

    const items = Array.isArray(r.data?.items) ? r.data.items : [];
    if (!items.length) break;

    for (const it of items) {
      const g = it?.gift;
      if (!g) continue;

      const mName = g.modelTitle || g.modelName || null;
      const bName = g.backdropName || null;

      if (wantModels.size && !wantModels.has(normTraitName(mName))) continue;
      if (wantBacks.size && !wantBacks.has(normTraitName(bName))) continue;

      const amountNano = it.amount ?? g.salePriceWithoutFee ?? g.salePrice ?? null;
      const ton = amountNano != null ? Number(amountNano) / 1e9 : NaN;
      if (!Number.isFinite(ton) || ton <= 0) continue;

      const base = (g.collectionTitle || g.collectionName || g.title || gift).trim();
      const number = g.number ?? null;
      const name = number != null ? `${base} #${number}` : base;

      const giftName = g.name || giftNameFallbackFromCollectionAndNumber(base, number) || null;
      const urlTelegram = giftName && String(giftName).includes('-') ? `https://t.me/nft/${giftName}` : 'https://t.me/mrkt';
      const urlMarket = g.id ? mrktLotUrlFromId(g.id) : 'https://t.me/mrkt';

      sales.push({ ts: it.date || null, priceTon: ton, name, model: mName, backdrop: bName, urlTelegram, urlMarket });
      prices.push(ton);

      if (sales.length >= SALES_HISTORY_TARGET) break;
    }

    cursor = r.data?.cursor || '';
    pages++;
    if (!cursor) break;

    if (SALES_HISTORY_THROTTLE_MS > 0) await sleep(SALES_HISTORY_THROTTLE_MS);
  }

  prices.sort((a, b) => a - b);
  return { ok: true, approxPriceTon: median(prices), sales, note: null };
}

async function mrktBuy({ id, priceNano }) {
  const body = { ids: [id], prices: { [id]: priceNano } };
  const r = await mrktPostJson('/gifts/buy', body);
  if (!r.ok) return { ok: false, reason: mrktState.lastFailMsg || 'BUY_ERROR', data: r.data, text: r.text };

  const data = r.data;
  const okItem = Array.isArray(data)
    ? data.find((x) => x?.source?.type === 'buy_gift' && x?.userGift?.isMine === true && String(x?.userGift?.id || '') === String(id))
    : null;

  if (!okItem) return { ok: false, reason: 'BUY_NOT_CONFIRMED', data };
  return { ok: true, okItem };
}

function isNoFundsError(obj) {
  const s = JSON.stringify(obj?.data || obj || '').toLowerCase();
  return s.includes('not enough') || s.includes('insufficient') || s.includes('balance');
}

// Global cheapest scan
async function mrktGlobalCheapestLotsReal() {
  const now = nowMs();
  if (globalCheapestCache.lots.length && now - globalCheapestCache.time < GLOBAL_SCAN_CACHE_TTL_MS) {
    return { ok: true, lots: globalCheapestCache.lots, note: globalCheapestCache.note || null };
  }

  const cols = await mrktGetCollections();
  const sorted = cols
    .map((c) => ({ ...c, floorTon: c.floorNano != null ? tonFromNano(c.floorNano) : null }))
    .filter((c) => c.floorTon != null)
    .sort((a, b) => a.floorTon - b.floorTon)
    .slice(0, Math.max(2, GLOBAL_SCAN_COLLECTIONS));

  const lots = [];
  for (const c of sorted) {
    const r = await mrktFetchSalingPage({
      collectionNames: [c.name],
      modelNames: [],
      backdropNames: [],
      cursor: '',
      ordering: 'Price',
      lowToHigh: true,
      count: 5,
    });
    if (r.ok && r.gifts?.length) {
      const lot = salingGiftToLot(r.gifts[0]);
      if (lot) lots.push(lot);
    }
  }

  lots.sort((a, b) => a.priceTon - b.priceTon);
  const out = lots.slice(0, WEBAPP_LOTS_LIMIT);

  globalCheapestCache.time = now;
  globalCheapestCache.lots = out;
  globalCheapestCache.note = 'Глобальный список ограничен (защита от 429).';

  return { ok: true, lots: out, note: globalCheapestCache.note };
}

// ===================== Notifications (SUBS) =====================
// По твоей просьбе:
// - убрать строку с названием/номером
// - оставить Price/Model/Backdrop + t.me/nft ссылку
// - кнопка только MRKT
async function notifyFloorToUser(userId, sub, lot, newFloor) {
  const chatId = userChatId(userId);
  const lines = [];
  lines.push(`Подписка #${sub.num}`);
  lines.push(`Цена: ${newFloor.toFixed(3)} TON`);
  if (lot?.model) lines.push(`Model: ${lot.model}`);
  if (lot?.backdrop) lines.push(`Backdrop: ${lot.backdrop}`);
  lines.push(lot?.urlTelegram || 'https://t.me/mrkt');

  const reply_markup = lot?.urlMarket ? { inline_keyboard: [[{ text: 'MRKT', url: lot.urlMarket }]] } : undefined;
  await sendMessageSafe(chatId, lines.join('\n'), { disable_web_page_preview: false, reply_markup });
}

// ===================== Subs worker =====================
let isSubsChecking = false;

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

        const sf = normalizeFilters(sub.filters || {});
        if (!sf.gifts.length) continue;

        const stateKey = `${userId}:${sub.id}`;
        const prev = subStates.get(stateKey) || { floor: null, emptyStreak: 0, pausedUntil: 0 };

        const r = await mrktSearchLotsByFilters(sf, 1);
        if (!r.ok) continue;

        const lot = r.gifts?.length ? r.gifts[0] : null;
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

        if (canNotify && lot && (prevFloor == null || Number(prevFloor) !== Number(newFloor))) {
          await notifyFloorToUser(userId, sub, lot, newFloor);
          floorNotifs++;
        }

        subStates.set(stateKey, { ...prev, floor: newFloor, emptyStreak });
      }
    }

    return { processedSubs, floorNotifs };
  } catch (e) {
    console.error('subs error:', e);
    return { processedSubs: 0, floorNotifs: 0 };
  } finally {
    isSubsChecking = false;
  }
}

// ===================== AutoBuy =====================
let isAutoBuying = false;

async function autoBuyCycle() {
  if (!AUTO_BUY_GLOBAL) return;
  if (MODE !== 'real') return;
  if (!MRKT_AUTH_RUNTIME) return;
  if (isAutoBuying) return;

  isAutoBuying = true;
  try {
    for (const [userId, user] of users.entries()) {
      if (!user.enabled) continue;

      const subs = Array.isArray(user.subscriptions) ? user.subscriptions : [];
      const eligible = subs.filter((s) => s && s.enabled && s.autoBuyEnabled && s.maxAutoBuyTon != null && (s.filters?.gifts?.length || s.filters?.gift));
      if (!eligible.length) continue;

      let buysDone = 0;

      for (const sub of eligible) {
        if (buysDone >= AUTO_BUY_MAX_BUYS_PER_USER_PER_CYCLE) break;

        const maxBuy = Number(sub.maxAutoBuyTon);
        if (!Number.isFinite(maxBuy) || maxBuy <= 0) continue;

        const sf = normalizeFilters(sub.filters || {});
        if (!sf.gifts.length) continue;

        const foundAt = Date.now();
        const r = await mrktSearchLotsByFilters(sf, 1);
        if (!r.ok || !r.gifts?.length) continue;

        let candidate = null;
        for (const lot of r.gifts.slice(0, 30)) {
          if (lot.priceTon <= maxBuy) { candidate = lot; break; }
        }
        if (!candidate) continue;

        const attemptKey = `${userId}:${candidate.id}`;
        const lastAttempt = autoBuyRecentAttempts.get(attemptKey);
        if (lastAttempt && nowMs() - lastAttempt < AUTO_BUY_ATTEMPT_TTL_MS) continue;
        autoBuyRecentAttempts.set(attemptKey, nowMs());

        const chatId = userChatId(userId);

        if (AUTO_BUY_DRY_RUN) {
          await sendMessageSafe(
            chatId,
            `AutoBuy (DRY)\nПодписка #${sub.num}\nЦена: ${candidate.priceTon.toFixed(3)} TON\n${candidate.urlTelegram}`,
            { disable_web_page_preview: false, reply_markup: { inline_keyboard: [[{ text: 'MRKT', url: candidate.urlMarket }]] } }
          );
          buysDone++;
          continue;
        }

        const buyStart = Date.now();
        const buyRes = await mrktBuy({ id: candidate.id, priceNano: candidate.priceNano });
        const boughtAt = Date.now();
        const latencyMs = boughtAt - buyStart;

        if (buyRes.ok) {
          pushPurchase(getOrCreateUser(userId), {
            tsFound: foundAt,
            tsBought: boughtAt,
            latencyMs,
            title: candidate.name,
            priceTon: candidate.priceTon,
            urlTelegram: candidate.urlTelegram,
            urlMarket: candidate.urlMarket,
            lotId: candidate.id,
            giftName: candidate.giftName || '',
            model: candidate.model || '',
            backdrop: candidate.backdrop || '',
            collection: candidate.collectionName || '',
          });
          scheduleSave();

          await sendMessageSafe(
            chatId,
            `✅ AutoBuy OK\n${candidate.priceTon.toFixed(3)} TON\n${candidate.urlTelegram}\nLatency: ${latencyMs}ms`,
            { disable_web_page_preview: false, reply_markup: { inline_keyboard: [[{ text: 'MRKT', url: candidate.urlMarket }]] } }
          );

          buysDone++;
          if (AUTO_BUY_DISABLE_AFTER_SUCCESS) {
            sub.autoBuyEnabled = false;
            scheduleSave();
          }
        } else {
          if (isNoFundsError(buyRes)) {
            for (const s2 of subs) if (s2) s2.autoBuyEnabled = false;
            scheduleSave();
            await sendMessageSafe(chatId, `❌ AutoBuy STOP: no funds`, { disable_web_page_preview: true });
          } else {
            await sendMessageSafe(chatId, `❌ AutoBuy FAIL: ${buyRes.reason}`, { disable_web_page_preview: true });
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
    const sess = mrktSessionRuntime || (await loadMrktSessionFromRedis());
    const txt =
      `Status\n` +
      `MRKT_AUTH: ${MRKT_AUTH_RUNTIME ? 'YES' : 'NO'} (${maskToken(MRKT_AUTH_RUNTIME) || '-'})\n` +
      `MRKT session: ${sess ? 'YES' : 'NO'}\n` +
      `MRKT last fail: ${mrktState.lastFailMsg || '-'}\n` +
      `pauseUntil: ${mrktState.pauseUntil ? new Date(mrktState.pauseUntil).toLocaleTimeString('ru-RU') : '-'}\n` +
      `Redis: ${redis ? 'YES' : 'NO'}\n` +
      `AutoBuy: ${AUTO_BUY_GLOBAL ? 'ON' : 'OFF'}${AUTO_BUY_DRY_RUN ? ' (DRY)' : ''}\n`;
    await sendMessageSafe(msg.chat.id, txt, { reply_markup: MAIN_KEYBOARD });
  }
});

// ===================== Web server + API =====================
const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '4mb' }));

app.get('/health', (req, res) => res.status(200).send('ok'));

function auth(req, res, next) {
  const initData = String(req.headers['x-tg-init-data'] || '');
  const v = verifyTelegramWebAppInitData(initData);
  if (!v.ok) return res.status(401).json({ ok: false, reason: v.reason });
  req.userId = v.userId;
  req.tgUser = v.user || null;
  next();
}

// Proxy images
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

// FAST state (no MRKT calls!)
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
      autoBuy: { global: AUTO_BUY_GLOBAL, dry: AUTO_BUY_DRY_RUN },
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
    const next = normalizeFilters(b.filters);
    // keep giftLabels only for selected gifts
    const labels = (next.giftLabels && typeof next.giftLabels === 'object') ? next.giftLabels : {};
    const cleanLabels = {};
    for (const g of next.gifts) cleanLabels[g] = labels[g] || g;
    next.giftLabels = cleanLabels;
    u.filters = next;
  }
  scheduleSave();
  res.json({ ok: true });
});

// collections + suggest
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
      }));
    return res.json({ ok: true, items });
  }

  if (kind === 'backdrop') {
    if (!gift) return res.json({ ok: true, items: [] });
    const backs = await mrktGetBackdropsForGift(gift);
    const items = backs
      .filter((b) => !q || b.name.toLowerCase().includes(q))
      .slice(0, WEBAPP_SUGGEST_LIMIT)
      .map((b) => ({ label: b.name, value: b.name, colorHex: b.centerHex || '#000000' }));
    return res.json({ ok: true, items });
  }

  res.status(400).json({ ok: false, reason: 'BAD_KIND' });
});

// market lots (cached)
app.get('/api/mrkt/lots', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const f = normalizeFilters(u.filters || {});
  const cacheKey = `lots|${req.userId}|${JSON.stringify(f)}`;
  const cached = lotsCache.get(cacheKey);
  if (cached && nowMs() - cached.time < LOTS_CACHE_TTL_MS) return res.json(cached.data);

  if (!f.gifts.length) {
    const r = await mrktGlobalCheapestLotsReal().catch(() => ({ ok: false, lots: [], note: 'global scan error' }));
    const lots = (r.lots || []).slice(0, WEBAPP_LOTS_LIMIT).map((lot) => ({
      ...lot,
      imgUrl: lot.giftName ? `/img/gift?name=${encodeURIComponent(lot.giftName)}` : null,
      listedMsk: lot.listedAt ? new Date(Date.parse(lot.listedAt)).toLocaleString('ru-RU', { timeZone: 'Europe/Moscow' }) : null,
    }));
    const payload = { ok: true, lots, note: r.note || null };
    lotsCache.set(cacheKey, { time: nowMs(), data: payload });
    return res.json(payload);
  }

  const r = await mrktSearchLotsByFilters(f, WEBAPP_LOTS_PAGES || MRKT_PAGES);
  if (!r.ok) return res.json({ ok: false, reason: r.reason, lots: [] });

  const lots = (r.gifts || []).slice(0, WEBAPP_LOTS_LIMIT).map((lot) => ({
    ...lot,
    imgUrl: lot.giftName ? `/img/gift?name=${encodeURIComponent(lot.giftName)}` : null,
    listedMsk: lot.listedAt ? new Date(Date.parse(lot.listedAt)).toLocaleString('ru-RU', { timeZone: 'Europe/Moscow' }) : null,
  }));

  const payload = { ok: true, lots, note: null };
  lotsCache.set(cacheKey, { time: nowMs(), data: payload });
  res.json(payload);
});

// max offer for current filters (exact + collection) — on demand, cached
app.get('/api/mrkt/maxoffer_current', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const f = normalizeFilters(u.filters || {});
  if (!f.gifts.length) return res.json({ ok: true, exactMaxOfferTon: null, collectionMaxOfferTon: null, note: 'Выбери gift' });
  if (f.gifts.length !== 1) return res.json({ ok: true, exactMaxOfferTon: null, collectionMaxOfferTon: null, note: 'Max offer работает для 1 gift' });

  const gift = f.gifts[0];
  const model = (f.models || []).length === 1 ? f.models[0] : null;
  const backdrop = (f.backdrops || []).length === 1 ? f.backdrops[0] : null;

  const oExact = await mrktOrdersFetch({ gift, model, backdrop });
  const oCol = await mrktOrdersFetch({ gift, model: null, backdrop: null });

  const exact = oExact.ok ? maxOfferTonFromOrders(oExact.orders) : null;
  const col = oCol.ok ? maxOfferTonFromOrders(oCol.orders) : null;

  res.json({ ok: true, exactMaxOfferTon: exact, collectionMaxOfferTon: col, note: null });
});

// lot details
app.post('/api/lot/details', auth, async (req, res) => {
  const lot = req.body?.lot || null;
  const lotId = String(lot?.id || '').trim();
  if (!lotId) return res.status(400).json({ ok: false, reason: 'NO_LOT' });

  const cached = detailsCache.get(lotId);
  if (cached && nowMs() - cached.time < DETAILS_CACHE_TTL_MS) return res.json(cached.data);

  const gift = String(lot?.collectionName || '').trim();
  const model = lot?.model ? String(lot.model) : null;
  const backdrop = lot?.backdrop ? String(lot.backdrop) : null;

  let floorExact = null;
  let floorCollection = null;
  let offerExact = null;
  let offerCollection = null;

  if (gift) {
    const exact = await mrktSearchLotsByFilters({ gifts: [gift], models: model ? [model] : [], backdrops: backdrop ? [backdrop] : [], numberPrefix: '' }, 1);
    if (exact.ok && exact.gifts?.length) floorExact = exact.gifts[0].priceTon;

    const col = await mrktSearchLotsByFilters({ gifts: [gift], models: [], backdrops: [], numberPrefix: '' }, 1);
    if (col.ok && col.gifts?.length) floorCollection = col.gifts[0].priceTon;

    const o1 = await mrktOrdersFetch({ gift, model, backdrop });
    if (o1.ok) offerExact = maxOfferTonFromOrders(o1.orders);

    const o2 = await mrktOrdersFetch({ gift, model: null, backdrop: null });
    if (o2.ok) offerCollection = maxOfferTonFromOrders(o2.orders);
  }

  const salesHistory = gift
    ? await mrktFeedSales({ gift, modelNames: model ? [model] : [], backdropNames: backdrop ? [backdrop] : [] })
    : { ok: true, approxPriceTon: null, sales: [], note: 'no gift' };

  const payload = { ok: true, floors: { exact: floorExact, collection: floorCollection }, offers: { exact: offerExact, collection: offerCollection }, salesHistory };
  detailsCache.set(lotId, { time: nowMs(), data: payload });
  res.json(payload);
});

// buy
app.post('/api/mrkt/buy', auth, async (req, res) => {
  const id = String(req.body?.id || '').trim();
  const priceNano = Number(req.body?.priceNano);
  const lot = req.body?.lot || null;

  if (!id) return res.status(400).json({ ok: false, reason: 'NO_ID' });
  if (!Number.isFinite(priceNano) || priceNano <= 0) return res.status(400).json({ ok: false, reason: 'BAD_PRICE' });

  const foundAt = Date.now();
  const buyStart = Date.now();
  const r = await mrktBuy({ id, priceNano });
  const boughtAt = Date.now();
  const latencyMs = boughtAt - buyStart;

  if (!r.ok) return res.status(502).json({ ok: false, reason: r.reason });

  const u = getOrCreateUser(req.userId);
  pushPurchase(u, {
    tsListed: lot?.listedAt ? Date.parse(lot.listedAt) : null,
    tsFound: foundAt,
    tsBought: boughtAt,
    latencyMs,
    title: lot?.name || 'Gift',
    priceTon: priceNano / 1e9,
    urlTelegram: lot?.urlTelegram || '',
    urlMarket: lot?.urlMarket || mrktLotUrlFromId(id),
    lotId: id,
    giftName: lot?.giftName || '',
    model: lot?.model || '',
    backdrop: lot?.backdrop || '',
    collection: lot?.collectionName || '',
  });
  scheduleSave();

  res.json({ ok: true, title: lot?.name || 'Gift', priceTon: priceNano / 1e9 });
});

// profile
app.get('/api/profile', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const purchases = (u.purchases || []).slice(0, 80).map((p) => ({
    title: p.title,
    lotId: p.lotId || null,
    priceTon: p.priceTon,
    listedMsk: p.tsListed ? new Date(p.tsListed).toLocaleString('ru-RU', { timeZone: 'Europe/Moscow' }) : null,
    foundMsk: p.tsFound ? new Date(p.tsFound).toLocaleString('ru-RU', { timeZone: 'Europe/Moscow' }) : null,
    boughtMsk: p.tsBought ? new Date(p.tsBought).toLocaleString('ru-RU', { timeZone: 'Europe/Moscow' }) : null,
    latencyMs: p.latencyMs ?? null,
    imgUrl: p.giftName ? `/img/gift?name=${encodeURIComponent(p.giftName)}` : null,
    urlTelegram: p.urlTelegram || null,
    urlMarket: p.urlMarket || null,
    model: p.model || null,
    backdrop: p.backdrop || null,
  }));
  res.json({ ok: true, user: req.tgUser, purchases });
});

// subs endpoints (fast)
app.post('/api/sub/create', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const r = makeSubFromCurrentFilters(u);
  if (!r.ok) return res.status(400).json({ ok: false, reason: r.reason });
  u.subscriptions.push(r.sub);
  renumberSubs(u);
  scheduleSave();
  res.json({ ok: true });
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

app.post('/api/sub/details', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const s = findSub(u, String(req.body?.id || ''));
  if (!s) return res.status(404).json({ ok: false, reason: 'NOT_FOUND' });

  const sf = normalizeFilters(s.filters || {});
  const gift = sf.gifts.length === 1 ? sf.gifts[0] : null;
  const model = gift && sf.models.length === 1 ? sf.models[0] : null;
  const backdrop = gift && sf.backdrops.length === 1 ? sf.backdrops[0] : null;

  let floorExact = null;
  let floorCollection = null;
  let offerExact = null;
  let offerCollection = null;

  if (gift) {
    const ex = await mrktSearchLotsByFilters({ gifts: [gift], models: sf.models, backdrops: sf.backdrops, numberPrefix: '' }, 1);
    if (ex.ok && ex.gifts?.length) floorExact = ex.gifts[0].priceTon;

    const col = await mrktSearchLotsByFilters({ gifts: [gift], models: [], backdrops: [], numberPrefix: '' }, 1);
    if (col.ok && col.gifts?.length) floorCollection = col.gifts[0].priceTon;

    const o1 = await mrktOrdersFetch({ gift, model, backdrop });
    if (o1.ok) offerExact = maxOfferTonFromOrders(o1.orders);

    const o2 = await mrktOrdersFetch({ gift, model: null, backdrop: null });
    if (o2.ok) offerCollection = maxOfferTonFromOrders(o2.orders);
  }

  const salesHistory = gift
    ? await mrktFeedSales({ gift, modelNames: sf.models, backdropNames: sf.backdrops })
    : { ok: true, approxPriceTon: null, sales: [], note: 'multi-gift' };

  res.json({ ok: true, sub: s, floors: { exact: floorExact, collection: floorCollection }, offers: { exact: offerExact, collection: offerCollection }, salesHistory });
});

// ========== ADMIN ==========
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

  const payloadJson = req.body?.payloadJson;
  let data = null;
  let photo = null;

  if (typeof payloadJson === 'string' && payloadJson.trim()) {
    const j = tryParseJsonMaybe(payloadJson);
    if (!j || typeof j !== 'object') return res.status(400).json({ ok: false, reason: 'BAD_PAYLOAD_JSON' });
    data = typeof j.data === 'string' ? j.data : null;
    photo = (j.photo === undefined) ? null : j.photo; // keep object/null/string
  } else {
    data = typeof req.body?.data === 'string' ? req.body.data : null;
    const rawPhoto = req.body?.photo;
    if (rawPhoto == null || rawPhoto === '') photo = null;
    else if (typeof rawPhoto === 'string') {
      const pj = tryParseJsonMaybe(rawPhoto);
      photo = pj != null ? pj : rawPhoto;
    } else {
      photo = rawPhoto;
    }
  }

  if (!data || !String(data).includes('hash=')) return res.status(400).json({ ok: false, reason: 'BAD_DATA_NO_HASH' });

  const sess = { data: String(data), photo };
  mrktSessionRuntime = sess;
  if (redis) await saveMrktSessionToRedis(sess);

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

// ===================== Telegram webhook/polling =====================
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

setInterval(() => { checkSubscriptionsForAllUsers().catch((e) => console.error('subs error:', e)); }, SUBS_CHECK_INTERVAL_MS);
setInterval(() => { autoBuyCycle().catch((e) => console.error('autobuy error:', e)); }, AUTO_BUY_CHECK_INTERVAL_MS);

(async () => {
  if (REDIS_URL) {
    await initRedis();
    if (redis) {
      const t = await redisGet(REDIS_KEY_MRKT_AUTH);
      if (t && String(t).trim()) MRKT_AUTH_RUNTIME = String(t).trim();

      const sess = await loadMrktSessionFromRedis();
      if (sess) mrktSessionRuntime = sess;

      const raw = await redisGet(REDIS_KEY_STATE);
      if (raw) { try { importState(JSON.parse(raw)); } catch {} }
    }
  } else {
    console.warn('REDIS_URL not set => state/session/token won’t persist after restart');
  }
  console.log('Bot started. /start');
})();

// ===================== WebApp HTML/JS =====================
const WEBAPP_HTML = `<!doctype html>
<html lang="ru"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"/>
<title>${APP_TITLE}</title>
<style>
:root{--bg:#0b0f14;--card:#101826;--text:#e5e7eb;--muted:#9ca3af;--border:#223044;--input:#0f172a;--btn:#182235;--btnText:#e5e7eb;--accent:#22c55e;--danger:#ef4444}
*{box-sizing:border-box}
body{margin:0;padding:14px;background:var(--bg);color:var(--text);font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial}
h2{margin:0 0 10px 0;font-size:18px}
.card{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:12px;margin:10px 0}
label{display:block;font-size:12px;color:var(--muted);margin-bottom:4px}
.row{display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end}
.field{position:relative;flex:1 1 200px;min-width:140px}
.inpWrap{position:relative}
input,textarea{width:100%;padding:10px;border:1px solid var(--border);border-radius:12px;background:var(--input);color:var(--text);outline:none;font-size:13px}
button{padding:10px 12px;border:1px solid var(--border);border-radius:12px;background:var(--btn);color:var(--btnText);cursor:pointer}
.primary{border-color:var(--accent);color:#052e16;background:var(--accent);font-weight:950}
.small{padding:8px 10px;border-radius:10px;font-size:13px}
.xbtn{position:absolute;right:8px;top:50%;transform:translateY(-50%);width:20px;height:20px;border-radius:8px;border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.05);color:rgba(255,255,255,.65);cursor:pointer;display:flex;align-items:center;justify-content:center}
.hr{height:1px;background:var(--border);margin:10px 0}
.muted{color:var(--muted);font-size:12px}
#err{display:none;border-color:var(--danger);color:#ffd1d1;white-space:pre-wrap;word-break:break-word}
.tabs{display:flex;gap:8px;flex-wrap:wrap}
.tabbtn{border-radius:999px;padding:8px 12px;font-size:13px}
.tabbtn.active{border-color:var(--accent);color:var(--accent)}
.sug{border:1px solid var(--border);border-radius:14px;overflow:auto;background:var(--card);max-height:380px;position:absolute;top:calc(100% + 6px);left:0;right:0;z-index:1000000;box-shadow:0 14px 40px rgba(0,0,0,.45)}
.field.open{z-index:999999}
.sugHead{display:flex;justify-content:space-between;align-items:center;padding:8px 10px;border-bottom:1px solid var(--border);position:sticky;top:0;background:var(--card);z-index:2}
.sug .item{width:100%;text-align:left;border:0;background:transparent;padding:10px;display:flex;gap:10px;align-items:center}
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

/* bottom sheet */
.sheetWrap{position:fixed;inset:0;background:rgba(0,0,0,.35);display:flex;align-items:flex-end;justify-content:center;z-index:50000;padding:10px;opacity:0;visibility:hidden;pointer-events:none;transition:opacity .18s ease,visibility .18s ease}
.sheetWrap.show{opacity:1;visibility:visible;pointer-events:auto}
.sheet{width:min(980px,96vw);height:min(80vh,860px);background:var(--card);border:1px solid var(--border);border-radius:22px 22px 14px 14px;padding:12px;box-shadow:0 30px 90px rgba(0,0,0,.5);display:flex;flex-direction:column;gap:10px;transform:translateY(20px);transition:transform .18s ease}
.sheetWrap.show .sheet{transform:translateY(0)}
.handle{width:44px;height:5px;border-radius:999px;background:rgba(255,255,255,.18);align-self:center}
.sheetHeader{display:flex;justify-content:space-between;gap:12px;align-items:flex-start}
.sheetBody{overflow:auto}
.sheetImg{width:100%;max-height:260px;object-fit:cover;border-radius:14px;border:1px solid var(--border);background:rgba(255,255,255,.03)}
.kv{display:flex;gap:10px;flex-wrap:wrap}
.kv .p{padding:6px 10px;border:1px solid var(--border);border-radius:999px;font-size:12px;color:var(--muted)}
.sale{border:1px solid var(--border);border-radius:12px;padding:10px;background:rgba(255,255,255,.02)}
.purchRow{display:flex;gap:10px;align-items:center}
.purchRow img{width:52px;height:52px;border-radius:14px;border:1px solid var(--border);object-fit:cover;background:rgba(255,255,255,.03);flex:0 0 auto}
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
    <button id="toProfile" class="small">Покупки</button>
  </div>

  <div id="lotsLoading" class="loaderLine"><div class="spinner"></div><div>Загрузка лотов…</div></div>

  <div id="status" class="muted" style="margin-top:10px"></div>
  <div class="hr"></div>
  <div><b>Лоты</b> <span class="muted">(клик → меню)</span></div>
  <div id="lots" class="grid"></div>
</div>

<div id="subs" class="card" style="display:none">
  <h3 style="margin:0 0 8px 0;font-size:15px">Подписки</h3>
  <div class="row">
    <button id="subCreate">Создать из текущих фильтров</button>
    <button id="subRefresh">Обновить</button>
  </div>
  <div id="subsLoading" class="loaderLine"><div class="spinner"></div><div>Загрузка подписок…</div></div>
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
  <div class="muted">Вставь <b>полный JSON Payload</b> из Network запроса MRKT <code>/api/v1/auth</code> (POST). После Save будет автоматический refresh токена.</div>
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
    <div class="kv" id="sheetKV"></div>
    <div class="row" id="sheetBtns"></div>

    <div class="hr"></div>
    <div><b>История продаж</b></div>
    <div id="sheetSales" class="sheetBody"></div>
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

  if (!initData) { document.body.innerHTML = '<div style="padding:16px;font-family:system-ui;color:#fff;background:#0b0f14"><h3>Открой панель из Telegram</h3></div>'; return; }

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

  function setTab(name){
    ['market','subs','profile','admin'].forEach(x => el(x).style.display = (x===name?'block':'none'));
    document.querySelectorAll('.tabbtn').forEach(b => b.classList.toggle('active', b.dataset.tab===name));
  }
  document.querySelectorAll('.tabbtn').forEach(b => b.onclick = async () => {
    setTab(b.dataset.tab);
    if (b.dataset.tab === 'profile') await refreshProfile().catch(()=>{});
    if (b.dataset.tab === 'admin') await refreshAdmin().catch(()=>{});
  });

  // z-index control
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
    if(!items||!items.length){ hideSug(id); return; }

    const head =
      '<div class="sugHead">' +
        '<b>'+title+'</b>' +
        '<span class="muted">тап = добавить/убрать</span>' +
      '</div>';

    b.innerHTML = head + items.map(x => {
      const sel = isSelected(x.value) ? '✅ ' : '';
      const thumb = x.imgUrl
        ? '<img class="thumb" src="'+x.imgUrl+'" referrerpolicy="no-referrer"/>'
        : (x.colorHex
            ? '<div class="thumb color"><div class="colorFill" style="background:'+x.colorHex+'"></div></div>'
            : '<div class="thumb"></div>'
          );

      return '<button type="button" class="item" data-v="'+String(x.value).replace(/"/g,'&quot;')+'">'+thumb+
        '<div style="min-width:0;flex:1">' +
          '<div style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap"><b>'+sel+x.label+'</b></div>' +
          (x.sub?'<div class="muted" style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'+x.sub+'</div>':'')+
        '</div></button>';
    }).join('');

    b.style.display='block';
    b.onclick = (e) => {
      const btn = e.target.closest('button[data-v]');
      if(!btn) return;
      onToggle(btn.getAttribute('data-v'));
    };
  }

  // selection
  let sel = { gifts: [], giftLabels: {}, models: [], backdrops: [], numberPrefix: '' };
  function giftsInputText(){
    if (!sel.gifts.length) return '';
    const labels = sel.gifts.map(v => sel.giftLabels[v] || v);
    return labels.join(', ');
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

  // debounce helpers
  const timers = { gift: null, model: null, backdrop: null };
  function debounce(kind, fn, ms=250){
    clearTimeout(timers[kind]);
    timers[kind] = setTimeout(fn, ms);
  }

  async function showGiftSug(){
    openField('giftField');
    const q = el('gift').value.trim();
    const r = await api('/api/mrkt/collections?q='+encodeURIComponent(q));
    const items = r.items || [];
    renderSug('giftSug', 'Gift', items,
      (v)=> isSelectedNorm(sel.gifts, v),
      (v)=>{
        const next = toggleIn(sel.gifts, v);
        if (next.length !== 1) { sel.models = []; sel.backdrops = []; }
        sel.gifts = next;

        const lbl = (r.mapLabel||{})[v] || v;
        sel.giftLabels[v] = lbl;

        el('gift').value = giftsInputText();
        el('model').value = listInputText(sel.models);
        el('backdrop').value = listInputText(sel.backdrops);
      }
    );
  }

  async function showModelSug(){
    openField('modelField');
    if (sel.gifts.length !== 1) { hideSug('modelSug'); return; }
    const gift = sel.gifts[0];
    const q = el('model').value.trim();
    const r = await api('/api/mrkt/suggest?kind=model&gift='+encodeURIComponent(gift)+'&q='+encodeURIComponent(q));
    const items = r.items || [];
    renderSug('modelSug', 'Model', items,
      (v)=> isSelectedNorm(sel.models, v),
      (v)=>{
        sel.models = toggleIn(sel.models, v);
        el('model').value = listInputText(sel.models);
      }
    );
  }

  async function showBackdropSug(){
    openField('backdropField');
    if (sel.gifts.length !== 1) { hideSug('backdropSug'); return; }
    const gift = sel.gifts[0];
    const q = el('backdrop').value.trim();
    const r = await api('/api/mrkt/suggest?kind=backdrop&gift='+encodeURIComponent(gift)+'&q='+encodeURIComponent(q));
    const items = r.items || [];
    renderSug('backdropSug', 'Backdrop', items,
      (v)=> isSelectedNorm(sel.backdrops, v),
      (v)=>{
        sel.backdrops = toggleIn(sel.backdrops, v);
        el('backdrop').value = listInputText(sel.backdrops);
      }
    );
  }

  // bind inputs with debounce (less lag)
  el('gift').addEventListener('focus', wrap('lots', ()=>showGiftSug()));
  el('gift').addEventListener('click', wrap('lots', ()=>showGiftSug()));
  el('gift').addEventListener('input', ()=> debounce('gift', ()=>wrap('lots', ()=>showGiftSug())(), 220));

  el('model').addEventListener('focus', wrap('lots', ()=>showModelSug()));
  el('model').addEventListener('click', wrap('lots', ()=>showModelSug()));
  el('model').addEventListener('input', ()=> debounce('model', ()=>wrap('lots', ()=>showModelSug())(), 220));

  el('backdrop').addEventListener('focus', wrap('lots', ()=>showBackdropSug()));
  el('backdrop').addEventListener('click', wrap('lots', ()=>showBackdropSug()));
  el('backdrop').addEventListener('input', ()=> debounce('backdrop', ()=>wrap('lots', ()=>showBackdropSug())(), 220));

  document.addEventListener('click', (e) => {
    if(!e.target.closest('#gift,#model,#backdrop,.sug')) {
      hideSug('giftSug'); hideSug('modelSug'); hideSug('backdropSug');
      closeAllFields();
    }
  });

  // Clear buttons (X)
  document.body.addEventListener('click', (e) => {
    const b = e.target.closest('button[data-clear]');
    if(!b) return;
    const what = b.getAttribute('data-clear');
    wrap('lots', async()=>{
      if(what==='gift'){
        sel.gifts=[]; sel.giftLabels={}; sel.models=[]; sel.backdrops=[];
        el('gift').value=''; el('model').value=''; el('backdrop').value='';
      }
      if(what==='model'){ sel.models=[]; el('model').value=''; }
      if(what==='backdrop'){ sel.backdrops=[]; el('backdrop').value=''; }
      if(what==='number'){ el('number').value=''; }
      await patchFilters();
      await refreshAll();
    })();
  });

  // bottom sheet
  const sheetWrap = el('sheetWrap');
  const sheetTitle = el('sheetTitle');
  const sheetSub = el('sheetSub');
  const sheetTop = el('sheetTop');
  const sheetKV = el('sheetKV');
  const sheetBtns = el('sheetBtns');
  const sheetSales = el('sheetSales');
  const sheetImg = el('sheetImg');
  el('sheetClose').onclick = ()=>sheetWrap.classList.remove('show');
  sheetWrap.addEventListener('click',(e)=>{ if(e.target===sheetWrap) sheetWrap.classList.remove('show'); });

  function openSheet(title, sub){
    sheetTitle.textContent=title||'';
    sheetSub.textContent=sub||'';
    sheetTop.textContent='';
    sheetKV.innerHTML='';
    sheetBtns.innerHTML='';
    sheetSales.innerHTML='';
    sheetImg.style.display='none';
    sheetImg.src='';
    sheetWrap.classList.add('show');
  }
  function pill(txt){ return '<div class="p">'+txt+'</div>'; }

  function renderSales(resp){
    if(!resp || resp.ok===false){
      sheetSales.innerHTML = '<i class="muted">Нет данных</i>';
      return;
    }
    const approx = resp.approxPriceTon;
    const head = approx != null ? ('Примерная цена продажи: <b>'+approx.toFixed(3)+' TON</b>') : 'Примерная цена продажи: <b>нет данных</b>';
    let html = '<div class="muted" style="margin-bottom:10px">'+head+'</div>';

    const items = resp.sales || [];
    if(!items.length){
      html += '<i class="muted">Нет продаж</i>';
      sheetSales.innerHTML = html;
      return;
    }

    html += items.slice(0, 12).map(x=>{
      const dt = x.ts ? new Date(x.ts).toLocaleString('ru-RU',{timeZone:'Europe/Moscow'}) : '';
      const btns =
        '<div style="display:flex;gap:8px;margin-top:8px">' +
          '<button class="small" onclick="window.open(\\''+x.urlMarket+'\\',\\'_blank\\')">MRKT</button>' +
          '<button class="small" onclick="window.open(\\''+x.urlTelegram+'\\',\\'_blank\\')">NFT</button>' +
        '</div>';

      return '<div class="sale">'+
        '<div style="display:flex;justify-content:space-between;gap:10px"><b>'+x.priceTon.toFixed(3)+' TON</b><span class="muted">'+dt+'</span></div>'+
        (x.model?'<div class="muted">Model: '+x.model+'</div>':'')+
        (x.backdrop?'<div class="muted">Backdrop: '+x.backdrop+'</div>':'')+
        btns +
      '</div>';
    }).join('<div style="height:8px"></div>');

    sheetSales.innerHTML = html;
  }

  function renderLots(resp){
    const box=el('lots');
    if(resp.ok===false){ box.innerHTML='<div style="color:#ef4444"><b>'+resp.reason+'</b></div>'; return; }
    const lots=resp.lots||[];
    el('status').textContent = resp.note || '';

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

        openSheet('Лот', lot.name);

        if (lot.imgUrl) { sheetImg.style.display='block'; sheetImg.src = lot.imgUrl; sheetImg.referrerPolicy='no-referrer'; }

        sheetTop.innerHTML =
          '<div><b>Цена:</b> '+lot.priceTon.toFixed(3)+' TON</div>' +
          (lot.model?('<div class="muted">Model: '+lot.model+'</div>'):'') +
          (lot.backdrop?('<div class="muted">Backdrop: '+lot.backdrop+'</div>'):'') +
          (lot.listedMsk?('<div class="muted">Listed: '+lot.listedMsk+'</div>'):'') +
          '<div class="muted" style="margin-top:6px">ID: '+lot.id+'</div>';

        const det = await api('/api/lot/details', { method:'POST', body: JSON.stringify({ lot }) });

        sheetKV.innerHTML = [
          det?.offers?.exact!=null ? pill('Max offer (точно): <b>'+det.offers.exact.toFixed(3)+' TON</b>') : pill('Max offer (точно): —'),
          det?.offers?.collection!=null ? pill('Max offer (колл.): <b>'+det.offers.collection.toFixed(3)+' TON</b>') : pill('Max offer (колл.): —'),
          det?.floors?.exact!=null ? pill('Floor (точно): <b>'+det.floors.exact.toFixed(3)+' TON</b>') : pill('Floor (точно): —'),
          det?.floors?.collection!=null ? pill('Floor (колл.): <b>'+det.floors.collection.toFixed(3)+' TON</b>') : pill('Floor (колл.): —'),
        ].join('');

        sheetBtns.innerHTML = '';
        const mkBtn = (label, action, strong=false) => {
          const b = document.createElement('button');
          b.className = 'small';
          b.textContent = label;
          if (strong){
            b.style.borderColor='var(--accent)';
            b.style.background='var(--accent)';
            b.style.color='#052e16';
            b.style.fontWeight='900';
          }
          b.onclick = action;
          sheetBtns.appendChild(b);
        };

        mkBtn('Buy', async()=>{
          const ok = confirm('Купить?\\n'+lot.name+'\\nЦена: '+lot.priceTon.toFixed(3)+' TON');
          if(!ok) return;
          const r = await api('/api/mrkt/buy', { method:'POST', body: JSON.stringify({ id: lot.id, priceNano: lot.priceNano, lot }) });
          alert('Куплено: '+r.title+' за '+r.priceTon.toFixed(3)+' TON');
        }, true);

        mkBtn('MRKT', ()=> tg?.openTelegramLink ? tg.openTelegramLink(lot.urlMarket) : window.open(lot.urlMarket,'_blank'));
        mkBtn('NFT', ()=> tg?.openTelegramLink ? tg.openTelegramLink(lot.urlTelegram) : window.open(lot.urlTelegram,'_blank'));

        renderSales(det?.salesHistory);
      });
    });
  }

  async function refreshState(){
    const st = await api('/api/state');

    sel.gifts = st.user.filters.gifts || [];
    sel.giftLabels = st.user.filters.giftLabels || {};
    sel.models = st.user.filters.models || [];
    sel.backdrops = st.user.filters.backdrops || [];
    el('number').value = st.user.filters.numberPrefix || '';

    el('gift').value = (sel.gifts||[]).map(v=>sel.giftLabels[v]||v).join(', ');
    el('model').value = (sel.models||[]).join(', ');
    el('backdrop').value = (sel.backdrops||[]).join(', ');

    if(st.api.isAdmin) el('adminTabBtn').style.display='inline-block';

    // subs list
    const subs=st.user.subscriptions||[];
    const box=el('subsList');
    if(!subs.length){ box.innerHTML='<i class="muted">Подписок нет</i>'; }
    else{
      box.innerHTML = subs.map(s=>{
        const thumb = s.ui?.thumbKey ? '<img class="thumb" src="/img/cdn?key='+encodeURIComponent(s.ui.thumbKey)+'" referrerpolicy="no-referrer"/>' : '<div class="thumb"></div>';
        const sw = (s.ui?.swatches||[]).map(h => '<span style="display:inline-block;width:14px;height:14px;border-radius:999px;border:1px solid rgba(255,255,255,.18);background:'+h+'"></span>').join(' ');
        const gifts = (s.filters.gifts||[]).slice(0,2).join(', ');
        const more = (s.filters.gifts||[]).length>2 ? ' +' + ((s.filters.gifts||[]).length-2) : '';
        const notifyTxt = (s.maxNotifyTon==null)?'∞':String(s.maxNotifyTon);
        const buyTxt = (s.maxAutoBuyTon==null)?'-':String(s.maxAutoBuyTon);
        return '<div class="card">'+
          '<div style="display:flex;gap:10px;align-items:center">'+thumb+
            '<div style="min-width:0;flex:1"><b>#'+s.num+' '+(s.enabled?'ON':'OFF')+'</b>'+
              '<div class="muted ellipsis">'+gifts+more+'</div>'+
              (sw?('<div style="margin-top:6px;display:flex;gap:6px;flex-wrap:wrap">'+sw+'</div>'):'')+
            '</div>'+
            '<button class="small" data-act="subInfo" data-id="'+s.id+'">Info</button>'+
          '</div>'+
          '<div class="muted">Notify max: '+notifyTxt+' TON</div>'+
          '<div class="muted">AutoBuy: '+(s.autoBuyEnabled?'ON':'OFF')+' | Max: '+buyTxt+' TON</div>'+
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

  async function refreshMarketData(){
    const lots = await api('/api/mrkt/lots');
    renderLots(lots);
  }

  async function refreshAll(){
    await refreshState();
    await refreshMarketData();
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
            const img = p.imgUrl ? '<img src="'+p.imgUrl+'" referrerpolicy="no-referrer" loading="lazy"/>' : '<div class="thumb"></div>';
            const links =
              (p.urlMarket ? '<button class="small" onclick="window.open(\\''+p.urlMarket+'\\',\\'_blank\\')">MRKT</button>' : '') +
              (p.urlTelegram ? '<button class="small" onclick="window.open(\\''+p.urlTelegram+'\\',\\'_blank\\')">NFT</button>' : '');
            return '<div class="card">'+
              '<div class="purchRow">'+img+
                '<div style="min-width:0">'+
                  '<div class="ellipsis"><b>'+p.title+'</b></div>'+
                  (p.model?'<div class="muted ellipsis">Model: '+p.model+'</div>':'')+
                  (p.backdrop?'<div class="muted ellipsis">Backdrop: '+p.backdrop+'</div>':'')+
                '</div>'+
              '</div>'+
              '<div class="muted" style="margin-top:8px">Listed: '+(p.listedMsk||'-')+'</div>'+
              '<div class="muted">Found: '+(p.foundMsk||'-')+'</div>'+
              '<div class="muted">Buy: '+(p.boughtMsk||'-')+(p.latencyMs!=null?(' · Latency '+p.latencyMs+' ms'):'')+'</div>'+
              '<div class="muted">Price: '+Number(p.priceTon).toFixed(3)+' TON</div>'+
              (p.lotId?('<div class="muted">ID: '+p.lotId+'</div>'):'')+
              '<div class="row" style="margin-top:8px">'+links+'</div>'+
            '</div>';
          }).join('')
        : '<i class="muted">Покупок пока нет</i>';
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
      'lastFail: '+(r.mrktLastFail||'-')+'\\n'+
      'refresh lastReason: '+(r.refresh?.lastReason||'OK')+'\\n'+
      'refresh lastStatus: '+(r.refresh?.lastStatus||'-');
  }

  // Buttons
  el('apply').onclick = wrap('lots', async()=>{ await patchFilters(); await refreshAll(); });
  el('refresh').onclick = wrap('lots', async()=>{ await refreshAll(); });
  el('toProfile').onclick = async () => { setTab('profile'); await refreshProfile().catch(()=>{}); };

  // Subs buttons
  el('subCreate').onclick = wrap('subs', async()=>{ await api('/api/sub/create',{method:'POST'}); await refreshState(); });
  el('subRefresh').onclick = wrap('subs', async()=>{ await refreshState(); });

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

      if(act==='subInfo'){
        const r = await api('/api/sub/details',{method:'POST',body:JSON.stringify({id})});
        openSheet('Подписка', '#'+r.sub.num+' '+(r.sub.enabled?'ON':'OFF'));
        sheetTop.innerHTML =
          '<div class="muted">Gifts: '+(r.sub.filters.gifts||[]).join(', ')+'</div>' +
          ((r.sub.filters.models||[]).length?('<div class="muted">Models: '+r.sub.filters.models.join(', ')+'</div>'):'') +
          ((r.sub.filters.backdrops||[]).length?('<div class="muted">Backdrops: '+r.sub.filters.backdrops.join(', ')+'</div>'):'') +
          (r.sub.filters.numberPrefix?('<div class="muted">Number prefix: '+r.sub.filters.numberPrefix+'</div>'):'') +
          '<div class="muted" style="margin-top:6px">Notify max: '+(r.sub.maxNotifyTon==null?'∞':r.sub.maxNotifyTon)+' TON</div>'+
          '<div class="muted">AutoBuy max: '+(r.sub.maxAutoBuyTon==null?'-':r.sub.maxAutoBuyTon)+' TON</div>';

        sheetKV.innerHTML = [
          r?.offers?.exact!=null ? pill('Max offer (точно): <b>'+r.offers.exact.toFixed(3)+' TON</b>') : pill('Max offer (точно): —'),
          r?.offers?.collection!=null ? pill('Max offer (колл.): <b>'+r.offers.collection.toFixed(3)+' TON</b>') : pill('Max offer (колл.): —'),
          r?.floors?.exact!=null ? pill('Floor (точно): <b>'+r.floors.exact.toFixed(3)+' TON</b>') : pill('Floor (точно): —'),
          r?.floors?.collection!=null ? pill('Floor (колл.): <b>'+r.floors.collection.toFixed(3)+' TON</b>') : pill('Floor (колл.): —'),
        ].join('');

        renderSales(r.salesHistory);
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
    alert('Collections: '+r.collectionsCount+'\\nToken: '+(r.tokenMask||'-')+'\\nFail: '+(r.lastFail||'-'));
    await refreshAdmin();
  }));

  // initial load
  wrap('lots', async()=>{ await refreshAll(); })();
})();`;
