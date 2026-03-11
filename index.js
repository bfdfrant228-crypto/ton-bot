/**
 * v22-full-ui-rps-fix (2026-03-11)
 *
 * Single file: Telegram bot + Express + WebApp (MRKT)
 * Key changes:
 * - iOS-friendly: no white bounce flashes (but scrollbar color can't be changed on iPhone)
 * - Multi-select gifts works reliably (search query separated from selected display)
 * - Auto-retry after RPS wait for: lots, collections, suggest(model/backdrop)
 * - Thumbnails everywhere via MRKT CDN thumbKey (fast + stable)
 * - Market: purchases history by current filters (only purchases made via this bot/panel)
 * - Max offer collection/exact shown in lot sheet + sub info
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

let MRKT_AUTH_RUNTIME = (process.env.MRKT_AUTH || '').trim() || null;

// throttling
const MRKT_TIMEOUT_MS = Number(process.env.MRKT_TIMEOUT_MS || 12000);
const MRKT_MIN_GAP_MS = Number(process.env.MRKT_MIN_GAP_MS || 1200);
const MRKT_429_DEFAULT_PAUSE_MS = Number(process.env.MRKT_429_DEFAULT_PAUSE_MS || 4500);

// if gate requires waiting longer than this inside HTTP request => return fast note instead of sleeping
const MRKT_HTTP_MAX_WAIT_MS = Number(process.env.MRKT_HTTP_MAX_WAIT_MS || 800);

// limits
const MRKT_COUNT = Number(process.env.MRKT_COUNT || 12);
const MRKT_PAGES = Number(process.env.MRKT_PAGES || 1);
const MRKT_ORDERS_COUNT = Number(process.env.MRKT_ORDERS_COUNT || 25);

const WEBAPP_LOTS_LIMIT = Number(process.env.WEBAPP_LOTS_LIMIT || 24);
const WEBAPP_LOTS_PAGES = Number(process.env.WEBAPP_LOTS_PAGES || 1);
const WEBAPP_SUGGEST_LIMIT = Number(process.env.WEBAPP_SUGGEST_LIMIT || 120);

const GLOBAL_SCAN_COLLECTIONS = Number(process.env.GLOBAL_SCAN_COLLECTIONS || 2);
const GLOBAL_SCAN_CACHE_TTL_MS = Number(process.env.GLOBAL_SCAN_CACHE_TTL_MS || 120_000);

// caches
const CACHE_TTL_MS = Number(process.env.CACHE_TTL_MS || 5 * 60_000);
const OFFERS_CACHE_TTL_MS = Number(process.env.OFFERS_CACHE_TTL_MS || 20_000);
const LOTS_CACHE_TTL_MS = Number(process.env.LOTS_CACHE_TTL_MS || 2500);
const DETAILS_CACHE_TTL_MS = Number(process.env.DETAILS_CACHE_TTL_MS || 4000);
const SALES_CACHE_TTL_MS = Number(process.env.SALES_CACHE_TTL_MS || 30_000);

// subs
const SUBS_CHECK_INTERVAL_MS = Number(process.env.SUBS_CHECK_INTERVAL_MS || 30000);
const SUBS_MAX_NOTIFICATIONS_PER_CYCLE = Number(process.env.SUBS_MAX_NOTIFICATIONS_PER_CYCLE || 6);
const SUBS_EMPTY_CONFIRM = Number(process.env.SUBS_EMPTY_CONFIRM || 2);

// AutoBuy (keep OFF until stable)
const AUTO_BUY_GLOBAL = String(process.env.AUTO_BUY_GLOBAL || '0') === '1';
const AUTO_BUY_DRY_RUN = String(process.env.AUTO_BUY_DRY_RUN || '1') === '1';
const AUTO_BUY_CHECK_INTERVAL_MS = Number(process.env.AUTO_BUY_CHECK_INTERVAL_MS || 6000);
const AUTO_BUY_MAX_BUYS_PER_USER_PER_CYCLE = Number(process.env.AUTO_BUY_MAX_BUYS_PER_USER_PER_CYCLE || 1);
const AUTO_BUY_ATTEMPT_TTL_MS = Number(process.env.AUTO_BUY_ATTEMPT_TTL_MS || 60_000);
const AUTO_BUY_DISABLE_AFTER_SUCCESS = String(process.env.AUTO_BUY_DISABLE_AFTER_SUCCESS || '0') === '1';

// sales history
const SALES_HISTORY_TARGET = Number(process.env.SALES_HISTORY_TARGET || 18);
const SALES_HISTORY_MAX_PAGES = Number(process.env.SALES_HISTORY_MAX_PAGES || 10);
const SALES_HISTORY_COUNT_PER_PAGE = Number(process.env.SALES_HISTORY_COUNT_PER_PAGE || 50);
const SALES_HISTORY_THROTTLE_MS = Number(process.env.SALES_HISTORY_THROTTLE_MS || 220);
const SALES_HISTORY_TIME_BUDGET_MS = Number(process.env.SALES_HISTORY_TIME_BUDGET_MS || 9000);

const MRKT_AUTH_REFRESH_COOLDOWN_MS = Number(process.env.MRKT_AUTH_REFRESH_COOLDOWN_MS || 8000);

// Redis keys
const REDIS_KEY_STATE = 'bot:state:main';
const REDIS_KEY_MRKT_AUTH = 'mrkt:auth:token';
const REDIS_KEY_MRKT_SESSION = 'mrkt:session:admin'; // { data:string, photo:any }

console.log('v22-full-ui-rps-fix start', {
  MODE,
  PUBLIC_URL: !!PUBLIC_URL,
  WEBAPP_URL: !!WEBAPP_URL,
  ADMIN_USER_ID,
  REDIS: !!REDIS_URL,
  MRKT_AUTH_SET: !!MRKT_AUTH_RUNTIME,
  MRKT_MIN_GAP_MS,
  MRKT_HTTP_MAX_WAIT_MS,
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
function maskToken(t) {
  const s = String(t || '').trim();
  if (!s) return '';
  if (s.length <= 10) return s;
  return s.slice(0, 4) + '…' + s.slice(-4);
}
function mrktLotUrlFromId(id) { return id ? `https://t.me/mrkt/app?startapp=${String(id).replace(/-/g, '')}` : 'https://t.me/mrkt'; }

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
  const s = Math.max(1, Math.ceil(ms / 1000));
  return `${s}s`;
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
    ui: { thumbKey: null, swatches: [] },
  };
  return { ok: true, sub };
}

function pushPurchase(user, entry) {
  if (!user.purchases) user.purchases = [];
  user.purchases.unshift({
    tsFound: entry.tsFound || null,
    tsBought: entry.tsBought || Date.now(),
    latencyMs: entry.latencyMs || null,
    title: String(entry.title || ''),
    priceTon: Number(entry.priceTon || 0),
    urlTelegram: String(entry.urlTelegram || ''),
    urlMarket: String(entry.urlMarket || ''),
    lotId: String(entry.lotId || ''),
    collection: String(entry.collection || ''),
    model: String(entry.model || ''),
    backdrop: String(entry.backdrop || ''),
    number: entry.number == null ? null : Number(entry.number),
    thumbKey: entry.thumbKey || null,
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
      number: p.number == null ? null : Number(p.number),
      thumbKey: p.thumbKey || null,
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
async function saveMrktSessionToRedis(sess) {
  if (!redis) return;
  await redisSet(REDIS_KEY_MRKT_SESSION, JSON.stringify(sess), { EX: WEBAPP_AUTH_MAX_AGE_SEC });
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

    collectionsCache = { time: 0, items: [] };
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

// ===================== MRKT queue + non-blocking gate =====================
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

function setPauseFrom429(res) {
  const ra = res?.headers?.get?.('retry-after');
  const ms = ra ? (Number(ra) * 1000) : MRKT_429_DEFAULT_PAUSE_MS;
  const until = nowMs() + Math.max(1000, ms || MRKT_429_DEFAULT_PAUSE_MS);
  mrktState.pauseUntil = Math.max(mrktState.pauseUntil || 0, until);
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
      if (res.status === 429) setPauseFrom429(res);

      if ((res.status === 401 || res.status === 403) && retry) {
        const rr = await tryRefreshMrktToken(`HTTP_${res.status}:${path}`, { force: false });
        if (rr.ok) return mrktGetJson(path, { retry: false });
      }

      mrktState.lastFailMsg = extractMrktErrorMessage(txt) || `HTTP ${res.status}`;
      return { ok: false, status: res.status, data, text: txt, waitMs: Math.max(0, mrktState.pauseUntil - nowMs()) };
    }

    mrktState.lastFailMsg = null;
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
      if (res.status === 429) setPauseFrom429(res);

      if ((res.status === 401 || res.status === 403) && retry) {
        const rr = await tryRefreshMrktToken(`HTTP_${res.status}:${path}`, { force: false });
        if (rr.ok) return mrktPostJson(path, bodyObj, { retry: false });
      }

      mrktState.lastFailMsg = extractMrktErrorMessage(txt) || `HTTP ${res.status}`;
      return { ok: false, status: res.status, data, text: txt, waitMs: Math.max(0, mrktState.pauseUntil - nowMs()) };
    }

    mrktState.lastFailMsg = null;
    return { ok: true, status: res.status, data, text: txt };
  });
}

// ===================== MRKT caches =====================
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

function buildSalingBody({ count = 20, cursor = '', collectionNames = [], modelNames = [], backdropNames = [], ordering = 'Price', lowToHigh = true, number = null } = {}) {
  return {
    count, cursor,
    collectionNames, modelNames, backdropNames,
    symbolNames: [],
    ordering, lowToHigh,
    maxPrice: null, minPrice: null,
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
  if (!r.ok) return { ok: false, reason: r.status === 429 ? 'RPS_WAIT' : 'ERROR', waitMs: r.waitMs || 0, gifts: [], cursor: '' };

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

  // thumbnail keys (best-effort)
  const thumbKey =
    g.modelStickerThumbnailKey ||
    g.modelStickerKey ||
    g.stickerThumbnailKey ||
    null;

  const urlMarket = g.id ? mrktLotUrlFromId(g.id) : 'https://t.me/mrkt';
  const urlTelegram = (g.name && String(g.name).includes('-')) ? `https://t.me/nft/${g.name}` : 'https://t.me/mrkt';

  return {
    id: String(g.id || ''),
    name: displayName,
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

async function mrktSearchLotsByFilters(filters, pagesLimit) {
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
      ordering: exactNumber != null ? 'Latest' : (prefix ? 'Latest' : 'Price'),
      lowToHigh: exactNumber != null ? false : (prefix ? false : true),
      count: MRKT_COUNT,
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
  const out = r.ok
    ? { ok: true, orders: Array.isArray(r.data?.orders) ? r.data.orders : [], waitMs: 0 }
    : { ok: false, orders: [], waitMs: r.waitMs || 0 };

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
  const key = `sales|${gift}|m=${(modelNames||[]).join('|')}|b=${(backdropNames||[]).join('|')}`;
  const now = nowMs();
  const cached = salesCache.get(key);
  if (cached && now - cached.time < SALES_CACHE_TTL_MS) return cached.data;

  const data = await (async () => {
    if (!gift) return { ok: true, approxPriceTon: null, sales: [], note: 'no gift' };

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
      if (!r.ok) {
        if (r.status === 429) return { ok: true, approxPriceTon: null, sales: [], note: `RPS limit, wait ${fmtWaitMs(r.waitMs || 1000)}` };
        break;
      }

      const items = Array.isArray(r.data?.items) ? r.data.items : [];
      if (!items.length) break;

      for (const it of items) {
        const g = it?.gift;
        if (!g) continue;
        const amountNano = it.amount ?? g.salePriceWithoutFee ?? g.salePrice ?? null;
        const ton = amountNano != null ? Number(amountNano) / 1e9 : NaN;
        if (!Number.isFinite(ton) || ton <= 0) continue;

        prices.push(ton);
        sales.push({
          ts: it.date || null,
          priceTon: ton,
          urlMarket: g.id ? mrktLotUrlFromId(g.id) : 'https://t.me/mrkt',
          urlTelegram: (g.name && String(g.name).includes('-')) ? `https://t.me/nft/${g.name}` : 'https://t.me/mrkt',
          model: g.modelTitle || g.modelName || null,
          backdrop: g.backdropName || null,
        });

        if (sales.length >= SALES_HISTORY_TARGET) break;
      }

      cursor = r.data?.cursor || '';
      pages++;
      if (!cursor) break;

      if (SALES_HISTORY_THROTTLE_MS > 0) await sleep(SALES_HISTORY_THROTTLE_MS);
    }

    prices.sort((a, b) => a - b);
    return { ok: true, approxPriceTon: median(prices), sales, note: null };
  })();

  salesCache.set(key, { time: now, data });
  return data;
}

// ===================== Sub UI build =====================
async function buildSubUi(sub) {
  const f = normalizeFilters(sub.filters || {});
  const gift = f.gifts.length === 1 ? f.gifts[0] : null;
  if (!gift) return { thumbKey: null, swatches: [] };

  let thumbKey = null;

  // model thumb
  if (f.models.length === 1) {
    const models = await mrktGetModelsForGift(gift);
    const m = models.find((x) => normTraitName(x.name) === normTraitName(f.models[0]));
    if (m?.thumbKey) thumbKey = m.thumbKey;
  }

  // collection thumb fallback
  if (!thumbKey) {
    const cols = await mrktGetCollections();
    const c = cols.find((x) => x.name === gift);
    if (c?.thumbKey) thumbKey = c.thumbKey;
  }

  // swatches
  let swatches = [];
  if (f.backdrops.length) {
    const backs = await mrktGetBackdropsForGift(gift);
    const map = new Map(backs.map((b) => [normTraitName(b.name), b.centerHex]));
    swatches = f.backdrops.map((b) => map.get(normTraitName(b)) || null).filter(Boolean).slice(0, 8);
  }

  return { thumbKey: thumbKey || null, swatches };
}

// ===================== Sub notify =====================
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
  if (MODE !== 'real') return;
  if (isSubsChecking && !manual) return;

  isSubsChecking = true;
  try {
    let sent = 0;

    for (const [userId, user] of users.entries()) {
      if (!user.enabled) continue;

      const subs = Array.isArray(user.subscriptions) ? user.subscriptions : [];
      const active = subs.filter((s) => s && s.enabled);
      if (!active.length) continue;

      for (const sub of active) {
        if (sent >= SUBS_MAX_NOTIFICATIONS_PER_CYCLE) break;

        const sf = normalizeFilters(sub.filters || {});
        if (!sf.gifts.length) continue;

        const stateKey = `${userId}:${sub.id}`;
        const prev = subStates.get(stateKey) || { floor: null, emptyStreak: 0 };

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
          sent++;
        }

        subStates.set(stateKey, { ...prev, floor: newFloor, emptyStreak });
      }
    }
  } catch (e) {
    console.error('subs error:', e);
  } finally {
    isSubsChecking = false;
  }
}

// ===================== AutoBuy (disabled by vars) =====================
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
      const eligible = subs.filter((s) => s && s.enabled && s.autoBuyEnabled && s.maxAutoBuyTon != null);
      if (!eligible.length) continue;

      let buysDone = 0;
      for (const sub of eligible) {
        if (buysDone >= AUTO_BUY_MAX_BUYS_PER_USER_PER_CYCLE) break;

        const maxBuy = Number(sub.maxAutoBuyTon);
        if (!Number.isFinite(maxBuy) || maxBuy <= 0) continue;

        const sf = normalizeFilters(sub.filters || {});
        if (!sf.gifts.length) continue;

        const r = await mrktSearchLotsByFilters(sf, 1);
        if (!r.ok || !r.gifts?.length) continue;

        const candidate = r.gifts.find((x) => x.priceTon <= maxBuy) || null;
        if (!candidate) continue;

        const attemptKey = `${userId}:${candidate.id}`;
        const lastAttempt = autoBuyRecentAttempts.get(attemptKey);
        if (lastAttempt && nowMs() - lastAttempt < AUTO_BUY_ATTEMPT_TTL_MS) continue;
        autoBuyRecentAttempts.set(attemptKey, nowMs());

        const chatId = userChatId(userId);

        if (AUTO_BUY_DRY_RUN) {
          await sendMessageSafe(chatId, `AutoBuy (DRY)\nЦена: ${candidate.priceTon.toFixed(3)} TON\n${candidate.urlTelegram}`, {
            disable_web_page_preview: false,
            reply_markup: { inline_keyboard: [[{ text: 'MRKT', url: candidate.urlMarket }]] }
          });
          buysDone++;
          continue;
        }

        const buyStart = Date.now();
        const buyRes = await mrktPostJson('/gifts/buy', { ids: [candidate.id], prices: { [candidate.id]: candidate.priceNano } });
        const boughtAt = Date.now();
        const latencyMs = boughtAt - buyStart;

        if (buyRes.ok) {
          await sendMessageSafe(chatId, `✅ AutoBuy OK\n${candidate.priceTon.toFixed(3)} TON\n${candidate.urlTelegram}\nLatency ${latencyMs}ms`, {
            disable_web_page_preview: false,
            reply_markup: { inline_keyboard: [[{ text: 'MRKT', url: candidate.urlMarket }]] }
          });
          buysDone++;
          if (AUTO_BUY_DISABLE_AFTER_SUCCESS) {
            sub.autoBuyEnabled = false;
            scheduleSave();
          }
        } else {
          if (isNoFundsError(buyRes)) {
            await sendMessageSafe(chatId, `❌ AutoBuy stop: no funds`, { disable_web_page_preview: true });
          } else {
            await sendMessageSafe(chatId, `❌ AutoBuy FAIL`, { disable_web_page_preview: true });
          }
        }
      }
    }
  } catch (e) {
    console.error('autobuy error:', e);
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
      `lastFail: ${mrktState.lastFailMsg || '-'}\n` +
      `pauseUntil: ${mrktState.pauseUntil ? new Date(mrktState.pauseUntil).toLocaleTimeString('ru-RU') : '-'}\n` +
      `Redis: ${redis ? 'YES' : 'NO'}\n`;
    await sendMessageSafe(msg.chat.id, txt, { reply_markup: MAIN_KEYBOARD });
  }
});

// ===================== Express =====================
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

// proxy images
app.get('/img/cdn', async (req, res) => {
  const key = String(req.query.key || '').trim();
  if (!key) return res.status(400).send('no key');
  const url = joinUrl(MRKT_CDN_BASE, key);
  const r = await fetchWithTimeout(url, { method: 'GET', headers: { Accept: 'image/*' } }, 8000).catch(() => null);
  if (!r || !r.ok) return res.status(404).send('not found');
  streamFetchToRes(r, res, 'image/webp');
});

// UI
app.get('/', (req, res) => { res.setHeader('Content-Type','text/html; charset=utf-8'); res.send(WEBAPP_HTML); });
app.get('/app.js', (req, res) => { res.setHeader('Content-Type','application/javascript; charset=utf-8'); res.send(WEBAPP_JS); });

// FAST state (no MRKT calls)
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
  if (b.filters && typeof b.filters === 'object') {
    const next = normalizeFilters(b.filters);
    const labels = (next.giftLabels && typeof next.giftLabels === 'object') ? next.giftLabels : {};
    const cleanLabels = {};
    for (const g of next.gifts) cleanLabels[g] = labels[g] || g;
    next.giftLabels = cleanLabels;
    u.filters = next;
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

  // pass waitMs if paused
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
        const rarity =
          m.rarityPerMille != null && Number.isFinite(Number(m.rarityPerMille))
            ? ` (${(Number(m.rarityPerMille) / 10).toFixed(1)}%)`
            : '';
        const imgUrl = m.thumbKey ? `/img/cdn?key=${encodeURIComponent(m.thumbKey)}` : null;
        const floor = m.floorNano != null ? tonFromNano(m.floorNano) : null;
        return { label: m.name + rarity, value: m.name, imgUrl, sub: floor != null ? `min: ${floor.toFixed(3)} TON` : 'min: —' };
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

// market lots
app.get('/api/mrkt/lots', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const f = normalizeFilters(u.filters || {});
  const cacheKey = `lots|${req.userId}|${JSON.stringify(f)}`;
  const cached = lotsCache.get(cacheKey);
  if (cached && nowMs() - cached.time < LOTS_CACHE_TTL_MS) return res.json(cached.data);

  if (!f.gifts.length) {
    const r = await mrktGlobalCheapestLotsReal();
    const lots = (r.lots || []).slice(0, WEBAPP_LOTS_LIMIT).map((lot) => ({
      ...lot,
      imgUrl: lot.thumbKey ? `/img/cdn?key=${encodeURIComponent(lot.thumbKey)}` : null,
      listedMsk: lot.listedAt ? new Date(Date.parse(lot.listedAt)).toLocaleString('ru-RU', { timeZone: 'Europe/Moscow' }) : null,
    }));
    const payload = { ok: true, lots, note: r.note || null, waitMs: Math.max(0, (mrktState.pauseUntil || 0) - nowMs()) };
    lotsCache.set(cacheKey, { time: nowMs(), data: payload });
    return res.json(payload);
  }

  const r = await mrktSearchLotsByFilters(f, WEBAPP_LOTS_PAGES || MRKT_PAGES);
  if (!r.ok) {
    const payload = { ok: true, lots: [], note: `MRKT RPS limit, wait ${fmtWaitMs(r.waitMs || 1000)}`, waitMs: r.waitMs || 1000 };
    lotsCache.set(cacheKey, { time: nowMs(), data: payload });
    return res.json(payload);
  }

  const lots = (r.gifts || []).slice(0, WEBAPP_LOTS_LIMIT).map((lot) => ({
    ...lot,
    imgUrl: lot.thumbKey ? `/img/cdn?key=${encodeURIComponent(lot.thumbKey)}` : null,
    listedMsk: lot.listedAt ? new Date(Date.parse(lot.listedAt)).toLocaleString('ru-RU', { timeZone: 'Europe/Moscow' }) : null,
  }));

  const payload = { ok: true, lots, note: null, waitMs: 0 };
  lotsCache.set(cacheKey, { time: nowMs(), data: payload });
  res.json(payload);
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
    const ex = await mrktSearchLotsByFilters({ gifts: [gift], models: model ? [model] : [], backdrops: backdrop ? [backdrop] : [], numberPrefix: '' }, 1);
    if (ex.ok && ex.gifts?.length) floorExact = ex.gifts[0].priceTon;

    const col = await mrktSearchLotsByFilters({ gifts: [gift], models: [], backdrops: [], numberPrefix: '' }, 1);
    if (col.ok && col.gifts?.length) floorCollection = col.gifts[0].priceTon;

    const o1 = await mrktOrdersFetch({ gift, model, backdrop });
    const o2 = await mrktOrdersFetch({ gift, model: null, backdrop: null });

    offerExact = o1.ok ? maxOfferTonFromOrders(o1.orders) : null;
    offerCollection = o2.ok ? maxOfferTonFromOrders(o2.orders) : null;
  }

  const salesHistory = gift
    ? await mrktFeedSales({ gift, modelNames: model ? [model] : [], backdropNames: backdrop ? [backdrop] : [] })
    : { ok: true, approxPriceTon: null, sales: [], note: 'no gift' };

  const payload = { ok: true, floors: { exact: floorExact, collection: floorCollection }, offers: { exact: offerExact, collection: offerCollection }, salesHistory };
  detailsCache.set(lotId, { time: nowMs(), data: payload });
  res.json(payload);
});

// profile
app.get('/api/profile', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const purchases = (u.purchases || []).slice(0, 80).map((p) => ({
    title: p.title,
    lotId: p.lotId || null,
    priceTon: p.priceTon,
    boughtMsk: p.tsBought ? new Date(p.tsBought).toLocaleString('ru-RU', { timeZone: 'Europe/Moscow' }) : null,
    latencyMs: p.latencyMs ?? null,
    imgUrl: p.thumbKey ? `/img/cdn?key=${encodeURIComponent(p.thumbKey)}` : null,
    urlTelegram: p.urlTelegram || null,
    urlMarket: p.urlMarket || null,
    model: p.model || null,
    backdrop: p.backdrop || null,
    number: p.number ?? null,
  }));
  res.json({ ok: true, user: req.tgUser, purchases });
});

// purchases by filters (only local bot/panel purchases)
app.get('/api/purchases/by_filters', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const f = normalizeFilters(u.filters || {});
  const list = (u.purchases || []).slice(0, 300);

  const wantGifts = new Set((f.gifts || []).map(norm));
  const wantModel = f.models.length === 1 ? normTraitName(f.models[0]) : null;
  const wantBackdrop = f.backdrops.length === 1 ? normTraitName(f.backdrops[0]) : null;
  const prefix = f.numberPrefix || '';
  const exactNumber = prefix && prefix.length >= 4 ? Number(prefix) : null;

  const filtered = list.filter((p) => {
    // collection match (best effort)
    if (wantGifts.size) {
      const c = norm(p.collection || '');
      if (!wantGifts.has(c)) return false;
    }
    if (wantModel && normTraitName(p.model) !== wantModel) return false;
    if (wantBackdrop && normTraitName(p.backdrop) !== wantBackdrop) return false;

    if (exactNumber != null) {
      if (p.number == null || Number(p.number) !== exactNumber) return false;
    } else if (prefix) {
      if (p.number == null || !String(p.number).startsWith(prefix)) return false;
    }
    return true;
  }).slice(0, 40);

  const items = filtered.map((p) => ({
    title: p.title,
    lotId: p.lotId || null,
    priceTon: p.priceTon,
    boughtMsk: p.tsBought ? new Date(p.tsBought).toLocaleString('ru-RU', { timeZone: 'Europe/Moscow' }) : null,
    imgUrl: p.thumbKey ? `/img/cdn?key=${encodeURIComponent(p.thumbKey)}` : null,
    urlTelegram: p.urlTelegram || null,
    urlMarket: p.urlMarket || null,
    model: p.model || null,
    backdrop: p.backdrop || null,
    number: p.number ?? null,
  }));

  // NOTE to user
  const note = `Показываются только покупки, сделанные через эту панель/бота.`;
  res.json({ ok: true, items, note });
});

// subs endpoints
app.post('/api/sub/create', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const r = makeSubFromCurrentFilters(u);
  if (!r.ok) return res.status(400).json({ ok: false, reason: r.reason });

  r.sub.ui = await buildSubUi(r.sub);

  u.subscriptions.push(r.sub);
  renumberSubs(u);
  scheduleSave();
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
    const o2 = await mrktOrdersFetch({ gift, model: null, backdrop: null });

    offerExact = o1.ok ? maxOfferTonFromOrders(o1.orders) : null;
    offerCollection = o2.ok ? maxOfferTonFromOrders(o2.orders) : null;
  }

  const salesHistory = gift
    ? await mrktFeedSales({ gift, modelNames: sf.models, backdropNames: sf.backdrops })
    : { ok: true, approxPriceTon: null, sales: [], note: 'multi-gift' };

  res.json({ ok: true, sub: s, floors: { exact: floorExact, collection: floorCollection }, offers: { exact: offerExact, collection: offerCollection }, salesHistory });
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

  const payloadJson = req.body?.payloadJson;
  let data = null;
  let photo = null;

  if (typeof payloadJson === 'string' && payloadJson.trim()) {
    const j = tryParseJsonMaybe(payloadJson);
    if (!j || typeof j !== 'object') return res.status(400).json({ ok: false, reason: 'BAD_PAYLOAD_JSON' });
    data = typeof j.data === 'string' ? j.data : null;
    photo = (j.photo === undefined) ? null : j.photo;
  }

  if (!data || !String(data).includes('hash=')) return res.status(400).json({ ok: false, reason: 'BAD_DATA_NO_HASH' });

  const sess = { data: String(data), photo };
  mrktSessionRuntime = sess;
  if (redis) await saveMrktSessionToRedis(sess);

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
  res.json({ ok: true, collectionsCount: cols.length, tokenMask: MRKT_AUTH_RUNTIME ? maskToken(MRKT_AUTH_RUNTIME) : null, lastFail: mrktState.lastFailMsg || null });
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
    console.warn('REDIS_URL not set => state/session/token won’t persist');
  }
  console.log('Bot started. /start');
})();
