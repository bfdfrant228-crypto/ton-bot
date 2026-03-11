/**
 * v23 (2026-03-11)
 * Fixes:
 * - WEBAPP_HTML/WEBAPP_JS defined => no Internal Server Error
 * - Market: MRKT sales history by current filters (feed type=sale) + median
 * - Multi-select dropdown UX (query separated from selected display)
 * - Auto retry after RPS_WAIT for lots + dropdowns
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

// throttling / RPS
const MRKT_TIMEOUT_MS = Number(process.env.MRKT_TIMEOUT_MS || 12000);
const MRKT_MIN_GAP_MS = Number(process.env.MRKT_MIN_GAP_MS || 1400);
const MRKT_429_DEFAULT_PAUSE_MS = Number(process.env.MRKT_429_DEFAULT_PAUSE_MS || 4500);
const MRKT_HTTP_MAX_WAIT_MS = Number(process.env.MRKT_HTTP_MAX_WAIT_MS || 800);

// limits
const MRKT_COUNT = Number(process.env.MRKT_COUNT || 12);
const MRKT_PAGES = Number(process.env.MRKT_PAGES || 1);
const MRKT_ORDERS_COUNT = Number(process.env.MRKT_ORDERS_COUNT || 25);

const WEBAPP_LOTS_LIMIT = Number(process.env.WEBAPP_LOTS_LIMIT || 24);
const WEBAPP_LOTS_PAGES = Number(process.env.WEBAPP_LOTS_PAGES || 1);
const WEBAPP_SUGGEST_LIMIT = Number(process.env.WEBAPP_SUGGEST_LIMIT || 120);

// global scan (when no gift selected)
const GLOBAL_SCAN_COLLECTIONS = Number(process.env.GLOBAL_SCAN_COLLECTIONS || 2);
const GLOBAL_SCAN_CACHE_TTL_MS = Number(process.env.GLOBAL_SCAN_CACHE_TTL_MS || 120_000);

// caches
const CACHE_TTL_MS = Number(process.env.CACHE_TTL_MS || 5 * 60_000);
const OFFERS_CACHE_TTL_MS = Number(process.env.OFFERS_CACHE_TTL_MS || 20_000);
const LOTS_CACHE_TTL_MS = Number(process.env.LOTS_CACHE_TTL_MS || 2500);
const DETAILS_CACHE_TTL_MS = Number(process.env.DETAILS_CACHE_TTL_MS || 4000);
const SALES_CACHE_TTL_MS = Number(process.env.SALES_CACHE_TTL_MS || 30_000);

// sales history
const SALES_HISTORY_TARGET = Number(process.env.SALES_HISTORY_TARGET || 18);
const SALES_HISTORY_MAX_PAGES = Number(process.env.SALES_HISTORY_MAX_PAGES || 12);
const SALES_HISTORY_COUNT_PER_PAGE = Number(process.env.SALES_HISTORY_COUNT_PER_PAGE || 50);
const SALES_HISTORY_THROTTLE_MS = Number(process.env.SALES_HISTORY_THROTTLE_MS || 220);
const SALES_HISTORY_TIME_BUDGET_MS = Number(process.env.SALES_HISTORY_TIME_BUDGET_MS || 9000);

// subs
const SUBS_CHECK_INTERVAL_MS = Number(process.env.SUBS_CHECK_INTERVAL_MS || 30000);
const SUBS_MAX_NOTIFICATIONS_PER_CYCLE = Number(process.env.SUBS_MAX_NOTIFICATIONS_PER_CYCLE || 6);
const SUBS_EMPTY_CONFIRM = Number(process.env.SUBS_EMPTY_CONFIRM || 2);

// AutoBuy (keep OFF)
const AUTO_BUY_GLOBAL = String(process.env.AUTO_BUY_GLOBAL || '0') === '1';
const AUTO_BUY_DRY_RUN = String(process.env.AUTO_BUY_DRY_RUN || '1') === '1';

console.log('v23 start', {
  MODE,
  PUBLIC_URL: !!PUBLIC_URL,
  WEBAPP_URL: !!WEBAPP_URL,
  REDIS: !!REDIS_URL,
  MRKT_AUTH_SET: !!MRKT_AUTH_RUNTIME,
  MRKT_MIN_GAP_MS,
  MRKT_HTTP_MAX_WAIT_MS,
  AUTO_BUY_GLOBAL,
  AUTO_BUY_DRY_RUN,
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
function fmtWaitMs(ms) {
  const s = Math.max(1, Math.ceil(Number(ms || 0) / 1000));
  return `${s}s`;
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

// ===================== Redis (optional) =====================
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
const REDIS_KEY_STATE = 'bot:state:main';

const users = new Map();      // userId -> {chatId, enabled, filters, subscriptions}
const subStates = new Map();  // `${userId}:${subId}` -> {floor, emptyStreak}

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
    });
  }
  return users.get(userId);
}

function userChatId(userId) {
  const u = users.get(userId);
  return (u?.chatId && Number.isFinite(Number(u.chatId))) ? Number(u.chatId) : userId;
}

function renumberSubs(user) {
  const subs = Array.isArray(user.subscriptions) ? user.subscriptions : [];
  subs.forEach((s, idx) => { if (s) s.num = idx + 1; });
}

function exportState() {
  const out = { users: {} };
  for (const [userId, u] of users.entries()) {
    out.users[String(userId)] = {
      enabled: u?.enabled !== false,
      chatId: u?.chatId ?? null,
      filters: u?.filters,
      subscriptions: Array.isArray(u?.subscriptions) ? u.subscriptions : [],
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
    };
    safe.subscriptions = safe.subscriptions
      .filter((s) => s && typeof s === 'object')
      .map((s) => ({
        id: s.id || `sub_${Date.now()}_${Math.random().toString(16).slice(2, 6)}`,
        num: typeof s.num === 'number' ? s.num : 0,
        enabled: s.enabled !== false,
        maxNotifyTon: s.maxNotifyTon == null ? null : Number(s.maxNotifyTon),
        filters: normalizeFilters(s.filters || {}),
      }));
    renumberSubs(safe);

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

// ===================== MRKT gate + queue =====================
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

async function mrktGetJson(path) {
  return mrktRunExclusive(async () => {
    if (!MRKT_AUTH_RUNTIME) return { ok: false, status: 401, data: null, text: 'NO_AUTH' };

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
      mrktState.lastFailMsg = extractMrktErrorMessage(txt) || `HTTP ${res.status}`;
      return { ok: false, status: res.status, data, text: txt, waitMs: Math.max(0, mrktState.pauseUntil - nowMs()) };
    }

    mrktState.lastFailMsg = null;
    return { ok: true, status: res.status, data, text: txt };
  });
}

async function mrktPostJson(path, bodyObj) {
  return mrktRunExclusive(async () => {
    if (!MRKT_AUTH_RUNTIME) return { ok: false, status: 401, data: null, text: 'NO_AUTH' };

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
      mrktState.lastFailMsg = extractMrktErrorMessage(txt) || `HTTP ${res.status}`;
      return { ok: false, status: res.status, data, text: txt, waitMs: Math.max(0, mrktState.pauseUntil - nowMs()) };
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

// MRKT sales history (feed type=sale) by gift+filters
async function mrktFeedSales({ gift, modelNames = [], backdropNames = [] }) {
  const key = `sales|${gift}|m=${(modelNames||[]).join('|')}|b=${(backdropNames||[]).join('|')}`;
  const now = nowMs();
  const cached = salesCache.get(key);
  if (cached && now - cached.time < SALES_CACHE_TTL_MS) return cached.data;

  const data = await (async () => {
    if (!gift) return { ok: true, approxPriceTon: null, sales: [], note: 'Выбери 1 gift' };

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
        if (r.status === 429) return { ok: true, approxPriceTon: null, sales: [], note: `RPS limit, wait ${fmtWaitMs(r.waitMs || 1000)}`, waitMs: r.waitMs || 1000 };
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
          model: g.modelTitle || g.modelName || null,
          backdrop: g.backdropName || null,
          urlMarket: g.id ? mrktLotUrlFromId(g.id) : 'https://t.me/mrkt',
          urlTelegram: (g.name && String(g.name).includes('-')) ? `https://t.me/nft/${g.name}` : 'https://t.me/mrkt',
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

// Global cheapest scan (when gift not selected)
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

// ===================== Subs worker (floor notify) =====================
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

async function notifyFloorToUser(userId, sub, lot, newFloor) {
  const chatId = userChatId(userId);

  const lines = [];
  lines.push(`Подписка #${sub.num}`);
  lines.push(`Цена: ${newFloor.toFixed(3)} TON`);
  if (lot?.model) lines.push(`Model: ${lot.model}`);
  if (lot?.backdrop) lines.push(`Backdrop: ${lot.backdrop}`);
  lines.push(lot?.urlTelegram || 'https://t.me/mrkt');

  const reply_markup = lot?.urlMarket
    ? { inline_keyboard: [[{ text: 'MRKT', url: lot.urlMarket }]] }
    : undefined;

  await sendMessageSafe(chatId, lines.join('\n'), { disable_web_page_preview: false, reply_markup });
}

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

// ===================== Telegram bot + Express =====================
const bot = new TelegramBot(TELEGRAM_TOKEN, { polling: false });
const MAIN_KEYBOARD = { keyboard: [[{ text: '📌 Статус' }]], resize_keyboard: true };

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
      `lastFail: ${mrktState.lastFailMsg || '-'}\n` +
      `pauseUntil: ${mrktState.pauseUntil ? new Date(mrktState.pauseUntil).toLocaleTimeString('ru-RU') : '-'}\n` +
      `Redis: ${redis ? 'YES' : 'NO'}\n` +
      `Mode: ${MODE}\n`;
    await sendMessageSafe(msg.chat.id, txt, { reply_markup: MAIN_KEYBOARD });
  }
});

// ===================== WebApp HTML/JS (DEFINED!) =====================
const WEBAPP_HTML = `<!doctype html>
<html lang="ru"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"/>
<title>${APP_TITLE}</title>
<style>
:root{--bg:#0b0f14;--card:#101826;--text:#e5e7eb;--muted:#9ca3af;--border:#223044;--input:#0f172a;--btn:#182235;--accent:#22c55e;--danger:#ef4444}
*{box-sizing:border-box}
html,body{background:var(--bg); overscroll-behavior: none;}
body{margin:0;padding:14px;color:var(--text);font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial}
h2{margin:0 0 10px 0;font-size:18px}
.card{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:12px;margin:10px 0}
label{display:block;font-size:12px;color:var(--muted);margin-bottom:4px}
.row{display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end}
.field{position:relative;flex:1 1 200px;min-width:140px}
.inpWrap{position:relative}
input{width:100%;padding:10px;border:1px solid var(--border);border-radius:12px;background:var(--input);color:var(--text);outline:none;font-size:13px}
button{padding:10px 12px;border:1px solid var(--border);border-radius:12px;background:var(--btn);color:var(--text);cursor:pointer}
.primary{border-color:var(--accent);background:var(--accent);color:#052e16;font-weight:950}
.small{padding:8px 10px;border-radius:10px;font-size:13px}
.xbtn{position:absolute;right:8px;top:50%;transform:translateY(-50%);width:20px;height:20px;border-radius:8px;border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.05);color:rgba(255,255,255,.65);cursor:pointer;display:flex;align-items:center;justify-content:center}
#err{display:none;border-color:var(--danger);color:#ffd1d1;white-space:pre-wrap;word-break:break-word}
.tabs{display:flex;gap:8px;flex-wrap:wrap}
.tabbtn{border-radius:999px;padding:8px 12px;font-size:13px}
.tabbtn.active{border-color:var(--accent);color:var(--accent)}
.sug{border:1px solid var(--border);border-radius:14px;overflow:auto;background:var(--card);max-height:380px;position:absolute;top:calc(100% + 6px);left:0;right:0;z-index:1000000;box-shadow:0 14px 40px rgba(0,0,0,.45);-webkit-overflow-scrolling:touch}
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
.muted{color:var(--muted);font-size:12px}
.loaderLine{display:none;align-items:center;gap:10px;color:var(--muted);font-size:12px;margin-top:10px}
.spinner{width:16px;height:16px;border-radius:999px;border:3px solid rgba(255,255,255,.2);border-top-color:var(--accent);animation:spin .8s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}

/* sheet */
.sheetWrap{position:fixed;inset:0;background:rgba(0,0,0,.35);display:flex;align-items:flex-end;justify-content:center;z-index:50000;padding:10px;opacity:0;visibility:hidden;pointer-events:none;transition:opacity .18s ease,visibility .18s ease}
.sheetWrap.show{opacity:1;visibility:visible;pointer-events:auto}
.sheet{width:min(980px,96vw);height:min(82vh,880px);background:var(--card);border:1px solid var(--border);border-radius:22px 22px 14px 14px;padding:12px;box-shadow:0 30px 90px rgba(0,0,0,.5);display:flex;flex-direction:column;gap:10px;transform:translateY(20px);transition:transform .18s ease}
.sheetWrap.show .sheet{transform:translateY(0)}
.handle{width:44px;height:5px;border-radius:999px;background:rgba(255,255,255,.18);align-self:center}
.sheetHeader{display:flex;justify-content:space-between;gap:12px;align-items:flex-start}
.sheetBody{overflow:auto;-webkit-overflow-scrolling:touch}
.kv{display:flex;gap:10px;flex-wrap:wrap}
.kv .p{padding:6px 10px;border:1px solid var(--border);border-radius:999px;font-size:12px;color:var(--muted)}
.sale{border:1px solid var(--border);border-radius:12px;padding:10px;background:rgba(255,255,255,.02)}
</style></head>
<body>
<h2>${APP_TITLE}</h2>
<div id="err" class="card"></div>

<div class="tabs">
  <button class="tabbtn active" data-tab="market">Market</button>
  <button class="tabbtn" data-tab="subs">Subscriptions</button>
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
    <button id="salesByFilters" class="small">История продаж (MRKT)</button>
  </div>

  <div id="lotsLoading" class="loaderLine"><div class="spinner"></div><div>Загрузка лотов…</div></div>
  <div id="status" class="muted" style="margin-top:10px"></div>
  <div style="margin-top:10px"><b>Лоты</b> <span class="muted">(клик → детали)</span></div>
  <div id="lots" class="grid"></div>
</div>

<div id="subs" class="card" style="display:none">
  <h3 style="margin:0 0 8px 0;font-size:15px">Подписки (floor notify)</h3>
  <div class="muted">Подписки в этом v23 оставлены простыми. Если нужно — верну расширенное UI как раньше.</div>
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
    <div id="sheetTop" class="muted"></div>
    <div class="kv" id="sheetKV"></div>
    <div class="sheetBody" id="sheetBody"></div>
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

  function setTab(name){
    ['market','subs'].forEach(x => el(x).style.display = (x===name?'block':'none'));
    document.querySelectorAll('.tabbtn').forEach(b => b.classList.toggle('active', b.dataset.tab===name));
  }
  document.querySelectorAll('.tabbtn').forEach(b => b.onclick = () => setTab(b.dataset.tab));

  const lotsLoading = el('lotsLoading');
  function setLoading(on){ lotsLoading.style.display = on ? 'flex' : 'none'; }

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

  // sheet
  const sheetWrap = el('sheetWrap');
  const sheetTitle = el('sheetTitle');
  const sheetSub = el('sheetSub');
  const sheetTop = el('sheetTop');
  const sheetKV = el('sheetKV');
  const sheetBody = el('sheetBody');
  el('sheetClose').onclick = ()=>sheetWrap.classList.remove('show');
  sheetWrap.addEventListener('click',(e)=>{ if(e.target===sheetWrap) sheetWrap.classList.remove('show'); });

  function openSheet(title, sub){
    sheetTitle.textContent=title||'';
    sheetSub.textContent=sub||'';
    sheetTop.textContent='';
    sheetKV.innerHTML='';
    sheetBody.innerHTML='';
    sheetWrap.classList.add('show');
  }
  function pill(txt){ return '<div class="p">'+txt+'</div>'; }

  function renderSales(resp){
    if(!resp || resp.ok===false){
      sheetBody.innerHTML = '<i class="muted">Нет данных</i>';
      return;
    }
    const approx = resp.approxPriceTon;
    const note = resp.note ? ('<div class="muted" style="margin-bottom:10px">'+resp.note+'</div>') : '';
    const head = approx != null ? ('Примерная цена продажи (median): <b>'+approx.toFixed(3)+' TON</b>') : 'Примерная цена продажи: <b>нет данных</b>';

    let html = note + '<div class="muted" style="margin-bottom:10px">'+head+'</div>';
    const items = resp.sales || [];
    if(!items.length){
      html += '<i class="muted">Нет продаж</i>';
      sheetBody.innerHTML = html;
      return;
    }

    html += items.slice(0, 18).map(x=>{
      const dt = x.ts ? new Date(x.ts).toLocaleString('ru-RU',{timeZone:'Europe/Moscow'}) : '';
      return '<div class="sale">'+
        '<div style="display:flex;justify-content:space-between;gap:10px"><b>'+x.priceTon.toFixed(3)+' TON</b><span class="muted">'+dt+'</span></div>'+
        (x.model?'<div class="muted">Model: '+x.model+'</div>':'')+
        (x.backdrop?'<div class="muted">Backdrop: '+x.backdrop+'</div>':'')+
        '<div style="display:flex;gap:8px;margin-top:8px">' +
          '<button class="small" data-open="'+x.urlMarket+'">MRKT</button>' +
          '<button class="small" data-open="'+x.urlTelegram+'">NFT</button>' +
        '</div>'+
      '</div>';
    }).join('<div style="height:8px"></div>');

    sheetBody.innerHTML = html;
    sheetBody.querySelectorAll('button[data-open]').forEach(btn=>{
      btn.onclick = () => openTg(btn.getAttribute('data-open'));
    });
  }

  // dropdowns
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
    await api('/api/state/patch', {method:'POST', body: JSON.stringify({filters:{
      gifts: sel.gifts,
      giftLabels: sel.giftLabels,
      models: sel.models,
      backdrops: sel.backdrops,
      numberPrefix: el('number').value.trim()
    }})});
  }

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
    b.onpointerdown = (e) => { e.stopPropagation(); };
    b.onclick = (e) => {
      const btn = e.target.closest('button[data-v]');
      if(!btn) return;
      onToggle(btn.getAttribute('data-v'));
    };
  }

  // debounce
  const timers = { gift: null, model: null, backdrop: null };
  function debounce(kind, fn, ms=220){
    clearTimeout(timers[kind]);
    timers[kind] = setTimeout(fn, ms);
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

  // auto retry helper
  function scheduleRetryIfWait(resp, fn){
    const waitMs = resp && Number(resp.waitMs || 0);
    if (!waitMs || waitMs < 500 || waitMs > 20000) return;
    setTimeout(() => { fn().catch(()=>{}); }, waitMs + 150);
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

        const lbl = (r.mapLabel||{})[v] || v;
        sel.giftLabels[v] = lbl;

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

  el('gift').addEventListener('focus', ()=>{ setLoading(true); showGiftSug().finally(()=>setLoading(false)); });
  el('gift').addEventListener('click', ()=>{ setLoading(true); showGiftSug().finally(()=>setLoading(false)); });
  el('gift').addEventListener('input', ()=> debounce('gift', ()=>{ setLoading(true); showGiftSug().finally(()=>setLoading(false)); }, 240));

  el('model').addEventListener('focus', ()=>{ setLoading(true); showModelSug().finally(()=>setLoading(false)); });
  el('model').addEventListener('click', ()=>{ setLoading(true); showModelSug().finally(()=>setLoading(false)); });
  el('model').addEventListener('input', ()=> debounce('model', ()=>{ setLoading(true); showModelSug().finally(()=>setLoading(false)); }, 240));

  el('backdrop').addEventListener('focus', ()=>{ setLoading(true); showBackdropSug().finally(()=>setLoading(false)); });
  el('backdrop').addEventListener('click', ()=>{ setLoading(true); showBackdropSug().finally(()=>setLoading(false)); });
  el('backdrop').addEventListener('input', ()=> debounce('backdrop', ()=>{ setLoading(true); showBackdropSug().finally(()=>setLoading(false)); }, 240));

  document.addEventListener('pointerdown', (e) => {
    if(!e.target.closest('#gift,#model,#backdrop,.sug')) {
      hideSug('giftSug'); hideSug('modelSug'); hideSug('backdropSug');
      closeAllFields();
    }
  });

  // clear X
  document.body.addEventListener('click', (e) => {
    const b = e.target.closest('button[data-clear]');
    if(!b) return;
    const what = b.getAttribute('data-clear');

    (async()=>{
      if(what==='gift'){ sel.gifts=[]; sel.giftLabels={}; sel.models=[]; sel.backdrops=[]; el('gift').value=''; el('model').value=''; el('backdrop').value=''; }
      if(what==='model'){ sel.models=[]; el('model').value=''; }
      if(what==='backdrop'){ sel.backdrops=[]; el('backdrop').value=''; }
      if(what==='number'){ el('number').value=''; }
      await patchFilters();
      await refreshLots();
    })().catch(e=>showErr(e.message||String(e)));
  });

  function renderLots(resp){
    const box=el('lots');
    el('status').textContent = resp.note || '';

    const lots=resp.lots||[];
    if(!lots.length){ box.innerHTML='<i class="muted">Лотов не найдено</i>'; return; }

    box.innerHTML = lots.map(x=>{
      const img = x.imgUrl ? '<img src="'+x.imgUrl+'" referrerpolicy="no-referrer" loading="lazy"/>' : '';
      return '<div class="lot" data-id="'+x.id+'">'+
        (img || '<div style="aspect-ratio:1/1;border:1px solid rgba(255,255,255,.10);border-radius:14px"></div>')+
        '<div class="price">'+x.priceTon.toFixed(3)+' TON</div>'+
        '<div style="font-weight:900;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'+x.name+'</div>'+
        (x.model?'<div class="muted">Model: '+x.model+'</div>':'')+
        (x.backdrop?'<div class="muted">Backdrop: '+x.backdrop+'</div>':'')+
      '</div>';
    }).join('');

    const map = new Map(lots.map(l => [String(l.id), l]));
    box.querySelectorAll('.lot').forEach(node=>{
      node.onclick = async()=>{
        const id = node.getAttribute('data-id');
        const lot = map.get(String(id));
        if(!lot) return;

        openSheet('Лот', lot.name);
        sheetTop.innerHTML =
          '<div><b>Цена:</b> '+lot.priceTon.toFixed(3)+' TON</div>' +
          (lot.model?('<div class="muted">Model: '+lot.model+'</div>'):'') +
          (lot.backdrop?('<div class="muted">Backdrop: '+lot.backdrop+'</div>'):'') +
          (lot.listedMsk?('<div class="muted">Listed: '+lot.listedMsk+'</div>'):'') +
          '<div class="muted" style="margin-top:6px">ID: '+lot.id+'</div>';

        setLoading(true);
        try{
          const det = await api('/api/lot/details', { method:'POST', body: JSON.stringify({ lot }) });

          sheetKV.innerHTML = [
            det?.offers?.exact!=null ? pill('Max offer (exact): <b>'+det.offers.exact.toFixed(3)+' TON</b>') : pill('Max offer (exact): —'),
            det?.offers?.collection!=null ? pill('Max offer (col): <b>'+det.offers.collection.toFixed(3)+' TON</b>') : pill('Max offer (col): —'),
            det?.floors?.exact!=null ? pill('Floor (exact): <b>'+det.floors.exact.toFixed(3)+' TON</b>') : pill('Floor (exact): —'),
            det?.floors?.collection!=null ? pill('Floor (col): <b>'+det.floors.collection.toFixed(3)+' TON</b>') : pill('Floor (col): —'),
          ].join('');

          renderSales(det?.salesHistory);
        } finally {
          setLoading(false);
        }
      };
    });
  }

  async function refreshState(){
    const st = await api('/api/state');
    sel.gifts = st.user.filters.gifts || [];
    sel.giftLabels = st.user.filters.giftLabels || {};
    sel.models = st.user.filters.models || [];
    sel.backdrops = st.user.filters.backdrops || [];
    el('number').value = st.user.filters.numberPrefix || '';
    el('gift').value = giftsInputText();
    el('model').value = listInputText(sel.models);
    el('backdrop').value = listInputText(sel.backdrops);
  }

  async function refreshLots(){
    hideErr();
    setLoading(true);
    try{
      const r = await api('/api/mrkt/lots');
      renderLots(r);
      scheduleRetryIfWait(r, refreshLots);
    }catch(e){
      showErr(e.message || String(e));
    }finally{
      setLoading(false);
    }
  }

  el('apply').onclick = async()=>{
    await patchFilters();
    await refreshState();
    await refreshLots();
  };
  el('refresh').onclick = async()=>{ await refreshState(); await refreshLots(); };

  el('salesByFilters').onclick = async()=>{
    openSheet('История продаж', 'MRKT feed по текущим фильтрам');
    sheetTop.innerHTML = '<div class="muted">Загрузка…</div>';
    setLoading(true);
    try{
      const r = await api('/api/mrkt/sales_by_filters');
      sheetTop.innerHTML = '';
      renderSales(r);
      scheduleRetryIfWait(r, async()=> {
        const rr = await api('/api/mrkt/sales_by_filters');
        renderSales(rr);
      });
    } finally {
      setLoading(false);
    }
  };

  // init
  (async()=>{
    try{
      await refreshState();
      await refreshLots();
    }catch(e){
      showErr(e.message || String(e));
    }
  })();
})();`;

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

// state
app.get('/api/state', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  res.json({
    ok: true,
    api: {
      isAdmin: isAdmin(req.userId),
      mrktAuthSet: !!MRKT_AUTH_RUNTIME,
      mrktAuthMask: MRKT_AUTH_RUNTIME ? maskToken(MRKT_AUTH_RUNTIME) : null,
      mrktLastFail: mrktState.lastFailMsg || null,
      mrktPauseUntil: mrktState.pauseUntil || 0,
    },
    user: { enabled: !!u.enabled, filters: u.filters },
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

// lots
app.get('/api/mrkt/lots', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const f = normalizeFilters(u.filters || {});
  const cacheKey = `lots|${req.userId}|${JSON.stringify(f)}`;
  const cached = lotsCache.get(cacheKey);
  if (cached && nowMs() - cached.time < LOTS_CACHE_TTL_MS) return res.json(cached.data);

  let lots = [];
  let note = null;
  let waitMs = 0;

  if (!f.gifts.length) {
    const r = await mrktGlobalCheapestLotsReal();
    note = r.note || null;
    lots = (r.lots || []).slice(0, WEBAPP_LOTS_LIMIT);
  } else {
    const r = await mrktSearchLotsByFilters(f, WEBAPP_LOTS_PAGES || MRKT_PAGES);
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
    imgUrl: lot.thumbKey ? `/img/cdn?key=${encodeURIComponent(lot.thumbKey)}` : null,
    listedMsk: lot.listedAt ? new Date(Date.parse(lot.listedAt)).toLocaleString('ru-RU', { timeZone: 'Europe/Moscow' }) : null,
  }));

  const payload = { ok: true, lots: mapped, note, waitMs };
  lotsCache.set(cacheKey, { time: nowMs(), data: payload });
  res.json(payload);
});

// lot details (offers + floors + sales history)
app.post('/api/lot/details', auth, async (req, res) => {
  const lot = req.body?.lot || null;
  const lotId = String(lot?.id || '').trim();
  if (!lotId) return res.status(400).json({ ok: false, reason: 'NO_LOT' });

  const cached = detailsCache.get(lotId);
  if (cached && nowMs() - cached.time < DETAILS_CACHE_TTL_MS) return res.json(cached.data);

  const gift = String(lot?.collectionName || '').trim();
  const model = lot?.model ? String(lot.model) : null;
  const backdrop = lot?.backdrop ? String(lot.backdrop) : null;

  let floorExact = null, floorCollection = null, offerExact = null, offerCollection = null;

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

// Market: sales history by current filters (THIS is what you asked)
app.get('/api/mrkt/sales_by_filters', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const f = normalizeFilters(u.filters || {});
  if (f.gifts.length !== 1) {
    return res.json({ ok: true, approxPriceTon: null, sales: [], note: 'Выбери ровно 1 gift для истории продаж (иначе слишком широкая выборка).' });
  }
  const gift = f.gifts[0];
  const out = await mrktFeedSales({ gift, modelNames: f.models, backdropNames: f.backdrops });
  res.json(out);
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

setInterval(() => { checkSubscriptionsForAllUsers().catch((e) => console.error('subs error:', e)); }, SUBS_CHECK_INTERVAL_MS);

// bootstrap
(async () => {
  if (REDIS_URL) {
    await initRedis();
    if (redis) {
      const raw = await redisGet(REDIS_KEY_STATE);
      if (raw) { try { importState(JSON.parse(raw)); } catch {} }
    }
  } else {
    console.warn('REDIS_URL not set => state will not persist');
  }
  console.log('Bot started. /start');
})();
