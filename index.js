/**
 * MRKT Panel + Bot
 * Files:
 * - index.js
 * - public/index.html
 * - public/app.js
 * - public/style.css
 *
 * Key points:
 * - Multi-select gifts/models/backdrops (OR-logic), saved in state/subs/autobuy
 * - Admin token update without restart (Redis)
 * - Chat notifications: TEXT + image URL (Telegram preview), NO sendPhoto
 * - WebApp: bottom-sheet lot menu + sales history + profile purchases with images/buttons
 *
 * Node 18+
 * deps: express, node-telegram-bot-api, redis(optional)
 */

const TelegramBot = require('node-telegram-bot-api');
const express = require('express');
const crypto = require('crypto');
const path = require('path');
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

// token runtime (can be overwritten via Admin panel -> Redis)
let MRKT_AUTH_RUNTIME = (process.env.MRKT_AUTH || '').trim() || null;

// intervals
const SUBS_CHECK_INTERVAL_MS = Number(process.env.SUBS_CHECK_INTERVAL_MS || 9000);

// AutoBuy
const AUTO_BUY_GLOBAL = String(process.env.AUTO_BUY_GLOBAL || '1') === '1';
const AUTO_BUY_DRY_RUN = String(process.env.AUTO_BUY_DRY_RUN || '1') === '1';
const AUTO_BUY_CHECK_INTERVAL_MS = Number(process.env.AUTO_BUY_CHECK_INTERVAL_MS || 2500);
const AUTO_BUY_MAX_BUYS_PER_USER_PER_CYCLE = Number(process.env.AUTO_BUY_MAX_BUYS_PER_USER_PER_CYCLE || 1);
const AUTO_BUY_ATTEMPT_TTL_MS = Number(process.env.AUTO_BUY_ATTEMPT_TTL_MS || 60_000);
const AUTO_BUY_NO_FUNDS_PAUSE_MS = Number(process.env.AUTO_BUY_NO_FUNDS_PAUSE_MS || 10 * 60 * 1000);
const AUTO_BUY_DISABLE_AFTER_SUCCESS = String(process.env.AUTO_BUY_DISABLE_AFTER_SUCCESS || '0') === '1';

// requests
const MRKT_TIMEOUT_MS = Number(process.env.MRKT_TIMEOUT_MS || 10000);
const MRKT_COUNT = Number(process.env.MRKT_COUNT || 20);
const MRKT_THROTTLE_MS = Number(process.env.MRKT_THROTTLE_MS || 120);
const MRKT_ORDERS_COUNT = Number(process.env.MRKT_ORDERS_COUNT || 30);

// WebApp limits
const WEBAPP_SUGGEST_LIMIT = Number(process.env.WEBAPP_SUGGEST_LIMIT || 80);
const WEBAPP_LOTS_LIMIT = Number(process.env.WEBAPP_LOTS_LIMIT || 40);
const WEBAPP_LOTS_PAGES = Number(process.env.WEBAPP_LOTS_PAGES || 1);

// Subs behavior
const SUBS_MAX_NOTIFICATIONS_PER_CYCLE = Number(process.env.SUBS_MAX_NOTIFICATIONS_PER_CYCLE || 8);
const SUBS_EMPTY_CONFIRM = Number(process.env.SUBS_EMPTY_CONFIRM || 2);

// sales history (feed sale)
const SALES_HISTORY_TARGET = Number(process.env.SALES_HISTORY_TARGET || 24);
const SALES_HISTORY_MAX_PAGES = Number(process.env.SALES_HISTORY_MAX_PAGES || 20);
const SALES_HISTORY_COUNT_PER_PAGE = Number(process.env.SALES_HISTORY_COUNT_PER_PAGE || 50);
const SALES_HISTORY_THROTTLE_MS = Number(process.env.SALES_HISTORY_THROTTLE_MS || 120);
const SALES_HISTORY_TIME_BUDGET_MS = Number(process.env.SALES_HISTORY_TIME_BUDGET_MS || 15000);

// MRKT auth expiry notify
const MRKT_AUTH_NOTIFY_COOLDOWN_MS = Number(process.env.MRKT_AUTH_NOTIFY_COOLDOWN_MS || 60 * 60 * 1000); // 1h
let mrktAuthLastNotifiedAt = 0;

// redis keys
const REDIS_KEY_MRKT_AUTH = 'mrkt:auth:token';
const REDIS_KEY_STATE = 'bot:state:main';

console.log('Start', {
  MODE,
  WEBAPP_URL: !!WEBAPP_URL,
  ADMIN_USER_ID,
  REDIS: !!REDIS_URL,
  MRKT_AUTH: !!MRKT_AUTH_RUNTIME,
  AUTO_BUY_GLOBAL,
  AUTO_BUY_DRY_RUN
});

// ===================== Telegram bot =====================
const bot = new TelegramBot(token, { polling: true });
const MAIN_KEYBOARD = { keyboard: [[{ text: '📌 Статус' }]], resize_keyboard: true };

// ===================== State =====================
const users = new Map();      // userId -> user
const subStates = new Map();  // `${userId}:${subId}` -> { floor, emptyStreak, pausedUntil }

let isSubsChecking = false;
let isAutoBuying = false;

const mrktState = {
  lastOkAt: 0,
  lastFailAt: 0,
  lastFailMsg: null,
  lastFailStatus: null,
  lastFailEndpoint: null,
};

const autoBuyDebug = {
  lastAt: 0,
  eligible: 0,
  scanned: 0,
  candidates: 0,
  buys: 0,
  lastReason: '',
};

const autoBuyRecentAttempts = new Map(); // `${userId}:${giftId}` -> ts

// caches
const CACHE_TTL_MS = 5 * 60_000;
let collectionsCache = { time: 0, items: [] };
const modelsCache = new Map();     // gift -> {time, items}
const backdropsCache = new Map();  // gift -> {time, items}
const offersCache = new Map();     // key -> {time, data}
const OFFERS_CACHE_TTL_MS = 15_000;

// ===================== helpers =====================
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const nowMs = () => Date.now();

function makeReq() {
  return `${Date.now()}_${Math.random().toString(16).slice(2)}`;
}
function norm(s) { return String(s || '').toLowerCase().trim().replace(/\s+/g, ' '); }

function parseTonInput(x) {
  const s = String(x ?? '').trim();
  if (!s) return NaN;
  const cleaned = s.replace(',', '.').replace(':', '.').replace(/\s+/g, '');
  const v = Number(cleaned);
  return Number.isFinite(v) ? v : NaN;
}
function cleanDigitsPrefix(v, maxLen = 12) { return String(v || '').replace(/\D/g, '').slice(0, maxLen); }

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

function normalizeFilters(f) {
  const gifts = uniqNorm(ensureArray(f?.gifts || f?.gift));
  const giftLabels = (f?.giftLabels && typeof f.giftLabels === 'object') ? f.giftLabels : {};
  const models = uniqNorm(ensureArray(f?.models || f?.model));
  const backdrops = uniqNorm(ensureArray(f?.backdrops || f?.backdrop));
  const numberPrefix = cleanDigitsPrefix(f?.numberPrefix || '');

  // if multiple gifts => clear model/backdrop (MRKT often returns empty otherwise)
  return {
    gifts,
    giftLabels,
    models: gifts.length === 1 ? models : [],
    backdrops: gifts.length === 1 ? backdrops : [],
    numberPrefix,
  };
}

function formatMskMs(ts) {
  const d = new Date(ts);
  const ms = String(d.getMilliseconds()).padStart(3, '0');
  const fmt = new Intl.DateTimeFormat('ru-RU', {
    timeZone: 'Europe/Moscow',
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hour12: false,
  });
  const parts = fmt.formatToParts(d);
  const get = (t) => parts.find((p) => p.type === t)?.value || '';
  return `${get('year')}-${get('month')}-${get('day')} ${get('hour')}:${get('minute')}:${get('second')}.${ms} MSK`;
}

function mrktLotUrlFromId(id) {
  if (!id) return 'https://t.me/mrkt';
  return `https://t.me/mrkt/app?startapp=${String(id).replace(/-/g, '')}`;
}

function fragmentGiftRemoteUrlFromGiftName(giftName) {
  if (!giftName) return null;
  const slug = String(giftName).trim().toLowerCase();
  const base = FRAGMENT_GIFT_IMG_BASE.endsWith('/') ? FRAGMENT_GIFT_IMG_BASE : (FRAGMENT_GIFT_IMG_BASE + '/');
  return base + encodeURIComponent(slug) + '.medium.jpg';
}

function giftNameFallbackFromCollectionAndNumber(collectionTitleOrName, number) {
  const base = String(collectionTitleOrName || '').replace(/\s+/g, '');
  if (!base || number == null) return null;
  return `${base}-${number}`.toLowerCase();
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
        if (Array.isArray(v) && v.length) return String(v[0]).slice(0, 220);
        if (typeof v === 'string') return String(v).slice(0, 220);
      }
    }
    if (typeof j?.message === 'string') return j.message.slice(0, 220);
    if (typeof j?.title === 'string') return j.title.slice(0, 220);
  } catch {}
  return s.slice(0, 220);
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

function joinUrl(base, key) {
  const b = String(base || '').endsWith('/') ? String(base) : (String(base) + '/');
  const k = String(key || '').startsWith('/') ? String(key).slice(1) : String(key);
  return b + k;
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

  r.arrayBuffer()
    .then((ab) => res.end(Buffer.from(ab)))
    .catch(() => res.status(502).end('bad gateway'));
}

// ===================== WebApp auth =====================
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
      chatId: u?.chatId ?? null,
      filters: u?.filters,
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
      }));
    renumberSubs(safe);

    safe.purchases = (safe.purchases || []).slice(0, 300).map((p) => ({
      tsListed: p.tsListed || null,
      tsFound: p.tsFound || null,
      tsBought: p.tsBought || p.ts || null,
      latencyMs: p.latencyMs || null,
      title: String(p.title || ''),
      priceTon: Number(p.priceTon || 0),
      urlTelegram: String(p.urlTelegram || ''),
      urlMarket: String(p.urlMarket || ''),
      giftName: String(p.giftName || ''),
      model: String(p.model || ''),
      backdrop: String(p.backdrop || ''),
      collection: String(p.collection || ''),
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

// ===================== User =====================
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
  };
  return { ok: true, sub };
}

function pushPurchase(user, entry) {
  if (!user.purchases) user.purchases = [];
  user.purchases.unshift({
    tsListed: entry.tsListed || null,
    tsFound: entry.tsFound || null,
    tsBought: entry.tsBought || Date.now(),
    latencyMs: entry.latencyMs || null,
    title: entry.title || '',
    priceTon: entry.priceTon || 0,
    urlTelegram: entry.urlTelegram || '',
    urlMarket: entry.urlMarket || '',
    giftName: entry.giftName || '',
    model: entry.model || '',
    backdrop: entry.backdrop || '',
    collection: entry.collection || '',
  });
  user.purchases = user.purchases.slice(0, 300);
}

// ===================== MRKT HTTP =====================
function mrktHeaders() {
  return {
    ...(MRKT_AUTH_RUNTIME ? { Authorization: MRKT_AUTH_RUNTIME } : {}),
    'Content-Type': 'application/json; charset=utf-8',
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
  mrktState.lastFailStatus = null;
  mrktState.lastFailEndpoint = null;
}

async function notifyAdminMrktAuthExpired(statusCode) {
  if (!ADMIN_USER_ID) return;
  const now = nowMs();
  if (now - mrktAuthLastNotifiedAt < MRKT_AUTH_NOTIFY_COOLDOWN_MS) return;
  mrktAuthLastNotifiedAt = now;

  try {
    await sendMessageSafe(
      ADMIN_USER_ID,
      `⚠️ MRKT_AUTH не работает (HTTP ${statusCode}).\nОткрой панель → Admin и вставь новый токен.`,
      { disable_web_page_preview: true }
    );
  } catch {}
}

async function markMrktFail(endpoint, status, bodyText = null) {
  mrktState.lastFailAt = nowMs();
  mrktState.lastFailEndpoint = endpoint || null;
  mrktState.lastFailStatus = status || null;
  mrktState.lastFailMsg = extractMrktErrorMessage(bodyText) || (status ? `HTTP ${status}` : 'MRKT error');

  if (status === 401 || status === 403) {
    await notifyAdminMrktAuthExpired(status);
  }
}

async function mrktPostJson(path, bodyObj) {
  if (!MRKT_AUTH_RUNTIME) return { ok: false, status: 401, data: null, text: 'NO_AUTH' };

  const reqVal = bodyObj?.req ? String(bodyObj.req) : makeReq();
  const body = { ...(bodyObj || {}), req: reqVal };
  const url = `${MRKT_API_URL}${path}${path.includes('?') ? '&' : '?'}req=${encodeURIComponent(reqVal)}`;

  const res = await fetchWithTimeout(
    url,
    { method: 'POST', headers: mrktHeaders(), body: JSON.stringify(body) },
    MRKT_TIMEOUT_MS
  ).catch(() => null);

  if (!res) {
    await markMrktFail(path, 0, 'FETCH_ERROR');
    return { ok: false, status: 0, data: null, text: 'FETCH_ERROR' };
  }

  const txt = await res.text().catch(() => '');
  let data = null;
  try { data = txt ? JSON.parse(txt) : null; } catch { data = null; }

  if (!res.ok) {
    await markMrktFail(path, res.status, txt);
    return { ok: false, status: res.status, data, text: txt };
  }

  markMrktOk();
  return { ok: true, status: res.status, data, text: txt };
}

// ===================== MRKT API =====================
async function mrktGetCollections() {
  const now = nowMs();
  if (collectionsCache.items.length && now - collectionsCache.time < CACHE_TTL_MS) return collectionsCache.items;

  const res = await fetchWithTimeout(
    `${MRKT_API_URL}/gifts/collections`,
    { method: 'GET', headers: mrktHeaders() },
    MRKT_TIMEOUT_MS
  ).catch(() => null);

  if (!res || !res.ok) {
    const txt = await res?.text?.().catch(() => '') || '';
    await markMrktFail('/gifts/collections', res?.status || 0, txt);
    collectionsCache = { time: now, items: [] };
    return [];
  }

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

function tonFromNano(nano) {
  const x = Number(nano);
  return Number.isFinite(x) ? x / 1e9 : null;
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
    const key = norm(name);
    if (!map.has(key)) {
      map.set(key, {
        name: String(name),
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

  const r = await mrktPostJson('/gifts/backdrops', { collections: [giftName] });
  if (!r.ok) return [];

  const arr = Array.isArray(r.data) ? r.data : [];
  const map = new Map();
  for (const it of arr) {
    const name = it.backdropName || it.name || null;
    if (!name) continue;
    const key = norm(name);
    if (!map.has(key)) {
      const v = it.backdropColorsCenterColor ?? it.colorsCenterColor ?? it.centerColor ?? null;
      const num = Number(v);
      const hex = Number.isFinite(num)
        ? ('#' + ((num >>> 0).toString(16).padStart(6, '0')).slice(-6))
        : null;
      map.set(key, { name: String(name), centerHex: hex || '#000000' });
    }
  }
  const items = Array.from(map.values());
  backdropsCache.set(giftName, { time: nowMs(), items });
  return items;
}

function buildSalingBody({ count = 20, cursor = '', collectionNames = [], modelNames = [], backdropNames = [], ordering = 'Price', lowToHigh = true } = {}) {
  return {
    count,
    cursor,
    collectionNames,
    modelNames,
    backdropNames,
    symbolNames: [],
    craftable: null,
    giftType: null,
    isCrafted: null,
    isNew: null,
    isPremarket: null,
    isTransferable: null,
    luckyBuy: null,
    removeSelfSales: null,
    ordering,
    lowToHigh,
    maxPrice: null,
    minPrice: null,
    number: null,
    query: null,
    tgCanBeCraftedFrom: null
  };
}

async function mrktFetchSalingPage({ collectionNames, modelNames, backdropNames, cursor, ordering, lowToHigh, count }) {
  const body = buildSalingBody({
    count: Number(count || MRKT_COUNT),
    cursor: cursor || '',
    collectionNames: Array.isArray(collectionNames) ? collectionNames : [],
    modelNames: Array.isArray(modelNames) ? modelNames : [],
    backdropNames: Array.isArray(backdropNames) ? backdrops : [],
    ordering: ordering || 'Price',
    lowToHigh: !!lowToHigh,
  });

  // BUGFIX: backdropNames mapping above
  body.backdropNames = Array.isArray(backdropNames) ? backdropNames : [];

  const r = await mrktPostJson('/gifts/saling', body);
  if (!r.ok) return { ok: false, reason: mrktState.lastFailMsg || 'SALING_ERROR', gifts: [], cursor: '' };

  const gifts = Array.isArray(r.data?.gifts) ? r.data.gifts : [];
  const nextCursor = r.data?.cursor || '';
  return { ok: true, reason: 'OK', gifts, cursor: nextCursor };
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
  if (!r.ok) {
    const out = { ok: false, reason: mrktState.lastFailMsg || 'ORDERS_ERROR', orders: [] };
    offersCache.set(key, { time: now, data: out });
    return out;
  }

  const orders = Array.isArray(r.data?.orders) ? r.data.orders : [];
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

async function mrktFeedSales({ gift, model, backdrop }) {
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
      collectionNames: gift ? [gift] : [],
      modelNames: gift && model ? [model] : [],
      backdropNames: gift && backdrop ? [backdrop] : [],
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

      const amountNano = it.amount ?? g.salePriceWithoutFee ?? g.salePrice ?? null;
      const ton = amountNano != null ? Number(amountNano) / 1e9 : NaN;
      if (!Number.isFinite(ton) || ton <= 0) continue;

      const base = (g.collectionTitle || g.collectionName || g.title || gift || 'Gift').trim();
      const number = g.number ?? null;
      const name = number != null ? `${base} #${number}` : base;

      const giftName = g.name || giftNameFallbackFromCollectionAndNumber(base, number) || null;
      const imgUrl = giftName ? fragmentGiftRemoteUrlFromGiftName(giftName) : null;
      const urlTelegram = giftName ? `https://t.me/nft/${giftName}` : 'https://t.me/mrkt';
      const urlMarket = g.id ? mrktLotUrlFromId(g.id) : 'https://t.me/mrkt';

      sales.push({
        tsMsk: it.date ? formatMskMs(it.date) : '',
        priceTon: ton,
        name,
        model: g.modelTitle || g.modelName || null,
        backdrop: g.backdropName || null,
        imgUrl,
        urlTelegram,
        urlMarket,
      });

      prices.push(ton);
      if (sales.length >= SALES_HISTORY_TARGET) break;
    }

    cursor = r.data?.cursor || '';
    pages++;
    if (!cursor) break;
    if (SALES_HISTORY_THROTTLE_MS > 0) await sleep(SALES_HISTORY_THROTTLE_MS);
  }

  prices.sort((a, b) => a - b);
  const approx = prices.length
    ? (prices.length % 2 ? prices[(prices.length - 1) / 2] : (prices[prices.length / 2 - 1] + prices[prices.length / 2]) / 2)
    : null;

  return { ok: true, approxPriceTon: approx, sales };
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
  return s.includes('not enough') || s.includes('insufficient') || s.includes('no funds') || s.includes('low balance') || s.includes('balance');
}

// Map MRKT gift -> lot
function salingGiftToLot(g) {
  const nano = (g?.salePriceWithoutFee && Number(g.salePriceWithoutFee) > 0) ? g.salePriceWithoutFee : g?.salePrice;
  const priceTon = Number(nano) / 1e9;
  if (!Number.isFinite(priceTon) || priceTon <= 0) return null;

  const numberVal = g.number ?? null;
  const baseName = (g.collectionTitle || g.collectionName || g.title || 'Gift').trim();
  const displayName = numberVal != null ? `${baseName} #${numberVal}` : baseName;

  const giftName = g.name || giftNameFallbackFromCollectionAndNumber(baseName, numberVal) || null;
  const urlTelegram = giftName ? `https://t.me/nft/${giftName}` : 'https://t.me/mrkt';
  const urlMarket = g.id ? mrktLotUrlFromId(g.id) : 'https://t.me/mrkt';

  const listedAt = g.receivedDate || null; // best-effort

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
    collectionName: g.collectionName || g.collectionTitle || g.title || null,
    listedAt,
  };
}

async function mrktSearchLotsByFilters(filters, pagesLimit) {
  const f = normalizeFilters(filters || {});
  const prefix = f.numberPrefix;
  const pagesToScan = Math.max(1, Number(pagesLimit || 1));
  let cursor = '';
  const out = [];

  for (let page = 0; page < pagesToScan; page++) {
    const r = await mrktFetchSalingPage({
      collectionNames: f.gifts,
      modelNames: f.models,
      backdropNames: f.backdrops,
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
    if (MRKT_THROTTLE_MS > 0) await sleep(MRKT_THROTTLE_MS);
  }

  out.sort((a, b) => a.priceTon - b.priceTon);
  return { ok: true, reason: 'OK', gifts: out };
}

// ===================== Chat notifications (NO sendPhoto) =====================
function buildChatTextWithPreview({
  title,
  priceTon,
  model,
  backdrop,
  giftName,
  urlTelegram,
  urlMarket,
  listedAtMs,
  foundAtMs,
  boughtAtMs,
  latencyMs
}) {
  const lines = [];
  lines.push(title);
  lines.push(`Цена: ${priceTon.toFixed(3)} TON`);
  if (model) lines.push(`Model: ${model}`);
  if (backdrop) lines.push(`Backdrop: ${backdrop}`);

  if (listedAtMs) lines.push(`Listed: ${formatMskMs(listedAtMs)}`);
  if (foundAtMs) lines.push(`Found: ${formatMskMs(foundAtMs)}`);
  if (boughtAtMs) lines.push(`Buy: ${formatMskMs(boughtAtMs)}`);
  if (latencyMs != null) lines.push(`Latency: ${latencyMs} ms`);

  // This line gives Telegram preview image
  if (giftName) {
    const imgUrl = fragmentGiftRemoteUrlFromGiftName(giftName);
    if (imgUrl) lines.push(imgUrl);
  }

  if (urlTelegram) lines.push(`NFT: ${urlTelegram}`);
  if (urlMarket) lines.push(`MRKT: ${urlMarket}`);

  return lines.join('\n');
}

// ===================== Subs notifier =====================
async function notifyFloorToUser(userId, sub, lot) {
  const chatId = userChatId(userId);

  const listedAtMs = lot?.listedAt ? Date.parse(lot.listedAt) : null;
  const now = Date.now();

  const text = buildChatTextWithPreview({
    title: `Подписка #${sub.num} — ${lot.name}`,
    priceTon: lot.priceTon,
    model: lot.model,
    backdrop: lot.backdrop,
    giftName: lot.giftName,
    urlTelegram: lot.urlTelegram,
    urlMarket: lot.urlMarket,
    listedAtMs,
    foundAtMs: now,
  });

  await sendMessageSafe(chatId, text, { disable_web_page_preview: false });
}

async function checkSubscriptionsForAllUsers({ manual = false } = {}) {
  if (MODE !== 'real') return;
  if (isSubsChecking && !manual) return;

  isSubsChecking = true;
  try {
    let floorNotifs = 0;

    for (const [userId, user] of users.entries()) {
      if (!user.enabled) continue;

      const subs = Array.isArray(user.subscriptions) ? user.subscriptions : [];
      const active = subs.filter((s) => s && s.enabled);
      if (!active.length) continue;

      for (const sub of active) {
        if (floorNotifs >= SUBS_MAX_NOTIFICATIONS_PER_CYCLE) break;

        const sf = normalizeFilters(sub.filters || {});
        if (!sf.gifts.length) continue;

        const stateKey = `${userId}:${sub.id}`;
        const prev = subStates.get(stateKey) || { floor: null, emptyStreak: 0, pausedUntil: 0 };

        const r = await mrktFetchSalingPage({
          collectionNames: sf.gifts,
          modelNames: sf.models,
          backdropNames: sf.backdrops,
          cursor: '',
          ordering: 'Price',
          lowToHigh: true,
          count: 20,
        });
        if (!r.ok) continue;

        const lot = r.gifts?.length ? salingGiftToLot(r.gifts[0]) : null;
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
          await notifyFloorToUser(userId, sub, lot);
          floorNotifs++;
        }

        subStates.set(stateKey, { ...prev, floor: newFloor, emptyStreak });
      }
    }
  } finally {
    isSubsChecking = false;
  }
}

// ===================== AutoBuy =====================
async function autoBuyCycle() {
  if (!AUTO_BUY_GLOBAL) return;
  if (MODE !== 'real') return;
  if (!MRKT_AUTH_RUNTIME) return;
  if (isAutoBuying) return;

  isAutoBuying = true;

  autoBuyDebug.lastAt = Date.now();
  autoBuyDebug.eligible = 0;
  autoBuyDebug.scanned = 0;
  autoBuyDebug.candidates = 0;
  autoBuyDebug.buys = 0;
  autoBuyDebug.lastReason = '';

  try {
    for (const [userId, user] of users.entries()) {
      const subs = Array.isArray(user.subscriptions) ? user.subscriptions : [];
      const eligible = subs.filter((s) => s && s.enabled && s.autoBuyEnabled && s.maxAutoBuyTon != null && (s.filters?.gifts?.length || s.filters?.gift));

      autoBuyDebug.eligible += eligible.length;
      if (!eligible.length) continue;

      let buysDone = 0;

      for (const sub of eligible) {
        if (buysDone >= AUTO_BUY_MAX_BUYS_PER_USER_PER_CYCLE) break;

        const maxBuy = Number(sub.maxAutoBuyTon);
        if (!Number.isFinite(maxBuy) || maxBuy <= 0) { autoBuyDebug.lastReason = 'bad maxAutoBuyTon'; continue; }

        const stateKey = `${userId}:${sub.id}`;
        const st = subStates.get(stateKey) || { floor: null, emptyStreak: 0, pausedUntil: 0 };
        if (st.pausedUntil && nowMs() < st.pausedUntil) continue;

        const sf = normalizeFilters(sub.filters || {});
        if (!sf.gifts.length) continue;

        const foundAt = Date.now();

        const r = await mrktFetchSalingPage({
          collectionNames: sf.gifts,
          modelNames: sf.models,
          backdropNames: sf.backdrops,
          cursor: '',
          ordering: 'Price',
          lowToHigh: true,
          count: 20,
        });
        if (!r.ok) { autoBuyDebug.lastReason = `saling error: ${r.reason}`; continue; }

        autoBuyDebug.scanned += (r.gifts || []).length;

        const prefix = sf.numberPrefix || '';
        let candidate = null;

        for (const g of r.gifts || []) {
          const lot = salingGiftToLot(g);
          if (!lot) continue;

          if (prefix) {
            const numStr = lot.number == null ? '' : String(lot.number);
            if (!numStr.startsWith(prefix)) continue;
          }

          if (lot.priceTon <= maxBuy) { candidate = lot; break; }
        }

        if (!candidate) { autoBuyDebug.lastReason = 'no candidate <= max'; continue; }

        autoBuyDebug.candidates++;

        const attemptKey = `${userId}:${candidate.id}`;
        const lastAttempt = autoBuyRecentAttempts.get(attemptKey);
        if (lastAttempt && nowMs() - lastAttempt < AUTO_BUY_ATTEMPT_TTL_MS) { autoBuyDebug.lastReason = 'attempt ttl'; continue; }
        autoBuyRecentAttempts.set(attemptKey, nowMs());

        const chatId = userChatId(userId);
        const listedAtMs = candidate.listedAt ? Date.parse(candidate.listedAt) : null;

        if (AUTO_BUY_DRY_RUN) {
          const text = buildChatTextWithPreview({
            title: `AutoBuy (DRY) — Подписка #${sub.num} — ${candidate.name}`,
            priceTon: candidate.priceTon,
            model: candidate.model,
            backdrop: candidate.backdrop,
            giftName: candidate.giftName,
            urlTelegram: candidate.urlTelegram,
            urlMarket: candidate.urlMarket,
            listedAtMs,
            foundAtMs: foundAt,
          });

          await sendMessageSafe(chatId, text, { disable_web_page_preview: false });
          autoBuyDebug.buys++;
          buysDone++;
          continue;
        }

        const buyStart = Date.now();
        const buyRes = await mrktBuy({ id: candidate.id, priceNano: candidate.priceNano });
        const boughtAt = Date.now();
        const latencyMs = boughtAt - buyStart;

        if (buyRes.ok) {
          pushPurchase(getOrCreateUser(userId), {
            tsListed: listedAtMs || null,
            tsFound: foundAt,
            tsBought: boughtAt,
            latencyMs,
            title: candidate.name,
            priceTon: candidate.priceTon,
            urlTelegram: candidate.urlTelegram,
            urlMarket: candidate.urlMarket,
            giftName: candidate.giftName || '',
            model: candidate.model || '',
            backdrop: candidate.backdrop || '',
            collection: candidate.collectionName || '',
          });
          scheduleSave();

          const text = buildChatTextWithPreview({
            title: `AutoBuy OK — Подписка #${sub.num} — ${candidate.name}`,
            priceTon: candidate.priceTon,
            model: candidate.model,
            backdrop: candidate.backdrop,
            giftName: candidate.giftName,
            urlTelegram: candidate.urlTelegram,
            urlMarket: candidate.urlMarket,
            listedAtMs,
            foundAtMs: foundAt,
            boughtAtMs: boughtAt,
            latencyMs,
          });

          await sendMessageSafe(chatId, text, { disable_web_page_preview: false });

          autoBuyDebug.buys++;
          buysDone++;

          if (AUTO_BUY_DISABLE_AFTER_SUCCESS) {
            sub.autoBuyEnabled = false;
            scheduleSave();
          }
        } else {
          if (isNoFundsError(buyRes)) {
            for (const s2 of subs) if (s2) s2.autoBuyEnabled = false;
            subStates.set(stateKey, { ...st, pausedUntil: nowMs() + AUTO_BUY_NO_FUNDS_PAUSE_MS });
            scheduleSave();
            autoBuyDebug.lastReason = 'no funds';
          } else {
            autoBuyDebug.lastReason = `buy error: ${buyRes.reason}`;
            await sendMessageSafe(chatId, `AutoBuy FAIL — Подписка #${sub.num}\n${buyRes.reason}`, { disable_web_page_preview: true });
          }
        }

        if (MRKT_THROTTLE_MS > 0) await sleep(MRKT_THROTTLE_MS);
      }
    }
  } catch (e) {
    autoBuyDebug.lastReason = `crash: ${String(e?.message || e).slice(0, 160)}`;
    console.error('autoBuyCycle error:', e);
  } finally {
    isAutoBuying = false;
  }
}

// ===================== Telegram handlers =====================
bot.onText(/^\/start\b/, async (msg) => {
  const u = getOrCreateUser(msg.from.id);
  u.chatId = msg.chat.id;
  scheduleSave();

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
  const userId = msg.from?.id;
  if (!userId) return;

  const u = getOrCreateUser(userId);
  u.chatId = msg.chat.id;
  scheduleSave();

  if ((msg.text || '') === '📌 Статус') {
    const txt =
      `Status:\n` +
      `• MRKT_AUTH: ${MRKT_AUTH_RUNTIME ? 'YES' : 'NO'}\n` +
      `• Redis: ${redis ? 'YES' : 'NO'}\n` +
      `• AutoBuy: ${AUTO_BUY_GLOBAL ? 'ON' : 'OFF'}${AUTO_BUY_DRY_RUN ? ' (DRY)' : ''}\n` +
      `• MRKT fail: ${mrktState.lastFailMsg || '-'}\n`;
    await sendMessageSafe(msg.chat.id, txt, { reply_markup: MAIN_KEYBOARD });
  }
});

// ===================== Express web =====================
const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '2mb' }));

// static public folder
app.use(express.static(path.join(__dirname, 'public')));
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

// image proxy: MRKT CDN
app.get('/img/cdn', async (req, res) => {
  const key = String(req.query.key || '').trim();
  if (!key) return res.status(400).send('no key');
  const url = joinUrl(MRKT_CDN_BASE, key);

  const r = await fetchWithTimeout(url, { method: 'GET', headers: { Accept: 'image/*' } }, 8000).catch(() => null);
  if (!r || !r.ok) return res.status(404).send('not found');
  streamFetchToRes(r, res, 'image/webp');
});

// API state
app.get('/api/state', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const mask = MRKT_AUTH_RUNTIME ? MRKT_AUTH_RUNTIME.slice(0, 4) + '…' + MRKT_AUTH_RUNTIME.slice(-4) : '';

  res.json({
    ok: true,
    api: { mrktAuthSet: !!MRKT_AUTH_RUNTIME, isAdmin: isAdmin(req.userId), mrktAuthMask: mask },
    user: { enabled: !!u.enabled, filters: u.filters, subscriptions: u.subscriptions || [] }
  });
});

app.post('/api/state/patch', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const b = req.body || {};
  if (b.filters && typeof b.filters === 'object') {
    const next = normalizeFilters(b.filters);

    // keep labels only for selected gifts
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
    const floorTon = x.floorNano != null ? tonFromNano(x.floorNano) : null;
    return {
      label: x.title || x.name,
      value: x.name,
      imgUrl: x.thumbKey ? `/img/cdn?key=${encodeURIComponent(x.thumbKey)}` : null,
      sub: floorTon != null ? `floor: ${floorTon.toFixed(3)} TON` : null
    };
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

// lots
app.get('/api/mrkt/lots', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const r = await mrktSearchLotsByFilters(u.filters || {}, WEBAPP_LOTS_PAGES);
  if (!r.ok) return res.json({ ok: false, reason: r.reason, lots: [] });

  const lots = (r.gifts || []).slice(0, WEBAPP_LOTS_LIMIT).map((lot) => {
    const imgUrl = lot.giftName ? fragmentGiftRemoteUrlFromGiftName(lot.giftName) : null;
    const listedMsk = lot.listedAt ? formatMskMs(Date.parse(lot.listedAt)) : null;
    return { ...lot, imgUrl, listedMsk };
  });

  res.json({ ok: true, lots });
});

// max offer current
app.get('/api/mrkt/maxoffer_current', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const f = normalizeFilters(u.filters || {});
  if (f.gifts.length !== 1) {
    return res.json({ ok: true, exactMaxOfferTon: null, collectionMaxOfferTon: null, note: 'Max offer работает только для 1 gift' });
  }
  const gift = f.gifts[0];
  const exact = await mrktOrdersFetch({ gift, model: f.models[0] || null, backdrop: f.backdrops[0] || null });
  const col = await mrktOrdersFetch({ gift, model: null, backdrop: null });

  res.json({
    ok: true,
    exactMaxOfferTon: exact.ok ? maxOfferTonFromOrders(exact.orders) : null,
    collectionMaxOfferTon: col.ok ? maxOfferTonFromOrders(col.orders) : null,
    note: null
  });
});

// sales history current
app.get('/api/mrkt/sales_history_current', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const f = normalizeFilters(u.filters || {});
  const gift = f.gifts.length === 1 ? f.gifts[0] : null;
  const model = gift && f.models.length === 1 ? f.models[0] : null;
  const backdrop = gift && f.backdrops.length === 1 ? f.backdrops[0] : null;

  const r = await mrktFeedSales({ gift, model, backdrop });
  res.json(r);
});

// lot details
app.post('/api/lot/details', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const id = String(req.body?.id || '').trim();
  if (!id) return res.status(400).json({ ok: false, reason: 'NO_ID' });

  const f = normalizeFilters(u.filters || {});

  let exactLot = null;
  let colLot = null;
  let offersExact = { ok: true, maxOfferTon: null };
  let offersCollection = { ok: true, maxOfferTon: null };

  if (f.gifts.length === 1) {
    const exact = await mrktSearchLotsByFilters(f, 1);
    const col = await mrktSearchLotsByFilters({ gifts: f.gifts, giftLabels: f.giftLabels, models: [], backdrops: [], numberPrefix: '' }, 1);
    exactLot = exact.ok ? (exact.gifts[0] || null) : null;
    colLot = col.ok ? (col.gifts[0] || null) : null;

    const gift = f.gifts[0];
    const o1 = await mrktOrdersFetch({ gift, model: f.models[0] || null, backdrop: f.backdrops[0] || null });
    offersExact = { ok: o1.ok, maxOfferTon: o1.ok ? maxOfferTonFromOrders(o1.orders) : null };
    const o2 = await mrktOrdersFetch({ gift, model: null, backdrop: null });
    offersCollection = { ok: o2.ok, maxOfferTon: o2.ok ? maxOfferTonFromOrders(o2.orders) : null };
  }

  const gift = f.gifts.length === 1 ? f.gifts[0] : null;
  const model = gift && f.models.length === 1 ? f.models[0] : null;
  const backdrop = gift && f.backdrops.length === 1 ? f.backdrops[0] : null;
  const salesHistory = await mrktFeedSales({ gift, model, backdrop });

  res.json({
    ok: true,
    floors: {
      exact: exactLot ? { priceTon: exactLot.priceTon } : null,
      collection: colLot ? { priceTon: colLot.priceTon } : null,
    },
    offers: { exact: offersExact, collection: offersCollection },
    salesHistory
  });
});

// buy from webapp
app.post('/api/mrkt/buy', auth, async (req, res) => {
  const id = String(req.body?.id || '').trim();
  const priceNano = Number(req.body?.priceNano);
  if (!id) return res.status(400).json({ ok: false, reason: 'NO_ID' });
  if (!Number.isFinite(priceNano) || priceNano <= 0) return res.status(400).json({ ok: false, reason: 'BAD_PRICE' });

  const foundAt = Date.now();
  const buyStart = Date.now();
  const r = await mrktBuy({ id, priceNano });
  const boughtAt = Date.now();
  const latencyMs = boughtAt - buyStart;

  if (!r.ok) return res.status(502).json({ ok: false, reason: r.reason });

  const g = r.okItem?.userGift || null;
  const title = `${g?.collectionTitle || g?.collectionName || g?.title || 'Gift'}${g?.number != null ? ` #${g.number}` : ''}`;
  const priceTon = priceNano / 1e9;
  const giftName = g?.name || giftNameFallbackFromCollectionAndNumber(g?.collectionTitle || g?.collectionName || g?.title || '', g?.number) || '';
  const urlTelegram = giftName ? `https://t.me/nft/${giftName}` : '';
  const urlMarket = mrktLotUrlFromId(id);

  const u = getOrCreateUser(req.userId);
  pushPurchase(u, {
    tsListed: null,
    tsFound: foundAt,
    tsBought: boughtAt,
    latencyMs,
    title,
    priceTon,
    urlTelegram,
    urlMarket,
    giftName,
    model: g?.modelTitle || g?.modelName || '',
    backdrop: g?.backdropName || '',
    collection: g?.collectionTitle || g?.collectionName || '',
  });
  scheduleSave();

  res.json({ ok: true, title, priceTon });
});

// profile
app.get('/api/profile', auth, async (req, res) => {
  const u = getOrCreateUser(req.userId);
  const purchases = (u.purchases || []).map((p) => ({
    title: p.title,
    priceTon: p.priceTon,
    listedMsk: p.tsListed ? formatMskMs(p.tsListed) : null,
    foundMsk: p.tsFound ? formatMskMs(p.tsFound) : null,
    boughtMsk: p.tsBought ? formatMskMs(p.tsBought) : null,
    latencyMs: p.latencyMs ?? null,
    imgUrl: p.giftName ? fragmentGiftRemoteUrlFromGiftName(p.giftName) : null,
    urlTelegram: p.urlTelegram || (p.giftName ? `https://t.me/nft/${p.giftName}` : null),
    urlMarket: p.urlMarket || null,
    model: p.model || null,
    backdrop: p.backdrop || null
  }));
  res.json({ ok: true, user: req.tgUser, purchases });
});

// subscriptions API
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

  const raw = req.body?.maxNotifyTon;
  const str = String(raw ?? '').trim();
  if (!str) {
    s.maxNotifyTon = null;
    await persistNow().catch(() => scheduleSave());
    return res.json({ ok: true });
  }

  const v = parseTonInput(str);
  if (!Number.isFinite(v) || v <= 0) return res.status(400).json({ ok: false, reason: 'BAD_MAX' });

  s.maxNotifyTon = v;
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

  const v = parseTonInput(req.body?.maxAutoBuyTon);
  if (!Number.isFinite(v) || v <= 0) return res.status(400).json({ ok: false, reason: 'BAD_MAX' });

  s.maxAutoBuyTon = v;
  await persistNow().catch(() => scheduleSave());
  res.json({ ok: true });
});

// admin
app.get('/api/admin/status', auth, async (req, res) => {
  if (!isAdmin(req.userId)) return res.status(403).json({ ok: false, reason: 'NOT_ADMIN' });
  const mask = MRKT_AUTH_RUNTIME ? MRKT_AUTH_RUNTIME.slice(0, 4) + '…' + MRKT_AUTH_RUNTIME.slice(-4) : '';
  res.json({
    ok: true,
    mrktAuthMask: mask,
    mrktLastFailMsg: mrktState.lastFailMsg || null,
    mrktLastFailStatus: mrktState.lastFailStatus || null,
    mrktLastFailEndpoint: mrktState.lastFailEndpoint || null,
    autoBuy: { ...autoBuyDebug },
  });
});

app.post('/api/admin/mrkt_auth', auth, async (req, res) => {
  if (!isAdmin(req.userId)) return res.status(403).json({ ok: false, reason: 'NOT_ADMIN' });
  const t = String(req.body?.token || '').trim();
  if (!t) return res.status(400).json({ ok: false, reason: 'EMPTY_TOKEN' });

  MRKT_AUTH_RUNTIME = t;
  if (redis) {
    try { await saveMrktAuthToRedis(t); } catch {}
  }

  // drop caches
  collectionsCache = { time: 0, items: [] };
  modelsCache.clear();
  backdropsCache.clear();
  offersCache.clear();

  res.json({ ok: true });
});

// start server
const port = Number(process.env.PORT || 3000);
app.listen(port, '0.0.0.0', () => console.log('Web listening on', port));

// ===================== workers =====================
setInterval(() => { checkSubscriptionsForAllUsers().catch((e) => console.error('subs error:', e)); }, SUBS_CHECK_INTERVAL_MS);
setInterval(() => { autoBuyCycle().catch((e) => console.error('autobuy error:', e)); }, AUTO_BUY_CHECK_INTERVAL_MS);

// ===================== bootstrap =====================
(async () => {
  if (REDIS_URL) {
    await initRedis();
    if (redis) {
      await loadMrktAuthFromRedis();
      await loadState();
    }
  } else {
    console.warn('REDIS_URL not set => state not persistent');
  }
  console.log('Bot started. /start');
})();
