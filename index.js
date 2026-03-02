/**
 * MRKT Panel + Bot (single file)
 * v12-full (2026-03-02)
 *
 * Restored:
 * - Admin token update field (MRKT_AUTH) without restart (stored in Redis if set)
 * - Purchases history + milliseconds timestamps + latency
 * - Lot click -> nice bottom sheet with Buy/MRKT/NFT + Max Offer + Sales history
 * - Section loaders (not full-screen)
 * - Subscriptions messages + AutoBuy messages with photos
 *
 * Node 18+
 * deps: express, node-telegram-bot-api, (optional) redis
 */

const TelegramBot = require('node-telegram-bot-api');
const express = require('express');
const crypto = require('crypto');
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
let MRKT_AUTH_RUNTIME = (process.env.MRKT_AUTH || '').trim() || null;

// Intervals
const SUBS_CHECK_INTERVAL_MS = Number(process.env.SUBS_CHECK_INTERVAL_MS || 9000);

// AutoBuy
const AUTO_BUY_GLOBAL = String(process.env.AUTO_BUY_GLOBAL || '1') === '1';
const AUTO_BUY_DRY_RUN = String(process.env.AUTO_BUY_DRY_RUN || '1') === '1'; // safe default
const AUTO_BUY_CHECK_INTERVAL_MS = Number(process.env.AUTO_BUY_CHECK_INTERVAL_MS || 2500);
const AUTO_BUY_MAX_BUYS_PER_USER_PER_CYCLE = Number(process.env.AUTO_BUY_MAX_BUYS_PER_USER_PER_CYCLE || 1);
const AUTO_BUY_ATTEMPT_TTL_MS = Number(process.env.AUTO_BUY_ATTEMPT_TTL_MS || 60_000); // 1m
const AUTO_BUY_NO_FUNDS_PAUSE_MS = Number(process.env.AUTO_BUY_NO_FUNDS_PAUSE_MS || 10 * 60 * 1000);
const AUTO_BUY_DISABLE_AFTER_SUCCESS = String(process.env.AUTO_BUY_DISABLE_AFTER_SUCCESS || '0') === '1';

// Requests
const MRKT_TIMEOUT_MS = Number(process.env.MRKT_TIMEOUT_MS || 10000);
const MRKT_COUNT = Number(process.env.MRKT_COUNT || 20);
const MRKT_THROTTLE_MS = Number(process.env.MRKT_THROTTLE_MS || 120);
const MRKT_ORDERS_COUNT = Number(process.env.MRKT_ORDERS_COUNT || 30);

// WebApp limits
const WEBAPP_LOTS_LIMIT = Number(process.env.WEBAPP_LOTS_LIMIT || 40);
const WEBAPP_LOTS_PAGES = Number(process.env.WEBAPP_LOTS_PAGES || 1);
const WEBAPP_SUGGEST_LIMIT = Number(process.env.WEBAPP_SUGGEST_LIMIT || 80);

// Subs behavior
const SUBS_MAX_NOTIFICATIONS_PER_CYCLE = Number(process.env.SUBS_MAX_NOTIFICATIONS_PER_CYCLE || 8);
const SUBS_EMPTY_CONFIRM = Number(process.env.SUBS_EMPTY_CONFIRM || 2);

// Sales history (MRKT feed)
const SALES_HISTORY_TARGET = Number(process.env.SALES_HISTORY_TARGET || 30);
const SALES_HISTORY_MAX_PAGES = Number(process.env.SALES_HISTORY_MAX_PAGES || 20);
const SALES_HISTORY_COUNT_PER_PAGE = Number(process.env.SALES_HISTORY_COUNT_PER_PAGE || 50);
const SALES_HISTORY_THROTTLE_MS = Number(process.env.SALES_HISTORY_THROTTLE_MS || 120);
const SALES_HISTORY_TIME_BUDGET_MS = Number(process.env.SALES_HISTORY_TIME_BUDGET_MS || 15000);

// Redis keys
const REDIS_KEY_MRKT_AUTH = 'mrkt:auth:token';
const REDIS_KEY_STATE = 'bot:state:main';

console.log('v12-full start', {
  MODE,
  WEBAPP_URL: !!WEBAPP_URL,
  ADMIN_USER_ID,
  REDIS: !!REDIS_URL,
  MRKT_AUTH: !!MRKT_AUTH_RUNTIME,
  AUTO_BUY_GLOBAL,
  AUTO_BUY_DRY_RUN,
});

// ===================== Telegram bot =====================
const bot = new TelegramBot(token, { polling: true });
const MAIN_KEYBOARD = { keyboard: [[{ text: '📌 Статус' }]], resize_keyboard: true };

// ===================== State =====================
const users = new Map(); // userId -> user
const subStates = new Map(); // `${userId}:${subId}` -> { floor, emptyStreak, pausedUntil }

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
const modelsCache = new Map();
const backdropsCache = new Map();

const offersCache = new Map();
const OFFERS_CACHE_TTL_MS = 15_000;

const thumbsCache = new Map();
const THUMBS_CACHE_TTL_MS = 5 * 60_000;

// ===================== Helpers =====================
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const nowMs = () => Date.now();

function makeReq() {
  return `${Date.now()}_${Math.random().toString(16).slice(2)}`;
}
function norm(s) { return String(s || '').toLowerCase().trim().replace(/\s+/g, ' '); }
function normTraitName(s) { return norm(s).replace(/\s*\([^)]*%[^)]*\)\s*/g, ' ').replace(/\s+/g, ' ').trim(); }
function sameTrait(actual, expectedLower) { if (!expectedLower) return true; return normTraitName(actual) === normTraitName(expectedLower); }
function parseTonInput(x) {
  const s = String(x ?? '').trim();
  if (!s) return NaN;
  const cleaned = s.replace(',', '.').replace(':', '.').replace(/\s+/g, '');
  const v = Number(cleaned);
  return Number.isFinite(v) ? v : NaN;
}
function cleanDigitsPrefix(v, maxLen = 12) { return String(v || '').replace(/\D/g, '').slice(0, maxLen); }
function joinUrl(base, key) {
  const b = String(base || '').endsWith('/') ? String(base) : String(base) + '/';
  const k = String(key || '').startsWith('/') ? String(key).slice(1) : String(key);
  return b + k;
}
function tonFromNano(nano) { const x = Number(nano); return Number.isFinite(x) ? x / 1e9 : null; }
function mrktLotUrlFromId(id) { if (!id) return 'https://t.me/mrkt'; return `https://t.me/mrkt/app?startapp=${String(id).replace(/-/g, '')}`; }

function fragmentGiftRemoteUrlFromGiftName(giftName) {
  if (!giftName) return null;
  const slug = String(giftName).trim().toLowerCase();
  return joinUrl(FRAGMENT_GIFT_IMG_BASE, `${encodeURIComponent(slug)}.medium.jpg`);
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

// time with ms (MSK)
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

async function sendPhotoSafe(chatId, photoUrl, caption, opts = {}) {
  while (true) {
    try {
      return await bot.sendPhoto(chatId, photoUrl, {
        caption,
        parse_mode: opts.parse_mode,
        reply_markup: opts.reply_markup,
      });
    } catch (e) {
      const retryAfter =
        e?.response?.body?.parameters?.retry_after ??
        e?.response?.parameters?.retry_after ??
        null;
      if (retryAfter) { await sleep((Number(retryAfter) + 1) * 1000); continue; }
      // fallback to text if photo fails
      try {
        return await sendMessageSafe(chatId, caption, { reply_markup: opts.reply_markup, disable_web_page_preview: false });
      } catch {}
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
      chatId: u?.chatId ?? null,
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
      chatId: u?.chatId ?? null,
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
      tsFound: p.tsFound || null,
      tsBought: p.tsBought || p.ts || null,
      latencyMs: p.latencyMs || null,
      title: String(p.title || ''),
      priceTon: Number(p.priceTon || 0),
      urlTelegram: String(p.urlTelegram || ''),
      urlMarket: String(p.urlMarket || ''),
      giftName: String(p.giftName || ''),
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

// ===================== User state =====================
function getOrCreateUser(userId) {
  if (!users.has(userId)) {
    users.set(userId, {
      enabled: true,
      chatId: null,
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
    tsFound: entry.tsFound || null,
    tsBought: entry.tsBought || Date.now(),
    latencyMs: entry.latencyMs || null,
    title: entry.title || '',
    priceTon: entry.priceTon || 0,
    urlTelegram: entry.urlTelegram || '',
    urlMarket: entry.urlMarket || '',
    giftName: entry.giftName || '',
  });
  user.purchases = user.purchases.slice(0, 200);
}

function userChatId(userId) {
  const u = users.get(userId);
  const cid = u?.chatId;
  if (cid && Number.isFinite(Number(cid))) return Number(cid);
  return userId; // fallback
}

// ===================== MRKT headers + status =====================
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
async function markMrktFail(endpoint, status, bodyText = null) {
  mrktState.lastFailAt = nowMs();
  mrktState.lastFailEndpoint = endpoint || null;
  mrktState.lastFailStatus = status || null;
  mrktState.lastFailMsg = extractMrktErrorMessage(bodyText) || (status ? `HTTP ${status}` : 'MRKT error');
}

// add req in body + query (safe)
async function mrktPostJson(path, bodyObj) {
  if (!MRKT_AUTH_RUNTIME) return { ok: false, status: 401, data: null, text: 'NO_AUTH' };

  const reqVal = bodyObj?.req ? String(bodyObj.req) : makeReq();
  const body = { ...(bodyObj || {}), req: reqVal };
  const url = `${MRKT_API_URL}${path}${path.includes('?') ? '&' : '?'}req=${encodeURIComponent(reqVal)}`;

  const res = await fetchWithTimeout(url, { method: 'POST', headers: mrktHeaders(), body: JSON.stringify(body) }, MRKT_TIMEOUT_MS).catch(() => null);
  if (!res) { await markMrktFail(path, 0, 'FETCH_ERROR'); return { ok: false, status: 0, data: null, text: 'FETCH_ERROR' }; }

  const txt = await res.text().catch(() => '');
  let data = null;
  try { data = txt ? JSON.parse(txt) : null; } catch { data = null; }

  if (!res.ok) { await markMrktFail(path, res.status, txt); return { ok: false, status: res.status, data, text: txt }; }

  markMrktOk();
  return { ok: true, status: res.status, data, text: txt };
}

// ===================== MRKT API =====================
async function mrktGetCollections() {
  const now = nowMs();
  if (collectionsCache.items.length && now - collectionsCache.time < CACHE_TTL_MS) return collectionsCache.items;

  const res = await fetchWithTimeout(`${MRKT_API_URL}/gifts/collections`, { method: 'GET', headers: mrktHeaders() }, MRKT_TIMEOUT_MS).catch(() => null);
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

  const r = await mrktPostJson('/gifts/backdrops', { collections: [giftName] });
  if (!r.ok) return [];

  const arr = Array.isArray(r.data) ? r.data : [];
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
          const v = it.backdropColorsCenterColor ?? it.colorsCenterColor ?? it.centerColor ?? null;
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

// payload close to DevTools
function buildSalingBody({
  count = 20,
  cursor = '',
  collectionNames = [],
  modelNames = [],
  backdropNames = [],
  symbolNames = [],
  ordering = 'Price',
  lowToHigh = true,
  maxPrice = null,
  minPrice = null,
  number = null,
  query = null,
  removeSelfSales = null,
} = {}) {
  return {
    count,
    cursor,
    collectionNames,
    modelNames,
    backdropNames,
    symbolNames,

    craftable: null,
    giftType: null,
    isCrafted: null,
    isNew: null,
    isPremarket: null,
    isTransferable: null,
    luckyBuy: null,
    removeSelfSales,

    ordering,
    lowToHigh,

    maxPrice,
    minPrice,
    number,
    query,

    tgCanBeCraftedFrom: null,
    isOnSale: null,
    isOnAuction: null,
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
  // Best effort. If /feed changes again -> return empty.
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

      sales.push({
        ts: it.date || null,
        tsMsk: it.date ? formatMskMs(it.date) : '',
        priceTon: ton,
        name,
        model: g.modelTitle || g.modelName || null,
        backdrop: g.backdropName || null,
        number,
        imgUrl,
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
  const approx = prices.length ? (prices.length % 2 ? prices[(prices.length - 1) / 2] : (prices[prices.length / 2 - 1] + prices[prices.length / 2]) / 2) : null;

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
    symbol: g.symbolName || null,
    number: numberVal,
    collectionName: g.collectionName || g.collectionTitle || g.title || null,
  };
}

// Search lots by filters
async function mrktSearchLotsByFilters({ gift, model, backdrop, numberPrefix }, pagesLimit) {
  const prefix = cleanDigitsPrefix(numberPrefix || '');
  const pagesToScan = Math.max(1, Number(pagesLimit || 1));
  let cursor = '';
  const out = [];

  const collectionNames = gift ? [gift] : [];
  const modelNames = (gift && model) ? [model] : [];
  const backdropNames = (gift && backdrop) ? [backdrop] : [];

  for (let page = 0; page < pagesToScan; page++) {
    const r = await mrktFetchSalingPage({
      collectionNames,
      modelNames,
      backdropNames,
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

// ===================== Subs notifier =====================
async function notifyFloorToUser(userId, sub, lot, newFloor) {
  const chatId = userChatId(userId);

  const ts = Date.now();
  const caption =
    `Подписка #${sub.num}\n` +
    `${lot?.name || sub.filters.gift}\n` +
    `Цена: ${newFloor.toFixed(3)} TON\n` +
    (lot?.model ? `Model: ${lot.model}\n` : '') +
    (lot?.backdrop ? `Backdrop: ${lot.backdrop}\n` : '') +
    `Время: ${formatMskMs(ts)}\n`;

  const img = lot?.giftName ? fragmentGiftRemoteUrlFromGiftName(lot.giftName) : null;

  const reply_markup = lot?.urlMarket ? {
    inline_keyboard: [[
      { text: 'MRKT', url: lot.urlMarket },
      { text: 'NFT', url: lot.urlTelegram || 'https://t.me/mrkt' },
    ]]
  } : undefined;

  if (img) {
    await sendPhotoSafe(chatId, img, caption.trim(), { reply_markup });
  } else {
    await sendMessageSafe(chatId, caption.trim(), { disable_web_page_preview: false, reply_markup });
  }
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
        if (!sub.filters.gift) continue;

        const stateKey = `${userId}:${sub.id}`;
        const prev = subStates.get(stateKey) || { floor: null, emptyStreak: 0, pausedUntil: 0 };

        const r = await mrktFetchSalingPage({
          collectionNames: [sub.filters.gift],
          modelNames: sub.filters.model ? [sub.filters.model] : [],
          backdropNames: sub.filters.backdrop ? [sub.filters.backdrop] : [],
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

// ===================== AutoBuy (cheapest lot <= max) =====================
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
      const eligible = subs.filter((s) => s && s.enabled && s.autoBuyEnabled && s.maxAutoBuyTon != null && s.filters.gift);

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
        const st = subStates.get(stateKey) || { floor: null, emptyStreak: 0, pausedUntil: 0 };
        if (st.pausedUntil && nowMs() < st.pausedUntil) continue;

        const foundAt = Date.now();

        const r = await mrktFetchSalingPage({
          collectionNames: [sub.filters.gift],
          modelNames: sub.filters.model ? [sub.filters.model] : [],
          backdropNames: sub.filters.backdrop ? [sub.filters.backdrop] : [],
          cursor: '',
          ordering: 'Price',
          lowToHigh: true,
          count: 20,
        });

        if (!r.ok) { autoBuyDebug.lastReason = `saling error: ${r.reason}`; continue; }

        autoBuyDebug.scanned += (r.gifts || []).length;

        const prefix = cleanDigitsPrefix(sub.filters.numberPrefix || '');
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
        if (lastAttempt && nowMs() - lastAttempt < AUTO_BUY_ATTEMPT_TTL_MS) {
          autoBuyDebug.lastReason = 'attempt ttl';
          continue;
        }
        autoBuyRecentAttempts.set(attemptKey, nowMs());

        const chatId = userChatId(userId);

        const img = candidate.giftName ? fragmentGiftRemoteUrlFromGiftName(candidate.giftName) : null;
        const reply_markup = {
          inline_keyboard: [[
            { text: 'MRKT', url: candidate.urlMarket },
            { text: 'NFT', url: candidate.urlTelegram || 'https://t.me/mrkt' },
          ]]
        };

        if (AUTO_BUY_DRY_RUN) {
          const caption =
            `AutoBuy (DRY)\n` +
            `Подписка #${sub.num}\n` +
            `${candidate.name}\n` +
            `Цена: ${candidate.priceTon.toFixed(3)} TON\n` +
            `Found: ${formatMskMs(foundAt)}\n`;

          if (img) await sendPhotoSafe(chatId, img, caption.trim(), { reply_markup });
          else await sendMessageSafe(chatId, caption.trim(), { disable_web_page_preview: true, reply_markup });

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
            tsFound: foundAt,
            tsBought: boughtAt,
            latencyMs,
            title: candidate.name,
            priceTon: candidate.priceTon,
            urlTelegram: candidate.urlTelegram,
            urlMarket: candidate.urlMarket,
            giftName: candidate.giftName || '',
          });
          scheduleSave();

          const caption =
            `AutoBuy OK\n` +
            `Подписка #${sub.num}\n` +
            `Куплено: ${candidate.name}\n` +
            `Цена: ${candidate.priceTon.toFixed(3)} TON\n` +
            `Found: ${formatMskMs(foundAt)}\n` +
            `Buy: ${formatMskMs(boughtAt)}\n` +
            `Latency: ${latencyMs} ms\n`;

          if (img) await sendPhotoSafe(chatId, img, caption.trim(), { reply_markup });
          else await sendMessageSafe(chatId, caption.trim(), { disable_web_page_preview: true, reply_markup });

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
            await sendMessageSafe(chatId, `AutoBuy FAIL\nПодписка #${sub.num}\n${buyRes.reason}`, { disable_web_page_preview: true });
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

// ===================== Telegram commands =====================
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

  const t = msg.text || '';
  if (t === '📌 Статус') {
    const txt =
      `Status:\n` +
      `• MRKT_AUTH: ${MRKT_AUTH_RUNTIME ? 'YES' : 'NO'}\n` +
      `• Redis: ${redis ? 'YES' : 'NO'}\n` +
      `• AutoBuy: ${AUTO_BUY_GLOBAL ? 'ON' : 'OFF'}${AUTO_BUY_DRY_RUN ? ' (DRY)' : ''}\n` +
      `• MRKT fail: ${mrktState.lastFailMsg || '-'}\n`;
    await sendMessageSafe(msg.chat.id, txt, { reply_markup: MAIN_KEYBOARD });
  }
});

// ===================== WebApp UI (HTML/JS) =====================
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
input{width:100%;padding:10px;border:1px solid var(--border);border-radius:12px;background:var(--input);color:var(--text);outline:none;font-size:13px}
button{padding:10px 12px;border:1px solid var(--border);border-radius:12px;background:var(--btn);color:var(--btnText);cursor:pointer}
.primary{border-color:var(--accent);color:#052e16;background:var(--accent);font-weight:950}
.small{padding:8px 10px;border-radius:10px;font-size:13px}
.xbtn{position:absolute;right:8px;top:50%;transform:translateY(-50%);width:20px;height:20px;border-radius:8px;border:1px solid rgba(255,255,255,.10);background:rgba(255,255,255,.05);color:rgba(255,255,255,.55);cursor:pointer;display:flex;align-items:center;justify-content:center}
.hr{height:1px;background:var(--border);margin:10px 0}
.muted{color:var(--muted);font-size:12px}
#err{display:none;border-color:var(--danger);color:#ffd1d1;white-space:pre-wrap;word-break:break-word}
.tabs{display:flex;gap:8px;flex-wrap:wrap}
.tabbtn{border-radius:999px;padding:8px 12px;font-size:13px}
.tabbtn.active{border-color:var(--accent);color:var(--accent)}
.sug{border:1px solid var(--border);border-radius:14px;overflow:auto;background:var(--card);max-height:360px;position:absolute;top:calc(100% + 6px);left:0;right:0;z-index:9999;box-shadow:0 14px 40px rgba(0,0,0,.45)}
.sug .item{width:100%;text-align:left;border:0;background:transparent;padding:10px;display:flex;gap:10px;align-items:center}
.sug .item:hover{background:rgba(255,255,255,.06)}
.thumb{width:44px;height:44px;border-radius:14px;object-fit:cover;background:rgba(255,255,255,.06);border:1px solid var(--border);flex:0 0 auto}
.dot{width:14px;height:14px;border-radius:999px;border:1px solid var(--border);display:inline-block}
.grid{display:grid;gap:10px;margin-top:10px;grid-template-columns: repeat(auto-fill, minmax(170px, 1fr))}
@media (max-width: 520px){.grid{grid-template-columns: repeat(2, minmax(0, 1fr));}}
.lot{border:1px solid var(--border);border-radius:16px;padding:10px;background:rgba(255,255,255,.02);cursor:pointer}
.lot img{width:100%;aspect-ratio:1/1;object-fit:cover;border-radius:14px;border:1px solid var(--border)}
.price{font-size:15px;font-weight:950;margin-top:8px}
.ellipsis{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.badge{padding:4px 8px;border-radius:999px;border:1px solid var(--border);color:var(--muted);font-size:12px}

.loaderLine{display:none;align-items:center;gap:10px;color:var(--muted);font-size:12px;margin-top:10px}
.spinner{width:16px;height:16px;border-radius:999px;border:3px solid rgba(255,255,255,.2);border-top-color:var(--accent);animation:spin .8s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}

/* bottom sheet */
.sheetWrap{position:fixed;inset:0;background:rgba(0,0,0,.35);display:flex;align-items:flex-end;justify-content:center;z-index:50000;padding:10px;opacity:0;visibility:hidden;pointer-events:none;transition:opacity .18s ease,visibility .18s ease}
.sheetWrap.show{opacity:1;visibility:visible;pointer-events:auto}
.sheet{width:min(980px,96vw);height:min(78vh,820px);background:var(--card);border:1px solid var(--border);border-radius:22px 22px 14px 14px;padding:12px;box-shadow:0 30px 90px rgba(0,0,0,.5);display:flex;flex-direction:column;gap:10px;transform:translateY(20px);transition:transform .18s ease}
.sheetWrap.show .sheet{transform:translateY(0)}
.handle{width:44px;height:5px;border-radius:999px;background:rgba(255,255,255,.18);align-self:center}
.sheetHeader{display:flex;justify-content:space-between;gap:12px;align-items:flex-start}
.sheetBody{overflow:auto}
.sheetImg{width:100%;max-height:230px;object-fit:cover;border-radius:14px;border:1px solid var(--border);background:rgba(255,255,255,.03)}
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
  <button class="tabbtn" data-tab="profile">Profile</button>
  <button class="tabbtn" data-tab="admin" id="adminTabBtn" style="display:none">Admin</button>
</div>

<div id="market" class="card">
  <h3 style="margin:0 0 8px 0;font-size:15px">Фильтры</h3>
  <div class="row">
    <div class="field">
      <label>Gift</label>
      <div class="inpWrap">
        <input id="gift" placeholder="Нажми чтобы выбрать" autocomplete="off"/>
        <button class="xbtn" data-clear="gift" type="button">×</button>
      </div>
      <div id="giftSug" class="sug" style="display:none"></div>
    </div>
    <div class="field">
      <label>Model</label>
      <div class="inpWrap">
        <input id="model" placeholder="Нажми чтобы выбрать" autocomplete="off"/>
        <button class="xbtn" data-clear="model" type="button">×</button>
      </div>
      <div id="modelSug" class="sug" style="display:none"></div>
    </div>
    <div class="field">
      <label>Backdrop</label>
      <div class="inpWrap">
        <input id="backdrop" placeholder="Нажми чтобы выбрать" autocomplete="off"/>
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
    <button id="clearAll">Очистить</button>
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
  <div class="muted">Обновление MRKT_AUTH без перезапуска (сохранится в Redis).</div>
  <div class="row" style="margin-top:10px">
    <div class="field"><label>Текущий токен (маска)</label><input id="tokMask" disabled/></div>
    <div class="field"><label>Новый MRKT_AUTH</label><input id="tokNew" placeholder="вставь токен"/></div>
    <button id="tokSave">Сохранить</button>
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

    <img id="sheetImg" class="sheetImg" style="display:none" />
    <div id="sheetTop" class="muted"></div>
    <div class="kv" id="sheetKV"></div>
    <div class="row" id="sheetBtns"></div>

    <div class="hr"></div>
    <div><b>История продаж</b> <span class="muted">(примерная цена продажи + последние сделки)</span></div>
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

  function hideSug(id){ const b=el(id); b.style.display='none'; b.innerHTML=''; }
  function renderSug(id, items, onPick){
    const b=el(id);
    if(!items||!items.length){ hideSug(id); return; }
    b.innerHTML = items.map(x => {
      const thumb = x.imgUrl ? '<img class="thumb" src="'+x.imgUrl+'" referrerpolicy="no-referrer"/>' : '<div class="thumb"></div>';
      return '<button type="button" class="item" data-v="'+x.value.replace(/"/g,'&quot;')+'">'+thumb+
        '<div style="min-width:0;flex:1">' +
          '<div style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap"><b>'+x.label+'</b></div>' +
          (x.sub?'<div class="muted" style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap">'+x.sub+'</div>':'')+
        '</div></button>';
    }).join('');
    b.style.display='block';
    b.onclick = (e) => { const btn = e.target.closest('button[data-v]'); if(!btn) return; onPick(btn.getAttribute('data-v')); hideSug(id); };
  }

  let selectedGiftValue = '';

  async function showGiftSug(){
    const q = el('gift').value.trim();
    const r = await api('/api/mrkt/collections?q='+encodeURIComponent(q));
    renderSug('giftSug', r.items||[], (v) => {
      selectedGiftValue=v;
      el('gift').value=(r.mapLabel||{})[v]||v;
      el('model').value='';
      el('backdrop').value='';
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
    const b=el('backdropSug');
    if(!r.items||!r.items.length){ hideSug('backdropSug'); return; }
    b.innerHTML = r.items.map(x => {
      const dot = x.colorHex ? '<span class="dot" style="background:'+x.colorHex+'"></span>' : '<span class="dot"></span>';
      return '<button type="button" class="item" data-v="'+x.value.replace(/"/g,'&quot;')+'">'+
        '<div class="thumb" style="display:flex;align-items:center;justify-content:center">'+dot+'</div>'+
        '<div style="min-width:0;flex:1"><div style="overflow:hidden;text-overflow:ellipsis;white-space:nowrap"><b>'+x.label+'</b></div></div></button>';
    }).join('');
    b.style.display='block';
    b.onclick = (e) => { const btn = e.target.closest('button[data-v]'); if(!btn) return; el('backdrop').value=btn.getAttribute('data-v'); hideSug('backdropSug'); };
  }

  async function patchFilters(){
    await api('/api/state/patch',{method:'POST',body:JSON.stringify({filters:{
      gift:selectedGiftValue,
      giftLabel:el('gift').value.trim(),
      model:el('model').value.trim(),
      backdrop:el('backdrop').value.trim(),
      numberPrefix:el('number').value.trim()
    }})});
  }

  function wrap(which, fn){
    return async () => {
      hideErr();
      setLoading(which, true);
      try { await fn(); } catch(e){ showErr(e.message || String(e)); }
      finally { setLoading(which, false); }
    };
  }

  el('gift').addEventListener('focus', wrap('lots', showGiftSug));
  el('gift').addEventListener('click', wrap('lots', showGiftSug));
  el('gift').addEventListener('input', wrap('lots', async()=>{ selectedGiftValue=''; await showGiftSug(); }));

  el('model').addEventListener('focus', wrap('lots', showModelSug));
  el('model').addEventListener('click', wrap('lots', showModelSug));
  el('model').addEventListener('input', wrap('lots', showModelSug));

  el('backdrop').addEventListener('focus', wrap('lots', showBackdropSug));
  el('backdrop').addEventListener('click', wrap('lots', showBackdropSug));
  el('backdrop').addEventListener('input', wrap('lots', showBackdropSug));

  document.addEventListener('click', (e) => {
    if(!e.target.closest('#gift,#model,#backdrop,.sug')){ hideSug('giftSug'); hideSug('modelSug'); hideSug('backdropSug'); }
  });

  document.body.addEventListener('click', (e) => {
    const b = e.target.closest('button[data-clear]');
    if(!b) return;
    const what = b.getAttribute('data-clear');
    wrap('lots', async()=>{
      if(what==='gift'){ selectedGiftValue=''; el('gift').value=''; el('model').value=''; el('backdrop').value=''; el('number').value=''; }
      if(what==='model'){ el('model').value=''; }
      if(what==='backdrop'){ el('backdrop').value=''; }
      if(what==='number'){ el('number').value=''; }
      await patchFilters();
      await refreshState(); await refreshMarketData();
    })();
  });

  el('clearAll').onclick = wrap('lots', async()=>{
    selectedGiftValue=''; el('gift').value=''; el('model').value=''; el('backdrop').value=''; el('number').value='';
    await patchFilters(); await refreshState(); await refreshMarketData();
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
      return '<div class="sale">'+
        '<div style="display:flex;justify-content:space-between;gap:10px"><b>'+x.priceTon.toFixed(3)+' TON</b><span class="muted">'+(x.tsMsk||'')+'</span></div>'+
        (x.model?'<div class="muted">Model: '+x.model+'</div>':'')+
        (x.backdrop?'<div class="muted">Backdrop: '+x.backdrop+'</div>':'')+
      '</div>';
    }).join('<div style="height:8px"></div>');

    sheetSales.innerHTML = html;
  }

  function renderLots(resp){
    const box=el('lots');
    if(resp.ok===false){ box.innerHTML='<div style="color:#ef4444"><b>'+resp.reason+'</b></div>'; return; }
    const lots=resp.lots||[];
    if(!lots.length){ box.innerHTML='<i class="muted">Лотов не найдено</i>'; return; }

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
          '<div class="muted" style="margin-top:6px">ID: '+lot.id+'</div>';

        // fetch details (offers + history)
        const det = await api('/api/lot/details', { method:'POST', body: JSON.stringify({ id: lot.id }) });

        // pills
        sheetKV.innerHTML = [
          det?.offers?.exact?.maxOfferTon!=null ? pill('Max offer (точно): <b>'+det.offers.exact.maxOfferTon.toFixed(3)+' TON</b>') : pill('Max offer (точно): —'),
          det?.offers?.collection?.maxOfferTon!=null ? pill('Max offer (колл.): <b>'+det.offers.collection.maxOfferTon.toFixed(3)+' TON</b>') : pill('Max offer (колл.): —'),
          det?.floors?.exact?.priceTon!=null ? pill('Floor (точно): <b>'+det.floors.exact.priceTon.toFixed(3)+' TON</b>') : pill('Floor (точно): —'),
          det?.floors?.collection?.priceTon!=null ? pill('Floor (колл.): <b>'+det.floors.collection.priceTon.toFixed(3)+' TON</b>') : pill('Floor (колл.): —'),
        ].join('');

        // buttons
        sheetBtns.innerHTML = '';
        const btn = (label, urlOrAct) => {
          const b = document.createElement('button');
          b.className = 'small';
          b.textContent = label;
          if (urlOrAct.startsWith('http')) {
            b.onclick = () => tg?.openTelegramLink ? tg.openTelegramLink(urlOrAct) : window.open(urlOrAct, '_blank');
          } else if (urlOrAct === 'BUY') {
            b.style.borderColor = 'var(--accent)';
            b.style.background = 'var(--accent)';
            b.style.color = '#052e16';
            b.style.fontWeight = '900';
            b.onclick = async () => {
              const ok = confirm('Купить?\\n'+lot.name+'\\nЦена: '+lot.priceTon.toFixed(3)+' TON');
              if(!ok) return;
              const r = await api('/api/mrkt/buy', { method:'POST', body: JSON.stringify({ id: lot.id, priceNano: lot.priceNano }) });
              alert('Куплено: '+r.title+' за '+r.priceTon.toFixed(3)+' TON');
            };
          }
          sheetBtns.appendChild(b);
        };

        btn('Buy', 'BUY');
        btn('MRKT', lot.urlMarket);
        btn('NFT', lot.urlTelegram);

        renderSales(det?.salesHistory);
      });
    });
  }

  async function refreshState(){
    const st = await api('/api/state');
    el('status').textContent = 'MRKT_AUTH: '+(st.api.mrktAuthSet?'YES':'NO');
    el('gift').value = st.user.filters.giftLabel || st.user.filters.gift || '';
    selectedGiftValue = st.user.filters.gift || '';
    el('model').value = st.user.filters.model || '';
    el('backdrop').value = st.user.filters.backdrop || '';
    el('number').value = st.user.filters.numberPrefix || '';

    // subs
    const subs=st.user.subscriptions||[];
    const box=el('subsList');
    if(!subs.length){ box.innerHTML='<i class="muted">Подписок нет</i>'; }
    else{
      box.innerHTML = subs.map(s=>{
        const img = s.thumbUrl ? '<img class="thumb" src="'+s.thumbUrl+'" referrerpolicy="no-referrer"/>' : '<div class="thumb"></div>';
        const dot = s.backdropHex ? '<span class="dot" style="background:'+s.backdropHex+'"></span>' : '<span class="dot"></span>';
        const notifyTxt = (s.maxNotifyTon==null)?'∞':String(s.maxNotifyTon);
        const buyTxt = (s.maxAutoBuyTon==null)?'-':String(s.maxAutoBuyTon);

        return '<div class="card">'+
          '<div style="display:flex;gap:10px;align-items:center">'+img+
            '<div style="min-width:0;flex:1"><b>#'+s.num+' '+(s.enabled?'ON':'OFF')+'</b> '+dot+
              '<div class="muted ellipsis">'+(s.filters.gift||'')+'</div></div>'+
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

    if(st.api.isAdmin) el('adminTabBtn').style.display='inline-block';
  }

  async function refreshMarketData(){
    const lots = await api('/api/mrkt/lots');
    renderLots(lots);
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
            return '<div class="card">'+
              '<b class="ellipsis">'+p.title+'</b>'+
              '<div class="muted">Found: '+(p.foundMsk||'-')+'</div>'+
              '<div class="muted">Buy: '+(p.boughtMsk||'-')+(p.latencyMs!=null?(' · Latency '+p.latencyMs+' ms'):'')+'</div>'+
              '<div class="muted">Price: '+Number(p.priceTon).toFixed(3)+' TON</div>'+
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
      'MRKT fail: '+(r.mrktLastFailMsg||'-')+'\\n'+
      'endpoint: '+(r.mrktLastFailEndpoint||'-')+'\\n'+
      'status: '+(r.mrktLastFailStatus||'-')+'\\n'+
      'AutoBuy: eligible='+r.autoBuy.eligible+
      ', scanned='+r.autoBuy.scanned+
      ', candidates='+r.autoBuy.candidates+
      ', buys='+r.autoBuy.buys+
      '\\nreason: '+(r.autoBuy.lastReason||'-');

    el('tokMask').value = r.mrktAuthMask || '';
  }

  // buttons
  el('apply').onclick = wrap('lots', async()=>{ await patchFilters(); await refreshState(); await refreshMarketData(); });
  el('refresh').onclick = wrap('lots', async()=>{ await refreshMarketData(); });

  el('subCreate').onclick = wrap('subs', async()=>{ await api('/api/sub/create',{method:'POST'}); await refreshState(); });
  el('subRefresh').onclick = wrap('subs', async()=>{ await refreshState(); });

  // subs actions + info
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
        sheetTop.innerHTML = '<div class="muted">Gift: '+r.sub.filters.gift+'</div>' +
          (r.sub.filters.model?('<div class="muted">Model: '+r.sub.filters.model+'</div>'):'') +
          (r.sub.filters.backdrop?('<div class="muted">Backdrop: '+r.sub.filters.backdrop+'</div>'):'') +
          '<div class="muted" style="margin-top:6px">Notify max: '+(r.sub.maxNotifyTon==null?'∞':r.sub.maxNotifyTon)+' TON</div>'+
          '<div class="muted">AutoBuy max: '+(r.sub.maxAutoBuyTon==null?'-':r.sub.maxAutoBuyTon)+' TON</div>';

        sheetKV.innerHTML = [
          r.offers?.exact?.maxOfferTon!=null ? pill('Max offer (точно): <b>'+r.offers.exact.maxOfferTon.toFixed(3)+' TON</b>') : pill('Max offer (точно): —'),
          r.offers?.collection?.maxOfferTon!=null ? pill('Max offer (колл.): <b>'+r.offers.collection.maxOfferTon.toFixed(3)+' TON</b>') : pill('Max offer (колл.): —'),
          r.floors?.exact?.priceTon!=null ? pill('Floor (точно): <b>'+r.floors.exact.priceTon.toFixed(3)+' TON</b>') : pill('Floor (точно): —'),
          r.floors?.collection?.priceTon!=null ? pill('Floor (колл.): <b>'+r.floors.collection.priceTon.toFixed(3)+' TON</b>') : pill('Floor (колл.): —'),
        ].join('');

        sheetBtns.innerHTML = '';
        renderSales(r.salesHistory);
      }

      await refreshState();
    })();
  });

  el('tokSave')?.addEventListener('click', wrap('subs', async()=>{
    const t = el('tokNew').value.trim();
    if(!t) throw new Error('Вставь токен');
    await api('/api/admin/mrkt_auth',{method:'POST',body:JSON.stringify({token:t})});
    el('tokNew').value='';
    await refreshAdmin();
  }));

  // initial load
  wrap('lots', async()=>{ await refreshState(); await refreshMarketData(); })();
})();`;

// ===================== Web server + API =====================
function streamFetchToRes(r, res, fallbackContentType) {
  res.setHeader('Content-Type', r.headers.get('content-type') || fallbackContentType);
  res.setHeader('Cache-Control', 'public, max-age=86400');
  try {
    if (r.body && Readable.fromWeb) { Readable.fromWeb(r.body).pipe(res); return; }
  } catch {}
  r.arrayBuffer().then((ab) => res.end(Buffer.from(ab))).catch(() => res.status(502).end('bad gateway'));
}

function startWebServer() {
  const app = express();
  app.disable('x-powered-by');
  app.use(express.json({ limit: '1mb' }));

  app.get('/', (req, res) => { res.setHeader('Content-Type', 'text/html; charset=utf-8'); res.send(WEBAPP_HTML); });
  app.get('/app.js', (req, res) => { res.setHeader('Content-Type', 'application/javascript; charset=utf-8'); res.send(WEBAPP_JS); });
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

  app.get('/img/gift', async (req, res) => {
    const name = String(req.query.name || '').trim();
    if (!name) return res.status(400).send('no name');
    const url = fragmentGiftRemoteUrlFromGiftName(name);
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

  app.get('/api/state', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    const mask = MRKT_AUTH_RUNTIME ? MRKT_AUTH_RUNTIME.slice(0, 4) + '…' + MRKT_AUTH_RUNTIME.slice(-4) : '';

    const subs = Array.isArray(u.subscriptions) ? u.subscriptions : [];
    const enriched = [];
    for (const s of subs) {
      const key = `thumb|${s.filters.gift || ''}|${s.filters.model || ''}|${s.filters.backdrop || ''}`;
      const now = nowMs();
      const cached = thumbsCache.get(key);
      if (cached && now - cached.time < THUMBS_CACHE_TTL_MS) {
        enriched.push({ ...s, thumbUrl: cached.thumbUrl, backdropHex: cached.backdropHex });
        continue;
      }

      let thumbUrl = null;
      let backdropHex = null;

      if (s.filters.gift && s.filters.backdrop) {
        const backs = await mrktGetBackdropsForGift(s.filters.gift);
        const b = backs.find((x) => norm(x.name) === norm(s.filters.backdrop));
        backdropHex = b?.centerHex || null;
      }
      if (s.filters.gift && s.filters.model) {
        const models = await mrktGetModelsForGift(s.filters.gift);
        const m = models.find((x) => norm(x.name) === norm(s.filters.model));
        if (m?.thumbKey) thumbUrl = `/img/cdn?key=${encodeURIComponent(m.thumbKey)}`;
      }
      if (!thumbUrl && s.filters.gift) {
        const cols = await mrktGetCollections();
        const c = cols.find((x) => x.name === s.filters.gift);
        if (c?.thumbKey) thumbUrl = `/img/cdn?key=${encodeURIComponent(c.thumbKey)}`;
      }

      thumbsCache.set(key, { time: now, thumbUrl, backdropHex });
      enriched.push({ ...s, thumbUrl, backdropHex });
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
        .map((b) => ({ label: b.name, value: b.name, colorHex: b.centerHex || null }));
      return res.json({ ok: true, items });
    }

    res.status(400).json({ ok: false, reason: 'BAD_KIND' });
  });

  app.get('/api/mrkt/lots', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);

    const r = await mrktSearchLotsByFilters({
      gift: u.filters.gift || '',
      model: u.filters.model || '',
      backdrop: u.filters.backdrop || '',
      numberPrefix: u.filters.numberPrefix || '',
    }, WEBAPP_LOTS_PAGES);

    if (!r.ok) return res.json({ ok: false, reason: r.reason, lots: [] });

    let backMap = new Map();
    if (u.filters.gift) {
      const backs = await mrktGetBackdropsForGift(u.filters.gift);
      backMap = new Map(backs.map((b) => [norm(b.name), b]));
    }

    const lots = (r.gifts || []).slice(0, WEBAPP_LOTS_LIMIT).map((lot) => {
      const b = lot.backdrop ? backMap.get(norm(lot.backdrop)) : null;
      const imgUrl = lot.giftName ? fragmentGiftRemoteUrlFromGiftName(lot.giftName) : null;

      return {
        id: lot.id,
        name: lot.name,
        number: lot.number,
        priceTon: lot.priceTon,
        priceNano: lot.priceNano,
        urlTelegram: lot.urlTelegram,
        urlMarket: lot.urlMarket,
        model: lot.model,
        backdrop: lot.backdrop,
        imgUrl,
        backdropColorHex: b?.centerHex || null,
      };
    });

    res.json({ ok: true, lots });
  });

  app.post('/api/lot/details', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    const id = String(req.body?.id || '').trim();
    if (!id) return res.status(400).json({ ok: false, reason: 'NO_ID' });

    const gift = u.filters.gift || '';
    const model = u.filters.model || '';
    const backdrop = u.filters.backdrop || '';
    const numberPrefix = u.filters.numberPrefix || '';

    // Floors
    const exact = gift ? await mrktSearchLotsByFilters({ gift, model, backdrop, numberPrefix }, 1) : { ok: true, gifts: [] };
    const col = gift ? await mrktSearchLotsByFilters({ gift, model: '', backdrop: '', numberPrefix: '' }, 1) : { ok: true, gifts: [] };

    const exactLot = exact.ok ? (exact.gifts[0] || null) : null;
    const colLot = col.ok ? (col.gifts[0] || null) : null;

    // Offers
    let offersExact = { ok: true, maxOfferTon: null };
    let offersCollection = { ok: true, maxOfferTon: null };
    if (gift) {
      const o1 = await mrktOrdersFetch({ gift, model, backdrop });
      offersExact = { ok: o1.ok, maxOfferTon: o1.ok ? maxOfferTonFromOrders(o1.orders) : null };
      const o2 = await mrktOrdersFetch({ gift, model: '', backdrop: '' });
      offersCollection = { ok: o2.ok, maxOfferTon: o2.ok ? maxOfferTonFromOrders(o2.orders) : null };
    }

    // Sales history
    const salesHistory = gift ? await mrktFeedSales({ gift, model, backdrop }) : { ok: true, approxPriceTon: null, sales: [] };

    res.json({
      ok: true,
      floors: {
        exact: exactLot ? { priceTon: exactLot.priceTon } : null,
        collection: colLot ? { priceTon: colLot.priceTon } : null,
      },
      offers: { exact: offersExact, collection: offersCollection },
      salesHistory,
    });
  });

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
    const titleBase = `${g?.collectionTitle || g?.collectionName || g?.title || 'Gift'}${g?.number != null ? ` #${g.number}` : ''}`;
    const priceTon = priceNano / 1e9;
    const giftName = g?.name || giftNameFallbackFromCollectionAndNumber(g?.collectionTitle || g?.collectionName || g?.title || '', g?.number) || '';
    const urlTelegram = giftName && String(giftName).includes('-') ? `https://t.me/nft/${giftName}` : '';
    const urlMarket = mrktLotUrlFromId(id);

    const u = getOrCreateUser(req.userId);
    pushPurchase(u, { tsFound: foundAt, tsBought: boughtAt, latencyMs, title: titleBase, priceTon, urlTelegram, urlMarket, giftName });
    scheduleSave();

    res.json({ ok: true, title: titleBase, priceTon });
  });

  app.get('/api/profile', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    const purchases = (u.purchases || []).map((p) => ({
      title: p.title,
      priceTon: p.priceTon,
      foundMsk: p.tsFound ? formatMskMs(p.tsFound) : null,
      boughtMsk: p.tsBought ? formatMskMs(p.tsBought) : null,
      latencyMs: p.latencyMs ?? null,
    }));
    res.json({ ok: true, user: req.tgUser, purchases });
  });

  // subs
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

  app.post('/api/sub/details', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    const s = findSub(u, String(req.body?.id || ''));
    if (!s) return res.status(404).json({ ok: false, reason: 'NOT_FOUND' });

    const gift = s.filters.gift || '';
    const model = s.filters.model || '';
    const backdrop = s.filters.backdrop || '';
    const numberPrefix = s.filters.numberPrefix || '';

    const exact = gift ? await mrktSearchLotsByFilters({ gift, model, backdrop, numberPrefix }, 1) : { ok: true, gifts: [] };
    const col = gift ? await mrktSearchLotsByFilters({ gift, model: '', backdrop: '', numberPrefix: '' }, 1) : { ok: true, gifts: [] };

    const exactLot = exact.ok ? (exact.gifts[0] || null) : null;
    const colLot = col.ok ? (col.gifts[0] || null) : null;

    let offersExact = { ok: true, maxOfferTon: null };
    let offersCollection = { ok: true, maxOfferTon: null };
    if (gift) {
      const o1 = await mrktOrdersFetch({ gift, model, backdrop });
      offersExact = { ok: o1.ok, maxOfferTon: o1.ok ? maxOfferTonFromOrders(o1.orders) : null };
      const o2 = await mrktOrdersFetch({ gift, model: '', backdrop: '' });
      offersCollection = { ok: o2.ok, maxOfferTon: o2.ok ? maxOfferTonFromOrders(o2.orders) : null };
    }

    const salesHistory = gift ? await mrktFeedSales({ gift, model, backdrop }) : { ok: true, approxPriceTon: null, sales: [] };

    res.json({
      ok: true,
      sub: s,
      floors: {
        exact: exactLot ? { priceTon: exactLot.priceTon } : null,
        collection: colLot ? { priceTon: colLot.priceTon } : null,
      },
      offers: { exact: offersExact, collection: offersCollection },
      salesHistory,
    });
  });

  // Admin token set (no restart)
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
    thumbsCache.clear();

    res.json({ ok: true });
  });

  const port = Number(process.env.PORT || 3000);
  app.listen(port, '0.0.0.0', () => console.log('WebApp listening on', port));
}

startWebServer();

// ===================== intervals =====================
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
    console.warn('REDIS_URL not set => subscriptions will not persist after restart');
  }
  console.log('Bot started. /start');
})();
