/**
 * ton-bot MRKT + WebApp Panel (single-file)
 * version: 2026-02-28-webapp-v8-ui-fixes-collections-img-price-subs-admin-token
 */

const TelegramBot = require('node-telegram-bot-api');
const express = require('express');
const crypto = require('crypto');
const { Readable } = require('stream');

// =====================
// ENV
// =====================
const token = process.env.TELEGRAM_TOKEN;
if (!token) {
  console.error('TELEGRAM_TOKEN not set');
  process.exit(1);
}

const MODE = process.env.MODE || 'real';

// WebApp
const WEBAPP_URL = process.env.WEBAPP_URL || null;
const WEBAPP_AUTH_MAX_AGE_SEC = Number(process.env.WEBAPP_AUTH_MAX_AGE_SEC || 86400);

// Admin
const ADMIN_USER_ID = process.env.ADMIN_USER_ID ? Number(process.env.ADMIN_USER_ID) : null;

// Redis
const REDIS_URL = process.env.REDIS_URL || process.env.REDIS_PRIVATE_URL || null;

// MRKT
const MRKT_API_URL = 'https://api.tgmrkt.io/api/v1';
const MRKT_CDN_BASE = (process.env.MRKT_CDN_BASE || 'https://cdn.tgmrkt.io/').trim();
const FRAGMENT_GIFT_IMG_BASE = (process.env.FRAGMENT_GIFT_IMG_BASE || 'https://nft.fragment.com/gift/').trim();

let MRKT_AUTH_RUNTIME = (process.env.MRKT_AUTH || '').trim() || null;

// intervals
const SUBS_CHECK_INTERVAL_MS = Number(process.env.SUBS_CHECK_INTERVAL_MS || 9000);

// request tuning
const MRKT_TIMEOUT_MS = Number(process.env.MRKT_TIMEOUT_MS || 9000);
const MRKT_COUNT = Number(process.env.MRKT_COUNT || 50);
const MRKT_PAGES = Number(process.env.MRKT_PAGES || 6);

// WebApp lists
const WEBAPP_LOTS_LIMIT = Number(process.env.WEBAPP_LOTS_LIMIT || 25);
const WEBAPP_LOTS_PAGES = Number(process.env.WEBAPP_LOTS_PAGES || 1);
const WEBAPP_SUGGEST_LIMIT = Number(process.env.WEBAPP_SUGGEST_LIMIT || 40);

// Subscriptions behavior
const SUBS_MAX_NOTIFICATIONS_PER_CYCLE = Number(process.env.SUBS_MAX_NOTIFICATIONS_PER_CYCLE || 8);
const SUBS_EMPTY_CONFIRM = Number(process.env.SUBS_EMPTY_CONFIRM || 2);

// Price (history)
const PRICE_HISTORY_TARGET_SALES = Number(process.env.PRICE_HISTORY_TARGET_SALES || 60);
const PRICE_HISTORY_MAX_PAGES = Number(process.env.PRICE_HISTORY_MAX_PAGES || 40);
const PRICE_HISTORY_COUNT_PER_PAGE = Number(process.env.PRICE_HISTORY_COUNT_PER_PAGE || 50);
const PRICE_HISTORY_TIME_BUDGET_MS = Number(process.env.PRICE_HISTORY_TIME_BUDGET_MS || 15000);

// MRKT feed
const MRKT_FEED_THROTTLE_MS = Number(process.env.MRKT_FEED_THROTTLE_MS || 120);

// Token expiry alert
const MRKT_AUTH_NOTIFY_COOLDOWN_MS = Number(process.env.MRKT_AUTH_NOTIFY_COOLDOWN_MS || 60 * 60 * 1000);

console.log('v8 start', {
  MODE,
  WEBAPP_URL: !!WEBAPP_URL,
  ADMIN_USER_ID,
  REDIS: !!REDIS_URL,
  MRKT_AUTH: !!MRKT_AUTH_RUNTIME,
  MRKT_CDN_BASE,
});

// =====================
// Telegram bot
// =====================
const bot = new TelegramBot(token, { polling: true });

const MAIN_KEYBOARD = {
  keyboard: [
    [{ text: '📌 Статус' }],
  ],
  resize_keyboard: true,
};

// =====================
// State
// =====================
const users = new Map(); // userId -> state
const subStates = new Map(); // `${userId}:${subId}:MRKT` -> { floor, emptyStreak }

const mrktAuthState = {
  lastOkAt: 0,
  lastFailAt: 0,
  lastFailCode: null,
  lastNotifiedAt: 0,
};

// caches
const CACHE_TTL_MS = 5 * 60_000;
let collectionsCache = { time: 0, items: [] }; // from /gifts/collections
const modelsCache = new Map(); // gift -> { time, items:[{name, rarityPerMille, thumbKey, floorNano}] }
const backdropsCache = new Map(); // gift -> { time, items:[{name, rarityPerMille, centerHex}] }

// price cache
const priceCache = new Map(); // key -> { time, data }
const PRICE_CACHE_TTL_MS = 30_000;

// =====================
// Helpers
// =====================
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const nowMs = () => Date.now();

function n(x) {
  const v = Number(String(x).replace(',', '.'));
  return Number.isFinite(v) ? v : NaN;
}
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
function tonFromNano(nano) {
  const x = Number(nano);
  if (!Number.isFinite(x)) return null;
  return x / 1e9;
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

// =====================
// Redis: store MRKT token
// =====================
let redis = null;
const MRKT_AUTH_KEY = 'mrkt:auth:token';

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
    const t = await redis.get(MRKT_AUTH_KEY);
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
  await redis.set(MRKT_AUTH_KEY, String(t || '').trim());
}

// =====================
// User state
// =====================
function getOrCreateUser(userId) {
  if (!users.has(userId)) {
    users.set(userId, {
      enabled: true,              // used for subs notifications
      minPriceTon: 0,
      maxPriceTon: null,
      filters: {
        gift: '',                 // collectionName (MRKT)
        giftLabel: '',            // title label
        model: '',
        backdrop: '',
      },
      subscriptions: [],
    });
  }
  return users.get(userId);
}

function renumberSubs(user) {
  const subs = Array.isArray(user.subscriptions) ? user.subscriptions : [];
  subs.forEach((s, idx) => { if (s) s.num = idx + 1; });
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
    maxPriceTon: user.maxPriceTon ?? null,
    filters: {
      gift: user.filters.gift,
      model: user.filters.model || '',
      backdrop: user.filters.backdrop || '',
    },
  };
  return { ok: true, sub };
}

// =====================
// MRKT headers + admin notify
// =====================
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

  const text =
    `⚠️ MRKT_AUTH не работает (HTTP ${status}).\n` +
    `Зайди в панель → Admin → вставь новый токен.\n` +
    `Иначе MRKT запросы будут 401/403.`;

  try {
    await sendMessageSafe(ADMIN_USER_ID, text, { disable_web_page_preview: true });
  } catch {}
}

function markMrktOk() {
  mrktAuthState.lastOkAt = nowMs();
  mrktAuthState.lastFailAt = 0;
  mrktAuthState.lastFailCode = null;
}
async function markMrktFail(status) {
  mrktAuthState.lastFailAt = nowMs();
  mrktAuthState.lastFailCode = status;
  if (status === 401 || status === 403) {
    await notifyAdminMrktAuthExpired(status);
  }
}

// =====================
// MRKT API: collections/models/backdrops/lots/feed
// =====================
async function mrktGetCollections() {
  const now = nowMs();
  if (collectionsCache.items.length && now - collectionsCache.time < CACHE_TTL_MS) return collectionsCache.items;

  const res = await fetchWithTimeout(
    `${MRKT_API_URL}/gifts/collections`,
    { method: 'GET', headers: { Accept: 'application/json' } },
    MRKT_TIMEOUT_MS
  ).catch(() => null);

  if (!res || !res.ok) {
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

  const body = { collections: [giftName] };
  const res = await fetchWithTimeout(
    `${MRKT_API_URL}/gifts/models`,
    { method: 'POST', headers: mrktHeaders(), body: JSON.stringify(body) },
    MRKT_TIMEOUT_MS
  ).catch(() => null);

  if (!res) return [];
  if (!res.ok) { await markMrktFail(res.status); return []; }
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
  const res = await fetchWithTimeout(
    `${MRKT_API_URL}/gifts/backdrops`,
    { method: 'POST', headers: mrktHeaders(), body: JSON.stringify(body) },
    MRKT_TIMEOUT_MS
  ).catch(() => null);

  if (!res) return [];
  if (!res.ok) { await markMrktFail(res.status); return []; }
  markMrktOk();

  const data = await res.json().catch(() => null);
  const arr = Array.isArray(data) ? data : [];

  const map = new Map();
  for (const it of arr) {
    const name = it.backdropName;
    if (!name) continue;
    const key = norm(name);
    if (!map.has(key)) {
      map.set(key, {
        name: String(name),
        rarityPerMille: it.rarityPerMille ?? null,
        centerHex: intToHexColor(it.colorsCenterColor),
      });
    }
  }

  const items = Array.from(map.values());
  backdropsCache.set(giftName, { time: nowMs(), items });
  return items;
}

async function mrktFetchSalingPage({ collectionName, modelName, backdropName, cursor }) {
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
    number: null,
    count: MRKT_COUNT,
    cursor: cursor || '',
    query: null,
    promotedFirst: false,
  };

  const res = await fetchWithTimeout(
    `${MRKT_API_URL}/gifts/saling`,
    { method: 'POST', headers: mrktHeaders(), body: JSON.stringify(body) },
    MRKT_TIMEOUT_MS
  ).catch(() => null);

  if (!res) return { ok: false, reason: 'FETCH_ERROR', gifts: [], cursor: '' };
  if (!res.ok) { await markMrktFail(res.status); return { ok: false, reason: `HTTP_${res.status}`, gifts: [], cursor: '' }; }
  markMrktOk();

  const data = await res.json().catch(() => null);
  return { ok: true, reason: 'OK', gifts: Array.isArray(data?.gifts) ? data.gifts : [], cursor: data?.cursor || '' };
}

async function mrktSearchLots({ gift, model, backdrop }, minPriceTon, maxPriceTon, pagesLimit) {
  if (!gift) return { ok: false, reason: 'NO_GIFT', gifts: [] };

  let cursor = '';
  const out = [];

  for (let page = 0; page < pagesLimit; page++) {
    const r = await mrktFetchSalingPage({ collectionName: gift, modelName: model || null, backdropName: backdrop || null, cursor });
    if (!r.ok) return { ok: false, reason: r.reason, gifts: [] };

    for (const g of r.gifts) {
      const nano = g?.salePriceWithoutFee && Number(g.salePriceWithoutFee) > 0 ? g.salePriceWithoutFee : g?.salePrice;
      if (nano == null) continue;

      const priceTon = Number(nano) / 1e9;
      if (!Number.isFinite(priceTon) || priceTon <= 0) continue;
      if (!inRange(priceTon, minPriceTon, maxPriceTon)) continue;

      const baseName = (g.collectionTitle || g.collectionName || g.title || gift).trim();
      const number = g.number ?? null;
      const displayName = number ? `${baseName} #${number}` : baseName;

      const modelName = g.modelTitle || g.modelName || '';
      const backdropName = g.backdropName || '';
      if (model && !sameTrait(modelName, norm(model))) continue;
      if (backdrop && !sameTrait(backdropName, norm(backdrop))) continue;

      const giftName =
        g.name ||
        giftNameFallbackFromCollectionAndNumber(g.collectionTitle || g.collectionName || g.title, g.number) ||
        null;

      const urlTelegram = giftName && String(giftName).includes('-') ? `https://t.me/nft/${giftName}` : 'https://t.me/mrkt';
      const urlMarket = g.id ? mrktLotUrlFromId(g.id) : 'https://t.me/mrkt';

      out.push({
        id: g.id,
        name: displayName,
        giftName,
        priceTon,
        urlTelegram,
        urlMarket,
        model: modelName || null,
        backdrop: backdropName || null,
      });

      if (out.length >= 300) break;
    }

    cursor = r.cursor || '';
    if (!cursor || out.length >= 300) break;
  }

  out.sort((a, b) => a.priceTon - b.priceTon);
  return { ok: true, reason: 'OK', gifts: out };
}

async function mrktFeedFetch({ gift, model, backdrop, cursor, count, types }) {
  if (!MRKT_AUTH_RUNTIME) return { ok: false, reason: 'NO_AUTH', items: [], cursor: '' };

  const body = {
    count: Number(count || 50),
    cursor: cursor || '',
    collectionNames: gift ? [gift] : [],
    modelNames: model ? [model] : [],
    backdropNames: backdrop ? [backdrop] : [],
    lowToHigh: false,
    maxPrice: null,
    minPrice: null,
    number: null,
    ordering: 'Latest',
    query: null,
    type: Array.isArray(types) ? types : [],
  };

  const res = await fetchWithTimeout(
    `${MRKT_API_URL}/feed`,
    { method: 'POST', headers: mrktHeaders(), body: JSON.stringify(body) },
    MRKT_TIMEOUT_MS
  ).catch(() => null);

  if (!res) return { ok: false, reason: 'FETCH_ERROR', items: [], cursor: '' };
  if (!res.ok) { await markMrktFail(res.status); return { ok: false, reason: `HTTP_${res.status}`, items: [], cursor: '' }; }
  markMrktOk();

  const data = await res.json().catch(() => null);
  return { ok: true, reason: 'OK', items: Array.isArray(data?.items) ? data.items : [], cursor: data?.cursor || '' };
}

// =====================
// Price estimate (floor + median sales, with fallback)
// =====================
function median(sorted) {
  if (!sorted.length) return null;
  const L = sorted.length;
  return L % 2 ? sorted[(L - 1) / 2] : (sorted[L / 2 - 1] + sorted[L / 2]) / 2;
}

async function mrktHistoryMedianSales({ gift, model, backdrop }) {
  const key = `price|${gift}|${model || ''}|${backdrop || ''}`;
  const now = nowMs();
  const cached = priceCache.get(key);
  if (cached && now - cached.time < PRICE_CACHE_TTL_MS) return cached.data;

  const started = nowMs();
  let cursor = '';
  let pages = 0;
  const prices = [];
  const lastSales = [];

  while (pages < PRICE_HISTORY_MAX_PAGES && prices.length < PRICE_HISTORY_TARGET_SALES) {
    if (nowMs() - started > PRICE_HISTORY_TIME_BUDGET_MS) break;

    const r = await mrktFeedFetch({
      gift,
      model: model || null,
      backdrop: backdrop || null,
      cursor,
      count: PRICE_HISTORY_COUNT_PER_PAGE,
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
      if (lastSales.length < 25) {
        lastSales.push({
          ts: it.date || null,
          priceTon: ton,
          number: g.number ?? null,
          model: g.modelTitle || g.modelName || null,
          backdrop: g.backdropName || null,
        });
      }

      if (prices.length >= PRICE_HISTORY_TARGET_SALES) break;
    }

    cursor = r.cursor || '';
    pages++;
    if (!cursor) break;
    if (MRKT_FEED_THROTTLE_MS > 0) await sleep(MRKT_FEED_THROTTLE_MS);
  }

  prices.sort((a, b) => a - b);
  const data = {
    ok: true,
    median: median(prices),
    count: prices.length,
    lastSales,
  };

  priceCache.set(key, { time: now, data });
  return data;
}

async function mrktPriceSmart({ gift, model, backdrop }) {
  // 1) exact
  let h = await mrktHistoryMedianSales({ gift, model, backdrop });
  if (h.median != null && h.count >= 10) return { ...h, level: 'exact' };

  // 2) model only
  if (model) {
    const hm = await mrktHistoryMedianSales({ gift, model, backdrop: '' });
    if (hm.median != null && hm.count >= 10) return { ...hm, level: 'model' };
    if (hm.count > h.count) h = { ...hm, level: 'model' };
  }

  // 3) backdrop only
  if (backdrop) {
    const hb = await mrktHistoryMedianSales({ gift, model: '', backdrop });
    if (hb.median != null && hb.count >= 10) return { ...hb, level: 'backdrop' };
    if (hb.count > h.count) h = { ...hb, level: 'backdrop' };
  }

  // 4) collection only
  const hc = await mrktHistoryMedianSales({ gift, model: '', backdrop: '' });
  if (hc.count > h.count) h = { ...hc, level: 'collection' };

  return { ...h, level: h.level || 'unknown' };
}

// =====================
// Subscriptions loop (ONLY source of chat notifications)
// =====================
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

        const stateKey = `${userId}:${sub.id}:MRKT`;
        const prev = subStates.get(stateKey) || { floor: null, emptyStreak: 0 };

        const lots = await mrktSearchLots(
          { gift: sub.filters.gift, model: sub.filters.model, backdrop: sub.filters.backdrop },
          null,
          null,
          1
        );

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
        const max = sub.maxPriceTon != null ? Number(sub.maxPriceTon) : null;
        const canNotify = newFloor != null && (max == null || newFloor <= max);

        if (canNotify && newFloor != null && (prevFloor == null || Number(prevFloor) !== Number(newFloor))) {
          await sendMessageSafe(
            userId,
            `Подписка #${sub.num}\nGift: ${sub.filters.gift}\n` +
              (sub.filters.model ? `Model: ${sub.filters.model}\n` : '') +
              (sub.filters.backdrop ? `Backdrop: ${sub.filters.backdrop}\n` : '') +
              `Floor: ${newFloor.toFixed(3)} TON`,
            {
              disable_web_page_preview: true,
              reply_markup: lot?.urlMarket ? { inline_keyboard: [[{ text: 'Открыть MRKT', url: lot.urlMarket }]] } : undefined,
            }
          );
          floorNotifs++;
        }

        subStates.set(stateKey, { floor: newFloor, emptyStreak });
      }
    }

    return { processedSubs, floorNotifs };
  } finally {
    isSubsChecking = false;
  }
}

// =====================
// Telegram: menu button + status
// =====================
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
        menu_button: { type: 'web_app', text: 'Панель', web_app: { url: WEBAPP_URL } },
      });
    } catch {}
  }
  await sendMessageSafe(msg.chat.id, 'Открой меню “Панель” рядом со строкой ввода.', { reply_markup: MAIN_KEYBOARD });
});

bot.on('message', async (msg) => {
  const t = msg.text || '';
  if (t === '📌 Статус') {
    const txt =
      `Status:\n` +
      `• MRKT_AUTH: ${MRKT_AUTH_RUNTIME ? '✅' : '❌'}\n` +
      `• MRKT last ok: ${mrktAuthState.lastOkAt ? new Date(mrktAuthState.lastOkAt).toLocaleString() : '-'}\n` +
      `• MRKT last fail: ${mrktAuthState.lastFailAt ? `HTTP ${mrktAuthState.lastFailCode}` : '-'}\n` +
      `• Redis: ${redis ? '✅' : '❌'}`;
    await sendMessageSafe(msg.chat.id, txt, { reply_markup: MAIN_KEYBOARD });
  }
});

// =====================
// WebApp HTML/JS
// =====================
const WEBAPP_HTML = `<!doctype html><html lang="ru"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"/>
<title>MRKT Panel</title>
<style>
:root{--bg:#0b0f14;--card:#111827;--text:#e5e7eb;--muted:#9ca3af;--border:#1f2937;--input:#0f172a;--btn:#1f2937;--btnText:#e5e7eb;--danger:#ef4444;--accent:#22c55e}
*{box-sizing:border-box}
body{margin:0;padding:14px;background:var(--bg);color:var(--text);font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial}
h2{margin:0 0 10px 0}
.card{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:12px;margin:10px 0}
label{display:block;font-size:12px;color:var(--muted);margin-bottom:4px}
input{width:min(340px,86vw);padding:10px;border:1px solid var(--border);border-radius:12px;background:var(--input);color:var(--text);outline:none}
button{padding:10px 12px;border:1px solid var(--border);border-radius:12px;background:var(--btn);color:var(--btnText);cursor:pointer}
button:hover{filter:brightness(1.07)}
#err{display:none;border-color:var(--danger);color:var(--danger)}
.row{display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end}
.tabs{display:flex;gap:8px;flex-wrap:wrap}
.tabbtn{border-radius:999px;padding:8px 12px}
.tabbtn.active{border-color:var(--accent);color:var(--accent)}
.sug{border:1px solid var(--border);border-radius:14px;margin-top:6px;overflow:hidden;max-width:360px;background:rgba(255,255,255,.02);max-height:320px;overflow:auto}
.sug .item{width:100%;text-align:left;border:0;background:transparent;padding:10px;display:flex;gap:10px;align-items:center}
.sug .item:hover{background:rgba(255,255,255,.06)}
.thumb{width:46px;height:46px;border-radius:14px;object-fit:contain;background:rgba(255,255,255,.08);border:1px solid var(--border)}
.thumb.dot{border-radius:50%}
.badge{padding:4px 8px;border-radius:999px;border:1px solid var(--border);color:var(--muted);font-size:12px}
.hscroll{display:flex;gap:10px;overflow-x:auto;padding-bottom:6px;scroll-snap-type:x mandatory}
.lot{min-width:260px;max-width:260px;flex:0 0 auto;border:1px solid var(--border);border-radius:16px;padding:10px;background:rgba(255,255,255,.02);scroll-snap-align:start}
.lot img{width:100%;height:160px;object-fit:cover;background:rgba(255,255,255,.03);border-radius:14px;border:1px solid var(--border)}
.price{font-size:18px;font-weight:800;margin-top:8px}
.muted{color:var(--muted);font-size:13px}
.hr{height:1px;background:var(--border);margin:10px 0}
.table{display:flex;flex-direction:column;gap:8px}
.sale{padding:10px;border:1px solid var(--border);border-radius:12px;background:rgba(255,255,255,.02)}
</style></head><body>
<h2>MRKT Panel</h2>
<div id="err" class="card"></div>

<div class="tabs">
  <button class="tabbtn active" data-tab="market">Market</button>
  <button class="tabbtn" data-tab="subs">Subscriptions</button>
  <button class="tabbtn" data-tab="price">Price</button>
  <button class="tabbtn" data-tab="admin" id="adminTabBtn" style="display:none">Admin</button>
</div>

<div id="market" class="card">
  <h3 style="margin:0 0 8px 0">Фильтры</h3>

  <div class="row">
    <div>
      <label>Gift</label>
      <input id="gift" placeholder="Нажми и выбери" autocomplete="off"/>
      <div id="giftSug" class="sug" style="display:none"></div>
    </div>
    <div>
      <label>Model</label>
      <input id="model" placeholder="Нажми и выбери" autocomplete="off"/>
      <div id="modelSug" class="sug" style="display:none"></div>
    </div>
    <div>
      <label>Backdrop</label>
      <input id="backdrop" placeholder="Нажми и выбери" autocomplete="off"/>
      <div id="backdropSug" class="sug" style="display:none"></div>
    </div>
  </div>

  <div class="row" style="margin-top:10px">
    <div><label>Min TON</label><input id="minPrice" type="number" step="0.001"/></div>
    <div><label>Max TON</label><input id="maxPrice" type="number" step="0.001"/></div>
  </div>

  <div class="row" style="margin-top:10px">
    <button id="apply">Применить</button>
    <button id="lotsRefresh">🔄 Лоты</button>
  </div>

  <div id="status" class="muted" style="margin-top:10px"></div>
  <div id="lots" class="hscroll" style="margin-top:10px"></div>
</div>

<div id="subs" class="card" style="display:none">
  <h3 style="margin:0 0 8px 0">Подписки</h3>
  <div class="row">
    <button id="subCreate">➕ Создать из текущих фильтров</button>
    <button id="subCheckNow">🔎 Проверить сейчас</button>
    <button id="subRefresh">🔄 Обновить</button>
  </div>
  <div id="subsList" style="margin-top:10px"></div>
</div>

<div id="price" class="card" style="display:none">
  <h3 style="margin:0 0 8px 0">Цена подарка</h3>
  <div class="row">
    <button id="priceRefresh">🔄 Обновить</button>
  </div>
  <div id="priceBox" class="muted" style="margin-top:10px"></div>
  <div class="hr"></div>
  <div><b>Последние продажи (MRKT)</b></div>
  <div id="sales" class="table" style="margin-top:10px"></div>
</div>

<div id="admin" class="card" style="display:none">
  <h3 style="margin:0 0 8px 0">Admin</h3>
  <div class="muted">MRKT токен хранится в Redis. Обнови токен без Railway (можно с телефона).</div>
  <div class="row" style="margin-top:10px">
    <div><label>Текущий токен (маска)</label><input id="tokMask" disabled/></div>
    <div><label>Новый MRKT_AUTH</label><input id="tokNew" placeholder="вставь токен"/></div>
    <button id="tokSave">Сохранить токен</button>
  </div>
</div>

<script src="https://telegram.org/js/telegram-web-app.js"></script>
<script src="/app.js"></script>
</body></html>`;

const WEBAPP_JS = `(() => {
  const tg = window.Telegram?.WebApp;
  tg?.ready();

  const initData = tg?.initData || '';
  const el = (id) => document.getElementById(id);

  // Apply Telegram theme if available (avoid black-on-black)
  function applyTheme() {
    const p = tg?.themeParams || {};
    const root = document.documentElement;
    if (p.bg_color) root.style.setProperty('--bg', p.bg_color);
    if (p.secondary_bg_color) root.style.setProperty('--card', p.secondary_bg_color);
    if (p.text_color) root.style.setProperty('--text', p.text_color);
    if (p.hint_color) root.style.setProperty('--muted', p.hint_color);
    if (p.button_color) root.style.setProperty('--btn', p.button_color);
    if (p.button_text_color) root.style.setProperty('--btnText', p.button_text_color);
  }
  applyTheme();
  tg?.onEvent?.('themeChanged', applyTheme);

  if (!initData) {
    document.body.innerHTML = '<div style="padding:16px;font-family:system-ui;color:#fff;background:#0b0f14">' +
      '<h3>Открой панель из Telegram</h3>' +
      '<p>Эта страница работает только внутри Telegram WebApp (Menu Button “Панель”).</p>' +
      '</div>';
    return;
  }

  function showErr(msg){ const box=el('err'); box.style.display='block'; box.textContent=msg; }
  function hideErr(){ const box=el('err'); box.style.display='none'; box.textContent=''; }

  async function api(path, opts = {}) {
    const res = await fetch(path, {
      ...opts,
      headers: {
        'Content-Type':'application/json',
        'X-Tg-Init-Data': initData,
        ...(opts.headers||{})
      }
    });
    const txt = await res.text();
    let data=null;
    try{ data = txt?JSON.parse(txt):null; }catch{ data={raw:txt}; }
    if(!res.ok) throw new Error('HTTP '+res.status+': '+JSON.stringify(data).slice(0,400));
    return data;
  }

  // Tabs
  function setTab(name){
    ['market','subs','price','admin'].forEach(x => el(x).style.display = (x===name?'block':'none'));
    document.querySelectorAll('.tabbtn').forEach(b => b.classList.toggle('active', b.dataset.tab===name));
  }
  document.querySelectorAll('.tabbtn').forEach(b => b.onclick = () => setTab(b.dataset.tab));

  // Suggestions
  function hideSug(id){ const b=el(id); b.style.display='none'; b.innerHTML=''; }
  function renderSug(id, items, onPick){
    const b=el(id);
    if(!items||!items.length){ hideSug(id); return; }
    b.innerHTML = items.map(x => {
      const thumb = x.imgUrl
        ? '<img class="thumb" src="'+x.imgUrl+'" referrerpolicy="no-referrer"/>'
        : (x.colorHex ? '<div class="thumb dot" style="background:'+x.colorHex+'"></div>' : '<div class="thumb"></div>');

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

    // Stop page scroll when scrolling inside dropdown
    b.addEventListener('wheel', (ev) => ev.stopPropagation(), { passive:true });
  }

  // Horizontal wheel scroll for lots
  const lotsBox = el('lots');
  lotsBox.addEventListener('wheel', (e) => {
    if (Math.abs(e.deltaY) > Math.abs(e.deltaX)) {
      lotsBox.scrollLeft += e.deltaY;
      e.preventDefault();
    }
  }, { passive:false });

  // Open links
  document.body.addEventListener('click', (e) => {
    const btn = e.target.closest('button[data-open]');
    if(!btn) return;
    const url = btn.getAttribute('data-open');
    if(url) tg?.openTelegramLink ? tg.openTelegramLink(url) : window.open(url,'_blank');
  });

  let selectedGiftValue = '';

  function renderLots(resp){
    const box = el('lots');
    if(resp.ok===false){
      box.innerHTML = '<div style="color:#ef4444"><b>'+ (resp.reason||'error') +'</b></div>';
      return;
    }
    const lots = resp.lots||[];
    if(!lots.length){ box.innerHTML = '<i class="muted">Лотов не найдено</i>'; return; }
    box.innerHTML = lots.map(x => {
      const dot = x.backdropColorHex ? '<span class="badge" style="display:inline-flex;align-items:center;gap:8px"><span style="width:14px;height:14px;border-radius:50%;border:1px solid var(--border);background:'+x.backdropColorHex+'"></span>'+x.backdrop+'</span>' : '';
      const img = x.imgUrl ? '<img src="'+x.imgUrl+'" referrerpolicy="no-referrer" loading="lazy"/>' :
        '<div style="height:160px;border:1px solid rgba(255,255,255,.08);border-radius:14px"></div>';
      return '<div class="lot">'+img+
        '<div class="price">'+x.priceTon.toFixed(3)+' TON</div>'+
        '<div><b>'+x.name+'</b></div>'+
        (x.model?'<div class="muted">Model: '+x.model+'</div>':'')+
        (dot?'<div style="margin-top:6px">'+dot+'</div>':'')+
        '<div class="row" style="margin-top:8px">'+
          '<button data-open="'+x.urlTelegram+'">NFT</button>'+
          '<button data-open="'+x.urlMarket+'">MRKT</button>'+
        '</div></div>';
    }).join('');
  }

  function renderSubs(list){
    const box = el('subsList');
    if(!list.length){ box.innerHTML = '<i class="muted">Подписок нет</i>'; return; }
    box.innerHTML = list.map(s => {
      return '<div class="card" style="margin:8px 0">'+
        '<div><b>#'+s.num+'</b> '+(s.enabled?'ON':'OFF')+'</div>'+
        '<div class="muted">Gift: '+s.filters.gift+'</div>'+
        '<div class="muted">Model: '+(s.filters.model||'any')+'</div>'+
        '<div class="muted">Backdrop: '+(s.filters.backdrop||'any')+'</div>'+
        '<div class="muted">Max: '+(s.maxPriceTon==null?'∞':s.maxPriceTon)+'</div>'+
        '<div class="row" style="margin-top:8px">'+
          '<button data-act="subToggle" data-id="'+s.id+'">'+(s.enabled?'⏸':'▶️')+'</button>'+
          '<button data-act="subMax" data-id="'+s.id+'">💰 Max</button>'+
          '<button data-act="subDel" data-id="'+s.id+'">🗑</button>'+
        '</div></div>';
    }).join('');
  }

  function renderPrice(resp){
    const box = el('priceBox');
    if(resp.ok===false){
      box.innerHTML = '<b style="color:#ef4444">'+(resp.reason||'error')+'</b>';
      return;
    }
    box.innerHTML =
      '<div><b>Floor:</b> '+(resp.floor!=null?resp.floor.toFixed(3)+' TON':'нет')+'</div>' +
      '<div><b>История (медиана):</b> '+(resp.median!=null?resp.median.toFixed(3)+' TON':'нет')+' (n='+resp.count+', '+resp.level+')</div>';
  }

  function renderSales(resp){
    const box = el('sales');
    const list = resp.lastSales || [];
    if(!list.length){ box.innerHTML = '<i class="muted">Нет данных</i>'; return; }
    box.innerHTML = list.map(x => {
      return '<div class="sale">'+
        '<div><b>'+x.priceTon.toFixed(3)+' TON</b> '+(x.ts?('<span class="muted"> '+x.ts+'</span>'):'')+'</div>'+
        (x.model?'<div class="muted">Model: '+x.model+'</div>':'')+
        (x.backdrop?'<div class="muted">Backdrop: '+x.backdrop+'</div>':'')+
      '</div>';
    }).join('');
  }

  document.body.addEventListener('click', async (e) => {
    const btn = e.target.closest('button[data-act]');
    if(!btn) return;
    const act = btn.dataset.act;
    const id = btn.dataset.id;
    hideErr();
    try{
      if(act==='subToggle') await api('/api/sub/toggle',{method:'POST',body:JSON.stringify({id})});
      if(act==='subDel') await api('/api/sub/delete',{method:'POST',body:JSON.stringify({id})});
      if(act==='subMax'){
        const v = prompt('MAX TON:', '5');
        if(v==null) return;
        await api('/api/sub/setmax',{method:'POST',body:JSON.stringify({id, maxPriceTon:Number(v)})});
      }
      await refreshState();
    }catch(err){ showErr(err.message||String(err)); }
  });

  async function refreshState(){
    const st = await api('/api/state');

    el('status').textContent =
      'MRKT_AUTH: ' + (st.api.mrktAuthSet?'✅':'❌') + ' | ' +
      'Subs notify: ' + (st.user.enabled?'ON':'OFF');

    el('gift').value = st.user.filters.giftLabel || st.user.filters.gift || '';
    selectedGiftValue = st.user.filters.gift || '';
    el('model').value = st.user.filters.model || '';
    el('backdrop').value = st.user.filters.backdrop || '';
    el('minPrice').value = st.user.minPriceTon ?? 0;
    el('maxPrice').value = (st.user.maxPriceTon==null) ? '' : st.user.maxPriceTon;

    renderSubs(st.user.subscriptions||[]);

    if(st.api.isAdmin){
      el('adminTabBtn').style.display='inline-block';
      el('tokMask').value = st.api.mrktAuthMask || '';
    }
  }

  async function refreshLots(){
    try{
      const r = await api('/api/mrkt/lots');
      renderLots(r);
    }catch(err){
      renderLots({ok:false, reason: err.message});
    }
  }

  async function refreshPrice(){
    try{
      const r = await api('/api/mrkt/price');
      renderPrice(r);
      renderSales(r);
    }catch(err){
      renderPrice({ok:false, reason: err.message});
      renderSales({lastSales:[]});
    }
  }

  // Gift suggest: show list on focus (no need to type)
  async function showGiftSug(){
    const q = el('gift').value.trim();
    const r = await api('/api/mrkt/collections?q='+encodeURIComponent(q));
    renderSug('giftSug', r.items||[], (v) => {
      selectedGiftValue = v;
      el('gift').value = (r.mapLabel||{})[v] || v;
      el('model').value=''; el('backdrop').value='';
    });
  }
  el('gift').addEventListener('focus', ()=>{ showGiftSug().catch(()=>{}); });
  el('gift').addEventListener('input', ()=>{ showGiftSug().catch(()=>{}); });

  // Model suggest: show list on focus too
  async function showModelSug(){
    const gift = selectedGiftValue || '';
    const q = el('model').value.trim();
    if(!gift){ hideSug('modelSug'); return; }
    const r = await api('/api/mrkt/suggest?kind=model&gift='+encodeURIComponent(gift)+'&q='+encodeURIComponent(q));
    renderSug('modelSug', r.items||[], (v)=>{ el('model').value=v; el('backdrop').value=''; });
  }
  el('model').addEventListener('focus', ()=>{ showModelSug().catch(()=>{}); });
  el('model').addEventListener('input', ()=>{ showModelSug().catch(()=>{}); });

  // Backdrop suggest: show list on focus too
  async function showBackdropSug(){
    const gift = selectedGiftValue || '';
    const q = el('backdrop').value.trim();
    if(!gift){ hideSug('backdropSug'); return; }
    const r = await api('/api/mrkt/suggest?kind=backdrop&gift='+encodeURIComponent(gift)+'&q='+encodeURIComponent(q));
    renderSug('backdropSug', r.items||[], (v)=>{ el('backdrop').value=v; });
  }
  el('backdrop').addEventListener('focus', ()=>{ showBackdropSug().catch(()=>{}); });
  el('backdrop').addEventListener('input', ()=>{ showBackdropSug().catch(()=>{}); });

  document.addEventListener('click', (e) => {
    if(!e.target.closest('#gift,#model,#backdrop,.sug')){
      hideSug('giftSug'); hideSug('modelSug'); hideSug('backdropSug');
    }
  });

  // Buttons
  el('apply').onclick = async () => {
    hideErr();
    try{
      await api('/api/state/patch', {
        method:'POST',
        body: JSON.stringify({
          filters:{
            gift: selectedGiftValue || el('gift').value.trim(),
            giftLabel: el('gift').value.trim(),
            model: el('model').value.trim(),
            backdrop: el('backdrop').value.trim()
          },
          minPriceTon: Number(el('minPrice').value||0),
          maxPriceTon: el('maxPrice').value==='' ? null : Number(el('maxPrice').value)
        })
      });
      await refreshState();
      await refreshLots();
      await refreshPrice();
    }catch(err){ showErr(err.message||String(err)); }
  };

  el('lotsRefresh').onclick = async () => { hideErr(); await refreshLots(); };

  el('subCreate').onclick = async () => {
    hideErr();
    try{ await api('/api/sub/create',{method:'POST'}); await refreshState(); }
    catch(err){ showErr(err.message||String(err)); }
  };
  el('subRefresh').onclick = async () => { hideErr(); await refreshState(); };
  el('subCheckNow').onclick = async () => {
    hideErr();
    try{
      const r = await api('/api/sub/checknow',{method:'POST'});
      alert('Готово: subs='+r.processedSubs+', floorNotifs='+r.floorNotifs);
    }catch(err){ showErr(err.message||String(err)); }
  };

  el('priceRefresh').onclick = async () => { hideErr(); await refreshPrice(); };

  // Admin save token
  const tokSave = el('tokSave');
  if(tokSave){
    tokSave.onclick = async () => {
      hideErr();
      try{
        const t = el('tokNew').value.trim();
        if(!t) return showErr('Вставь токен.');
        await api('/api/admin/mrkt_auth',{method:'POST',body:JSON.stringify({token:t})});
        el('tokNew').value='';
        await refreshState();
      }catch(err){ showErr(err.message||String(err)); }
    };
  }

  // Init
  refreshState().then(()=>Promise.all([refreshLots(), refreshPrice()])).catch(e=>showErr(e.message||String(e)));
})();`;

// =====================
// Web server + API
// =====================
function startWebServer() {
  const app = express();
  app.use(express.json({ limit: '800kb' }));

  app.get('/health', (req, res) => res.status(200).send('ok'));
  app.get('/', (req, res) => {
    res.setHeader('Content-Type', 'text/html; charset=utf-8');
    res.send(WEBAPP_HTML);
  });
  app.get('/app.js', (req, res) => {
    res.setHeader('Content-Type', 'application/javascript; charset=utf-8');
    res.send(WEBAPP_JS);
  });

  function auth(req, res, next) {
    const initData = String(req.headers['x-tg-init-data'] || '');
    const v = verifyTelegramWebAppInitData(initData);
    if (!v.ok) return res.status(401).json({ ok: false, reason: v.reason });
    req.userId = v.userId;
    next();
  }
  const isAdmin = (userId) => ADMIN_USER_ID && Number(userId) === Number(ADMIN_USER_ID);

  // Proxy: Fragment gift image (for lots)
  app.get('/img/gift', async (req, res) => {
    const name = String(req.query.name || '').trim();
    if (!name) return res.status(400).send('no name');

    const url = fragmentGiftRemoteUrl(name);
    const r = await fetchWithTimeout(url, { method: 'GET' }, 8000).catch(() => null);
    if (!r || !r.ok) return res.status(404).send('not found');

    const ct = r.headers.get('content-type') || 'image/jpeg';
    res.setHeader('Content-Type', ct);
    res.setHeader('Cache-Control', 'public, max-age=86400');

    const nodeStream = Readable.fromWeb(r.body);
    nodeStream.pipe(res);
  });

  // Proxy: MRKT CDN images (for collections/models thumbs)
  app.get('/img/cdn', async (req, res) => {
    const key = String(req.query.key || '').trim();
    if (!key) return res.status(400).send('no key');

    const url = joinUrl(MRKT_CDN_BASE, key);
    const r = await fetchWithTimeout(url, { method: 'GET', headers: { Accept: 'image/*' } }, 8000).catch(() => null);
    if (!r || !r.ok) return res.status(404).send('not found');

    const ct = r.headers.get('content-type') || 'image/webp';
    res.setHeader('Content-Type', ct);
    res.setHeader('Cache-Control', 'public, max-age=86400');

    const nodeStream = Readable.fromWeb(r.body);
    nodeStream.pipe(res);
  });

  app.get('/api/state', auth, (req, res) => {
    const u = getOrCreateUser(req.userId);
    const mask = MRKT_AUTH_RUNTIME ? MRKT_AUTH_RUNTIME.slice(0, 4) + '…' + MRKT_AUTH_RUNTIME.slice(-4) : '';
    res.json({
      ok: true,
      api: { mrktAuthSet: !!MRKT_AUTH_RUNTIME, isAdmin: isAdmin(req.userId), mrktAuthMask: mask },
      user: {
        enabled: !!u.enabled,
        minPriceTon: u.minPriceTon ?? 0,
        maxPriceTon: u.maxPriceTon ?? null,
        filters: u.filters,
        subscriptions: u.subscriptions || [],
      },
    });
  });

  // Collections suggest (with images + floor)
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
        sub: floorTon != null ? `floor: ${floorTon.toFixed(3)} TON` : null,
        badge: null,
      };
    });

    res.json({ ok: true, items, mapLabel });
  });

  // Model/backdrop suggest
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
          const pct = m.rarityPerMille != null ? formatPctFromPermille(m.rarityPerMille) : null;
          const floorTon = m.floorNano != null ? tonFromNano(m.floorNano) : null;
          return {
            label: m.name,
            value: m.name,
            imgUrl: m.thumbKey ? `/img/cdn?key=${encodeURIComponent(m.thumbKey)}` : null,
            sub: floorTon != null ? `min: ${floorTon.toFixed(3)} TON` : null,
            badge: pct,
          };
        });

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
          imgUrl: null,
          colorHex: b.centerHex || null,
          sub: null,
          badge: b.rarityPerMille != null ? formatPctFromPermille(b.rarityPerMille) : null,
        }));

      return res.json({ ok: true, items });
    }

    res.status(400).json({ ok: false, reason: 'BAD_KIND' });
  });

  // Patch state (also resolves gift if user typed title)
  app.post('/api/state/patch', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    const b = req.body || {};

    if (b.minPriceTon != null) u.minPriceTon = Number(b.minPriceTon) || 0;
    if (Object.prototype.hasOwnProperty.call(b, 'maxPriceTon')) u.maxPriceTon = b.maxPriceTon == null ? null : Number(b.maxPriceTon);

    if (b.filters && typeof b.filters === 'object') {
      const gift = typeof b.filters.gift === 'string' ? b.filters.gift.trim() : '';
      const giftLabel = typeof b.filters.giftLabel === 'string' ? b.filters.giftLabel.trim() : '';

      // resolve by title if needed
      let resolvedGift = gift;
      if (!resolvedGift && giftLabel) {
        const cols = await mrktGetCollections();
        const found = cols.find((c) => norm(c.title) === norm(giftLabel));
        if (found) resolvedGift = found.name;
      }

      if (resolvedGift) u.filters.gift = resolvedGift;
      if (giftLabel) u.filters.giftLabel = giftLabel;

      if (typeof b.filters.model === 'string') u.filters.model = b.filters.model.trim();
      if (typeof b.filters.backdrop === 'string') u.filters.backdrop = b.filters.backdrop.trim();
    }

    res.json({ ok: true });
  });

  // Lots list
  app.get('/api/mrkt/lots', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    if (!MRKT_AUTH_RUNTIME) return res.json({ ok: false, reason: 'MRKT_AUTH_NOT_SET', lots: [] });
    if (!u.filters.gift) return res.json({ ok: true, lots: [] });

    const minP = u.minPriceTon != null ? Number(u.minPriceTon) : 0;
    const maxP = u.maxPriceTon != null ? Number(u.maxPriceTon) : null;

    const r = await mrktSearchLots({ gift: u.filters.gift, model: u.filters.model, backdrop: u.filters.backdrop }, minP, maxP, WEBAPP_LOTS_PAGES);
    if (!r.ok) return res.json({ ok: false, reason: r.reason, lots: [] });

    const backs = await mrktGetBackdropsForGift(u.filters.gift);
    const backMap = new Map(backs.map((b) => [norm(b.name), b]));

    const lots = (r.gifts || [])
      .slice(0, WEBAPP_LOTS_LIMIT)
      .map((x) => {
        const b = x.backdrop ? backMap.get(norm(x.backdrop)) : null;
        const imgUrl = x.giftName ? `/img/gift?name=${encodeURIComponent(x.giftName)}` : null;
        return {
          id: x.id,
          name: x.name,
          priceTon: x.priceTon,
          urlTelegram: x.urlTelegram,
          urlMarket: x.urlMarket,
          model: x.model,
          backdrop: x.backdrop,
          imgUrl,
          backdropColorHex: b?.centerHex || null,
        };
      });

    res.json({ ok: true, lots });
  });

  // Price endpoint (floor + smart history)
  app.get('/api/mrkt/price', auth, async (req, res) => {
    const u = getOrCreateUser(req.userId);
    if (!MRKT_AUTH_RUNTIME) return res.json({ ok: false, reason: 'MRKT_AUTH_NOT_SET' });
    if (!u.filters.gift) return res.json({ ok: false, reason: 'NO_GIFT' });

    const floorLots = await mrktSearchLots(
      { gift: u.filters.gift, model: u.filters.model, backdrop: u.filters.backdrop },
      null,
      null,
      1
    );

    const floor = floorLots.ok && floorLots.gifts.length ? floorLots.gifts[0].priceTon : null;

    const hist = await mrktPriceSmart({
      gift: u.filters.gift,
      model: u.filters.model || '',
      backdrop: u.filters.backdrop || '',
    });

    res.json({
      ok: true,
      floor,
      median: hist.median,
      count: hist.count,
      level: hist.level,
      lastSales: hist.lastSales || [],
    });
  });

  // Subscriptions API
  app.post('/api/sub/create', auth, (req, res) => {
    const u = getOrCreateUser(req.userId);
    const r = makeSubFromCurrentFilters(u);
    if (!r.ok) return res.status(400).json({ ok: false, reason: r.reason });
    u.subscriptions.push(r.sub);
    renumberSubs(u);
    res.json({ ok: true });
  });
  app.post('/api/sub/toggle', auth, (req, res) => {
    const u = getOrCreateUser(req.userId);
    const id = req.body?.id;
    const s = findSub(u, id);
    if (!s) return res.status(404).json({ ok: false, reason: 'NOT_FOUND' });
    s.enabled = !s.enabled;
    res.json({ ok: true });
  });
  app.post('/api/sub/delete', auth, (req, res) => {
    const u = getOrCreateUser(req.userId);
    const id = req.body?.id;
    u.subscriptions = (u.subscriptions || []).filter((x) => x && x.id !== id);
    renumberSubs(u);
    res.json({ ok: true });
  });
  app.post('/api/sub/setmax', auth, (req, res) => {
    const u = getOrCreateUser(req.userId);
    const id = req.body?.id;
    const v = Number(req.body?.maxPriceTon);
    const s = findSub(u, id);
    if (!s) return res.status(404).json({ ok: false, reason: 'NOT_FOUND' });
    if (!Number.isFinite(v) || v <= 0) return res.status(400).json({ ok: false, reason: 'BAD_MAX' });
    s.maxPriceTon = v;
    res.json({ ok: true });
  });
  app.post('/api/sub/checknow', auth, async (req, res) => {
    const st = await checkSubscriptionsForAllUsers({ manual: true });
    res.json(st);
  });

  // Admin: update MRKT_AUTH in Redis (only you)
  app.post('/api/admin/mrkt_auth', auth, async (req, res) => {
    if (!isAdmin(req.userId)) return res.status(403).json({ ok: false, reason: 'NOT_ADMIN' });

    const t = String(req.body?.token || '').trim();
    if (!t) return res.status(400).json({ ok: false, reason: 'EMPTY_TOKEN' });

    MRKT_AUTH_RUNTIME = t;
    try { await saveMrktAuthToRedis(t); } catch {}

    modelsCache.clear();
    backdropsCache.clear();
    collectionsCache = { time: 0, items: [] };

    res.json({ ok: true });
  });

  const port = Number(process.env.PORT || 3000);
  app.listen(port, '0.0.0.0', () => console.log('WebApp listening on', port));
}

startWebServer();

// =====================
// Subscriptions interval
// =====================
setInterval(() => {
  checkSubscriptionsForAllUsers().catch((e) => console.error('subs error:', e));
}, SUBS_CHECK_INTERVAL_MS);

// =====================
// Bootstrap
// =====================
(async () => {
  if (REDIS_URL) {
    await initRedis();
    if (redis) await loadMrktAuthFromRedis();
  }
  console.log('Bot started. /start');
})();
