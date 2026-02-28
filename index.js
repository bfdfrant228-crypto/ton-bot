/**
 * ton-bot MRKT + WebApp Panel (single-file)
 * version: 2026-02-28-webapp-collections-subs-dark-v7-fixed
 */

const TelegramBot = require('node-telegram-bot-api');
const express = require('express');
const crypto = require('crypto');
const { Readable } = require('stream');

// ============ ENV ============
const token = process.env.TELEGRAM_TOKEN;
if (!token) {
  console.error('TELEGRAM_TOKEN not set');
  process.exit(1);
}

const MODE = process.env.MODE || 'real';
const WEBAPP_URL = process.env.WEBAPP_URL || null;
const WEBAPP_AUTH_MAX_AGE_SEC = Number(process.env.WEBAPP_AUTH_MAX_AGE_SEC || 86400);

const ADMIN_USER_ID = process.env.ADMIN_USER_ID ? Number(process.env.ADMIN_USER_ID) : null;

const REDIS_URL = process.env.REDIS_URL || process.env.REDIS_PRIVATE_URL || null;

const MRKT_API_URL = 'https://api.tgmrkt.io/api/v1';
const MRKT_CDN_BASE = (process.env.MRKT_CDN_BASE || 'https://cdn.tgmrkt.io/').trim();
const FRAGMENT_GIFT_IMG_BASE = (process.env.FRAGMENT_GIFT_IMG_BASE || 'https://nft.fragment.com/gift/').trim();

// FIX: MRKT_COLLECTIONS теперь определён
const MRKT_COLLECTIONS = String(process.env.MRKT_COLLECTIONS || '')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);

let MRKT_AUTH_RUNTIME = (process.env.MRKT_AUTH || '').trim() || null;

// intervals
const CHECK_INTERVAL_MS = Number(process.env.CHECK_INTERVAL_MS || 5000);
const SUBS_CHECK_INTERVAL_MS = Number(process.env.SUBS_CHECK_INTERVAL_MS || 9000);

// monitor
const MRKT_TIMEOUT_MS = Number(process.env.MRKT_TIMEOUT_MS || 8000);
const MRKT_COUNT = Number(process.env.MRKT_COUNT || 50);
const MRKT_PAGES = Number(process.env.MRKT_PAGES || 6);
const MRKT_PAGES_MONITOR = Number(process.env.MRKT_PAGES_MONITOR || 1);
const ONLY_CHEAPEST_PER_CHECK = String(process.env.ONLY_CHEAPEST_PER_CHECK || '1') !== '0';
const SEND_DELAY_MS = Number(process.env.SEND_DELAY_MS || 80);
const SENT_TTL_MS = Number(process.env.SENT_TTL_MS || 24 * 60 * 60 * 1000);

// webapp
const WEBAPP_LOTS_LIMIT = Number(process.env.WEBAPP_LOTS_LIMIT || 25);
const WEBAPP_LOTS_PAGES = Number(process.env.WEBAPP_LOTS_PAGES || 1);
const WEBAPP_SUGGEST_LIMIT = Number(process.env.WEBAPP_SUGGEST_LIMIT || 16);

// subs
const SUBS_MAX_NOTIFICATIONS_PER_CYCLE = Number(process.env.SUBS_MAX_NOTIFICATIONS_PER_CYCLE || 8);
const SUBS_EMPTY_CONFIRM = Number(process.env.SUBS_EMPTY_CONFIRM || 2);

// ============ init ============
console.log('v7 start', {
  MODE,
  WEBAPP_URL: !!WEBAPP_URL,
  REDIS: !!REDIS_URL,
  ADMIN_USER_ID,
  MRKT_AUTH: !!MRKT_AUTH_RUNTIME,
  MRKT_COLLECTIONS: MRKT_COLLECTIONS.length
});

const bot = new TelegramBot(token, { polling: true });

// ============ State ============
const users = new Map(); // userId -> state
const sentDeals = new Map(); // key -> ts
const subStates = new Map(); // `${userId}:${subId}:MRKT` -> { floor, emptyStreak, feedLastId }
let isChecking = false;
let isSubsChecking = false;

// caches
const CACHE_TTL_MS = 5 * 60_000;
let collectionsCache = { time: 0, items: [] }; // items: {name,title,thumbKey,floorNano,isHidden}
const modelsCache = new Map(); // giftName -> {time, items:[{name, rarityPerMille, thumbKey}]}
const backdropsCache = new Map(); // giftName -> {time, items:[{name, rarityPerMille, centerHex}]}

// ============ Helpers ============
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
function mrktLotUrlFromId(id) {
  if (!id) return 'https://t.me/mrkt';
  const appId = String(id).replace(/-/g, '');
  return `https://t.me/mrkt/app?startapp=${appId}`;
}
function inRange(price, minPrice, maxPrice) {
  if (!Number.isFinite(price)) return false;
  const min = minPrice != null ? Number(minPrice) : 0;
  const max = maxPrice != null ? Number(maxPrice) : null;
  if (Number.isFinite(min) && price < min) return false;
  if (max != null && Number.isFinite(max) && price > max) return false;
  return true;
}
function pruneSentDeals() {
  const now = nowMs();
  for (const [k, ts] of sentDeals.entries()) {
    if (now - ts > SENT_TTL_MS) sentDeals.delete(k);
  }
}
function formatPctFromPermille(perMille) {
  const v = Number(perMille);
  if (!Number.isFinite(v)) return null;
  const pct = v / 10;
  const s = pct % 1 === 0 ? pct.toFixed(0) : pct.toFixed(1);
  return `${s}%`;
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
  try {
    user = JSON.parse(params.get('user') || 'null');
  } catch {}
  const userId = user?.id;
  if (!userId) return { ok: false, reason: 'NO_USER' };
  return { ok: true, userId };
}

// ============ Redis (token storage) ============
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

// ============ User ============
function getOrCreateUser(userId) {
  if (!users.has(userId)) {
    users.set(userId, {
      enabled: true,
      minPriceTon: 0,
      maxPriceTon: null,
      state: null, // awaiting_max|awaiting_min
      filters: { gift: '', model: '', backdrop: '' }, // gift = collectionName
      subscriptions: [],
    });
  }
  return users.get(userId);
}
function renumberSubs(user) {
  const subs = Array.isArray(user.subscriptions) ? user.subscriptions : [];
  subs.forEach((s, idx) => {
    if (s) s.num = idx + 1;
  });
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
    filters: { gift: user.filters.gift, model: user.filters.model || '', backdrop: user.filters.backdrop || '' },
  };
  return { ok: true, sub };
}

// ============ MRKT Headers ============
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

// ============ MRKT: collections (gift list) ============
async function mrktGetCollections() {
  const now = nowMs();
  if (collectionsCache.items.length && now - collectionsCache.time < CACHE_TTL_MS) return collectionsCache.items;

  const res = await fetchWithTimeout(
    `${MRKT_API_URL}/gifts/collections`,
    { method: 'GET', headers: { Accept: 'application/json' } },
    MRKT_TIMEOUT_MS
  ).catch(() => null);

  if (!res || !res.ok) {
    // FIX: fallback использует MRKT_COLLECTIONS (теперь определена)
    const fallback = (MRKT_COLLECTIONS && MRKT_COLLECTIONS.length ? MRKT_COLLECTIONS : []).map((x) => ({
      name: x,
      title: x,
      modelStickerThumbnailKey: null,
      floorPriceNanoTons: null,
      isHidden: false,
    }));
    collectionsCache = { time: now, items: fallback };
    return fallback;
  }

  const data = await res.json().catch(() => null);
  const arr = Array.isArray(data) ? data : [];
  const items = arr
    .filter((x) => x && x.isHidden !== true)
    .map((x) => ({
      name: String(x.name || '').trim(),
      title: String(x.title || x.name || '').trim(),
      modelStickerThumbnailKey: x.modelStickerThumbnailKey || null,
      floorPriceNanoTons: x.floorPriceNanoTons ?? null,
      isHidden: !!x.isHidden,
    }))
    .filter((x) => x.name);

  collectionsCache = { time: now, items };
  return items;
}

// ============ MRKT: models/backdrops ============
async function mrktGetModelsForGift(giftName) {
  if (!giftName) return [];
  const cached = modelsCache.get(giftName);
  if (cached && nowMs() - cached.time < CACHE_TTL_MS) return cached.items;

  const body = { collections: [giftName] };
  const res = await fetchWithTimeout(
    `${MRKT_API_URL}/gifts/models`,
    { method: 'POST', headers: { ...mrktHeaders(), Authorization: MRKT_AUTH_RUNTIME || '' }, body: JSON.stringify(body) },
    MRKT_TIMEOUT_MS
  ).catch(() => null);

  if (!res || !res.ok) {
    modelsCache.set(giftName, { time: nowMs(), items: [] });
    return [];
  }

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
    { method: 'POST', headers: { ...mrktHeaders(), Authorization: MRKT_AUTH_RUNTIME || '' }, body: JSON.stringify(body) },
    MRKT_TIMEOUT_MS
  ).catch(() => null);

  if (!res || !res.ok) {
    backdropsCache.set(giftName, { time: nowMs(), items: [] });
    return [];
  }

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

// ============ MRKT: lots ============
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
  if (!res.ok) return { ok: false, reason: `HTTP_${res.status}`, gifts: [], cursor: '' };

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

      const giftName = g.name || giftNameFallbackFromCollectionAndNumber(g.collectionTitle || g.collectionName || g.title, g.number) || null;
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

// ============ Subscriptions check ============
async function checkSubscriptionsForAllUsers({ manual = false } = {}) {
  if (MODE !== 'real') return { processedSubs: 0, floorNotifs: 0, feedNotifs: 0 };
  if (isSubsChecking && !manual) return { processedSubs: 0, floorNotifs: 0, feedNotifs: 0 };

  isSubsChecking = true;
  try {
    let processedSubs = 0,
      floorNotifs = 0,
      feedNotifs = 0;

    for (const [userId, user] of users.entries()) {
      const subs = Array.isArray(user.subscriptions) ? user.subscriptions : [];
      const active = subs.filter((s) => s && s.enabled);

      for (const sub of active) {
        processedSubs++;
        if (floorNotifs >= SUBS_MAX_NOTIFICATIONS_PER_CYCLE) break;

        const stateKey = `${userId}:${sub.id}:MRKT`;
        const prev = subStates.get(stateKey) || { floor: null, emptyStreak: 0, feedLastId: null };

        const lots = await mrktSearchLots({ gift: sub.filters.gift, model: sub.filters.model, backdrop: sub.filters.backdrop }, null, null, 1);
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
        } else emptyStreak = 0;

        const prevFloor = prev.floor;
        const max = sub.maxPriceTon != null ? Number(sub.maxPriceTon) : null;
        const canNotify = newFloor != null && (max == null || newFloor <= max);

        if (canNotify && prevFloor !== newFloor && newFloor != null) {
          await sendMessageSafe(userId, `Подписка #${sub.num}\n${sub.filters.gift}\nФлор: ${newFloor.toFixed(3)} TON`, {
            disable_web_page_preview: true,
            reply_markup: lot?.urlMarket ? { inline_keyboard: [[{ text: 'Открыть MRKT', url: lot.urlMarket }]] } : undefined,
          });
          floorNotifs++;
        }

        subStates.set(stateKey, { ...prev, floor: newFloor, emptyStreak });
      }
    }

    return { processedSubs, floorNotifs, feedNotifs };
  } finally {
    isSubsChecking = false;
  }
}

// ============ Monitor ============
async function checkMarketsForAllUsers() {
  if (MODE !== 'real') return;
  if (isChecking) return;

  isChecking = true;
  try {
    pruneSentDeals();

    for (const [userId, user] of users.entries()) {
      if (!user.enabled) continue;
      if (!user.maxPriceTon) continue;
      if (!user.filters.gift) continue;

      const minP = user.minPriceTon != null ? Number(user.minPriceTon) : 0;
      const maxP = Number(user.maxPriceTon);

      const lots = await mrktSearchLots({ gift: user.filters.gift, model: user.filters.model, backdrop: user.filters.backdrop }, minP, maxP, MRKT_PAGES_MONITOR);
      if (!lots.ok || !lots.gifts.length) continue;

      const list = ONLY_CHEAPEST_PER_CHECK ? lots.gifts.slice(0, 1) : lots.gifts;

      for (const g of list) {
        const key = `${userId}:mrkt:${g.id}`;
        if (sentDeals.has(key)) continue;
        sentDeals.set(key, nowMs());

        await sendMessageSafe(userId, `Найден лот:\n${g.priceTon.toFixed(3)} TON\n${g.name}\n${g.urlTelegram}`, {
          disable_web_page_preview: false,
          reply_markup: { inline_keyboard: [[{ text: 'Открыть MRKT', url: g.urlMarket }]] },
        });

        if (SEND_DELAY_MS > 0) await sleep(SEND_DELAY_MS);
      }
    }
  } finally {
    isChecking = false;
  }
}

// ============ Telegram menu button ============
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

// ============ WebApp HTML/JS ============
const WEBAPP_HTML = `<!doctype html><html lang="ru"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"/>
<title>MRKT Panel</title>
<style>
:root{--bg:#0b0f14;--card:#111827;--text:#e5e7eb;--muted:#9ca3af;--border:#1f2937;--input:#0f172a;--btn:#1f2937;--btnText:#e5e7eb;--danger:#ef4444;--accent:#22c55e}
*{box-sizing:border-box} body{margin:0;padding:14px;background:var(--bg);color:var(--text);font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial}
h2{margin:0 0 10px 0}
.card{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:12px;margin:10px 0}
label{display:block;font-size:12px;color:var(--muted);margin-bottom:4px}
input{width:min(340px,86vw);padding:10px;border:1px solid var(--border);border-radius:12px;background:var(--input);color:var(--text);outline:none}
button{padding:10px 12px;border:1px solid var(--border);border-radius:12px;background:var(--btn);color:var(--btnText);cursor:pointer}
button:hover{filter:brightness(1.07)}
#err{display:none;border-color:var(--danger);color:var(--danger)}
.row{display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end}
.sug{border:1px solid var(--border);border-radius:14px;margin-top:6px;overflow:hidden;max-width:360px;background:rgba(255,255,255,.02)}
.sug .item{width:100%;text-align:left;border:0;background:transparent;padding:10px;display:flex;gap:10px;align-items:center}
.sug .item:hover{background:rgba(255,255,255,.06)}
.thumb{width:38px;height:38px;border-radius:12px;object-fit:cover;background:rgba(255,255,255,.08);border:1px solid var(--border)}
.dot{width:14px;height:14px;border-radius:50%;border:1px solid var(--border);display:inline-block}
.hscroll{display:flex;gap:10px;overflow-x:auto;padding-bottom:6px;scroll-snap-type:x mandatory}
.lot{min-width:260px;max-width:260px;flex:0 0 auto;border:1px solid var(--border);border-radius:16px;padding:10px;background:rgba(255,255,255,.02);scroll-snap-align:start}
.lot img{width:100%;height:160px;object-fit:cover;background:rgba(255,255,255,.03);border-radius:14px;border:1px solid var(--border)}
.price{font-size:18px;font-weight:800;margin-top:8px}
.muted{color:var(--muted);font-size:13px}
.tabs{display:flex;gap:8px;flex-wrap:wrap}
.tabbtn{border-radius:999px;padding:8px 12px}
.tabbtn.active{border-color:var(--accent);color:var(--accent)}
</style></head><body>
<h2>MRKT Panel</h2>
<div id="err" class="card"></div>
<div class="tabs">
  <button class="tabbtn active" data-tab="market">Market</button>
  <button class="tabbtn" data-tab="subs">Subscriptions</button>
  <button class="tabbtn" data-tab="admin" id="adminTabBtn" style="display:none">Admin</button>
</div>

<div id="market" class="card">
  <h3 style="margin:0 0 8px 0">Фильтры</h3>
  <div class="row">
    <div>
      <label>Gift</label>
      <input id="gift" placeholder="Начни вводить…" autocomplete="off"/>
      <div id="giftSug" class="sug" style="display:none"></div>
    </div>
    <div>
      <label>Model</label>
      <input id="model" placeholder="Любая" autocomplete="off"/>
      <div id="modelSug" class="sug" style="display:none"></div>
    </div>
    <div>
      <label>Backdrop</label>
      <input id="backdrop" placeholder="Любой" autocomplete="off"/>
      <div id="backdropSug" class="sug" style="display:none"></div>
    </div>
  </div>

  <div class="row" style="margin-top:10px">
    <div><label>Min TON</label><input id="minPrice" type="number" step="0.001"/></div>
    <div><label>Max TON</label><input id="maxPrice" type="number" step="0.001"/></div>
  </div>

  <div class="row" style="margin-top:10px">
    <button id="save">Сохранить</button>
    <button id="toggleMonitor">Мониторинг: ?</button>
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

<div id="admin" class="card" style="display:none">
  <h3 style="margin:0 0 8px 0">Admin</h3>
  <div class="muted">MRKT token хранится в Redis. Обнови токен без Railway.</div>
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

  function hideSug(id){ const b=el(id); b.style.display='none'; b.innerHTML=''; }
  function renderSug(id, items, onPick){
    const b=el(id);
    if(!items||!items.length){ hideSug(id); return; }
    b.innerHTML = items.map(x => {
      const thumb = x.imgUrl ? '<img class="thumb" src="'+x.imgUrl+'" referrerpolicy="no-referrer"/>' : '<div class="thumb"></div>';
      const dot = x.colorHex ? '<span class="dot" style="background:'+x.colorHex+'"></span>' : '';
      const sub = x.sub ? '<div class="muted">'+x.sub+'</div>' : '';
      return '<button type="button" class="item" data-v="'+x.value.replace(/"/g,'&quot;')+'">'+thumb+
        '<div style="display:flex;flex-direction:column;gap:2px"><div>'+(dot?dot+'&nbsp;':'')+x.label+'</div>'+sub+'</div></button>';
    }).join('');
    b.style.display='block';
    b.onclick = (e) => {
      const btn = e.target.closest('button[data-v]');
      if(!btn) return;
      onPick(btn.getAttribute('data-v'));
      hideSug(id);
    };
  }

  function setTab(name){
    ['market','subs','admin'].forEach(x => el(x).style.display = (x===name?'block':'none'));
    document.querySelectorAll('.tabbtn').forEach(b => b.classList.toggle('active', b.dataset.tab===name));
  }
  document.querySelectorAll('.tabbtn').forEach(b => b.onclick = () => setTab(b.dataset.tab));

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
      const dot = x.backdropColorHex ? '<span class="dot" style="background:'+x.backdropColorHex+'"></span>' : '';
      const img = x.imgUrl ? '<img src="'+x.imgUrl+'" referrerpolicy="no-referrer" loading="lazy"/>' :
        '<div style="height:160px;border:1px solid rgba(255,255,255,.08);border-radius:14px"></div>';
      return '<div class="lot">'+img+
        '<div class="price">'+x.priceTon.toFixed(3)+' TON</div>'+
        '<div><b>'+x.name+'</b></div>'+
        (x.model?'<div class="muted">Model: '+x.model+'</div>':'')+
        (x.backdrop?'<div class="muted">Backdrop: '+dot+' '+x.backdrop+'</div>':'')+
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
      'Monitor: ' + (st.user.enabled?'ON':'OFF');

    el('toggleMonitor').textContent = 'Мониторинг: ' + (st.user.enabled?'ON':'OFF');

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

  el('gift').addEventListener('focus', async () => {
    try{
      const q = el('gift').value.trim();
      const r = await api('/api/mrkt/collections?q='+encodeURIComponent(q));
      renderSug('giftSug', r.items||[], (v) => {
        selectedGiftValue = v;
        el('gift').value = (r.mapLabel||{})[v] || v;
        el('model').value=''; el('backdrop').value='';
      });
    }catch{}
  });
  el('gift').addEventListener('input', async () => {
    const q = el('gift').value.trim();
    try{
      const r = await api('/api/mrkt/collections?q='+encodeURIComponent(q));
      renderSug('giftSug', r.items||[], (v) => {
        selectedGiftValue = v;
        el('gift').value = (r.mapLabel||{})[v] || v;
        el('model').value=''; el('backdrop').value='';
      });
    }catch{ hideSug('giftSug'); }
  });

  let modelTimer=null;
  el('model').addEventListener('input', () => {
    if(modelTimer) clearTimeout(modelTimer);
    modelTimer = setTimeout(async ()=>{
      const gift = selectedGiftValue || '';
      const q = el('model').value.trim();
      if(!gift){ hideSug('modelSug'); return; }
      try{
        const r = await api('/api/mrkt/suggest?kind=model&gift='+encodeURIComponent(gift)+'&q='+encodeURIComponent(q));
        renderSug('modelSug', r.items||[], (v)=>{ el('model').value=v; el('backdrop').value=''; });
      }catch{ hideSug('modelSug'); }
    }, 200);
  });

  let backTimer=null;
  el('backdrop').addEventListener('input', () => {
    if(backTimer) clearTimeout(backTimer);
    backTimer = setTimeout(async ()=>{
      const gift = selectedGiftValue || '';
      const q = el('backdrop').value.trim();
      if(!gift){ hideSug('backdropSug'); return; }
      try{
        const r = await api('/api/mrkt/suggest?kind=backdrop&gift='+encodeURIComponent(gift)+'&q='+encodeURIComponent(q));
        renderSug('backdropSug', r.items||[], (v)=>{ el('backdrop').value=v; });
      }catch{ hideSug('backdropSug'); }
    }, 220);
  });

  document.addEventListener('click', (e) => {
    if(!e.target.closest('#gift,#model,#backdrop,.sug')){
      hideSug('giftSug'); hideSug('modelSug'); hideSug('backdropSug');
    }
  });

  el('save').onclick = async () => {
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
    }catch(err){ showErr(err.message||String(err)); }
  };

  el('toggleMonitor').onclick = async () => {
    hideErr();
    try{ await api('/api/action',{method:'POST',body:JSON.stringify({action:'toggleMonitor'})}); await refreshState(); }
    catch(err){ showErr(err.message||String(err)); }
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

  refreshState().then(refreshLots).catch(e=>showErr(e.message||String(e)));
})();`;

// ============ Web server + API ============
function startWebServer() {
  const app = express();
  app.use(express.json({ limit: '600kb' }));

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

  // image proxy (Fragment)
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

  app.get('/api/mrkt/collections', auth, async (req, res) => {
    const q = String(req.query.q || '').trim().toLowerCase();
    const all = await mrktGetCollections();
    const filtered = all
      .filter((x) => !q || x.title.toLowerCase().includes(q) || x.name.toLowerCase().includes(q))
      .slice(0, WEBAPP_SUGGEST_LIMIT);

    const mapLabel = {};
    const items = filtered.map((x) => {
      mapLabel[x.name] = x.title || x.name;
      const imgUrl = x.modelStickerThumbnailKey ? joinUrl(MRKT_CDN_BASE, x.modelStickerThumbnailKey) : null;
      const floorTon = x.floorPriceNanoTons != null ? Number(x.floorPriceNanoTons) / 1e9 : null;
      return {
        label: x.title || x.name,
        value: x.name,
        imgUrl,
        sub: floorTon != null ? `floor: ${floorTon.toFixed(3)} TON` : null,
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
          imgUrl: m.thumbKey ? joinUrl(MRKT_CDN_BASE, m.thumbKey) : null,
          sub: m.rarityPerMille != null ? formatPctFromPermille(m.rarityPerMille) : null,
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
          sub: b.rarityPerMille != null ? formatPctFromPermille(b.rarityPerMille) : null,
        }));
      return res.json({ ok: true, items });
    }

    res.status(400).json({ ok: false, reason: 'BAD_KIND' });
  });

  app.post('/api/state/patch', auth, (req, res) => {
    const u = getOrCreateUser(req.userId);
    const b = req.body || {};
    if (b.minPriceTon != null) u.minPriceTon = Number(b.minPriceTon) || 0;
    if (Object.prototype.hasOwnProperty.call(b, 'maxPriceTon')) u.maxPriceTon = b.maxPriceTon == null ? null : Number(b.maxPriceTon);

    if (b.filters && typeof b.filters === 'object') {
      if (typeof b.filters.gift === 'string') u.filters.gift = b.filters.gift.trim();
      if (typeof b.filters.giftLabel === 'string') u.filters.giftLabel = b.filters.giftLabel.trim();
      if (typeof b.filters.model === 'string') u.filters.model = b.filters.model.trim();
      if (typeof b.filters.backdrop === 'string') u.filters.backdrop = b.filters.backdrop.trim();
    }
    res.json({ ok: true });
  });

  app.post('/api/action', auth, (req, res) => {
    const u = getOrCreateUser(req.userId);
    const action = req.body?.action;
    if (action === 'toggleMonitor') {
      u.enabled = !u.enabled;
      return res.json({ ok: true });
    }
    res.status(400).json({ ok: false, reason: 'UNKNOWN_ACTION' });
  });

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

    const lots = (r.gifts || []).slice(0, WEBAPP_LOTS_LIMIT).map((x) => {
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

  // subs
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

  // admin token update
  app.post('/api/admin/mrkt_auth', auth, async (req, res) => {
    if (!isAdmin(req.userId)) return res.status(403).json({ ok: false, reason: 'NOT_ADMIN' });
    const t = String(req.body?.token || '').trim();
    if (!t) return res.status(400).json({ ok: false, reason: 'EMPTY_TOKEN' });
    MRKT_AUTH_RUNTIME = t;
    try {
      await saveMrktAuthToRedis(t);
    } catch {}
    modelsCache.clear();
    backdropsCache.clear();
    res.json({ ok: true });
  });

  const port = Number(process.env.PORT || 3000);
  app.listen(port, '0.0.0.0', () => console.log('WebApp listening on', port));
}

startWebServer();

// ============ intervals ============
setInterval(() => {
  checkMarketsForAllUsers().catch(() => {});
}, CHECK_INTERVAL_MS);

setInterval(() => {
  checkSubscriptionsForAllUsers().catch(() => {});
}, SUBS_CHECK_INTERVAL_MS);

// ============ bootstrap ============
(async () => {
  if (REDIS_URL) {
    await initRedis();
    if (redis) await loadMrktAuthFromRedis();
  }
  console.log('Bot started. /start');
})();
