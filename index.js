const TelegramBot = require('node-telegram-bot-api');

// ===== ENV =====
const token = process.env.TELEGRAM_TOKEN;
if (!token) { console.error('TELEGRAM_TOKEN not set'); process.exit(1); }

const MODE = process.env.MODE || 'real';

// speed / output
const CHECK_INTERVAL_MS = Number(process.env.CHECK_INTERVAL_MS || 7000);
const MAX_NOTIFICATIONS_PER_CHECK = Number(process.env.MAX_NOTIFICATIONS_PER_CHECK || 60);
const MAX_PER_MARKET = Number(process.env.MAX_PER_MARKET || 120);
const SEND_DELAY_MS = Number(process.env.SEND_DELAY_MS || 80);

// subscriptions
const SUBS_CHECK_INTERVAL_MS = Number(process.env.SUBS_CHECK_INTERVAL_MS || 9000);
const SUBS_MAX_NOTIFICATIONS_PER_CYCLE = Number(process.env.SUBS_MAX_NOTIFICATIONS_PER_CYCLE || 8);
const SUBS_EMPTY_CONFIRM = Number(process.env.SUBS_EMPTY_CONFIRM || 2);
const SUBS_MRKT_FEED_MAX_EVENTS_PER_CYCLE = Number(process.env.SUBS_MRKT_FEED_MAX_EVENTS_PER_CYCLE || 12);

// anti-spam
const SENT_TTL_MS = Number(process.env.SENT_TTL_MS || 24 * 60 * 60 * 1000);

// UI
const MAX_SEARCH_RESULTS = Number(process.env.MAX_SEARCH_RESULTS || 10);

// Redis
const REDIS_URL = process.env.REDIS_URL || process.env.REDIS_PRIVATE_URL || null;

// Optional admin chat for MRKT auth alerts
const ADMIN_CHAT_ID = process.env.ADMIN_CHAT_ID ? Number(process.env.ADMIN_CHAT_ID) : null;

// ===== Portal direct (CF on Railway) =====
const API_URL = 'https://portal-market.com/api/';
const PORTAL_THROTTLE_MS = Number(process.env.PORTAL_THROTTLE_MS || 250);
let portalNextAllowedAt = 0;

// deep-link Portal lot (если вдруг будет id)
const PORTAL_LOT_URL_TEMPLATE =
  process.env.PORTAL_LOT_URL_TEMPLATE || 'https://t.me/portals?startapp=gift_{id}';

// fees
const PORTAL_FEE = Number(process.env.PORTAL_FEE || 0.05);
const MRKT_FEE = Number(process.env.MRKT_FEE || 0);

// ===== MRKT =====
const MRKT_API_URL = 'https://api.tgmrkt.io/api/v1';
const MRKT_COUNT = Number(process.env.MRKT_COUNT || 50);
const MRKT_PAGES = Number(process.env.MRKT_PAGES || 6);

// MRKT feed/history
const MRKT_FEED_COUNT = Number(process.env.MRKT_FEED_COUNT || 80);
const MRKT_HISTORY_TARGET_SALES = Number(process.env.MRKT_HISTORY_TARGET_SALES || 60);
const MRKT_HISTORY_MAX_PAGES = Number(process.env.MRKT_HISTORY_MAX_PAGES || 240);
const MRKT_FEED_THROTTLE_MS = Number(process.env.MRKT_FEED_THROTTLE_MS || 110);
const MRKT_HISTORY_TIME_BUDGET_MS = Number(process.env.MRKT_HISTORY_TIME_BUDGET_MS || 20000);

const MRKT_FEED_NOTIFY_TYPES_RAW = String(process.env.MRKT_FEED_NOTIFY_TYPES || 'sale,listing,change_price');
const MRKT_FEED_NOTIFY_TYPES = new Set(
  MRKT_FEED_NOTIFY_TYPES_RAW.split(',').map(s => s.trim().toLowerCase()).filter(Boolean)
);

const MRKT_AUTH_NOTIFY_COOLDOWN_MS = Number(process.env.MRKT_AUTH_NOTIFY_COOLDOWN_MS || 60 * 60 * 1000);

// ===== Satellite =====
const SATELLITE_ENABLED = String(process.env.SATELLITE_ENABLED || '1') !== '0';
const SATELLITE_BASE = process.env.SATELLITE_BASE || 'https://gift-satellite.dev';
const SATELLITE_AUTH_TOKEN = process.env.SATELLITE_AUTH_TOKEN || '';
const SATELLITE_THROTTLE_MS = Number(process.env.SATELLITE_THROTTLE_MS || 120);
let satelliteNextAllowedAt = 0;

// ===== Tonnel via Satellite =====
const TONNEL_ENABLED = String(process.env.TONNEL_ENABLED || '1') !== '0';

// optional: direct Tonnel probe
const TONNEL_DIRECT_ENABLED = String(process.env.TONNEL_DIRECT_ENABLED || '0') === '1';
const TONNEL_USER_AUTH = process.env.TONNEL_USER_AUTH || null;
const TONNEL_ORIGIN = process.env.TONNEL_ORIGIN || 'https://market.tonnel.network';
const TONNEL_REFERER = process.env.TONNEL_REFERER || 'https://market.tonnel.network/';
const TONNEL_UA =
  process.env.TONNEL_UA ||
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36';

console.log('Bot version 2026-02-20-satellite-authToken-fix-v1');
console.log('MODE =', MODE);
console.log('REDIS_URL =', REDIS_URL ? 'set' : 'not set');
console.log('SATELLITE_ENABLED =', SATELLITE_ENABLED, 'SATELLITE_BASE =', SATELLITE_BASE);
console.log('SATELLITE_AUTH_TOKEN =', SATELLITE_AUTH_TOKEN ? 'set' : 'not set');
console.log('TONNEL_ENABLED =', TONNEL_ENABLED, 'TONNEL_DIRECT_ENABLED =', TONNEL_DIRECT_ENABLED);

const bot = new TelegramBot(token, { polling: true });

// ===== UI =====
const MAIN_KEYBOARD = {
  keyboard: [
    [{ text: '🔍 Запустить поиск' }, { text: '⏹ Остановить поиск' }],
    [{ text: '💰 Макс. цена' }, { text: '💵 Мин. цена' }],
    [{ text: '💸 Цена подарка' }, { text: '📡 Подписки' }],
    [{ text: '🎛 Фильтры' }, { text: '📌 Статус API' }],
  ],
  resize_keyboard: true,
};

// ===== State =====
const users = new Map();
const sentDeals = new Map();
const subStates = new Map();

let isChecking = false;
let isSubsChecking = false;

// caches
const historyCache = new Map();
const HISTORY_CACHE_TTL_MS = 60_000;

// MRKT auth status
const mrktAuthState = { ok: null, lastOkAt: 0, lastFailAt: 0, lastFailCode: null, lastNotifiedAt: 0 };

// probes
const portalState = { lastStatus: null, lastOk: null, lastBodyStart: '', lastAt: 0 };
const tonnelDirectState = { lastStatus: null, lastOk: null, lastBodyStart: '', lastAt: 0 };
const satelliteState = { lastStatus: null, lastOk: null, lastBodyStart: '', lastAt: 0 };

// ===== Catalog (so filters still work when Portal is down) =====
const catalog = {
  gifts: new Map(),            // giftLower -> display
  modelsByGift: new Map(),     // giftLower -> Map(modelLower -> display)
  backdropsByGift: new Map(),  // giftLower -> Map(backdropLower -> display)
};

function normGiftKey(name) { return String(name || '').toLowerCase().trim().replace(/\s+/g, ' '); }
function normTraitName(s) {
  return String(s || '')
    .toLowerCase()
    .trim()
    .replace(/\s*\([^)]*%[^)]*\)\s*/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}
function capWords(str) {
  return String(str || '').replace(/\w+(?:'\w+)?/g, (w) => (w ? w[0].toUpperCase() + w.slice(1) : w));
}
function sameTrait(actual, expectedLower) {
  if (!expectedLower) return true;
  return normTraitName(actual) === normTraitName(expectedLower);
}

function catalogPutGift(displayName) {
  const k = normGiftKey(displayName);
  if (!k) return;
  if (!catalog.gifts.has(k)) catalog.gifts.set(k, String(displayName));
}
function catalogPutModel(giftLower, modelDisplay) {
  const gl = normGiftKey(giftLower);
  const ml = normTraitName(modelDisplay);
  if (!gl || !ml) return;
  if (!catalog.modelsByGift.has(gl)) catalog.modelsByGift.set(gl, new Map());
  const m = catalog.modelsByGift.get(gl);
  if (!m.has(ml)) m.set(ml, String(modelDisplay));
}
function catalogPutBackdrop(giftLower, backdropDisplay) {
  const gl = normGiftKey(giftLower);
  const bl = normTraitName(backdropDisplay);
  if (!gl || !bl) return;
  if (!catalog.backdropsByGift.has(gl)) catalog.backdropsByGift.set(gl, new Map());
  const m = catalog.backdropsByGift.get(gl);
  if (!m.has(bl)) m.set(bl, String(backdropDisplay));
}
function catalogRegisterGiftObj({ giftName, model, backdrop }) {
  if (!giftName) return;
  catalogPutGift(giftName);
  const gl = normGiftKey(giftName);
  if (model) catalogPutModel(gl, model);
  if (backdrop) catalogPutBackdrop(gl, backdrop);
}
function catalogExport() {
  const gifts = Array.from(catalog.gifts.entries());
  const modelsByGift = {};
  const backdropsByGift = {};
  for (const [g, map] of catalog.modelsByGift.entries()) modelsByGift[g] = Array.from(map.entries());
  for (const [g, map] of catalog.backdropsByGift.entries()) backdropsByGift[g] = Array.from(map.entries());
  return { gifts, modelsByGift, backdropsByGift };
}
function catalogImport(data) {
  try {
    if (!data || typeof data !== 'object') return;
    if (Array.isArray(data.gifts)) for (const [k, v] of data.gifts) if (k && v) catalog.gifts.set(String(k), String(v));
    if (data.modelsByGift && typeof data.modelsByGift === 'object') {
      for (const [g, arr] of Object.entries(data.modelsByGift)) {
        if (!Array.isArray(arr)) continue;
        const map = new Map();
        for (const [k, v] of arr) if (k && v) map.set(String(k), String(v));
        catalog.modelsByGift.set(String(g), map);
      }
    }
    if (data.backdropsByGift && typeof data.backdropsByGift === 'object') {
      for (const [g, arr] of Object.entries(data.backdropsByGift)) {
        if (!Array.isArray(arr)) continue;
        const map = new Map();
        for (const [k, v] of arr) if (k && v) map.set(String(k), String(v));
        catalog.backdropsByGift.set(String(g), map);
      }
    }
  } catch {}
}

// ===== Helpers =====
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const nowMs = () => Date.now();

function n(x) { const v = Number(String(x).replace(',', '.')); return Number.isFinite(v) ? v : NaN; }
function inRange(price, minPrice, maxPrice) {
  if (!Number.isFinite(price)) return false;
  const min = minPrice != null ? Number(minPrice) : 0;
  const max = maxPrice != null ? Number(maxPrice) : null;
  if (Number.isFinite(min) && price < min) return false;
  if (max != null && Number.isFinite(max) && price > max) return false;
  return true;
}
function median(sorted) {
  if (!sorted.length) return null;
  const L = sorted.length;
  return L % 2 ? sorted[(L - 1) / 2] : (sorted[L / 2 - 1] + sorted[L / 2]) / 2;
}
function pruneSentDeals() {
  const now = nowMs();
  for (const [k, ts] of sentDeals.entries()) if (now - ts > SENT_TTL_MS) sentDeals.delete(k);
}
function clearUserSentDeals(userId) {
  const prefix = `${userId}:`;
  for (const k of Array.from(sentDeals.keys())) if (k.startsWith(prefix)) sentDeals.delete(k);
}
function percentChange(oldV, newV) {
  if (!oldV || !Number.isFinite(oldV) || oldV <= 0) return null;
  return ((newV - oldV) / oldV) * 100;
}
function shorten(s, max = 32) {
  const t = String(s || '');
  return t.length <= max ? t : t.slice(0, max - 1) + '…';
}

// Portal lot deep-link
function buildPortalLotUrl(id) {
  if (!id) return 'https://t.me/portals';
  if (!PORTAL_LOT_URL_TEMPLATE.includes('{id}')) return PORTAL_LOT_URL_TEMPLATE;
  return PORTAL_LOT_URL_TEMPLATE.replace('{id}', encodeURIComponent(String(id)));
}

// MRKT lot deep-link
function mrktLotUrlFromId(id) {
  if (!id) return 'https://t.me/mrkt';
  const appId = String(id).replace(/-/g, '');
  return `https://t.me/mrkt/app?startapp=${appId}`;
}

// Safe send
async function sendMessageSafe(chatId, text, opts) {
  while (true) {
    try {
      return await bot.sendMessage(chatId, text, opts);
    } catch (e) {
      const retryAfter =
        e?.response?.body?.parameters?.retry_after ??
        e?.response?.parameters?.retry_after ??
        null;
      if (retryAfter) { await sleep((Number(retryAfter) + 1) * 1000); continue; }
      throw e;
    }
  }
}

// ===== Redis persistence =====
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
function getOrCreateUser(userId) {
  if (!users.has(userId)) {
    users.set(userId, {
      enabled: true,
      minPriceTon: 0,
      maxPriceTon: null,
      state: null, // awaiting_max/awaiting_min/awaiting_gift_free/awaiting_model_free/awaiting_backdrop_free/awaiting_sub_max:<id>
      filters: {
        gifts: [],
        models: [],
        backdrops: [],
        markets: ['MRKT', ...(TONNEL_ENABLED ? ['Tonnel'] : []), 'Portal'],
      },
      subscriptions: [],
    });
  }
  return users.get(userId);
}
function exportState() {
  const out = { users: {} };
  for (const [userId, u] of users.entries()) {
    out.users[String(userId)] = {
      enabled: !!u.enabled,
      minPriceTon: typeof u.minPriceTon === 'number' ? u.minPriceTon : 0,
      maxPriceTon: typeof u.maxPriceTon === 'number' ? u.maxPriceTon : null,
      state: null,
      filters: u.filters,
      subscriptions: u.subscriptions || [],
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
      minPriceTon: typeof u?.minPriceTon === 'number' ? u.minPriceTon : 0,
      maxPriceTon: typeof u?.maxPriceTon === 'number' ? u.maxPriceTon : null,
      state: null,
      filters: {
        gifts: Array.isArray(u?.filters?.gifts) ? u.filters.gifts : [],
        models: Array.isArray(u?.filters?.models) ? u.filters.models : [],
        backdrops: Array.isArray(u?.filters?.backdrops) ? u.filters.backdrops : [],
        markets: Array.isArray(u?.filters?.markets)
          ? u.filters.markets
          : ['MRKT', ...(TONNEL_ENABLED ? ['Tonnel'] : []), 'Portal'],
      },
      subscriptions: Array.isArray(u?.subscriptions) ? u.subscriptions : [],
    };

    for (const s of safe.subscriptions) {
      if (!s || typeof s !== 'object') continue;
      if (!s.id) s.id = `sub_${Date.now()}_${Math.random().toString(16).slice(2, 6)}`;
      if (s.enabled == null) s.enabled = true;
      if (!s.filters) s.filters = {};
      if (!Array.isArray(s.filters.markets)) s.filters.markets = safe.filters.markets;
      if (s.maxPriceTon != null && !Number.isFinite(Number(s.maxPriceTon))) s.maxPriceTon = null;
      if (typeof s.filters.gift !== 'string') s.filters.gift = safe.filters.gifts[0] || '';
      if (typeof s.num !== 'number') s.num = 0;
    }

    renumberSubs(safe);
    users.set(userId, safe);
  }
}
async function loadState() {
  if (!redis) return;
  const keys = ['bot:state:v9', 'bot:state:v8', 'bot:state:v7', 'bot:state:v6', 'bot:state:v5'];
  for (const k of keys) {
    const raw = await redis.get(k);
    if (raw) { importState(JSON.parse(raw)); console.log('Loaded state from Redis key:', k, 'users:', users.size); return; }
  }
}
async function loadCatalog() {
  if (!redis) return;
  const raw = await redis.get('bot:catalog:v1');
  if (!raw) return;
  try { catalogImport(JSON.parse(raw)); console.log('Loaded catalog from Redis.'); } catch {}
}
let saveTimer = null;
function scheduleSave() {
  if (!redis) return;
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    saveState().catch((e) => console.error('saveState error:', e));
    saveCatalog().catch((e) => console.error('saveCatalog error:', e));
  }, 350);
}
async function saveState() { if (!redis) return; await redis.set('bot:state:v9', JSON.stringify(exportState())); }
async function saveCatalog() { if (!redis) return; await redis.set('bot:catalog:v1', JSON.stringify(catalogExport())); }

// ===== Portal probe (direct; CF expected) =====
function portalHeaders() {
  const auth = process.env.PORTAL_AUTH;
  return {
    ...(auth ? { Authorization: auth } : {}),
    Accept: 'application/json, text/plain, */*',
    Origin: 'https://portal-market.com',
    Referer: 'https://portal-market.com/',
  };
}
async function throttledPortalFetch(url, opts) {
  const now = nowMs();
  const wait = portalNextAllowedAt - now;
  if (wait > 0) await sleep(wait);
  portalNextAllowedAt = nowMs() + PORTAL_THROTTLE_MS;
  return fetch(url, opts);
}
async function portalProbe() {
  if (!process.env.PORTAL_AUTH) return { ok: false, status: null, note: 'PORTAL_AUTH not set', bodyStart: '' };
  const url = `${API_URL}collections?limit=1`;
  const res = await throttledPortalFetch(url, { method: 'GET', headers: portalHeaders() }).catch(() => null);
  if (!res) return { ok: false, status: null, note: 'fetch error', bodyStart: '' };
  const txt = await res.text().catch(() => '');
  portalState.lastAt = nowMs(); portalState.lastStatus = res.status; portalState.lastOk = res.ok; portalState.lastBodyStart = txt.slice(0, 220);
  return { ok: res.ok, status: res.status, contentType: res.headers.get('content-type') || '', bodyStart: txt.slice(0, 220) };
}

// ===== Satellite fetch (AUTH FIX) =====
function satelliteHeaders() {
  const h = { Accept: 'application/json' };
  if (SATELLITE_AUTH_TOKEN) {
    // пробуем сразу несколько вариантов — какой-то из них точно совпадёт с сервером
    h['Authorization'] = `Bearer ${SATELLITE_AUTH_TOKEN}`;
    h['authToken'] = SATELLITE_AUTH_TOKEN;
    h['x-auth-token'] = SATELLITE_AUTH_TOKEN;
  }
  return h;
}
async function throttledSatelliteFetch(url, opts) {
  const now = nowMs();
  const wait = satelliteNextAllowedAt - now;
  if (wait > 0) await sleep(wait);
  satelliteNextAllowedAt = nowMs() + SATELLITE_THROTTLE_MS;
  return fetch(url, opts);
}
const SATELLITE_TIMEOUT_MS = Number(process.env.SATELLITE_TIMEOUT_MS || 8000);

async function satelliteFetchJson(url) {
  // ловим кривой SATELLITE_BASE (например без https://)
  try {
    new URL(url);
  } catch (e) {
    return { ok: false, status: null, data: null, text: `bad url: ${e.message}` };
  }

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), SATELLITE_TIMEOUT_MS);

  let res;
  try {
    res = await throttledSatelliteFetch(url, {
      method: 'GET',
      headers: satelliteHeaders(),
      signal: controller.signal,
    });
  } catch (e) {
    clearTimeout(timer);
    return {
      ok: false,
      status: null,
      data: null,
      text: `fetch error: ${e?.name || 'Error'} ${e?.message || String(e)}`,
    };
  } finally {
    clearTimeout(timer);
  }

  const txt = await res.text().catch(() => '');
  let data = null;
  try {
    data = txt ? JSON.parse(txt) : null;
  } catch {
    data = null;
  }

  return { ok: res.ok, status: res.status, data, text: txt };
}
}
async function satelliteProbe() {
  if (!SATELLITE_ENABLED) return { ok: false, status: null, note: 'SATELLITE_DISABLED', bodyStart: '' };
  const url = `${SATELLITE_BASE}/api/history/floors?market=PORTALS`;
  const r = await satelliteFetchJson(url);
  satelliteState.lastAt = nowMs(); satelliteState.lastStatus = r.status; satelliteState.lastOk = r.ok; satelliteState.lastBodyStart = (r.text || '').slice(0, 220);
  return { ok: r.ok, status: r.status, contentType: 'application/json', bodyStart: (r.text || '').slice(0, 220) };
}

// ===== Satellite: Portal floors =====
let satPortalFloorsCache = { time: 0, data: null };
const SAT_PORTAL_FLOORS_TTL_MS = 30_000;

const satPortalModelsCache = new Map();
const SAT_PORTAL_MODELS_TTL_MS = 60_000;

async function satPortalFloors() {
  const now = nowMs();
  if (satPortalFloorsCache.data && now - satPortalFloorsCache.time < SAT_PORTAL_FLOORS_TTL_MS) return satPortalFloorsCache.data;
  if (!SATELLITE_ENABLED) return null;

  const url = `${SATELLITE_BASE}/api/history/floors?market=PORTALS`;
  const r = await satelliteFetchJson(url);
  if (!r.ok || !r.data || typeof r.data !== 'object') return null;

  for (const name of Object.keys(r.data)) catalogPutGift(name);
  scheduleSave();

  satPortalFloorsCache = { time: now, data: r.data };
  return r.data;
}

async function satPortalModelsFloors(collectionNameDisplay) {
  const now = nowMs();
  const giftKey = normGiftKey(collectionNameDisplay);
  const cached = satPortalModelsCache.get(giftKey);
  if (cached && now - cached.time < SAT_PORTAL_MODELS_TTL_MS) return cached.data;
  if (!SATELLITE_ENABLED) return null;

  const url = `${SATELLITE_BASE}/api/history/models-floors?collectionName=${encodeURIComponent(collectionNameDisplay)}&market=PORTALS`;
  const r = await satelliteFetchJson(url);
  if (!r.ok || !r.data || typeof r.data !== 'object') return null;

  for (const modelName of Object.keys(r.data)) catalogPutModel(giftKey, modelName);
  scheduleSave();

  satPortalModelsCache.set(giftKey, { time: now, data: r.data });
  return r.data;
}

function getGiftDisplay(giftLower) {
  if (!giftLower) return null;
  return catalog.gifts.get(giftLower) || capWords(giftLower);
}

function pickMapValueCaseInsensitive(mapObj, key) {
  if (!mapObj || typeof mapObj !== 'object' || !key) return null;
  if (mapObj[key] != null) return mapObj[key];
  const target = String(key).toLowerCase().trim();
  for (const [k, v] of Object.entries(mapObj)) {
    if (String(k).toLowerCase().trim() === target) return v;
  }
  return null;
}

async function satPortalFloorForFilters(giftLower, modelLower) {
  const giftDisplay = getGiftDisplay(giftLower);
  if (!giftDisplay) return null;

  if (modelLower) {
    const map = await satPortalModelsFloors(giftDisplay);
    if (map) {
      const price = pickMapValueCaseInsensitive(map, capWords(modelLower));
      if (price != null) return Number(price);
    }
    // fallback to gift floor
  }

  const floors = await satPortalFloors();
  if (!floors) return null;
  const price = pickMapValueCaseInsensitive(floors, giftDisplay);
  return price != null ? Number(price) : null;
}

// ===== Satellite: Tonnel lots =====
async function satTonnelSearchLots({ giftLower, modelLower, backdropLower }, minPriceTon, maxPriceTon) {
  if (!SATELLITE_ENABLED || !TONNEL_ENABLED) return { ok: false, reason: 'DISABLED', gifts: [] };
  if (!giftLower) return { ok: false, reason: 'NO_GIFT', gifts: [] };

  const giftDisplay = getGiftDisplay(giftLower);
  const model = modelLower ? capWords(modelLower) : '';
  const backdrop = backdropLower ? capWords(backdropLower) : '';

  const url =
    `${SATELLITE_BASE}/api/search/tonnel/${encodeURIComponent(giftDisplay)}` +
    `?models=${encodeURIComponent(model)}` +
    `&backdrops=${encodeURIComponent(backdrop)}` +
    `&notMinted=0`;

  const r = await satelliteFetchJson(url);

  if (!r.ok) {
    const msg = (r.data && r.data.message) ? String(r.data.message) : '';
    if (r.status === 500 && msg.toLowerCase().includes('no results')) return { ok: true, reason: 'OK', gifts: [] };
    return { ok: false, reason: `HTTP_${r.status}`, gifts: [] };
  }

  if (!Array.isArray(r.data)) return { ok: false, reason: 'BAD_FORMAT', gifts: [] };

  const out = [];
  for (const it of r.data) {
    const priceBase = Number(it?.formattedPrice ?? it?.price);
    if (!Number.isFinite(priceBase) || priceBase <= 0) continue;
    if (!inRange(priceBase, minPriceTon, maxPriceTon)) continue;

    const collectionName = it.collectionName || giftDisplay;
    const slug = it.slug || null;
    const giftId = it.giftId || null;

    const modelName = it.model || null;
    const backdropName = it.backdrop || null;
    const symbolName = it.symbol || null;

    if (modelLower && normTraitName(modelName) !== normTraitName(modelLower)) continue;
    if (backdropLower && normTraitName(backdropName) !== normTraitName(backdropLower)) continue;

    const urlTelegram = slug ? `https://t.me/nft/${slug}` : (it.link || 'https://t.me/tonnel_network_bot');
    const urlMarket = it.link || 'https://t.me/tonnel_network_bot';

    out.push({
      id: `tonnel_sat_${giftId || slug || Math.random()}`,
      market: 'Tonnel',
      name: slug ? `${collectionName} (${slug})` : collectionName,
      baseName: collectionName,
      priceTon: priceBase,
      urlTelegram,
      urlMarket,
      attrs: { model: modelName, backdrop: backdropName, symbol: symbolName },
      raw: it,
    });

    catalogRegisterGiftObj({ giftName: collectionName, model: modelName, backdrop: backdropName });
  }

  out.sort((a, b) => a.priceTon - b.priceTon);
  scheduleSave();
  return { ok: true, reason: 'OK', gifts: out.slice(0, MAX_PER_MARKET) };
}

// ===== MRKT auth helpers =====
function mrktAuthHeader() {
  const t = process.env.MRKT_AUTH;
  if (!t) return null;
  return t;
}
async function notifyMrktAuthExpired(statusCode) {
  const now = nowMs();
  if (now - mrktAuthState.lastNotifiedAt < MRKT_AUTH_NOTIFY_COOLDOWN_MS) return;
  mrktAuthState.lastNotifiedAt = now;

  const text = `⚠️ MRKT токен не работает (HTTP ${statusCode}).\nНужно обновить MRKT_AUTH в Railway.`;

  if (ADMIN_CHAT_ID && Number.isFinite(ADMIN_CHAT_ID)) {
    try { await sendMessageSafe(ADMIN_CHAT_ID, text, { disable_web_page_preview: true }); return; } catch {}
  }
  for (const [uid] of users.entries()) {
    try { await sendMessageSafe(uid, text, { disable_web_page_preview: true }); } catch {}
  }
}
function markMrktOk() { mrktAuthState.ok = true; mrktAuthState.lastOkAt = nowMs(); }
async function markMrktFailIfAuth(statusCode) {
  mrktAuthState.ok = false;
  mrktAuthState.lastFailAt = nowMs();
  mrktAuthState.lastFailCode = statusCode;
  if (statusCode === 401 || statusCode === 403) await notifyMrktAuthExpired(statusCode);
}

// ===== MRKT lots =====
async function mrktFetchSalingPage({ collectionName, modelName, backdropName, cursor }) {
  const auth = mrktAuthHeader();
  if (!auth) return { ok: false, reason: 'NO_AUTH', gifts: [], cursor: null };

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

  const res = await fetch(`${MRKT_API_URL}/gifts/saling`, {
    method: 'POST',
    headers: { Authorization: auth, 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify(body),
  }).catch(() => null);

  if (!res) return { ok: false, reason: 'FETCH_ERROR', gifts: [], cursor: null };
  if (!res.ok) { await markMrktFailIfAuth(res.status); return { ok: false, reason: `HTTP_${res.status}`, gifts: [], cursor: null }; }

  markMrktOk();

  const data = await res.json().catch(() => null);
  const gifts = Array.isArray(data?.gifts) ? data.gifts : [];
  const nextCursor = data?.cursor || data?.nextCursor || '';
  return { ok: true, reason: 'OK', gifts, cursor: nextCursor };
}

async function mrktSearchByFilters({ giftLower, modelLower, backdropLower }, minPriceTonLocal, maxPriceTonLocal) {
  const auth = mrktAuthHeader();
  if (!auth) return { ok: false, reason: 'NO_AUTH', gifts: [] };
  if (!giftLower) return { ok: false, reason: 'NO_GIFT', gifts: [] };

  const collectionName = getGiftDisplay(giftLower);
  const modelName = modelLower ? capWords(modelLower) : null;
  const backdropName = backdropLower ? capWords(backdropLower) : null;

  let cursor = '';
  const out = [];

  for (let page = 0; page < MRKT_PAGES; page++) {
    const r = await mrktFetchSalingPage({ collectionName, modelName, backdropName, cursor });
    if (!r.ok) return { ok: false, reason: r.reason, gifts: [] };

    for (const g of r.gifts) {
      const nanoA = g?.salePriceWithoutFee ?? null;
      const nanoB = g?.salePrice ?? null;
      const nano = nanoA != null && nanoB != null ? Math.min(Number(nanoA), Number(nanoB)) : (nanoA ?? nanoB);
      if (nano == null) continue;

      const priceTon = Number(nano) / 1e9;
      if (!Number.isFinite(priceTon) || priceTon <= 0) continue;
      if (!inRange(priceTon, minPriceTonLocal, maxPriceTonLocal)) continue;

      const baseName = (g.collectionTitle || g.collectionName || g.title || 'MRKT Gift').trim();
      const number = g.number ?? null;
      const displayName = number ? `${baseName} #${number}` : baseName;

      const model = g.modelTitle || g.modelName || null;
      const backdrop = g.backdropName || null;
      const symbol = g.symbolName || null;

      if (modelLower && !sameTrait(model, modelLower)) continue;
      if (backdropLower && !sameTrait(backdrop, backdropLower)) continue;

      let urlTelegram = 'https://t.me/mrkt';
      if (g.name && String(g.name).includes('-')) urlTelegram = `https://t.me/nft/${g.name}`;

      const urlMarket = g.id ? mrktLotUrlFromId(g.id) : 'https://t.me/mrkt';

      out.push({
        id: `mrkt_${g.id || g.name || displayName}`,
        market: 'MRKT',
        name: displayName,
        baseName,
        priceTon,
        urlTelegram,
        urlMarket,
        attrs: { model, backdrop, symbol },
        raw: g,
      });

      catalogRegisterGiftObj({ giftName: baseName, model, backdrop });
    }

    cursor = r.cursor || '';
    if (!cursor) break;
    if (out.length >= MAX_PER_MARKET) break;
  }

  out.sort((a, b) => a.priceTon - b.priceTon);
  scheduleSave();
  return { ok: true, reason: 'OK', gifts: out.slice(0, MAX_PER_MARKET) };
}

// ===== MRKT feed + history =====
async function mrktFeedFetch({ collectionName, modelName, backdropName, cursor, count, ordering = 'Latest', types = [] }) {
  const auth = mrktAuthHeader();
  if (!auth) return { ok: false, reason: 'NO_AUTH', items: [], cursor: null };

  const body = {
    count: Number(count || MRKT_FEED_COUNT),
    cursor: cursor || '',
    collectionNames: collectionName ? [collectionName] : [],
    modelNames: modelName ? [modelName] : [],
    backdropNames: backdropName ? [backdropName] : [],
    lowToHigh: false,
    maxPrice: null,
    minPrice: null,
    number: null,
    ordering: ordering || 'Latest',
    query: null,
    type: Array.isArray(types) ? types : [],
  };

  const res = await fetch(`${MRKT_API_URL}/feed`, {
    method: 'POST',
    headers: { Authorization: auth, 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify(body),
  }).catch(() => null);

  if (!res) return { ok: false, reason: 'FETCH_ERROR', items: [], cursor: null };
  if (!res.ok) { await markMrktFailIfAuth(res.status); return { ok: false, reason: `HTTP_${res.status}`, items: [], cursor: null }; }

  markMrktOk();

  const data = await res.json().catch(() => null);
  const items = Array.isArray(data?.items) ? data.items : [];
  const nextCursor = data?.cursor || data?.nextCursor || '';
  return { ok: true, reason: 'OK', items, cursor: nextCursor };
}

async function mrktHistorySalesEstimate({ giftLower, modelLower, backdropLower }) {
  const auth = mrktAuthHeader();
  if (!auth) return { ok: false, reason: 'NO_AUTH', median: null, count: 0 };
  if (!giftLower) return { ok: false, reason: 'NO_GIFT', median: null, count: 0 };

  const collectionName = getGiftDisplay(giftLower);
  const modelName = modelLower ? capWords(modelLower) : null;
  const backdropName = backdropLower ? capWords(backdropLower) : null;

  const key = `mrkt|hist|${collectionName}|${modelName || ''}|${backdropName || ''}|target=${MRKT_HISTORY_TARGET_SALES}`;
  const now = nowMs();
  const cached = historyCache.get(key);
  if (cached && now - cached.time < HISTORY_CACHE_TTL_MS) return cached;

  const started = nowMs();
  let cursor = '';
  const prices = [];
  let pages = 0;

  while (pages < MRKT_HISTORY_MAX_PAGES && prices.length < MRKT_HISTORY_TARGET_SALES) {
    if (nowMs() - started > MRKT_HISTORY_TIME_BUDGET_MS) break;

    const r = await mrktFeedFetch({
      collectionName,
      modelName,
      backdropName,
      cursor,
      count: MRKT_FEED_COUNT,
      ordering: 'Latest',
      types: ['sale'],
    });

    if (!r.ok) {
      const outErr = { ok: false, reason: r.reason, median: null, count: prices.length, time: now };
      historyCache.set(key, outErr);
      return outErr;
    }
    if (!r.items.length) break;

    for (const item of r.items) {
      const type = String(item?.type || '').toLowerCase();
      if (type && type !== 'sale') continue;

      const g = item?.gift || null;
      if (!g) continue;

      if (modelLower) {
        const m = g.modelTitle || g.modelName || null;
        if (!sameTrait(m, modelLower)) continue;
      }
      if (backdropLower) {
        const b = g.backdropName || null;
        if (!sameTrait(b, backdropLower)) continue;
      }

      const amountNano = item?.amount ?? g?.salePrice ?? null;
      const ton = Number(amountNano) / 1e9;
      if (!Number.isFinite(ton) || ton <= 0) continue;

      prices.push(ton);
      if (prices.length >= MRKT_HISTORY_TARGET_SALES) break;
    }

    cursor = r.cursor || '';
    pages++;
    if (!cursor) break;
    if (MRKT_FEED_THROTTLE_MS > 0) await sleep(MRKT_FEED_THROTTLE_MS);
  }

  prices.sort((a, b) => a - b);
  const out = { ok: true, reason: 'OK', median: median(prices), count: prices.length, time: now };
  historyCache.set(key, out);
  return out;
}

// Smart fallback for MRKT history (gift+model+backdrop → gift+model → gift only)
async function mrktSmartHistory({ giftLower, modelLower, backdropLower }) {
  let h = await mrktHistorySalesEstimate({ giftLower, modelLower, backdropLower });
  if (h.ok && h.median != null) return { ...h, level: 'gift+model+backdrop' };

  if (modelLower) {
    h = await mrktHistorySalesEstimate({ giftLower, modelLower, backdropLower: null });
    if (h.ok && h.median != null) return { ...h, level: 'gift+model' };
  }

  h = await mrktHistorySalesEstimate({ giftLower, modelLower: null, backdropLower: null });
  if (h.ok && h.median != null) return { ...h, level: 'gift only' };

  return { ok: true, median: null, count: 0, level: null };
}

// Notifications
async function sendDeal(userId, gift) {
  const lines = [];
  lines.push(`Price: ${gift.priceTon.toFixed(3)} TON`);
  lines.push(`Gift: ${gift.name}`);
  if (gift.attrs?.model) lines.push(`Model: ${gift.attrs.model}`);
  if (gift.attrs?.symbol) lines.push(`Symbol: ${gift.attrs.symbol}`);
  if (gift.attrs?.backdrop) lines.push(`Backdrop: ${gift.attrs.backdrop}`);
  lines.push(`Market: ${gift.market}`);
  if (gift.urlTelegram) lines.push(gift.urlTelegram);

  const btnText =
    gift.market === 'MRKT' ? 'Открыть MRKT' :
    gift.market === 'Tonnel' ? 'Открыть Tonnel' :
    'Открыть Portal';

  const reply_markup = gift.urlMarket ? { inline_keyboard: [[{ text: btnText, url: gift.urlMarket }]] } : undefined;

  await sendMessageSafe(userId, lines.join('\n'), { disable_web_page_preview: false, reply_markup });
}

// ===== Sellprice =====
async function sendSellPriceForUser(chatId, user) {
  if (!user.filters.gifts.length) {
    return sendMessageSafe(chatId, 'Сначала выбери подарок: 🎛 Фильтры → 🎁', { reply_markup: MAIN_KEYBOARD });
  }

  const giftLower = user.filters.gifts[0];
  const modelLower = user.filters.models[0] || null;
  const backdropLower = user.filters.backdrops[0] || null;

  const giftName = getGiftDisplay(giftLower);

  let text = 'Оценка цен продажи:\n\n';
  text += `Подарок: ${giftName}\n`;
  text += `Модель: ${modelLower ? capWords(modelLower) : 'любая'}\n`;
  text += `Фон: ${backdropLower ? capWords(backdropLower) : 'любой'}\n\n`;

  const markets = user.filters.markets || [];

  // Portal via Satellite floors
  if (markets.includes('Portal')) {
    const floor = await satPortalFloorForFilters(giftLower, modelLower);
    if (floor != null && Number.isFinite(floor)) {
      const net = floor * (1 - PORTAL_FEE);
      text += `Portal (Satellite floors):\n`;
      text += `  ~${Number(floor).toFixed(3)} TON (floor)\n`;
      text += `  Чистыми после комиссии ${(PORTAL_FEE * 100).toFixed(1)}%: ~${net.toFixed(3)} TON\n`;
    } else {
      text += `Portal (Satellite floors): нет данных\n`;
    }
  }

  // MRKT
  if (markets.includes('MRKT')) {
    const r = await mrktSearchByFilters({ giftLower, modelLower, backdropLower }, null, null);

    if (r.ok && r.gifts.length) {
      const best = r.gifts[0];
      const net = best.priceTon * (1 - MRKT_FEE);
      text += `\nMRKT:\n`;
      text += `  ~${best.priceTon.toFixed(3)} TON (floor)\n`;
      text += `  Комиссия ${(MRKT_FEE * 100).toFixed(1)}%: ~${net.toFixed(3)} TON чистыми\n`;
    } else if (r.ok) {
      text += `\nMRKT: активных лотов нет\n`;
      const hs = await mrktSmartHistory({ giftLower, modelLower, backdropLower });
      if (hs.ok && hs.median != null) {
        text += `MRKT (история продаж): ~${hs.median.toFixed(3)} TON (уровень: ${hs.level}, n=${hs.count})\n`;
      } else {
        text += `MRKT (история продаж): нет данных\n`;
      }
    } else {
      text += `\nMRKT: ошибка (${r.reason})\n`;
    }
  }

  // Tonnel via Satellite lots
  if (markets.includes('Tonnel')) {
    const r = await satTonnelSearchLots({ giftLower, modelLower, backdropLower }, null, null);
    if (r.ok && r.gifts.length) {
      const best = r.gifts[0];
      text += `\nTonnel (Satellite):\n`;
      text += `  ~${best.priceTon.toFixed(3)} TON (минимальный активный лот)\n`;
    } else if (r.ok) {
      text += `\nTonnel (Satellite): активных лотов нет\n`;
    } else {
      text += `\nTonnel (Satellite): ошибка (${r.reason})\n`;
    }
  }

  await sendMessageSafe(chatId, text, { reply_markup: MAIN_KEYBOARD });
}

// ===== Subscriptions =====
function formatMarkets(markets) {
  if (!markets || !markets.length) return 'нет';
  return markets.join('+');
}
function formatSubTitle(sub) {
  const gift = getGiftDisplay(sub.filters.gift);
  const model = sub.filters.model ? capWords(sub.filters.model) : 'Любая';
  const backdrop = sub.filters.backdrop ? capWords(sub.filters.backdrop) : 'Любой';
  const markets = formatMarkets(sub.filters.markets || []);
  const max = sub.maxPriceTon != null ? `${Number(sub.maxPriceTon).toFixed(3)} TON` : 'без лимита';
  return `#${sub.num}  ${sub.enabled ? 'ON' : 'OFF'}\nGift: ${gift}\nModel: ${model}\nBackdrop: ${backdrop}\nMarkets: ${markets}\nMax: ${max}`;
}
async function showSubsMenu(chatId) {
  const inlineKeyboard = {
    inline_keyboard: [
      [{ text: '➕ Создать из текущих фильтров', callback_data: 'sub_add_current' }],
      [{ text: '📄 Мои подписки', callback_data: 'sub_list' }],
      [{ text: '🔄 Проверить сейчас', callback_data: 'sub_check_now' }],
    ],
  };
  await sendMessageSafe(chatId, 'Подписки:', { reply_markup: inlineKeyboard });
}
async function showSubsList(chatId, user) {
  const subs = user.subscriptions || [];
  if (!subs.length) {
    return sendMessageSafe(chatId, 'Подписок нет.\nНажми: 📡 Подписки → ➕ Создать из текущих фильтров', { reply_markup: MAIN_KEYBOARD });
  }

  let text = 'Мои подписки:\n\n';
  for (const s of subs) text += formatSubTitle(s) + '\n\n';

  const inline_keyboard = subs.slice(0, 25).map((s) => ([
    { text: s.enabled ? `⏸ #${s.num}` : `▶️ #${s.num}`, callback_data: `sub_toggle:${s.id}` },
    { text: `💰 Max`, callback_data: `sub_setmax:${s.id}` },
    { text: `🗑`, callback_data: `sub_delete:${s.id}` },
  ]));

  await sendMessageSafe(chatId, text.slice(0, 3900), { reply_markup: { inline_keyboard } });
}
function findSub(user, subId) {
  const subs = user.subscriptions || [];
  return subs.find((s) => s && s.id === subId) || null;
}
function makeSubFromCurrentFilters(user) {
  const gift = user.filters.gifts[0] || '';
  if (!gift) return { ok: false, reason: 'NO_GIFT' };
  if (!Array.isArray(user.subscriptions)) user.subscriptions = [];

  const sub = {
    id: `sub_${Date.now()}_${Math.random().toString(16).slice(2, 6)}`,
    num: user.subscriptions.length + 1,
    enabled: true,
    maxPriceTon: user.maxPriceTon ?? null,
    filters: {
      gift,
      model: user.filters.models[0] || null,
      backdrop: user.filters.backdrops[0] || null,
      markets: Array.isArray(user.filters.markets) && user.filters.markets.length ? user.filters.markets : ['MRKT'],
    },
  };
  return { ok: true, sub };
}

async function notifySubFloor(userId, sub, market, prevFloor, newFloor, lot) {
  const gift = getGiftDisplay(sub.filters.gift);

  let text = `${gift}\n`;
  if (prevFloor == null) {
    text += `Флор: ${newFloor.toFixed(3)} TON\n`;
  } else {
    const pct = percentChange(prevFloor, newFloor);
    const pctTxt = pct == null ? '' : ` (${pct.toFixed(1)}%)`;
    text += `Флор изменился: ${Number(prevFloor).toFixed(3)} -> ${newFloor.toFixed(3)} TON${pctTxt}\n`;
  }
  if (sub.filters.model) text += `Model: ${capWords(sub.filters.model)}\n`;
  if (sub.filters.backdrop) text += `Backdrop: ${capWords(sub.filters.backdrop)}\n`;
  text += `Market: ${market}\n`;
  if (sub.maxPriceTon != null) text += `Max: ${Number(sub.maxPriceTon).toFixed(3)} TON\n`;
  if (lot?.urlTelegram) text += lot.urlTelegram;

  const reply_markup = lot?.urlMarket ? { inline_keyboard: [[{ text: 'Открыть', url: lot.urlMarket }]] } : undefined;

  await sendMessageSafe(userId, text.trim(), { disable_web_page_preview: false, reply_markup });
}

async function notifySubMrktEvent(userId, sub, item) {
  const type = String(item?.type || '').toLowerCase();
  if (!MRKT_FEED_NOTIFY_TYPES.has(type)) return;

  const gift = item?.gift;
  if (!gift) return;

  const title = gift.collectionTitle || gift.collectionName || gift.title || 'MRKT Gift';
  const name = gift.name && String(gift.name).includes('-') ? String(gift.name) : null;
  const number = gift.number ?? null;

  const amountNano = item.amount ?? gift.salePrice ?? null;
  const amountTon = Number(amountNano) / 1e9;

  let text = `MRKT событие: ${type}\n`;
  text += `${title}${number != null ? ` #${number}` : ''}\n`;
  if (gift.modelTitle || gift.modelName) text += `Model: ${gift.modelTitle || gift.modelName}\n`;
  if (gift.backdropName) text += `Backdrop: ${gift.backdropName}\n`;
  if (Number.isFinite(amountTon) && amountTon > 0) text += `Amount: ${amountTon.toFixed(3)} TON\n`;

  const urlTelegram = name ? `https://t.me/nft/${name}` : 'https://t.me/mrkt';
  const urlMarket = gift.id ? mrktLotUrlFromId(gift.id) : 'https://t.me/mrkt';

  text += urlTelegram;

  const reply_markup = urlMarket ? { inline_keyboard: [[{ text: 'Открыть MRKT', url: urlMarket }]] } : undefined;

  await sendMessageSafe(userId, text.trim(), { disable_web_page_preview: false, reply_markup });
}

async function getFloorForSub(market, sub) {
  const giftLower = sub.filters.gift;
  const modelLower = sub.filters.model ? normTraitName(sub.filters.model) : null;
  const backdropLower = sub.filters.backdrop ? normTraitName(sub.filters.backdrop) : null;

  if (market === 'Portal') {
    const floor = await satPortalFloorForFilters(giftLower, modelLower);
    if (floor == null || !Number.isFinite(floor)) return { ok: true, lot: null };
    return { ok: true, lot: { market: 'Portal', priceTon: Number(floor), urlTelegram: 'https://t.me/portals', urlMarket: 'https://t.me/portals' } };
  }

  if (market === 'MRKT') {
    const r = await mrktSearchByFilters({ giftLower, modelLower, backdropLower }, null, null);
    if (!r.ok) return { ok: false };
    return { ok: true, lot: r.gifts[0] || null };
  }

  if (market === 'Tonnel') {
    const r = await satTonnelSearchLots({ giftLower, modelLower, backdropLower }, null, null);
    if (!r.ok) return { ok: false };
    return { ok: true, lot: r.gifts[0] || null };
  }

  return { ok: false };
}

async function processMrktFeedForSub(userId, sub, stateKey, budgetEvents) {
  if (budgetEvents <= 0) return 0;
  const auth = mrktAuthHeader();
  if (!auth) return 0;

  const giftLower = sub.filters.gift;
  const modelLower = sub.filters.model ? normTraitName(sub.filters.model) : null;
  const backdropLower = sub.filters.backdrop ? normTraitName(sub.filters.backdrop) : null;

  const collectionName = getGiftDisplay(giftLower);
  const modelName = modelLower ? capWords(modelLower) : null;
  const backdropName = backdropLower ? capWords(backdropLower) : null;

  const st = subStates.get(stateKey) || { floor: null, emptyStreak: 0, lastNotifiedFloor: null, feedLastId: null };

  const r = await mrktFeedFetch({ collectionName, modelName, backdropName, cursor: '', count: MRKT_FEED_COUNT, ordering: 'Latest', types: [] });
  if (!r.ok || !r.items.length) return 0;

  const latestId = r.items[0]?.id || null;
  if (!latestId) return 0;

  if (!st.feedLastId) { subStates.set(stateKey, { ...st, feedLastId: latestId }); return 0; }

  const newItems = [];
  for (const it of r.items) {
    if (!it || !it.id) continue;
    if (it.id === st.feedLastId) break;
    newItems.push(it);
  }

  if (!newItems.length) { subStates.set(stateKey, { ...st, feedLastId: latestId }); return 0; }

  newItems.reverse();

  let sent = 0;
  for (const it of newItems) {
    if (sent >= budgetEvents) break;
    await notifySubMrktEvent(userId, sub, it);
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
    let processedSubs = 0, floorNotifs = 0, feedNotifs = 0;
    let globalFeedBudget = SUBS_MRKT_FEED_MAX_EVENTS_PER_CYCLE;

    for (const [userId, user] of users.entries()) {
      const subs = Array.isArray(user.subscriptions) ? user.subscriptions : [];
      const active = subs.filter((s) => s && s.enabled);
      if (!active.length) continue;

      for (const sub of active) {
        processedSubs++;
        if (floorNotifs >= SUBS_MAX_NOTIFICATIONS_PER_CYCLE) break;

        const markets = Array.isArray(sub.filters?.markets) && sub.filters.markets.length ? sub.filters.markets : ['MRKT'];

        for (const market of markets) {
          if (floorNotifs >= SUBS_MAX_NOTIFICATIONS_PER_CYCLE) break;

          const stateKey = `${userId}:${sub.id}:${market}`;
          const prevState = subStates.get(stateKey) || { floor: null, emptyStreak: 0, lastNotifiedFloor: null, feedLastId: null };

          const r = await getFloorForSub(market, sub);
          if (!r.ok) continue;

          const lot = r.lot;
          const newFloor = lot ? lot.priceTon : null;

          let emptyStreak = prevState.emptyStreak || 0;
          if (newFloor == null) {
            emptyStreak++;
            if (emptyStreak < SUBS_EMPTY_CONFIRM) { subStates.set(stateKey, { ...prevState, emptyStreak }); continue; }
          } else {
            emptyStreak = 0;
          }

          const prevFloor = prevState.floor;
          const max = sub.maxPriceTon != null ? Number(sub.maxPriceTon) : null;
          const canNotify = (newFloor != null) && (max == null || newFloor <= max);

          if (prevFloor == null && newFloor != null && canNotify) {
            if (prevState.lastNotifiedFloor == null || Number(prevState.lastNotifiedFloor) !== Number(newFloor)) {
              await notifySubFloor(userId, sub, market, null, newFloor, lot);
              floorNotifs++;
              subStates.set(stateKey, { ...prevState, floor: newFloor, emptyStreak, lastNotifiedFloor: newFloor });
            } else {
              subStates.set(stateKey, { ...prevState, floor: newFloor, emptyStreak });
            }
          } else if (prevFloor != null && newFloor != null && Number(prevFloor) !== Number(newFloor) && canNotify) {
            await notifySubFloor(userId, sub, market, prevFloor, newFloor, lot);
            floorNotifs++;
            subStates.set(stateKey, { ...prevState, floor: newFloor, emptyStreak, lastNotifiedFloor: newFloor });
          } else {
            subStates.set(stateKey, { ...prevState, floor: newFloor, emptyStreak });
          }

          if (market === 'MRKT' && globalFeedBudget > 0) {
            const sent = await processMrktFeedForSub(userId, sub, stateKey, globalFeedBudget);
            globalFeedBudget -= sent;
            feedNotifs += sent;
          }
        }
      }
    }

    return { processedSubs, floorNotifs, feedNotifs };
  } catch (e) {
    console.error('subs interval error:', e);
    return { processedSubs: 0, floorNotifs: 0, feedNotifs: 0 };
  } finally {
    isSubsChecking = false;
  }
}

// ===== Monitor cheap lots =====
async function checkMarketsForAllUsers() {
  if (MODE !== 'real') return;
  if (isChecking) return;

  isChecking = true;
  try {
    pruneSentDeals();

    for (const [userId, user] of users.entries()) {
      if (!user.enabled) continue;
      if (!user.maxPriceTon) continue;
      if (!user.filters.gifts.length) continue;

      const giftLower = user.filters.gifts[0];
      const modelLower = user.filters.models[0] || null;
      const backdropLower = user.filters.backdrops[0] || null;
      const markets = user.filters.markets || [];

      const minP = user.minPriceTon != null ? Number(user.minPriceTon) : 0;
      const maxP = Number(user.maxPriceTon);

      const found = [];

      if (markets.includes('MRKT') && mrktAuthHeader()) {
        const r = await mrktSearchByFilters({ giftLower, modelLower, backdropLower }, minP, maxP);
        if (r.ok && r.gifts?.length) found.push(...r.gifts);
      }

      if (markets.includes('Tonnel')) {
        const r = await satTonnelSearchLots({ giftLower, modelLower, backdropLower }, minP, maxP);
        if (r.ok && r.gifts?.length) found.push(...r.gifts);
      }

      found.sort((a, b) => a.priceTon - b.priceTon);

      let sent = 0;
      for (const gift of found) {
        if (sent >= MAX_NOTIFICATIONS_PER_CHECK) break;
        if (!inRange(gift.priceTon, minP, maxP)) continue;

        const key = `${userId}:${gift.id}`;
        if (sentDeals.has(key)) continue;

        sentDeals.set(key, nowMs());
        await sendDeal(userId, gift);
        sent++;

        if (SEND_DELAY_MS > 0) await sleep(SEND_DELAY_MS);
      }
    }
  } catch (e) {
    console.error('monitor interval error:', e);
  } finally {
    isChecking = false;
  }
}

// ===== Commands =====
bot.onText(/^\/start\b/, async (msg) => {
  getOrCreateUser(msg.from.id);
  await sendMessageSafe(msg.chat.id, 'Бот запущен.', { reply_markup: MAIN_KEYBOARD });
});

bot.onText(/^\/sellprice\b/, async (msg) => {
  const user = getOrCreateUser(msg.from.id);
  await sendSellPriceForUser(msg.chat.id, user);
});

bot.onText(/^\/status\b/, async (msg) => {
  const user = getOrCreateUser(msg.from.id);

  const text =
    `Настройки:\n` +
    `• Мониторинг: ${user.enabled ? 'ON' : 'OFF'}\n` +
    `• Диапазон: ${(user.minPriceTon ?? 0).toFixed(3)} .. ${user.maxPriceTon != null ? user.maxPriceTon.toFixed(3) : '∞'} TON\n` +
    `• Подарок: ${user.filters.gifts[0] || 'не выбран'}\n` +
    `• Модель: ${user.filters.models[0] || 'любая'}\n` +
    `• Фон: ${user.filters.backdrops[0] || 'любой'}\n` +
    `• Маркеты: ${(user.filters.markets || []).join('+')}\n` +
    `• Подписок: ${(user.subscriptions || []).length}\n` +
    `• Каталог gifts: ${catalog.gifts.size}\n\n` +
    `API:\n` +
    `• Portal auth: ${process.env.PORTAL_AUTH ? '✅' : '❌'}\n` +
    `• MRKT auth: ${process.env.MRKT_AUTH ? '✅' : '❌'}\n` +
    `• Satellite enabled: ${SATELLITE_ENABLED ? '✅' : '❌'}\n` +
    `• Satellite token: ${SATELLITE_AUTH_TOKEN ? '✅' : '❌'}\n` +
    `• Tonnel via Satellite: ${(SATELLITE_ENABLED && TONNEL_ENABLED) ? '✅' : '❌'}\n` +
    `• Redis: ${redis ? '✅' : '❌'}\n`;

  await sendMessageSafe(msg.chat.id, text, { reply_markup: MAIN_KEYBOARD });
});

// ===== Message handler =====
bot.on('message', async (msg) => {
  const userId = msg.from?.id;
  const chatId = msg.chat?.id;
  const text = msg.text;

  if (!userId || !chatId || !text) return;
  if (text.startsWith('/')) return;

  const user = getOrCreateUser(userId);
  const t = text.trim();
  const q = normGiftKey(t);

  // states
  if (user.state === 'awaiting_max') {
    const v = n(t);
    if (!Number.isFinite(v) || v <= 0) return sendMessageSafe(chatId, 'Введи MAX TON (пример: 12)', { reply_markup: MAIN_KEYBOARD });
    user.maxPriceTon = v;
    user.state = null;
    clearUserSentDeals(userId);
    scheduleSave();
    return sendMessageSafe(chatId, `Ок. MAX: ${v.toFixed(3)} TON`, { reply_markup: MAIN_KEYBOARD });
  }

  if (user.state === 'awaiting_min') {
    const v = n(t);
    if (!Number.isFinite(v) || v < 0) return sendMessageSafe(chatId, 'Введи MIN TON (0 = убрать)', { reply_markup: MAIN_KEYBOARD });
    user.minPriceTon = v === 0 ? 0 : v;
    user.state = null;
    clearUserSentDeals(userId);
    scheduleSave();
    return sendMessageSafe(chatId, `Ок. MIN: ${user.minPriceTon.toFixed(3)} TON`, { reply_markup: MAIN_KEYBOARD });
  }

  if (user.state === 'awaiting_gift_free') {
    user.state = null;
    if (!q) return sendMessageSafe(chatId, 'Пусто. Напиши название подарка.', { reply_markup: MAIN_KEYBOARD });
    user.filters.gifts = [q];
    user.filters.models = [];
    user.filters.backdrops = [];
    catalogPutGift(capWords(q));
    clearUserSentDeals(userId);
    scheduleSave();
    return sendMessageSafe(chatId, `Ок. Подарок: ${capWords(q)}`, { reply_markup: MAIN_KEYBOARD });
  }

  if (user.state === 'awaiting_model_free') {
    user.state = null;
    if (!q) return sendMessageSafe(chatId, 'Пусто. Напиши модель.', { reply_markup: MAIN_KEYBOARD });
    user.filters.models = [q];
    const gl = user.filters.gifts[0];
    if (gl) catalogPutModel(gl, capWords(q));
    clearUserSentDeals(userId);
    scheduleSave();
    return sendMessageSafe(chatId, `Ок. Модель: ${capWords(q)}`, { reply_markup: MAIN_KEYBOARD });
  }

  if (user.state === 'awaiting_backdrop_free') {
    user.state = null;
    if (!q) return sendMessageSafe(chatId, 'Пусто. Напиши фон.', { reply_markup: MAIN_KEYBOARD });
    user.filters.backdrops = [q];
    const gl = user.filters.gifts[0];
    if (gl) catalogPutBackdrop(gl, capWords(q));
    clearUserSentDeals(userId);
    scheduleSave();
    return sendMessageSafe(chatId, `Ок. Фон: ${capWords(q)}`, { reply_markup: MAIN_KEYBOARD });
  }

  if (typeof user.state === 'string' && user.state.startsWith('awaiting_sub_max:')) {
    const subId = user.state.split(':')[1];
    const v = n(t);
    if (!Number.isFinite(v) || v <= 0) return sendMessageSafe(chatId, 'Введи MAX для подписки (пример: 10).', { reply_markup: MAIN_KEYBOARD });

    const sub = findSub(user, subId);
    user.state = null;
    scheduleSave();

    if (!sub) return sendMessageSafe(chatId, 'Подписка не найдена.', { reply_markup: MAIN_KEYBOARD });
    sub.maxPriceTon = v;

    return sendMessageSafe(chatId, `Ок. MAX для подписки #${sub.num}: ${v.toFixed(3)} TON`, { reply_markup: MAIN_KEYBOARD });
  }

  // buttons
  if (t === '🔍 Запустить поиск') { user.enabled = true; scheduleSave(); return sendMessageSafe(chatId, 'Мониторинг включён.', { reply_markup: MAIN_KEYBOARD }); }
  if (t === '⏹ Остановить поиск') { user.enabled = false; scheduleSave(); return sendMessageSafe(chatId, 'Мониторинг остановлен.', { reply_markup: MAIN_KEYBOARD }); }
  if (t === '💰 Макс. цена') { user.state = 'awaiting_max'; scheduleSave(); return sendMessageSafe(chatId, 'Введи MAX TON:', { reply_markup: MAIN_KEYBOARD }); }
  if (t === '💵 Мин. цена') { user.state = 'awaiting_min'; scheduleSave(); return sendMessageSafe(chatId, 'Введи MIN TON (0 = убрать):', { reply_markup: MAIN_KEYBOARD }); }
  if (t === '💸 Цена подарка') return sendSellPriceForUser(chatId, user);
  if (t === '📡 Подписки') return showSubsMenu(chatId);

  if (t === '📌 Статус API') {
    const p = await portalProbe();
    const s = await satelliteProbe();

    let tn = { ok: false, status: null, contentType: '', bodyStart: '' };
    if (TONNEL_DIRECT_ENABLED && TONNEL_USER_AUTH) {
      const url = 'https://gifts2.tonnel.network/api/pageGifts';
      const headers = {
        Accept: 'application/json',
        'Content-Type': 'application/json',
        origin: TONNEL_ORIGIN,
        referer: TONNEL_REFERER,
        'user-agent': TONNEL_UA,
      };
      const body = {
        page: 1,
        limit: 1,
        sort: JSON.stringify({ message_post_time: -1, gift_id: -1 }),
        filter: JSON.stringify({ price: { $exists: true }, buyer: { $exists: false }, asset: 'TON' }),
        price_range: null,
        ref: 0,
        user_auth: TONNEL_USER_AUTH,
      };
      const res = await fetch(url, { method: 'POST', headers, body: JSON.stringify(body) }).catch(() => null);
      if (res) {
        const txt = await res.text().catch(() => '');
        tn = { ok: res.ok, status: res.status, contentType: res.headers.get('content-type') || '', bodyStart: txt.slice(0, 220) };
        tonnelDirectState.lastAt = nowMs(); tonnelDirectState.lastStatus = res.status; tonnelDirectState.lastOk = res.ok; tonnelDirectState.lastBodyStart = txt.slice(0, 220);
      }
    }

    const text =
      `API статус:\n` +
      `• Portal auth: ${process.env.PORTAL_AUTH ? '✅' : '❌'}\n` +
      `• Portal probe: ${p.status ?? '-'} ${p.ok ? 'OK' : 'FAIL'}\n` +
      (p.contentType ? `• Portal content-type: ${p.contentType}\n` : '') +
      (p.bodyStart ? `• Portal body start: ${p.bodyStart}\n` : '') +
      `\n• MRKT auth: ${process.env.MRKT_AUTH ? '✅' : '❌'}\n` +
      `• MRKT last ok: ${mrktAuthState.lastOkAt ? new Date(mrktAuthState.lastOkAt).toLocaleString() : '-'}\n` +
      `• MRKT last fail: ${mrktAuthState.lastFailAt ? `HTTP ${mrktAuthState.lastFailCode}` : '-'}\n` +
      `\n• Satellite: ${SATELLITE_ENABLED ? '✅' : '❌'}\n` +
      `• Satellite token: ${SATELLITE_AUTH_TOKEN ? '✅' : '❌'}\n` +
      `• Satellite probe: ${s.status ?? '-'} ${s.ok ? 'OK' : 'FAIL'}\n` +
      (s.bodyStart ? `• Satellite body start: ${s.bodyStart}\n` : '') +
      `\n• Tonnel via Satellite: ${(SATELLITE_ENABLED && TONNEL_ENABLED) ? '✅' : '❌'}\n` +
      (TONNEL_DIRECT_ENABLED ? `• Tonnel direct probe: ${tn.status ?? '-'} ${tn.ok ? 'OK' : 'FAIL'}\n` : '') +
      `\n• Redis: ${redis ? '✅' : '❌'}\n`;

    return sendMessageSafe(chatId, text, { reply_markup: MAIN_KEYBOARD });
  }

  if (t === '🎛 Фильтры') {
    const inlineKeyboard = {
      inline_keyboard: [
        [{ text: '✍️ Ввести подарок вручную', callback_data: 'gift_free' }],
        [{ text: '✍️ Ввести модель вручную', callback_data: 'model_free' }],
        [{ text: '✍️ Ввести фон вручную', callback_data: 'backdrop_free' }],
        [
          { text: '🅼 MRKT', callback_data: 'set_markets:MRKT' },
          ...(TONNEL_ENABLED ? [{ text: '🅣 Tonnel', callback_data: 'set_markets:Tonnel' }] : []),
          { text: '🅿 Portal (floors)', callback_data: 'set_markets:Portal' },
        ],
        [{ text: 'MRKT+Tonnel', callback_data: 'set_markets:MRKT_TONNEL' }],
        [{ text: 'ALL', callback_data: 'set_markets:ALL' }],
        [
          { text: '♻️ Сбросить модель', callback_data: 'clear_model' },
          { text: '♻️ Сбросить фон', callback_data: 'clear_backdrop' },
        ],
        [{ text: '♻️ Сбросить всё', callback_data: 'filters_clear' }],
        [{ text: 'ℹ️ Показать фильтры', callback_data: 'show_filters' }],
      ],
    };
    return sendMessageSafe(chatId, 'Настрой фильтры:', { reply_markup: inlineKeyboard });
  }

  return sendMessageSafe(chatId, 'Используй кнопки снизу или /status', { reply_markup: MAIN_KEYBOARD });
});

// ===== Callback handler =====
bot.on('callback_query', async (query) => {
  const userId = query.from.id;
  const chatId = query.message?.chat?.id;
  const data = query.data || '';
  const user = getOrCreateUser(userId);

  try {
    if (data === 'gift_free') {
      user.state = 'awaiting_gift_free'; scheduleSave();
      await sendMessageSafe(chatId, 'Напиши название подарка вручную (например: Victory Medal):', { reply_markup: MAIN_KEYBOARD });
    } else if (data === 'model_free') {
      user.state = 'awaiting_model_free'; scheduleSave();
      await sendMessageSafe(chatId, 'Напиши модель вручную (например: Queen Bee):', { reply_markup: MAIN_KEYBOARD });
    } else if (data === 'backdrop_free') {
      user.state = 'awaiting_backdrop_free'; scheduleSave();
      await sendMessageSafe(chatId, 'Напиши фон вручную (например: Old Gold):', { reply_markup: MAIN_KEYBOARD });
    } else if (data === 'clear_model') {
      user.filters.models = []; clearUserSentDeals(userId); scheduleSave();
      await sendMessageSafe(chatId, 'Модель сброшена.', { reply_markup: MAIN_KEYBOARD });
    } else if (data === 'clear_backdrop') {
      user.filters.backdrops = []; clearUserSentDeals(userId); scheduleSave();
      await sendMessageSafe(chatId, 'Фон сброшен.', { reply_markup: MAIN_KEYBOARD });
    } else if (data === 'filters_clear') {
      user.filters.gifts = []; user.filters.models = []; user.filters.backdrops = [];
      clearUserSentDeals(userId); scheduleSave();
      await sendMessageSafe(chatId, 'Все фильтры сброшены.', { reply_markup: MAIN_KEYBOARD });
    } else if (data === 'show_filters') {
      const text =
        `Фильтры:\n` +
        `• Подарок: ${user.filters.gifts[0] || 'не выбран'}\n` +
        `• Модель: ${user.filters.models[0] || 'любая'}\n` +
        `• Фон: ${user.filters.backdrops[0] || 'любой'}\n` +
        `• Маркеты: ${(user.filters.markets || []).join('+')}\n`;
      await sendMessageSafe(chatId, text, { reply_markup: MAIN_KEYBOARD });
    } else if (data.startsWith('set_markets:')) {
      const v = data.split(':')[1];
      if (v === 'MRKT') user.filters.markets = ['MRKT'];
      else if (v === 'Tonnel') user.filters.markets = ['Tonnel'];
      else if (v === 'Portal') user.filters.markets = ['Portal'];
      else if (v === 'MRKT_TONNEL') user.filters.markets = ['MRKT', 'Tonnel'];
      else user.filters.markets = ['MRKT', ...(TONNEL_ENABLED ? ['Tonnel'] : []), 'Portal'];
      clearUserSentDeals(userId); scheduleSave();
      await sendMessageSafe(chatId, `Ок. Маркеты: ${(user.filters.markets || []).join('+')}`, { reply_markup: MAIN_KEYBOARD });
    } else if (data === 'sub_add_current') {
      const r = makeSubFromCurrentFilters(user);
      if (!r.ok) {
        await sendMessageSafe(chatId, 'Сначала выбери подарок (введи вручную).', { reply_markup: MAIN_KEYBOARD });
      } else {
        user.subscriptions.push(r.sub);
        renumberSubs(user);
        scheduleSave();
        await sendMessageSafe(chatId, `Подписка создана: #${r.sub.num}`, { reply_markup: MAIN_KEYBOARD });
      }
    } else if (data === 'sub_list') {
      await showSubsList(chatId, user);
    } else if (data === 'sub_check_now') {
      await sendMessageSafe(chatId, 'Ок, проверяю подписки сейчас...', { reply_markup: MAIN_KEYBOARD });
      const st = await checkSubscriptionsForAllUsers({ manual: true });
      await sendMessageSafe(
        chatId,
        `Проверка подписок завершена.\n` +
          `• Подписок проверено: ${st.processedSubs}\n` +
          `• Уведомлений (флор): ${st.floorNotifs}\n` +
          `• Уведомлений (MRKT feed): ${st.feedNotifs}`,
        { reply_markup: MAIN_KEYBOARD }
      );
    } else if (data.startsWith('sub_toggle:')) {
      const subId = data.split(':')[1];
      const sub = findSub(user, subId);
      if (!sub) return sendMessageSafe(chatId, 'Подписка не найдена.', { reply_markup: MAIN_KEYBOARD });
      sub.enabled = !sub.enabled;
      scheduleSave();
      await sendMessageSafe(chatId, `Подписка #${sub.num}: ${sub.enabled ? 'ON' : 'OFF'}`, { reply_markup: MAIN_KEYBOARD });
    } else if (data.startsWith('sub_delete:')) {
      const subId = data.split(':')[1];
      user.subscriptions = (user.subscriptions || []).filter((s) => s && s.id !== subId);
      renumberSubs(user);
      scheduleSave();
      await sendMessageSafe(chatId, 'Подписка удалена.', { reply_markup: MAIN_KEYBOARD });
    } else if (data.startsWith('sub_setmax:')) {
      const subId = data.split(':')[1];
      const sub = findSub(user, subId);
      if (!sub) return sendMessageSafe(chatId, 'Подписка не найдена.', { reply_markup: MAIN_KEYBOARD });
      user.state = `awaiting_sub_max:${subId}`;
      scheduleSave();
      await sendMessageSafe(chatId, `Введи MAX TON для подписки #${sub.num}:`, { reply_markup: MAIN_KEYBOARD });
    }
  } catch (e) {
    console.error('callback_query error:', e);
  }

  bot.answerCallbackQuery(query.id).catch(() => {});
});

// ===== Intervals + bootstrap =====
setInterval(() => { checkMarketsForAllUsers().catch((e) => console.error('monitor interval error:', e)); }, CHECK_INTERVAL_MS);
setInterval(() => { checkSubscriptionsForAllUsers().catch((e) => console.error('subs interval error:', e)); }, SUBS_CHECK_INTERVAL_MS);

(async () => {
  if (REDIS_URL) {
    await initRedis();
    if (redis) { await loadState(); await loadCatalog(); }
  }
  console.log('Бот запущен. /start');
})();
