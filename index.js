const TelegramBot = require('node-telegram-bot-api');

// =====================
// ENV
// =====================
const token = process.env.TELEGRAM_TOKEN;
if (!token) {
  console.error('TELEGRAM_TOKEN not set');
  process.exit(1);
}

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

// MRKT feed events in subs
const SUBS_MRKT_FEED_MAX_EVENTS_PER_CYCLE = Number(process.env.SUBS_MRKT_FEED_MAX_EVENTS_PER_CYCLE || 12);

// anti-spam
const SENT_TTL_MS = Number(process.env.SENT_TTL_MS || 24 * 60 * 60 * 1000);

// UI
const MAX_SEARCH_RESULTS = Number(process.env.MAX_SEARCH_RESULTS || 10);

// Redis
const REDIS_URL = process.env.REDIS_URL || process.env.REDIS_PRIVATE_URL || null;

// Optional: куда слать уведомление про протухший MRKT_AUTH (если не задано — всем пользователям)
const ADMIN_CHAT_ID = process.env.ADMIN_CHAT_ID ? Number(process.env.ADMIN_CHAT_ID) : null;

// Portal
const API_URL = 'https://portal-market.com/api/';
const SORT_PRICE_ASC = '&sort_by=price+asc';

const PORTAL_FEE = Number(process.env.PORTAL_FEE || 0.05);

const PORTAL_THROTTLE_MS = Number(process.env.PORTAL_THROTTLE_MS || 250);
let portalNextAllowedAt = 0;

const PORTAL_LIMIT = Number(process.env.PORTAL_LIMIT || 50);
const PORTAL_PAGES = Number(process.env.PORTAL_PAGES || 4);

// Portal history (глубже)
const PORTAL_HISTORY_LIMIT = Number(process.env.PORTAL_HISTORY_LIMIT || 200);
const PORTAL_HISTORY_PAGES = Number(process.env.PORTAL_HISTORY_PAGES || 120);
const PORTAL_HISTORY_PAGE_DELAY_MS = Number(process.env.PORTAL_HISTORY_PAGE_DELAY_MS || 200);
const PORTAL_HISTORY_TARGET_BUYS = Number(process.env.PORTAL_HISTORY_TARGET_BUYS || 120);
const PORTAL_HISTORY_MIN_SAMPLES = Number(process.env.PORTAL_HISTORY_MIN_SAMPLES || 5);

const PORTAL_LOT_URL_TEMPLATE =
  process.env.PORTAL_LOT_URL_TEMPLATE ||
  'https://t.me/portals?startapp=gift_{id}';

const VALID_PORTAL_PREMARKET = new Set([
  'all',
  'only_premarket',
  'without_premarket',
  'draft',
  'listed',
  'sold',
]);
const PORTAL_PREMARKET_STATUS_RAW = String(process.env.PORTAL_PREMARKET_STATUS || 'without_premarket').trim();
const PORTAL_PREMARKET_STATUS = VALID_PORTAL_PREMARKET.has(PORTAL_PREMARKET_STATUS_RAW)
  ? PORTAL_PREMARKET_STATUS_RAW
  : 'without_premarket';

// MRKT
const MRKT_API_URL = 'https://api.tgmrkt.io/api/v1';
const MRKT_FEE = Number(process.env.MRKT_FEE || 0);

const MRKT_COUNT = Number(process.env.MRKT_COUNT || 50);
const MRKT_PAGES = Number(process.env.MRKT_PAGES || 6);

// MRKT history (/feed) — глубже
const MRKT_FEED_COUNT = Number(process.env.MRKT_FEED_COUNT || 50);
const MRKT_HISTORY_TARGET_SALES = Number(process.env.MRKT_HISTORY_TARGET_SALES || 160);
const MRKT_HISTORY_MAX_PAGES = Number(process.env.MRKT_HISTORY_MAX_PAGES || 200);
const MRKT_HISTORY_PAGE_DELAY_MS = Number(process.env.MRKT_HISTORY_PAGE_DELAY_MS || 80);
const MRKT_HISTORY_MIN_SAMPLES = Number(process.env.MRKT_HISTORY_MIN_SAMPLES || 5);

const MRKT_FEED_NOTIFY_TYPES_RAW = String(process.env.MRKT_FEED_NOTIFY_TYPES || 'sale,listing,change_price');
const MRKT_FEED_NOTIFY_TYPES = new Set(
  MRKT_FEED_NOTIFY_TYPES_RAW
    .split(',')
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean)
);

// MRKT auth notify throttling
const MRKT_AUTH_NOTIFY_COOLDOWN_MS = Number(process.env.MRKT_AUTH_NOTIFY_COOLDOWN_MS || 60 * 60 * 1000); // 1h

console.log('Bot version 2026-02-19-history-deeper-fallback-v1');
console.log('MODE =', MODE);
console.log('REDIS_URL =', REDIS_URL ? 'set' : 'not set');
console.log('PORTAL_HISTORY_PAGES/LIMIT/TARGET =', PORTAL_HISTORY_PAGES, '/', PORTAL_HISTORY_LIMIT, '/', PORTAL_HISTORY_TARGET_BUYS);
console.log('MRKT_HISTORY_MAX_PAGES/FEED_COUNT/TARGET =', MRKT_HISTORY_MAX_PAGES, '/', MRKT_FEED_COUNT, '/', MRKT_HISTORY_TARGET_SALES);

const bot = new TelegramBot(token, { polling: true });

// =====================
// UI
// =====================
const MAIN_KEYBOARD = {
  keyboard: [
    [{ text: '🔍 Запустить поиск' }, { text: '⏹ Остановить поиск' }],
    [{ text: '💰 Макс. цена' }, { text: '💵 Мин. цена' }],
    [{ text: '💸 Цена подарка' }, { text: '📡 Подписки' }],
    [{ text: '🎛 Фильтры' }, { text: '📌 Статус API' }],
  ],
  resize_keyboard: true,
};

// =====================
// State
// =====================
const users = new Map();
const sentDeals = new Map();
const subStates = new Map(); // `${userId}:${subId}:${market}` -> { floor, emptyStreak, lastNotifiedFloor, feedLastId }

let isChecking = false;
let isSubsChecking = false;

// caches
let collectionsCache = { time: 0, byLowerName: new Map() };
const COLLECTIONS_CACHE_TTL_MS = 10 * 60_000;

const filtersCache = new Map();
const FILTERS_CACHE_TTL_MS = 5 * 60_000;

const historyCache = new Map(); // key -> { time, ok, median, count, pages, level }
const HISTORY_CACHE_TTL_MS = 30_000;

// MRKT auth status
const mrktAuthState = {
  ok: null,
  lastOkAt: 0,
  lastFailAt: 0,
  lastFailCode: null,
  lastNotifiedAt: 0,
};

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
function capWords(str) {
  return String(str || '').replace(/\w+(?:'\w+)?/g, (w) => (w ? w[0].toUpperCase() + w.slice(1) : w));
}
function quotePlus(str) {
  return encodeURIComponent(str).replace(/%20/g, '+');
}
function shorten(s, max = 32) {
  const t = String(s || '');
  return t.length <= max ? t : t.slice(0, max - 1) + '…';
}
function pruneSentDeals() {
  const now = nowMs();
  for (const [k, ts] of sentDeals.entries()) {
    if (now - ts > SENT_TTL_MS) sentDeals.delete(k);
  }
}
function clearUserSentDeals(userId) {
  const prefix = `${userId}:`;
  for (const k of Array.from(sentDeals.keys())) {
    if (k.startsWith(prefix)) sentDeals.delete(k);
  }
}
function percentChange(oldV, newV) {
  if (!oldV || !Number.isFinite(oldV) || oldV <= 0) return null;
  return ((newV - oldV) / oldV) * 100;
}
function buildPortalLotUrl(id) {
  if (!id) return 'https://t.me/portals';
  if (!PORTAL_LOT_URL_TEMPLATE.includes('{id}')) return PORTAL_LOT_URL_TEMPLATE;
  return PORTAL_LOT_URL_TEMPLATE.replace('{id}', encodeURIComponent(String(id)));
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
function median(sorted) {
  if (!sorted.length) return null;
  const L = sorted.length;
  return L % 2 ? sorted[(L - 1) / 2] : (sorted[L / 2 - 1] + sorted[L / 2]) / 2;
}

// =====================
// Portal headers/throttle
// =====================
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
        const ms = (Number(retryAfter) + 1) * 1000;
        await sleep(ms);
        continue;
      }
      throw e;
    }
  }
}

// =====================
// MRKT auth alert
// =====================
function mrktAuthHeader() {
  const t = process.env.MRKT_AUTH;
  if (!t) return null;
  return t;
}

async function notifyMrktAuthExpired(statusCode) {
  const now = nowMs();
  if (now - mrktAuthState.lastNotifiedAt < MRKT_AUTH_NOTIFY_COOLDOWN_MS) return;

  mrktAuthState.lastNotifiedAt = now;

  const text = `MRKT токен не работает (HTTP ${statusCode}). Обнови MRKT_AUTH в Railway.`;

  if (ADMIN_CHAT_ID && Number.isFinite(ADMIN_CHAT_ID)) {
    try { await sendMessageSafe(ADMIN_CHAT_ID, text, { disable_web_page_preview: true }); return; } catch {}
  }

  for (const [uid] of users.entries()) {
    try { await sendMessageSafe(uid, text, { disable_web_page_preview: true }); } catch {}
  }
}

function markMrktOk() {
  mrktAuthState.ok = true;
  mrktAuthState.lastOkAt = nowMs();
}

async function markMrktFailIfAuth(statusCode) {
  mrktAuthState.ok = false;
  mrktAuthState.lastFailAt = nowMs();
  mrktAuthState.lastFailCode = statusCode;

  if (statusCode === 401 || statusCode === 403) {
    await notifyMrktAuthExpired(statusCode);
  }
}

// =====================
// Redis persistence
// =====================
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
      state: null,
      filters: { gifts: [], models: [], backdrops: [], markets: ['Portal', 'MRKT'] },
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
        markets: Array.isArray(u?.filters?.markets) ? u.filters.markets : ['Portal', 'MRKT'],
      },
      subscriptions: Array.isArray(u?.subscriptions) ? u.subscriptions : [],
    };

    for (const s of safe.subscriptions) {
      if (!s || typeof s !== 'object') continue;
      if (!s.id) s.id = `sub_${Date.now()}_${Math.random().toString(16).slice(2, 6)}`;
      if (s.enabled == null) s.enabled = true;
      if (!s.filters) s.filters = {};
      if (!Array.isArray(s.filters.markets)) s.filters.markets = ['Portal', 'MRKT'];
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
  const keys = ['bot:state:v10', 'bot:state:v9', 'bot:state:v8', 'bot:state:v7', 'bot:state:v6', 'bot:state:v5', 'bot:state:v4', 'bot:state:v3', 'bot:state:v2', 'bot:state:v1'];
  for (const k of keys) {
    const raw = await redis.get(k);
    if (raw) {
      importState(JSON.parse(raw));
      console.log('Loaded state from Redis key:', k, 'users:', users.size);
      return;
    }
  }
}

let saveTimer = null;
function scheduleSave() {
  if (!redis) return;
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    saveState().catch((e) => console.error('saveState error:', e));
  }, 250);
}

async function saveState() {
  if (!redis) return;
  await redis.set('bot:state:v10', JSON.stringify(exportState()));
}

// =====================
// Portal: collections / filters
// =====================
async function portalCollections(limit = 400) {
  const now = nowMs();
  if (collectionsCache.byLowerName.size && now - collectionsCache.time < COLLECTIONS_CACHE_TTL_MS) return collectionsCache;
  if (!process.env.PORTAL_AUTH) return (collectionsCache = { time: now, byLowerName: new Map() });

  const url = `${API_URL}collections?limit=${limit}`;
  const res = await throttledPortalFetch(url, { method: 'GET', headers: portalHeaders() }).catch(() => null);
  if (!res || !res.ok) return (collectionsCache = { time: now, byLowerName: new Map() });

  const data = await res.json().catch(() => null);
  const arr = Array.isArray(data?.collections) ? data.collections : Array.isArray(data) ? data : [];

  const byLowerName = new Map();
  for (const c of arr) {
    const name = String(c?.name || c?.title || '').trim();
    if (name) byLowerName.set(name.toLowerCase(), { name, raw: c });
  }

  return (collectionsCache = { time: now, byLowerName });
}

function portalCollectionId(raw) {
  return raw?.id || raw?.collection_id || raw?.collectionId || null;
}
function portalShortName(raw) {
  return raw?.short_name || raw?.shortName || null;
}

async function portalCollectionFilters(shortName) {
  if (!shortName || !process.env.PORTAL_AUTH) return null;

  const now = nowMs();
  const cached = filtersCache.get(shortName);
  if (cached && now - cached.time < FILTERS_CACHE_TTL_MS) return cached.data;

  const url = `${API_URL}collections/filters?short_names=${encodeURIComponent(shortName)}`;
  const res = await throttledPortalFetch(url, { method: 'GET', headers: portalHeaders() }).catch(() => null);
  if (!res || !res.ok) return null;

  const data = await res.json().catch(() => null);
  if (!data) return null;

  let block = null;
  if (data.collections && typeof data.collections === 'object') {
    const key = Object.keys(data.collections).find((k) => k.toLowerCase() === shortName.toLowerCase()) || shortName;
    block = data.collections[key] || null;
  } else if (data.floor_prices && typeof data.floor_prices === 'object') {
    const key = Object.keys(data.floor_prices).find((k) => k.toLowerCase() === shortName.toLowerCase()) || shortName;
    block = data.floor_prices[key] || null;
  }
  if (!block) return null;

  const out = { models: block.models || [], backdrops: block.backdrops || [] };
  filtersCache.set(shortName, { time: now, data: out });
  return out;
}

// =====================
// Rarity parsing for Portal filters
// =====================
function parseRarityNumber(v) {
  if (v == null) return null;
  if (typeof v === 'number') return Number.isFinite(v) ? v : null;
  if (typeof v === 'string') {
    const cleaned = v.trim().replace('%', '').replace('‰', '');
    const num = n(cleaned);
    return Number.isFinite(num) ? num : null;
  }
  return null;
}

function extractRarityValue(obj) {
  if (obj == null) return null;
  const directNum = parseRarityNumber(obj);
  if (directNum != null) return directNum;
  if (typeof obj !== 'object') return null;

  const direct =
    obj.rarityPermille ??
    obj.rarity_per_mille ??
    obj.rarityPerMille ??
    obj.rarity ??
    obj.rarity_percent ??
    obj.rarityPercent ??
    null;

  const v = extractRarityValue(direct);
  if (v != null) return v;

  for (const [k, val] of Object.entries(obj)) {
    if (String(k).toLowerCase().includes('rarity')) {
      const x = extractRarityValue(val);
      if (x != null) return x;
    }
  }
  for (const val of Object.values(obj)) {
    const inner = extractRarityValue(val);
    if (inner != null) return inner;
  }
  return null;
}

function extractModelTraits(block) {
  const map = new Map();
  const push = (name, rarity) => {
    const key = String(name).toLowerCase();
    if (!map.has(key)) map.set(key, { name: String(name), rarity: rarity ?? null });
    else {
      const prev = map.get(key);
      if (prev && prev.rarity == null && rarity != null) prev.rarity = rarity;
    }
  };

  if (!block) return [];
  if (Array.isArray(block)) {
    for (const item of block) {
      if (!item) continue;
      if (typeof item === 'string') push(item.trim(), null);
      else {
        const name = item.name || item.model || item.value || item.title;
        if (!name) continue;
        push(name, extractRarityValue(item));
      }
    }
  } else if (typeof block === 'object') {
    for (const [k, v] of Object.entries(block)) push(k, extractRarityValue(v));
  }

  const arr = Array.from(map.values());
  arr.sort((a, b) => {
    const ra = a.rarity == null ? Infinity : a.rarity;
    const rb = b.rarity == null ? Infinity : b.rarity;
    if (ra !== rb) return ra - rb;
    return a.name.localeCompare(b.name);
  });
  return arr;
}

function rarityLabelPercent(trait) {
  if (!trait || trait.rarity == null) return '';
  const v = Number(trait.rarity);
  if (!Number.isFinite(v)) return '';
  return `${v}%`;
}

// =====================
// Portal search
// =====================
async function portalSearchPage({ giftLower, modelLower, backdropLower, minPriceTon, maxPriceTon, offset, limit }) {
  if (!process.env.PORTAL_AUTH) return { ok: false, reason: 'NO_AUTH', gifts: [], collectionId: null };

  const { byLowerName } = await portalCollections(400);
  const col = byLowerName.get(giftLower);
  const giftName = col?.name || capWords(giftLower);
  const collectionId = portalCollectionId(col?.raw);

  let url = `${API_URL}nfts/search?offset=${offset}&limit=${limit}${SORT_PRICE_ASC}`;

  if (minPriceTon != null && Number.isFinite(minPriceTon)) url += `&min_price=${Number(minPriceTon)}`;
  if (maxPriceTon != null && Number.isFinite(maxPriceTon)) url += `&max_price=${Number(maxPriceTon)}`;

  if (collectionId) url += `&collection_ids=${encodeURIComponent(collectionId)}`;
  else url += `&filter_by_collections=${quotePlus(giftName)}`;

  if (modelLower) url += `&filter_by_models=${quotePlus(capWords(modelLower))}`;
  if (backdropLower) url += `&filter_by_backdrops=${quotePlus(capWords(backdropLower))}`;

  url += `&status=listed&exclude_bundled=true&premarket_status=${encodeURIComponent(PORTAL_PREMARKET_STATUS)}`;

  const res = await throttledPortalFetch(url, { method: 'GET', headers: portalHeaders() }).catch(() => null);
  if (!res) return { ok: false, reason: 'FETCH_ERROR', gifts: [], collectionId };
  if (!res.ok) return { ok: false, reason: `HTTP_${res.status}`, gifts: [], collectionId };

  const data = await res.json().catch(() => null);
  const results = Array.isArray(data?.results) ? data.results : [];

  const gifts = [];
  for (const nft of results) {
    const priceTon = n(nft?.price);
    if (!Number.isFinite(priceTon) || priceTon <= 0) continue;

    let model = null, backdrop = null, symbol = null;
    if (Array.isArray(nft.attributes)) {
      for (const a of nft.attributes) {
        if (!a?.type) continue;
        if (a.type === 'model') model = a.value;
        else if (a.type === 'backdrop') backdrop = a.value;
        else if (a.type === 'symbol') symbol = a.value;
      }
    }

    if (modelLower && norm(model) !== modelLower) continue;
    if (backdropLower && norm(backdrop) !== backdropLower) continue;

    const baseName = nft.name || 'NFT';
    const number = nft.external_collection_number ?? null;
    const displayName = number != null ? `${baseName} #${number}` : baseName;

    const urlTelegram = (() => {
      if (nft?.tg_id && String(nft.tg_id).includes('-')) return `https://t.me/nft/${nft.tg_id}`;
      const name = String(nft?.name || '').trim();
      const num = nft?.external_collection_number;
      if (!name || num == null) return 'https://t.me/portals';
      const slugName = name.replace(/[^a-z0-9]+/gi, '').toLowerCase();
      if (!slugName) return 'https://t.me/portals';
      return `https://t.me/nft/${slugName}-${num}`;
    })();

    gifts.push({
      id: `portal_${nft.id || nft.tg_id || displayName}`,
      market: 'Portal',
      name: displayName,
      baseName,
      priceTon,
      urlTelegram,
      urlMarket: buildPortalLotUrl(nft.id),
      attrs: { model, backdrop, symbol },
      raw: nft,
    });
  }

  gifts.sort((a, b) => a.priceTon - b.priceTon);
  return { ok: true, reason: 'OK', gifts, collectionId };
}

async function portalSearchByFilters({ giftLower, modelLower, backdropLower, minPriceTon, maxPriceTon }) {
  const all = [];
  let collectionId = null;

  for (let page = 0; page < PORTAL_PAGES; page++) {
    const offset = page * PORTAL_LIMIT;
    const r = await portalSearchPage({
      giftLower,
      modelLower,
      backdropLower,
      minPriceTon,
      maxPriceTon,
      offset,
      limit: PORTAL_LIMIT,
    });

    if (!r.ok) return r;
    if (collectionId == null) collectionId = r.collectionId;

    if (!r.gifts.length) break;
    all.push(...r.gifts);

    if (r.gifts.length < PORTAL_LIMIT) break;
    if (all.length >= MAX_PER_MARKET) break;
  }

  all.sort((a, b) => a.priceTon - b.priceTon);
  return { ok: true, reason: 'OK', gifts: all.slice(0, MAX_PER_MARKET), collectionId };
}

// =====================
// Portal history (levels fallback)
// =====================
function extractActionNft(act) {
  return act?.nft || act?.item || act?.gift || act?.asset || null;
}
function extractAttrsFromAny(nft) {
  let model = nft?.model || nft?.modelName || nft?.modelTitle || null;
  let backdrop = nft?.backdrop || nft?.backdropName || null;

  const attrs = nft?.attributes || nft?.attrs || null;
  if (Array.isArray(attrs)) {
    for (const a of attrs) {
      const t = String(a?.type || a?.trait_type || '').toLowerCase();
      if (!t) continue;
      if (t === 'model') model = a.value;
      else if (t === 'backdrop') backdrop = a.value;
    }
  } else if (attrs && typeof attrs === 'object') {
    if (attrs.model) model = attrs.model;
    if (attrs.backdrop) backdrop = attrs.backdrop;
  }

  return { model, backdrop };
}

async function portalHistoryEstimateExact({ collectionId, modelLower, backdropLower }) {
  if (!process.env.PORTAL_AUTH) return { ok: false, median: null, count: 0, pages: 0 };

  const prices = [];
  let page = 0;

  while (page < PORTAL_HISTORY_PAGES && prices.length < PORTAL_HISTORY_TARGET_BUYS) {
    const offset = page * PORTAL_HISTORY_LIMIT;
    let url = `${API_URL}market/actions/?offset=${offset}&limit=${PORTAL_HISTORY_LIMIT}&action_types=buy&sort_by=created_at+desc`;
    if (collectionId) url += `&collection_ids=${encodeURIComponent(collectionId)}`;

    const res = await throttledPortalFetch(url, { method: 'GET', headers: portalHeaders() }).catch(() => null);
    if (!res || !res.ok) break;

    const data = await res.json().catch(() => null);
    const actions = Array.isArray(data?.actions) ? data.actions : Array.isArray(data) ? data : [];
    if (!actions.length) break;

    for (const act of actions) {
      const nft = extractActionNft(act);
      if (!nft) continue;

      const actCollectionId =
        nft.collection_id || nft.collectionId || nft.collection?.id || null;
      if (collectionId && actCollectionId && actCollectionId !== collectionId) continue;

      const { model, backdrop } = extractAttrsFromAny(nft);
      if (modelLower && norm(model) !== modelLower) continue;
      if (backdropLower && norm(backdrop) !== backdropLower) continue;

      const amount =
        act.amount ?? act.price ?? act.ton_amount ?? act.tonAmount ?? act.value ?? null;

      const priceTon = n(amount);
      if (!Number.isFinite(priceTon) || priceTon <= 0) continue;

      prices.push(priceTon);
      if (prices.length >= PORTAL_HISTORY_TARGET_BUYS) break;
    }

    page++;
    await sleep(PORTAL_HISTORY_PAGE_DELAY_MS);
  }

  prices.sort((a, b) => a - b);
  return { ok: true, median: median(prices), count: prices.length, pages: page };
}

async function portalHistoryEstimateLevels({ collectionId, modelLower, backdropLower }) {
  const key = `portal_levels|${collectionId}|${modelLower || ''}|${backdropLower || ''}|target=${PORTAL_HISTORY_TARGET_BUYS}`;
  const now = nowMs();
  const cached = historyCache.get(key);
  if (cached && now - cached.time < HISTORY_CACHE_TTL_MS) return cached;

  // level 1: gift+model+backdrop
  let r1 = await portalHistoryEstimateExact({ collectionId, modelLower, backdropLower });
  if (r1.ok && r1.count >= PORTAL_HISTORY_MIN_SAMPLES) {
    const out = { ok: true, median: r1.median, count: r1.count, pages: r1.pages, level: 'gift+model+backdrop', time: now };
    historyCache.set(key, out);
    return out;
  }

  // level 2: gift+model (если была модель)
  if (modelLower) {
    const r2 = await portalHistoryEstimateExact({ collectionId, modelLower, backdropLower: null });
    if (r2.ok && r2.count >= PORTAL_HISTORY_MIN_SAMPLES) {
      const out = { ok: true, median: r2.median, count: r2.count, pages: r2.pages, level: 'gift+model', time: now };
      historyCache.set(key, out);
      return out;
    }
  }

  // level 3: gift only
  const r3 = await portalHistoryEstimateExact({ collectionId, modelLower: null, backdropLower: null });
  const out = { ok: true, median: r3.median, count: r3.count, pages: r3.pages, level: 'gift', time: now };
  historyCache.set(key, out);
  return out;
}

// =====================
// MRKT: saling (lots)
// =====================
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
    headers: {
      Authorization: auth,
      'Content-Type': 'application/json',
      Accept: 'application/json',
    },
    body: JSON.stringify(body),
  }).catch(() => null);

  if (!res) return { ok: false, reason: 'FETCH_ERROR', gifts: [], cursor: null };
  if (!res.ok) {
    await markMrktFailIfAuth(res.status);
    return { ok: false, reason: `HTTP_${res.status}`, gifts: [], cursor: null };
  }

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

  const collectionName = collectionsCache.byLowerName.get(giftLower)?.name || capWords(giftLower);
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

      if (modelLower && norm(model) !== modelLower) continue;
      if (backdropLower && norm(backdrop) !== backdropLower) continue;

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
      });
    }

    cursor = r.cursor || '';
    if (!cursor) break;
    if (out.length >= MAX_PER_MARKET) break;
  }

  out.sort((a, b) => a.priceTon - b.priceTon);
  return { ok: true, reason: 'OK', gifts: out.slice(0, MAX_PER_MARKET) };
}

// =====================
// MRKT: feed history + levels fallback
// =====================
async function mrktFeedFetch({ collectionName, modelName, backdropName, cursor, count, ordering = 'Latest' }) {
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
    type: [], // ВАЖНО: не ставим ['sale'], чтобы не словить несовместимость, фильтруем локально
  };

  const res = await fetch(`${MRKT_API_URL}/feed`, {
    method: 'POST',
    headers: {
      Authorization: auth,
      'Content-Type': 'application/json',
      Accept: 'application/json',
    },
    body: JSON.stringify(body),
  }).catch(() => null);

  if (!res) return { ok: false, reason: 'FETCH_ERROR', items: [], cursor: null };
  if (!res.ok) {
    await markMrktFailIfAuth(res.status);
    return { ok: false, reason: `HTTP_${res.status}`, items: [], cursor: null };
  }

  markMrktOk();

  const data = await res.json().catch(() => null);
  const items = Array.isArray(data?.items) ? data.items : [];
  const nextCursor = data?.cursor || data?.nextCursor || '';
  return { ok: true, reason: 'OK', items, cursor: nextCursor };
}

async function mrktHistorySalesExact({ collectionName, modelLower, backdropLower, modelNamePayload, backdropNamePayload }) {
  let cursor = '';
  const prices = [];
  let pages = 0;

  while (pages < MRKT_HISTORY_MAX_PAGES && prices.length < MRKT_HISTORY_TARGET_SALES) {
    const r = await mrktFeedFetch({
      collectionName,
      modelName: modelNamePayload,
      backdropName: backdropNamePayload,
      cursor,
      count: MRKT_FEED_COUNT,
      ordering: 'Latest',
    });

    if (!r.ok) return { ok: false, reason: r.reason, median: null, count: prices.length, pages };

    if (!r.items.length) break;

    for (const item of r.items) {
      if (String(item?.type || '').toLowerCase() !== 'sale') continue;

      const g = item?.gift;
      if (!g) continue;

      const m = g.modelName || g.modelTitle || null;
      const b = g.backdropName || null;

      // локальная строгая проверка фильтров (на случай если сервер не фильтрует)
      if (modelLower && norm(m) !== modelLower) continue;
      if (backdropLower && norm(b) !== backdropLower) continue;

      const ton = Number(item?.amount) / 1e9;
      if (!Number.isFinite(ton) || ton <= 0) continue;

      prices.push(ton);
      if (prices.length >= MRKT_HISTORY_TARGET_SALES) break;
    }

    cursor = r.cursor || '';
    pages++;
    if (!cursor) break;

    await sleep(MRKT_HISTORY_PAGE_DELAY_MS);
  }

  prices.sort((a, b) => a - b);
  return { ok: true, median: median(prices), count: prices.length, pages };
}

async function mrktHistorySalesLevels({ giftLower, modelLower, backdropLower }) {
  const auth = mrktAuthHeader();
  if (!auth) return { ok: false, reason: 'NO_AUTH', median: null, count: 0, pages: 0, level: null };
  if (!giftLower) return { ok: false, reason: 'NO_GIFT', median: null, count: 0, pages: 0, level: null };

  const collectionName = collectionsCache.byLowerName.get(giftLower)?.name || capWords(giftLower);
  const modelName = modelLower ? capWords(modelLower) : null;
  const backdropName = backdropLower ? capWords(backdropLower) : null;

  const key = `mrkt_levels|${collectionName}|${modelLower || ''}|${backdropLower || ''}|target=${MRKT_HISTORY_TARGET_SALES}`;
  const now = nowMs();
  const cached = historyCache.get(key);
  if (cached && now - cached.time < HISTORY_CACHE_TTL_MS) return cached;

  // level 1: gift+model+backdrop
  let r1 = await mrktHistorySalesExact({
    collectionName,
    modelLower,
    backdropLower,
    modelNamePayload: modelName,
    backdropNamePayload: backdropName,
  });
  if (r1.ok && r1.count >= MRKT_HISTORY_MIN_SAMPLES) {
    const out = { ok: true, median: r1.median, count: r1.count, pages: r1.pages, level: 'gift+model+backdrop', time: now };
    historyCache.set(key, out);
    return out;
  }

  // level 2: gift+model
  if (modelLower) {
    const r2 = await mrktHistorySalesExact({
      collectionName,
      modelLower,
      backdropLower: null,
      modelNamePayload: modelName,
      backdropNamePayload: null,
    });
    if (r2.ok && r2.count >= MRKT_HISTORY_MIN_SAMPLES) {
      const out = { ok: true, median: r2.median, count: r2.count, pages: r2.pages, level: 'gift+model', time: now };
      historyCache.set(key, out);
      return out;
    }
  }

  // level 3: gift only
  const r3 = await mrktHistorySalesExact({
    collectionName,
    modelLower: null,
    backdropLower: null,
    modelNamePayload: null,
    backdropNamePayload: null,
  });
  const out = { ok: true, median: r3.median, count: r3.count, pages: r3.pages, level: 'gift', time: now };
  historyCache.set(key, out);
  return out;
}

// =====================
// Sellprice (history only if no lots) + fallback levels
// =====================
async function sendSellPriceForUser(chatId, user) {
  if (!user.filters.gifts.length) {
    return sendMessageSafe(chatId, 'Сначала выбери подарок: 🎛 Фильтры → 🎁', { reply_markup: MAIN_KEYBOARD });
  }

  const giftLower = user.filters.gifts[0];
  const modelLower = user.filters.models[0] || null;
  const backdropLower = user.filters.backdrops[0] || null;

  const giftName = collectionsCache.byLowerName.get(giftLower)?.name || capWords(giftLower);

  let text = 'Оценка цен продажи:\n\n';
  text += `Подарок: ${giftName}\n`;
  text += `Модель: ${modelLower ? capWords(modelLower) : 'любая'}\n`;
  text += `Фон: ${backdropLower ? capWords(backdropLower) : 'любой'}\n\n`;

  // Portal
  if ((user.filters.markets || []).includes('Portal')) {
    const r = await portalSearchByFilters({ giftLower, modelLower, backdropLower, minPriceTon: null, maxPriceTon: null });

    if (r.ok && r.gifts.length) {
      const best = r.gifts[0];
      const net = best.priceTon * (1 - PORTAL_FEE);
      text += `Portal:\n  ~${best.priceTon.toFixed(3)} TON (минимальный активный лот)\n`;
      text += `  Чистыми после комиссии ${(PORTAL_FEE * 100).toFixed(1)}%: ~${net.toFixed(3)} TON\n`;
    } else if (r.ok) {
      text += `Portal: активных лотов по этим фильтрам нет\n`;

      if (r.collectionId) {
        const h = await portalHistoryEstimateLevels({ collectionId: r.collectionId, modelLower, backdropLower });
        if (h.ok && h.median != null && h.count > 0) {
          text += `Portal (история):\n  ~${h.median.toFixed(3)} TON (медиана, n=${h.count}; ${h.level})\n`;
        } else {
          text += `Portal (история): нет данных\n`;
        }
      } else {
        text += `Portal (история): нет collection_id\n`;
      }
    } else {
      text += `Portal: ошибка (${r.reason})\n`;
    }
  }

  // MRKT
  if ((user.filters.markets || []).includes('MRKT')) {
    const r = await mrktSearchByFilters({ giftLower, modelLower, backdropLower }, null, null);

    if (r.ok && r.gifts.length) {
      const best = r.gifts[0];
      const net = best.priceTon * (1 - MRKT_FEE);
      text += `\nMRKT:\n  ~${best.priceTon.toFixed(3)} TON (минимальный активный лот)\n`;
      text += `  Комиссия ${(MRKT_FEE * 100).toFixed(1)}%: ~${net.toFixed(3)} TON чистыми\n`;
    } else if (r.ok) {
      text += `\nMRKT: активных лотов по этим фильтрам нет\n`;

      const hs = await mrktHistorySalesLevels({ giftLower, modelLower, backdropLower });
      if (hs.ok && hs.median != null && hs.count > 0) {
        text += `MRKT (история продаж):\n  ~${hs.median.toFixed(3)} TON (медиана, n=${hs.count}; ${hs.level})\n`;
      } else {
        text += `MRKT (история продаж): нет данных\n`;
      }
    } else {
      text += `\nMRKT: ошибка (${r.reason})\n`;
    }
  }

  await sendMessageSafe(chatId, text, { reply_markup: MAIN_KEYBOARD });
}

// =====================
// Minimal commands for your check (чтобы не ломать твой текущий UI)
// =====================
bot.onText(/^\/start\b/, async (msg) => {
  getOrCreateUser(msg.from.id);
  await portalCollections(200).catch(() => {});
  await sendMessageSafe(msg.chat.id, 'Бот запущен.', { reply_markup: MAIN_KEYBOARD });
});

bot.onText(/^\/sellprice\b/, async (msg) => {
  const user = getOrCreateUser(msg.from.id);
  await portalCollections(200).catch(() => {});
  await sendSellPriceForUser(msg.chat.id, user);
});

bot.on('message', async (msg) => {
  const userId = msg.from?.id;
  const chatId = msg.chat?.id;
  const text = msg.text;
  if (!userId || !chatId || !text) return;
  if (text.startsWith('/')) return;

  const user = getOrCreateUser(userId);

  if (text.trim() === '💸 Цена подарка') {
    await portalCollections(200).catch(() => {});
    return sendSellPriceForUser(chatId, user);
  }

  // ВАЖНО: чтобы не перетирать твой текущий "идеальный" UI,
  // здесь оставлен минимум. Если хочешь — я волью эти изменения в твой полный файл 1-в-1.
});
