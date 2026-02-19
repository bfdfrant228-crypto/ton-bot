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

// Optional: куда слать уведомление про протухший MRKT_AUTH (если не задано — всем пользователям в users Map)
const ADMIN_CHAT_ID = process.env.ADMIN_CHAT_ID ? Number(process.env.ADMIN_CHAT_ID) : null;

// Portal
const API_URL = 'https://portal-market.com/api/';
const SORT_PRICE_ASC = '&sort_by=price+asc';

const PORTAL_FEE = Number(process.env.PORTAL_FEE || 0.05);

const PORTAL_THROTTLE_MS = Number(process.env.PORTAL_THROTTLE_MS || 250);
let portalNextAllowedAt = 0;

const PORTAL_LIMIT = Number(process.env.PORTAL_LIMIT || 50);
const PORTAL_PAGES = Number(process.env.PORTAL_PAGES || 4);

// ↑ ты просил увеличить выборку: по умолчанию сделали побольше
const PORTAL_HISTORY_LIMIT = Number(process.env.PORTAL_HISTORY_LIMIT || 150);
const PORTAL_HISTORY_PAGES = Number(process.env.PORTAL_HISTORY_PAGES || 30);
const PORTAL_HISTORY_PAGE_DELAY_MS = Number(process.env.PORTAL_HISTORY_PAGE_DELAY_MS || 250);

// ты уже поставил и оно работает: https://t.me/portals?startapp=gift_{id}
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

// MRKT feed/history
const MRKT_FEED_COUNT = Number(process.env.MRKT_FEED_COUNT || 50); // больше, чтобы быстрее находить sale
const MRKT_HISTORY_TARGET_SALES = Number(process.env.MRKT_HISTORY_TARGET_SALES || 60); // по фильтрам
const MRKT_HISTORY_MAX_PAGES = Number(process.env.MRKT_HISTORY_MAX_PAGES || 60); // ↑ ты просил увеличить
const MRKT_FEED_NOTIFY_TYPES_RAW = String(process.env.MRKT_FEED_NOTIFY_TYPES || 'sale,listing,change_price');
const MRKT_FEED_NOTIFY_TYPES = new Set(
  MRKT_FEED_NOTIFY_TYPES_RAW
    .split(',')
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean)
);

// MRKT auth notify throttling
const MRKT_AUTH_NOTIFY_COOLDOWN_MS = Number(process.env.MRKT_AUTH_NOTIFY_COOLDOWN_MS || 60 * 60 * 1000); // 1h

console.log('Bot version 2026-02-19-sellprice-clean-mrkt-history-authalert-subs-renum-v1');
console.log('MODE =', MODE);
console.log('CHECK_INTERVAL_MS =', CHECK_INTERVAL_MS);
console.log('SUBS_CHECK_INTERVAL_MS =', SUBS_CHECK_INTERVAL_MS);
console.log('REDIS_URL =', REDIS_URL ? 'set' : 'not set');
console.log('ADMIN_CHAT_ID =', ADMIN_CHAT_ID || 'not set');
console.log('PORTAL_PREMARKET_STATUS =', PORTAL_PREMARKET_STATUS);
console.log('PORTAL_LOT_URL_TEMPLATE =', PORTAL_LOT_URL_TEMPLATE);
console.log('MRKT_FEED_NOTIFY_TYPES =', Array.from(MRKT_FEED_NOTIFY_TYPES).join(', '));

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
const users = new Map();             // userId -> userState
const sentDeals = new Map();         // key -> ts

// `${userId}:${subId}:${market}` -> { floor, emptyStreak, lastNotifiedFloor, feedLastId }
const subStates = new Map();

let isChecking = false;
let isSubsChecking = false;

// caches
let collectionsCache = { time: 0, byLowerName: new Map() };
const COLLECTIONS_CACHE_TTL_MS = 10 * 60_000;

const filtersCache = new Map(); // shortName -> { time, data:{models, backdrops} }
const FILTERS_CACHE_TTL_MS = 5 * 60_000;

const historyCache = new Map(); // key -> { time, ok, median, count }
const HISTORY_CACHE_TTL_MS = 30_000;

// MRKT auth status
const mrktAuthState = {
  ok: null,              // true/false/null
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
  return t; // у тебя это просто token (без Bearer)
}

async function notifyMrktAuthExpired(statusCode) {
  const now = nowMs();
  if (now - mrktAuthState.lastNotifiedAt < MRKT_AUTH_NOTIFY_COOLDOWN_MS) return;

  mrktAuthState.lastNotifiedAt = now;

  const text =
    `⚠️ MRKT токен не работает (HTTP ${statusCode}).\n` +
    `Нужно обновить переменную MRKT_AUTH в Railway.\n` +
    `Пока токен не обновишь — MRKT запросы будут пропускаться.`;

  // 1) если задан ADMIN_CHAT_ID — пишем только туда
  if (ADMIN_CHAT_ID && Number.isFinite(ADMIN_CHAT_ID)) {
    try {
      await sendMessageSafe(ADMIN_CHAT_ID, text, { disable_web_page_preview: true });
      return;
    } catch {}
  }

  // 2) иначе пишем всем пользователям, кто есть в памяти
  for (const [uid] of users.entries()) {
    try {
      await sendMessageSafe(uid, text, { disable_web_page_preview: true });
    } catch {}
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

      // states:
      // awaiting_max
      // awaiting_min
      // awaiting_gift_search
      // awaiting_model_search
      // awaiting_backdrop_search
      // awaiting_sub_max:<subId>
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
  // просто 1..N в текущем порядке
  subs.forEach((s, idx) => {
    if (s) s.num = idx + 1;
  });
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
  const keys = ['bot:state:v8', 'bot:state:v7', 'bot:state:v6', 'bot:state:v5', 'bot:state:v4', 'bot:state:v3', 'bot:state:v2', 'bot:state:v1'];
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
  await redis.set('bot:state:v8', JSON.stringify(exportState()));
}

// =====================
// Portal: collections / filters
// =====================
async function portalCollections(limit = 400) {
  const now = nowMs();
  if (collectionsCache.byLowerName.size && now - collectionsCache.time < COLLECTIONS_CACHE_TTL_MS) {
    return collectionsCache;
  }
  if (!process.env.PORTAL_AUTH) {
    collectionsCache = { time: now, byLowerName: new Map() };
    return collectionsCache;
  }

  const url = `${API_URL}collections?limit=${limit}`;
  const res = await throttledPortalFetch(url, { method: 'GET', headers: portalHeaders() }).catch(() => null);
  if (!res || !res.ok) {
    collectionsCache = { time: now, byLowerName: new Map() };
    return collectionsCache;
  }

  const data = await res.json().catch(() => null);
  const arr = Array.isArray(data?.collections) ? data.collections : Array.isArray(data) ? data : [];

  const byLowerName = new Map();
  for (const c of arr) {
    const name = String(c?.name || c?.title || '').trim();
    if (!name) continue;
    byLowerName.set(name.toLowerCase(), { name, raw: c });
  }

  collectionsCache = { time: now, byLowerName };
  return collectionsCache;
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
    const key =
      Object.keys(data.collections).find((k) => k.toLowerCase() === shortName.toLowerCase()) ||
      shortName;
    block = data.collections[key] || null;
  } else if (data.floor_prices && typeof data.floor_prices === 'object') {
    const key =
      Object.keys(data.floor_prices).find((k) => k.toLowerCase() === shortName.toLowerCase()) ||
      shortName;
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
  const map = new Map(); // lower -> { name, rarity }
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
// Portal search (active lots) with paging
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

    const slug = (() => {
      if (nft?.tg_id && String(nft.tg_id).includes('-')) return String(nft.tg_id);
      const name = String(nft?.name || '').trim();
      const num = nft?.external_collection_number;
      if (!name || num == null) return null;
      const slugName = name.replace(/[^a-z0-9]+/gi, '').toLowerCase();
      if (!slugName) return null;
      return `${slugName}-${num}`;
    })();

    const urlTelegram = slug ? `https://t.me/nft/${slug}` : 'https://t.me/portals';

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
// Portal history (only used when no active lots)
// =====================
function extractActionNft(act) {
  return act?.nft || act?.item || act?.gift || act?.asset || null;
}
function extractAttrs(nft) {
  let model = nft?.model || null;
  let backdrop = nft?.backdrop || null;

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

async function portalHistoryEstimate({ collectionId, modelLower, backdropLower }) {
  if (!process.env.PORTAL_AUTH) return { ok: false, median: null, count: 0 };

  const key = `portal|${collectionId || ''}|${modelLower || ''}|${backdropLower || ''}`;
  const now = nowMs();
  const cached = historyCache.get(key);
  if (cached && now - cached.time < HISTORY_CACHE_TTL_MS) return cached;

  const prices = [];
  let page = 0;

  while (page < PORTAL_HISTORY_PAGES) {
    const offset = page * PORTAL_HISTORY_LIMIT;

    let url = `${API_URL}market/actions/?offset=${offset}&limit=${PORTAL_HISTORY_LIMIT}&action_types=buy&sort_by=created_at+desc`;
    if (collectionId) url += `&collection_ids=${encodeURIComponent(collectionId)}`;

    let res = await throttledPortalFetch(url, { method: 'GET', headers: portalHeaders() }).catch(() => null);
    if (res && !res.ok && res.status === 422) {
      url = `${API_URL}market/actions/?offset=${offset}&limit=${PORTAL_HISTORY_LIMIT}&action_types=buy`;
      res = await throttledPortalFetch(url, { method: 'GET', headers: portalHeaders() }).catch(() => null);
    }

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

      const { model, backdrop } = extractAttrs(nft);
      if (modelLower && norm(model) !== modelLower) continue;
      if (backdropLower && norm(backdrop) !== backdropLower) continue;

      const amount =
        act.amount ?? act.price ?? act.ton_amount ?? act.tonAmount ?? act.value ?? null;

      const priceTon = n(amount);
      if (!Number.isFinite(priceTon) || priceTon <= 0) continue;

      prices.push(priceTon);
    }

    if (prices.length >= 20) break;
    page++;
    await sleep(PORTAL_HISTORY_PAGE_DELAY_MS);
  }

  prices.sort((a, b) => a - b);

  const out = {
    ok: true,
    median: median(prices),
    count: prices.length,
    time: now,
  };
  historyCache.set(key, out);
  return out;
}

// =====================
// MRKT: saling (lots) with cursor
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
        raw: g,
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
// MRKT: FEED (history + events)
// =====================
async function mrktFeedFetch({ collectionName, modelName, backdropName, cursor, count, ordering = 'Latest', types = [] }) {
  const auth = mrktAuthHeader();
  if (!auth) return { ok: false, reason: 'NO_AUTH', items: [], cursor: null };

  // payload как у тебя из DevTools
  const body = {
    count: Number(count || MRKT_FEED_COUNT),
    cursor: cursor || '',
    collectionNames: collectionName ? [collectionName] : [],
    modelNames: modelName ? [modelName] : [],
    backdropNames: backdropName ? [backdropName] : [],
    lowToHigh: false,
    maxPrice: null,
    minPrice: null,
    modelNames: modelName ? [modelName] : [],
    number: null,
    ordering: ordering || 'Latest',
    query: null,
    type: Array.isArray(types) ? types : [],
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

// История продаж MRKT по фильтрам: медиана по sale
async function mrktHistorySalesEstimate({ giftLower, modelLower, backdropLower }) {
  const auth = mrktAuthHeader();
  if (!auth) return { ok: false, reason: 'NO_AUTH', median: null, count: 0 };
  if (!giftLower) return { ok: false, reason: 'NO_GIFT', median: null, count: 0 };

  const collectionName = collectionsCache.byLowerName.get(giftLower)?.name || capWords(giftLower);
  const modelName = modelLower ? capWords(modelLower) : null;
  const backdropName = backdropLower ? capWords(backdropLower) : null;

  const key = `mrkt_sales|${collectionName}|${modelName || ''}|${backdropName || ''}|target=${MRKT_HISTORY_TARGET_SALES}`;
  const now = nowMs();
  const cached = historyCache.get(key);
  if (cached && now - cached.time < HISTORY_CACHE_TTL_MS) return cached;

  let cursor = '';
  const prices = [];
  let pages = 0;

  // IMPORTANT: types=['sale'] чтобы не листать тонны listing/change_price
  while (pages < MRKT_HISTORY_MAX_PAGES && prices.length < MRKT_HISTORY_TARGET_SALES) {
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
      const amountNano = item?.amount;
      const ton = Number(amountNano) / 1e9;
      if (!Number.isFinite(ton) || ton <= 0) continue;
      prices.push(ton);
      if (prices.length >= MRKT_HISTORY_TARGET_SALES) break;
    }

    cursor = r.cursor || '';
    pages++;
    if (!cursor) break;
  }

  prices.sort((a, b) => a - b);

  const out = {
    ok: true,
    reason: 'OK',
    median: median(prices),
    count: prices.length,
    time: now,
  };

  historyCache.set(key, out);
  return out;
}

// =====================
// Notifications
// =====================
async function sendDeal(userId, gift) {
  const lines = [];
  lines.push(`Price: ${gift.priceTon.toFixed(3)} TON`);
  lines.push(`Gift: ${gift.name}`);
  if (gift.attrs?.model) lines.push(`Model: ${gift.attrs.model}`);
  if (gift.attrs?.symbol) lines.push(`Symbol: ${gift.attrs.symbol}`);
  if (gift.attrs?.backdrop) lines.push(`Backdrop: ${gift.attrs.backdrop}`);
  lines.push(`Market: ${gift.market}`);
  if (gift.urlTelegram) lines.push(gift.urlTelegram);

  const btnText = gift.market === 'Portal' ? 'Открыть Portal' : 'Открыть MRKT';
  const reply_markup = gift.urlMarket
    ? { inline_keyboard: [[{ text: btnText, url: gift.urlMarket }]] }
    : undefined;

  await sendMessageSafe(userId, lines.join('\n'), { disable_web_page_preview: true, reply_markup });
}

// =====================
// Sellprice (CLEAN): history only when no lots
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

  // Portal floor (или history fallback)
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
        const h = await portalHistoryEstimate({ collectionId: r.collectionId, modelLower, backdropLower });
        if (h.ok && h.median != null) {
          text += `Portal (история):\n  ~${h.median.toFixed(3)} TON (медиана, n=${h.count})\n`;
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

  // MRKT floor (или history fallback)
  if ((user.filters.markets || []).includes('MRKT')) {
    const r = await mrktSearchByFilters({ giftLower, modelLower, backdropLower }, null, null);

    if (r.ok && r.gifts.length) {
      const best = r.gifts[0];
      const net = best.priceTon * (1 - MRKT_FEE);
      text += `\nMRKT:\n  ~${best.priceTon.toFixed(3)} TON (минимальный активный лот)\n`;
      text += `  Комиссия ${(MRKT_FEE * 100).toFixed(1)}%: ~${net.toFixed(3)} TON чистыми\n`;
    } else if (r.ok) {
      text += `\nMRKT: активных лотов по этим фильтрам нет\n`;

      const hs = await mrktHistorySalesEstimate({ giftLower, modelLower, backdropLower });
      if (hs.ok && hs.median != null) {
        text += `MRKT (история продаж):\n  ~${hs.median.toFixed(3)} TON (медиана, n=${hs.count})\n`;
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
// Subscriptions
// =====================
function formatMarkets(markets) {
  if (!markets || !markets.length) return 'нет';
  if (markets.length === 2) return 'Portal+MRKT';
  return markets.join('+');
}

function formatSubTitle(sub) {
  const gift = collectionsCache.byLowerName.get(sub.filters.gift)?.name || capWords(sub.filters.gift);
  const model = sub.filters.model ? capWords(sub.filters.model) : 'Любая';
  const backdrop = sub.filters.backdrop ? capWords(sub.filters.backdrop) : 'Любой';
  const markets = formatMarkets(sub.filters.markets || ['Portal', 'MRKT']);
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
    return sendMessageSafe(chatId, 'Подписок нет.\nНажми: 📡 Подписки → ➕ Создать из текущих фильтров', {
      reply_markup: MAIN_KEYBOARD,
    });
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
    num: user.subscriptions.length + 1, // всегда подряд
    enabled: true,
    maxPriceTon: user.maxPriceTon ?? null,
    filters: {
      gift,
      model: user.filters.models[0] || null,
      backdrop: user.filters.backdrops[0] || null,
      markets: Array.isArray(user.filters.markets) && user.filters.markets.length ? user.filters.markets : ['Portal', 'MRKT'],
    },
  };

  return { ok: true, sub };
}

async function notifySubFloor(userId, sub, market, prevFloor, newFloor, lot) {
  const gift = collectionsCache.byLowerName.get(sub.filters.gift)?.name || capWords(sub.filters.gift);

  let text = `${gift}\n`;
  if (prevFloor == null) {
    text += `Новый флор: ${newFloor.toFixed(3)} TON\n`;
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

  const reply_markup = lot?.urlMarket
    ? { inline_keyboard: [[{ text: 'Открыть', url: lot.urlMarket }]] }
    : undefined;

  await sendMessageSafe(userId, text.trim(), { disable_web_page_preview: true, reply_markup });
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
  if (gift.modelTitle || gift.modelName) text += `Model: ${(gift.modelTitle || gift.modelName)}\n`;
  if (gift.backdropName) text += `Backdrop: ${gift.backdropName}\n`;
  if (Number.isFinite(amountTon) && amountTon > 0) text += `Amount: ${amountTon.toFixed(3)} TON\n`;

  const urlTelegram = name ? `https://t.me/nft/${name}` : 'https://t.me/mrkt';
  const urlMarket = gift.id ? mrktLotUrlFromId(gift.id) : 'https://t.me/mrkt';

  text += urlTelegram;

  const reply_markup = urlMarket
    ? { inline_keyboard: [[{ text: 'Открыть MRKT', url: urlMarket }]] }
    : undefined;

  await sendMessageSafe(userId, text.trim(), { disable_web_page_preview: true, reply_markup });
}

async function getFloorForSub(market, sub) {
  const giftLower = sub.filters.gift;
  const modelLower = sub.filters.model ? norm(sub.filters.model) : null;
  const backdropLower = sub.filters.backdrop ? norm(sub.filters.backdrop) : null;

  if (market === 'Portal') {
    const r = await portalSearchByFilters({
      giftLower,
      modelLower,
      backdropLower,
      minPriceTon: null,
      maxPriceTon: null,
    });
    if (!r.ok) return { ok: false };
    return { ok: true, lot: r.gifts[0] || null };
  }

  if (market === 'MRKT') {
    const r = await mrktSearchByFilters({ giftLower, modelLower, backdropLower }, null, null);
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
  const modelLower = sub.filters.model ? norm(sub.filters.model) : null;
  const backdropLower = sub.filters.backdrop ? norm(sub.filters.backdrop) : null;

  const collectionName = collectionsCache.byLowerName.get(giftLower)?.name || capWords(giftLower);
  const modelName = modelLower ? capWords(modelLower) : null;
  const backdropName = backdropLower ? capWords(backdropLower) : null;

  const st = subStates.get(stateKey) || { floor: null, emptyStreak: 0, lastNotifiedFloor: null, feedLastId: null };

  const r = await mrktFeedFetch({
    collectionName,
    modelName,
    backdropName,
    cursor: '',
    count: MRKT_FEED_COUNT,
    ordering: 'Latest',
    types: [], // события все
  });

  if (!r.ok) return 0;
  if (!r.items.length) return 0;

  const latestId = r.items[0]?.id || null;
  if (!latestId) return 0;

  // 1-й запуск: не спамим старьём
  if (!st.feedLastId) {
    subStates.set(stateKey, { ...st, feedLastId: latestId });
    return 0;
  }

  const newItems = [];
  for (const it of r.items) {
    if (!it || !it.id) continue;
    if (it.id === st.feedLastId) break;
    newItems.push(it);
  }

  if (!newItems.length) {
    subStates.set(stateKey, { ...st, feedLastId: latestId });
    return 0;
  }

  newItems.reverse(); // старые -> новые

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
    let processedSubs = 0;
    let floorNotifs = 0;
    let feedNotifs = 0;

    let globalFeedBudget = SUBS_MRKT_FEED_MAX_EVENTS_PER_CYCLE;

    for (const [userId, user] of users.entries()) {
      const subs = Array.isArray(user.subscriptions) ? user.subscriptions : [];
      const active = subs.filter((s) => s && s.enabled);
      if (!active.length) continue;

      for (const sub of active) {
        processedSubs++;
        if (floorNotifs >= SUBS_MAX_NOTIFICATIONS_PER_CYCLE) break;

        const markets = Array.isArray(sub.filters?.markets) && sub.filters.markets.length
          ? sub.filters.markets
          : ['Portal', 'MRKT'];

        for (const market of markets) {
          if (floorNotifs >= SUBS_MAX_NOTIFICATIONS_PER_CYCLE) break;

          const stateKey = `${userId}:${sub.id}:${market}`;
          const prevState = subStates.get(stateKey) || { floor: null, emptyStreak: 0, lastNotifiedFloor: null, feedLastId: null };

          // floor
          const r = await getFloorForSub(market, sub);
          if (!r.ok) continue;

          const lot = r.lot;
          const newFloor = lot ? lot.priceTon : null;

          let emptyStreak = prevState.emptyStreak || 0;
          if (newFloor == null) {
            emptyStreak++;
            if (emptyStreak < SUBS_EMPTY_CONFIRM) {
              subStates.set(stateKey, { ...prevState, emptyStreak });
              continue;
            }
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

          // MRKT feed events
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

// =====================
// Monitor cheap lots (burst)
// =====================
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
      const markets = user.filters.markets || ['Portal', 'MRKT'];

      const minP = user.minPriceTon != null ? Number(user.minPriceTon) : 0;
      const maxP = Number(user.maxPriceTon);

      const [portalRes, mrktRes] = await Promise.all([
        markets.includes('Portal')
          ? portalSearchByFilters({ giftLower, modelLower, backdropLower, minPriceTon: minP, maxPriceTon: maxP })
          : Promise.resolve(null),
        (markets.includes('MRKT') && mrktAuthHeader())
          ? mrktSearchByFilters({ giftLower, modelLower, backdropLower }, minP, maxP)
          : Promise.resolve(null),
      ]);

      const found = [];
      if (portalRes?.ok && portalRes.gifts?.length) found.push(...portalRes.gifts);
      if (mrktRes?.ok && mrktRes.gifts?.length) found.push(...mrktRes.gifts);

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

// =====================
// Commands
// =====================
bot.onText(/^\/start\b/, async (msg) => {
  getOrCreateUser(msg.from.id);
  await portalCollections(200).catch(() => {});
  await sendMessageSafe(msg.chat.id, 'Бот запущен.', { reply_markup: MAIN_KEYBOARD });
});

bot.onText(/^\/status\b/, async (msg) => {
  const user = getOrCreateUser(msg.from.id);
  await portalCollections(200).catch(() => {});
  const text =
    `Настройки:\n` +
    `• Мониторинг: ${user.enabled ? 'ON' : 'OFF'}\n` +
    `• Диапазон: ${(user.minPriceTon ?? 0).toFixed(3)} .. ${user.maxPriceTon != null ? user.maxPriceTon.toFixed(3) : '∞'} TON\n` +
    `• Подарок: ${user.filters.gifts[0] || 'не выбран'}\n` +
    `• Модель: ${user.filters.models[0] || 'любая'}\n` +
    `• Фон: ${user.filters.backdrops[0] || 'любой'}\n` +
    `• Маркеты: ${(user.filters.markets || []).join('+')}\n` +
    `• Подписок: ${(user.subscriptions || []).length}\n\n` +
    `API:\n` +
    `• Portal auth: ${process.env.PORTAL_AUTH ? '✅' : '❌'}\n` +
    `• MRKT auth: ${process.env.MRKT_AUTH ? '✅' : '❌'}\n` +
    `• MRKT last ok: ${mrktAuthState.lastOkAt ? new Date(mrktAuthState.lastOkAt).toLocaleString() : '-'}\n` +
    `• MRKT last fail: ${mrktAuthState.lastFailAt ? `HTTP ${mrktAuthState.lastFailCode} @ ${new Date(mrktAuthState.lastFailAt).toLocaleString()}` : '-'}\n` +
    `• Redis: ${redis ? '✅' : '❌'}\n`;
  await sendMessageSafe(msg.chat.id, text, { reply_markup: MAIN_KEYBOARD });
});

bot.onText(/^\/sellprice\b/, async (msg) => {
  const user = getOrCreateUser(msg.from.id);
  await portalCollections(200).catch(() => {});
  await sendSellPriceForUser(msg.chat.id, user);
});

// =====================
// Message handler (buttons + states)
// =====================
bot.on('message', async (msg) => {
  const userId = msg.from?.id;
  const chatId = msg.chat?.id;
  const text = msg.text;

  if (!userId || !chatId || !text) return;
  if (text.startsWith('/')) return;

  const user = getOrCreateUser(userId);
  const t = text.trim();
  const q = norm(t);

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

  if (user.state === 'awaiting_gift_search') {
    user.state = null;
    scheduleSave();

    const { byLowerName } = await portalCollections(400);
    const all = Array.from(byLowerName.values()).map((x) => x.name);
    const matched = all.filter((name) => name.toLowerCase().includes(q)).sort().slice(0, MAX_SEARCH_RESULTS);

    if (!matched.length) return sendMessageSafe(chatId, 'Ничего не нашёл по этому запросу.', { reply_markup: MAIN_KEYBOARD });

    return sendMessageSafe(chatId, 'Нашёл подарки, выбери:', {
      reply_markup: { inline_keyboard: matched.map((name) => [{ text: shorten(name, 32), callback_data: `set_gift:${name}` }]) },
    });
  }

  if (user.state === 'awaiting_model_search') {
    user.state = null;
    scheduleSave();
    if (!user.filters.gifts.length) return sendMessageSafe(chatId, 'Сначала выбери подарок.', { reply_markup: MAIN_KEYBOARD });

    const giftLower = user.filters.gifts[0];
    const col = collectionsCache.byLowerName.get(giftLower);
    const shortName = portalShortName(col?.raw);
    const f = await portalCollectionFilters(shortName);
    if (!f) return sendMessageSafe(chatId, 'Не удалось получить модели (Portal).', { reply_markup: MAIN_KEYBOARD });

    const traits = extractModelTraits(f.models);
    const matched = traits.filter((m) => String(m.name).toLowerCase().includes(q)).slice(0, MAX_SEARCH_RESULTS);
    if (!matched.length) return sendMessageSafe(chatId, 'Модель не найдена.', { reply_markup: MAIN_KEYBOARD });

    const inline_keyboard = matched.map((m) => [{
      text: `${shorten(m.name, 24)}${rarityLabelPercent(m) ? ` (${rarityLabelPercent(m)})` : ''}`,
      callback_data: `set_model:${m.name}`,
    }]);

    return sendMessageSafe(chatId, 'Выбери модель:', { reply_markup: { inline_keyboard } });
  }

  if (user.state === 'awaiting_backdrop_search') {
    user.state = null;
    scheduleSave();
    if (!user.filters.gifts.length) return sendMessageSafe(chatId, 'Сначала выбери подарок.', { reply_markup: MAIN_KEYBOARD });

    const giftLower = user.filters.gifts[0];
    const col = collectionsCache.byLowerName.get(giftLower);
    const shortName = portalShortName(col?.raw);
    const f = await portalCollectionFilters(shortName);
    if (!f) return sendMessageSafe(chatId, 'Не удалось получить фоны (Portal).', { reply_markup: MAIN_KEYBOARD });

    const backdrops = Array.isArray(f.backdrops)
      ? f.backdrops.map((x) => (typeof x === 'string' ? x : x?.name || x?.value || x?.title || '')).filter(Boolean)
      : Object.keys(f.backdrops || {});
    const matched = backdrops.filter((b) => String(b).toLowerCase().includes(q)).sort().slice(0, MAX_SEARCH_RESULTS);
    if (!matched.length) return sendMessageSafe(chatId, 'Фон не найден.', { reply_markup: MAIN_KEYBOARD });

    return sendMessageSafe(chatId, 'Выбери фон:', {
      reply_markup: { inline_keyboard: matched.map((name) => [{ text: shorten(name, 32), callback_data: `set_backdrop:${name}` }]) },
    });
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
  if (t === '🔍 Запустить поиск') {
    user.enabled = true; scheduleSave();
    return sendMessageSafe(chatId, 'Мониторинг включён.', { reply_markup: MAIN_KEYBOARD });
  }
  if (t === '⏹ Остановить поиск') {
    user.enabled = false; scheduleSave();
    return sendMessageSafe(chatId, 'Мониторинг остановлен.', { reply_markup: MAIN_KEYBOARD });
  }
  if (t === '💰 Макс. цена') {
    user.state = 'awaiting_max'; scheduleSave();
    return sendMessageSafe(chatId, 'Введи MAX TON:', { reply_markup: MAIN_KEYBOARD });
  }
  if (t === '💵 Мин. цена') {
    user.state = 'awaiting_min'; scheduleSave();
    return sendMessageSafe(chatId, 'Введи MIN TON (0 = убрать):', { reply_markup: MAIN_KEYBOARD });
  }
  if (t === '💸 Цена подарка') {
    await portalCollections(200).catch(() => {});
    return sendSellPriceForUser(chatId, user);
  }
  if (t === '📡 Подписки') return showSubsMenu(chatId);

  if (t === '📌 Статус API') {
    const text =
      `API статус:\n` +
      `• Portal auth: ${process.env.PORTAL_AUTH ? '✅' : '❌'}\n` +
      `• MRKT auth: ${process.env.MRKT_AUTH ? '✅' : '❌'}\n` +
      `• MRKT last ok: ${mrktAuthState.lastOkAt ? new Date(mrktAuthState.lastOkAt).toLocaleString() : '-'}\n` +
      `• MRKT last fail: ${mrktAuthState.lastFailAt ? `HTTP ${mrktAuthState.lastFailCode}` : '-'}\n` +
      `• Redis: ${redis ? '✅' : '❌'}\n`;
    return sendMessageSafe(chatId, text, { reply_markup: MAIN_KEYBOARD });
  }

  if (t === '🎛 Фильтры') {
    const inlineKeyboard = {
      inline_keyboard: [
        [{ text: '🎁 Выбрать подарок', callback_data: 'filter_gift' }],
        [
          { text: '🎯 Выбрать модель', callback_data: 'filter_model' },
          { text: '🎨 Выбрать фон', callback_data: 'filter_backdrop' },
        ],
        [
          { text: '🔍 Поиск подарка', callback_data: 'search_gift' },
          { text: '🔍 Поиск модели', callback_data: 'search_model' },
          { text: '🔍 Поиск фона', callback_data: 'search_backdrop' },
        ],
        [
          { text: '🅿 Только Portal', callback_data: 'set_markets:Portal' },
          { text: '🅼 Только MRKT', callback_data: 'set_markets:MRKT' },
          { text: '🅿+🅼 Оба', callback_data: 'set_markets:ALL' },
        ],
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

// =====================
// Callback handler (filters + subs)
// =====================
bot.on('callback_query', async (query) => {
  const userId = query.from.id;
  const chatId = query.message?.chat?.id;
  const data = query.data || '';
  const user = getOrCreateUser(userId);

  try {
    if (data === 'filter_gift') {
      const { byLowerName } = await portalCollections(400);
      const names = Array.from(byLowerName.values()).map((x) => x.name).sort().slice(0, 60);
      if (!names.length) {
        await sendMessageSafe(chatId, 'Portal недоступен. Проверь PORTAL_AUTH.', { reply_markup: MAIN_KEYBOARD });
      } else {
        await sendMessageSafe(chatId, 'Выбери подарок:', {
          reply_markup: { inline_keyboard: names.map((name) => [{ text: shorten(name, 32), callback_data: `set_gift:${name}` }]) },
        });
      }
    } else if (data === 'filter_model') {
      if (!user.filters.gifts.length) {
        await sendMessageSafe(chatId, 'Сначала выбери подарок.', { reply_markup: MAIN_KEYBOARD });
      } else {
        const giftLower = user.filters.gifts[0];
        const col = collectionsCache.byLowerName.get(giftLower);
        const shortName = portalShortName(col?.raw);
        const f = await portalCollectionFilters(shortName);
        if (!f) {
          await sendMessageSafe(chatId, 'Не удалось получить модели.', { reply_markup: MAIN_KEYBOARD });
        } else {
          const traits = extractModelTraits(f.models).slice(0, 80);
          const inline_keyboard = traits.map((m) => [{
            text: `${shorten(m.name, 24)}${rarityLabelPercent(m) ? ` (${rarityLabelPercent(m)})` : ''}`,
            callback_data: `set_model:${m.name}`,
          }]);
          await sendMessageSafe(chatId, 'Выбери модель:', { reply_markup: { inline_keyboard } });
        }
      }
    } else if (data === 'filter_backdrop') {
      if (!user.filters.gifts.length) {
        await sendMessageSafe(chatId, 'Сначала выбери подарок.', { reply_markup: MAIN_KEYBOARD });
      } else {
        const giftLower = user.filters.gifts[0];
        const col = collectionsCache.byLowerName.get(giftLower);
        const shortName = portalShortName(col?.raw);
        const f = await portalCollectionFilters(shortName);
        if (!f) {
          await sendMessageSafe(chatId, 'Не удалось получить фоны.', { reply_markup: MAIN_KEYBOARD });
        } else {
          const backdrops = Array.isArray(f.backdrops)
            ? f.backdrops.map((x) => (typeof x === 'string' ? x : x?.name || x?.value || x?.title || '')).filter(Boolean)
            : Object.keys(f.backdrops || {});
          const limited = backdrops.sort().slice(0, 80);
          await sendMessageSafe(chatId, 'Выбери фон:', {
            reply_markup: { inline_keyboard: limited.map((name) => [{ text: shorten(name, 32), callback_data: `set_backdrop:${name}` }]) },
          });
        }
      }
    } else if (data === 'search_gift') {
      user.state = 'awaiting_gift_search'; scheduleSave();
      await sendMessageSafe(chatId, 'Напиши часть названия подарка.', { reply_markup: MAIN_KEYBOARD });
    } else if (data === 'search_model') {
      if (!user.filters.gifts.length) return sendMessageSafe(chatId, 'Сначала выбери подарок.', { reply_markup: MAIN_KEYBOARD });
      user.state = 'awaiting_model_search'; scheduleSave();
      await sendMessageSafe(chatId, 'Напиши часть названия модели.', { reply_markup: MAIN_KEYBOARD });
    } else if (data === 'search_backdrop') {
      if (!user.filters.gifts.length) return sendMessageSafe(chatId, 'Сначала выбери подарок.', { reply_markup: MAIN_KEYBOARD });
      user.state = 'awaiting_backdrop_search'; scheduleSave();
      await sendMessageSafe(chatId, 'Напиши часть названия фона.', { reply_markup: MAIN_KEYBOARD });
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
    } else if (data.startsWith('set_gift:')) {
      const name = data.slice('set_gift:'.length).trim();
      user.filters.gifts = [name.toLowerCase()];
      user.filters.models = [];
      user.filters.backdrops = [];
      clearUserSentDeals(userId); scheduleSave();
      await sendMessageSafe(chatId, `Подарок выбран: ${name}`, { reply_markup: MAIN_KEYBOARD });
    } else if (data.startsWith('set_model:')) {
      const name = data.slice('set_model:'.length).trim();
      user.filters.models = [name.toLowerCase()];
      clearUserSentDeals(userId); scheduleSave();
      await sendMessageSafe(chatId, `Модель выбрана: ${name}`, { reply_markup: MAIN_KEYBOARD });
    } else if (data.startsWith('set_backdrop:')) {
      const name = data.slice('set_backdrop:'.length).trim();
      user.filters.backdrops = [name.toLowerCase()];
      clearUserSentDeals(userId); scheduleSave();
      await sendMessageSafe(chatId, `Фон выбран: ${name}`, { reply_markup: MAIN_KEYBOARD });
    } else if (data.startsWith('set_markets:')) {
      const v = data.split(':')[1];
      if (v === 'Portal') user.filters.markets = ['Portal'];
      else if (v === 'MRKT') user.filters.markets = ['MRKT'];
      else user.filters.markets = ['Portal', 'MRKT'];
      clearUserSentDeals(userId); scheduleSave();
      await sendMessageSafe(chatId, `Ок. Маркеты: ${(user.filters.markets || []).join('+')}`, { reply_markup: MAIN_KEYBOARD });
    }

    // subs
    else if (data === 'sub_add_current') {
      const r = makeSubFromCurrentFilters(user);
      if (!r.ok) {
        await sendMessageSafe(chatId, 'Сначала выбери подарок в фильтрах.', { reply_markup: MAIN_KEYBOARD });
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

// =====================
// Intervals + bootstrap
// =====================
setInterval(() => {
  checkMarketsForAllUsers().catch((e) => console.error('monitor interval error:', e));
}, CHECK_INTERVAL_MS);

setInterval(() => {
  checkSubscriptionsForAllUsers().catch((e) => console.error('subs interval error:', e));
}, SUBS_CHECK_INTERVAL_MS);

(async () => {
  if (REDIS_URL) {
    await initRedis();
    if (redis) await loadState();
  }
  await portalCollections(200).catch(() => {});
  console.log('Бот запущен. /start');
})();
