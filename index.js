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

// Redis (optional)
const REDIS_URL = process.env.REDIS_URL || process.env.REDIS_PRIVATE_URL || null;

// MRKT
const MRKT_API_URL = 'https://api.tgmrkt.io/api/v1';
const MRKT_AUTH = process.env.MRKT_AUTH || null;

const MRKT_COUNT = Number(process.env.MRKT_COUNT || 50);
const MRKT_PAGES = Number(process.env.MRKT_PAGES || 6);

// monitor
const CHECK_INTERVAL_MS = Number(process.env.CHECK_INTERVAL_MS || 7000);
const MAX_NOTIFICATIONS_PER_CHECK = Number(process.env.MAX_NOTIFICATIONS_PER_CHECK || 60);
const MAX_PER_MARKET = Number(process.env.MAX_PER_MARKET || 120);
const SEND_DELAY_MS = Number(process.env.SEND_DELAY_MS || 80);
const SENT_TTL_MS = Number(process.env.SENT_TTL_MS || 24 * 60 * 60 * 1000);

// timeouts / fast mode
const MRKT_TIMEOUT_MS = Number(process.env.MRKT_TIMEOUT_MS || 8000);
const MRKT_PAGES_MONITOR = Number(process.env.MRKT_PAGES_MONITOR || 1);
const ONLY_CHEAPEST_PER_CHECK = String(process.env.ONLY_CHEAPEST_PER_CHECK || '1') !== '0';

// UI
const MAX_SEARCH_RESULTS = Number(process.env.MAX_SEARCH_RESULTS || 10);

// subscriptions
const SUBS_CHECK_INTERVAL_MS = Number(process.env.SUBS_CHECK_INTERVAL_MS || 9000);
const SUBS_MAX_NOTIFICATIONS_PER_CYCLE = Number(process.env.SUBS_MAX_NOTIFICATIONS_PER_CYCLE || 8);
const SUBS_EMPTY_CONFIRM = Number(process.env.SUBS_EMPTY_CONFIRM || 2);
const SUBS_MRKT_FEED_MAX_EVENTS_PER_CYCLE = Number(process.env.SUBS_MRKT_FEED_MAX_EVENTS_PER_CYCLE || 12);

// MRKT feed/history
const MRKT_FEED_COUNT = Number(process.env.MRKT_FEED_COUNT || 80);
const MRKT_HISTORY_TARGET_SALES = Number(process.env.MRKT_HISTORY_TARGET_SALES || 60);
const MRKT_HISTORY_MAX_PAGES = Number(process.env.MRKT_HISTORY_MAX_PAGES || 240);
const MRKT_FEED_THROTTLE_MS = Number(process.env.MRKT_FEED_THROTTLE_MS || 110);
const MRKT_HISTORY_TIME_BUDGET_MS = Number(process.env.MRKT_HISTORY_TIME_BUDGET_MS || 20000);

const MRKT_FEED_NOTIFY_TYPES_RAW = String(process.env.MRKT_FEED_NOTIFY_TYPES || 'sale,listing,change_price');
const MRKT_FEED_NOTIFY_TYPES = new Set(
  MRKT_FEED_NOTIFY_TYPES_RAW.split(',').map((s) => s.trim().toLowerCase()).filter(Boolean)
);

// fees
const MRKT_FEE = Number(process.env.MRKT_FEE || 0);

// ===== AUTO BUY =====
const AUTO_BUY_GLOBAL = String(process.env.AUTO_BUY_GLOBAL || '0') === '1';
const AUTO_BUY_DRY_RUN = String(process.env.AUTO_BUY_DRY_RUN || '1') !== '0';

// автопокупка только новых listing после включения
const AUTO_BUY_ONLY_NEW_LISTINGS = String(process.env.AUTO_BUY_ONLY_NEW_LISTINGS || '1') !== '0';

// после успешной покупки — выключаем автопокупку (без дневных лимитов, просто safety)
const AUTO_BUY_DISABLE_AFTER_SUCCESS = String(process.env.AUTO_BUY_DISABLE_AFTER_SUCCESS || '1') !== '0';

// антиспам попыток
const AUTO_BUY_ATTEMPT_TTL_MS = Number(process.env.AUTO_BUY_ATTEMPT_TTL_MS || 30_000);
const AUTO_BUY_MAX_PER_CHECK = Number(process.env.AUTO_BUY_MAX_PER_CHECK || 1);

// если “нет денег” — ставим паузу
const AUTO_BUY_NO_FUNDS_PAUSE_MS = Number(process.env.AUTO_BUY_NO_FUNDS_PAUSE_MS || 10 * 60 * 1000);

// Collections list (for gift picker)
const MRKT_COLLECTIONS = String(process.env.MRKT_COLLECTIONS || '')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);

console.log('Bot version 2026-02-28-mrkt-autobuy-only-new-v1');
console.log('MODE=', MODE);
console.log('MRKT_AUTH=', MRKT_AUTH ? 'set' : 'not set');
console.log('MRKT_COLLECTIONS=', MRKT_COLLECTIONS.length ? MRKT_COLLECTIONS.length : 'not set');
console.log('AUTO_BUY_GLOBAL=', AUTO_BUY_GLOBAL);
console.log('AUTO_BUY_DRY_RUN=', AUTO_BUY_DRY_RUN);
console.log('AUTO_BUY_ONLY_NEW_LISTINGS=', AUTO_BUY_ONLY_NEW_LISTINGS);
console.log('AUTO_BUY_DISABLE_AFTER_SUCCESS=', AUTO_BUY_DISABLE_AFTER_SUCCESS);

const bot = new TelegramBot(token, { polling: true });

// =====================
// UI
// =====================
const MAIN_KEYBOARD = {
  keyboard: [
    [{ text: '🔍 Запустить поиск' }, { text: '⏹ Остановить поиск' }],
    [{ text: '💰 Макс. цена' }, { text: '💵 Мин. цена' }],
    [{ text: '💸 Цена подарка' }, { text: '📡 Подписки' }],
    [{ text: '🤖 Автопокупка' }, { text: '📌 Статус API' }],
    [{ text: '🎛 Фильтры' }],
  ],
  resize_keyboard: true,
};

// =====================
// State
// =====================
const users = new Map();
const sentDeals = new Map();
const subStates = new Map();

let isChecking = false;
let isSubsChecking = false;

const autoBuyLocks = new Set(); // userId
const autoBuyRecentAttempts = new Map(); // `${userId}:${giftId}` -> ts

const historyCache = new Map();
const HISTORY_CACHE_TTL_MS = 60_000;

const mrktAuthState = { ok: null, lastOkAt: 0, lastFailAt: 0, lastFailCode: null };

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
function shorten(s, max = 32) {
  const t = String(s || '');
  return t.length <= max ? t : t.slice(0, max - 1) + '…';
}
function normTraitName(s) {
  return norm(s).replace(/\s*\([^)]*%[^)]*\)\s*/g, ' ').replace(/\s+/g, ' ').trim();
}
function sameTrait(actual, expectedLower) {
  if (!expectedLower) return true;
  return normTraitName(actual) === normTraitName(expectedLower);
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
function mrktLotUrlFromId(id) {
  if (!id) return 'https://t.me/mrkt';
  const appId = String(id).replace(/-/g, '');
  return `https://t.me/mrkt/app?startapp=${appId}`;
}
function formatPercentFromPermille(perMille) {
  const v = Number(perMille);
  if (!Number.isFinite(v)) return '';
  const pct = v / 10;
  const s = pct % 1 === 0 ? String(pct.toFixed(0)) : String(pct.toFixed(1));
  return `${s}%`;
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

// =====================
// Redis persistence (optional)
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

      state: null, // awaiting_max|min|gift_search|model_search|backdrop_search|awaiting_sub_max:<id>

      autoBuyEnabled: false,
      autoBuyFeedLastId: null,
      autoBuyPausedUntil: 0,

      filters: { gift: '', model: '', backdrop: '' },

      subscriptions: [],

      tmp: { giftOptions: [], modelOptions: [], backdropOptions: [] },
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
      autoBuyEnabled: !!u.autoBuyEnabled,
      autoBuyFeedLastId: u.autoBuyFeedLastId || null,
      autoBuyPausedUntil: Number(u.autoBuyPausedUntil || 0),
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
      autoBuyEnabled: !!u?.autoBuyEnabled,
      autoBuyFeedLastId: u?.autoBuyFeedLastId || null,
      autoBuyPausedUntil: Number(u?.autoBuyPausedUntil || 0),
      filters: {
        gift: typeof u?.filters?.gift === 'string' ? u.filters.gift : '',
        model: typeof u?.filters?.model === 'string' ? u.filters.model : '',
        backdrop: typeof u?.filters?.backdrop === 'string' ? u.filters.backdrop : '',
      },
      subscriptions: Array.isArray(u?.subscriptions) ? u.subscriptions : [],
      tmp: { giftOptions: [], modelOptions: [], backdropOptions: [] },
    };

    for (const s of safe.subscriptions) {
      if (!s || typeof s !== 'object') continue;
      if (!s.id) s.id = `sub_${Date.now()}_${Math.random().toString(16).slice(2, 6)}`;
      if (s.enabled == null) s.enabled = true;
      if (!s.filters) s.filters = {};
      if (typeof s.filters.gift !== 'string') s.filters.gift = safe.filters.gift || '';
      if (typeof s.filters.model !== 'string') s.filters.model = safe.filters.model || '';
      if (typeof s.filters.backdrop !== 'string') s.filters.backdrop = safe.filters.backdrop || '';
      if (s.maxPriceTon != null && !Number.isFinite(Number(s.maxPriceTon))) s.maxPriceTon = null;
      if (typeof s.num !== 'number') s.num = 0;
    }

    renumberSubs(safe);
    users.set(userId, safe);
  }
}

async function loadState() {
  if (!redis) return;
  const raw = await redis.get('bot:state:mrkt:onlynew:v1');
  if (!raw) return;
  importState(JSON.parse(raw));
  console.log('Loaded state from Redis. users:', users.size);
}

let saveTimer = null;
function scheduleSave() {
  if (!redis) return;
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    saveState().catch((e) => console.error('saveState error:', e));
  }, 350);
}
async function saveState() {
  if (!redis) return;
  await redis.set('bot:state:mrkt:onlynew:v1', JSON.stringify(exportState()));
}

// =====================
// MRKT helpers
// =====================
async function markMrktFailIfAuth(statusCode) {
  mrktAuthState.ok = false;
  mrktAuthState.lastFailAt = nowMs();
  mrktAuthState.lastFailCode = statusCode;
}
function markMrktOk() {
  mrktAuthState.ok = true;
  mrktAuthState.lastOkAt = nowMs();
}

async function mrktFetchSalingPage({ collectionName, modelName, backdropName, cursor }) {
  if (!MRKT_AUTH) return { ok: false, reason: 'NO_AUTH', gifts: [], cursor: '' };

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

  const res = await fetchWithTimeout(`${MRKT_API_URL}/gifts/saling`, {
    method: 'POST',
    headers: { Authorization: MRKT_AUTH, 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify(body),
  }, MRKT_TIMEOUT_MS).catch(() => null);

  if (!res) return { ok: false, reason: 'FETCH_ERROR', gifts: [], cursor: '' };
  if (!res.ok) {
    await markMrktFailIfAuth(res.status);
    return { ok: false, reason: `HTTP_${res.status}`, gifts: [], cursor: '' };
  }
  markMrktOk();

  const data = await res.json().catch(() => null);
  const gifts = Array.isArray(data?.gifts) ? data.gifts : [];
  const nextCursor = data?.cursor || '';
  return { ok: true, reason: 'OK', gifts, cursor: nextCursor };
}

async function mrktSearchLots({ gift, model, backdrop }, minPriceTon, maxPriceTon, pagesLimit = MRKT_PAGES) {
  if (!gift) return { ok: false, reason: 'NO_GIFT', gifts: [] };

  let cursor = '';
  const out = [];

  for (let page = 0; page < pagesLimit; page++) {
    const r = await mrktFetchSalingPage({
      collectionName: gift,
      modelName: model || null,
      backdropName: backdrop || null,
      cursor,
    });
    if (!r.ok) return { ok: false, reason: r.reason, gifts: [] };

    for (const g of r.gifts) {
      const nano = (g?.salePriceWithoutFee && Number(g.salePriceWithoutFee) > 0)
        ? g.salePriceWithoutFee
        : g?.salePrice;

      if (nano == null) continue;

      const priceTon = Number(nano) / 1e9;
      if (!Number.isFinite(priceTon) || priceTon <= 0) continue;
      if (!inRange(priceTon, minPriceTon, maxPriceTon)) continue;

      const baseName = (g.collectionTitle || g.collectionName || g.title || gift).trim();
      const number = g.number ?? null;
      const displayName = number ? `${baseName} #${number}` : baseName;

      const modelName = g.modelTitle || g.modelName || '';
      const backdropName = g.backdropName || '';
      const symbolName = g.symbolName || '';

      if (model && !sameTrait(modelName, norm(model))) continue;
      if (backdrop && !sameTrait(backdropName, norm(backdrop))) continue;

      const urlTelegram = g.name && String(g.name).includes('-') ? `https://t.me/nft/${g.name}` : 'https://t.me/mrkt';
      const urlMarket = g.id ? mrktLotUrlFromId(g.id) : 'https://t.me/mrkt';

      out.push({
        id: g.id,
        market: 'MRKT',
        name: displayName,
        priceTon,
        urlTelegram,
        urlMarket,
        attrs: { model: modelName || null, backdrop: backdropName || null, symbol: symbolName || null },
        raw: g,
      });

      if (out.length >= MAX_PER_MARKET) break;
    }

    if (out.length >= MAX_PER_MARKET) break;

    cursor = r.cursor || '';
    if (!cursor) break;
  }

  out.sort((a, b) => a.priceTon - b.priceTon);
  return { ok: true, reason: 'OK', gifts: out.slice(0, MAX_PER_MARKET) };
}

async function mrktFeedFetch({ gift, model, backdrop, cursor, count, types }) {
  if (!MRKT_AUTH) return { ok: false, reason: 'NO_AUTH', items: [], cursor: '' };

  const body = {
    count: Number(count || MRKT_FEED_COUNT),
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

  const res = await fetchWithTimeout(`${MRKT_API_URL}/feed`, {
    method: 'POST',
    headers: { Authorization: MRKT_AUTH, 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify(body),
  }, MRKT_TIMEOUT_MS).catch(() => null);

  if (!res) return { ok: false, reason: 'FETCH_ERROR', items: [], cursor: '' };
  if (!res.ok) {
    await markMrktFailIfAuth(res.status);
    return { ok: false, reason: `HTTP_${res.status}`, items: [], cursor: '' };
  }
  markMrktOk();

  const data = await res.json().catch(() => null);
  const items = Array.isArray(data?.items) ? data.items : [];
  const nextCursor = data?.cursor || '';
  return { ok: true, reason: 'OK', items, cursor: nextCursor };
}

async function mrktBuy({ id, priceNano }) {
  if (!MRKT_AUTH) return { ok: false, reason: 'NO_AUTH' };

  const body = { ids: [id], prices: { [id]: priceNano } };

  const res = await fetchWithTimeout(`${MRKT_API_URL}/gifts/buy`, {
    method: 'POST',
    headers: { Authorization: MRKT_AUTH, 'Content-Type': 'application/json', Accept: 'application/json' },
    body: JSON.stringify(body),
  }, MRKT_TIMEOUT_MS).catch(() => null);

  if (!res) return { ok: false, reason: 'FETCH_ERROR' };

  const txt = await res.text().catch(() => '');
  let data = null;
  try { data = txt ? JSON.parse(txt) : null; } catch { data = null; }

  if (!res.ok) return { ok: false, reason: `HTTP_${res.status}`, data, text: txt.slice(0, 500) };

  // MRKT buy часто возвращает массив. Проверим, что покупка действительно произошла:
  const ok =
    Array.isArray(data) &&
    data.some((x) => x?.source?.type === 'buy_gift' && x?.userGift?.isMine === true);

  if (!ok) {
    return { ok: false, reason: 'BUY_NOT_CONFIRMED', data };
  }

  return { ok: true, data };
}

function isNoFundsError(r) {
  const text = JSON.stringify(r?.data || r?.text || '').toLowerCase();
  return (
    text.includes('not enough') ||
    text.includes('insufficient') ||
    text.includes('balance') ||
    text.includes('no funds') ||
    text.includes('low balance')
  );
}

// =====================
// tmp options
// =====================
function setTmpOptions(user, type, arr) {
  if (!user.tmp) user.tmp = { giftOptions: [], modelOptions: [], backdropOptions: [] };
  if (type === 'gift') user.tmp.giftOptions = arr;
  if (type === 'model') user.tmp.modelOptions = arr;
  if (type === 'backdrop') user.tmp.backdropOptions = arr;
}
function getTmpOption(user, type, idx) {
  const i = Number(idx);
  if (!Number.isFinite(i) || i < 0) return null;
  if (type === 'gift') return user.tmp?.giftOptions?.[i] ?? null;
  if (type === 'model') return user.tmp?.modelOptions?.[i] ?? null;
  if (type === 'backdrop') return user.tmp?.backdropOptions?.[i] ?? null;
  return null;
}
function buildOptionsKeyboard(type, arr, labelFn) {
  const inline_keyboard = [];
  for (let i = 0; i < arr.length; i++) {
    const label = labelFn ? labelFn(arr[i]) : String(arr[i]);
    inline_keyboard.push([{ text: shorten(label, 48), callback_data: `pick:${type}:${i}` }]);
  }
  return { inline_keyboard };
}

// =====================
// AUTO BUY (only new listings)
// =====================
async function initAutoBuyBaseline(userId, user) {
  // Берём последний listing id и сохраняем как baseline (чтобы не купить “старьё”)
  if (!AUTO_BUY_ONLY_NEW_LISTINGS) return;

  if (!user.filters.gift) {
    await sendMessageSafe(userId, 'Чтобы включить автопокупку, сначала выбери подарок в фильтрах.', { disable_web_page_preview: true });
    return;
  }

  const r = await mrktFeedFetch({
    gift: user.filters.gift,
    model: user.filters.model || null,
    backdrop: user.filters.backdrop || null,
    cursor: '',
    count: 20,
    types: ['listing'],
  });

  if (!r.ok || !r.items.length) {
    user.autoBuyFeedLastId = null;
    scheduleSave();
    return;
  }

  user.autoBuyFeedLastId = r.items[0]?.id || null;
  scheduleSave();
}

async function attemptAutoBuyFromNewListings(userId, user, minP, maxP) {
  if (!AUTO_BUY_GLOBAL) return false;
  if (!user.autoBuyEnabled) return false;
  if (AUTO_BUY_DRY_RUN == null) return false;

  if (!user.filters.gift) return false;

  const now = nowMs();
  if (user.autoBuyPausedUntil && now < user.autoBuyPausedUntil) return false;

  // lock
  if (autoBuyLocks.has(userId)) return false;
  autoBuyLocks.add(userId);

  try {
    // получаем новые listing события
    const r = await mrktFeedFetch({
      gift: user.filters.gift,
      model: user.filters.model || null,
      backdrop: user.filters.backdrop || null,
      cursor: '',
      count: MRKT_FEED_COUNT,
      types: ['listing'],
    });

    if (!r.ok || !r.items.length) return false;

    const latestId = r.items[0]?.id || null;
    if (!latestId) return false;

    // первый запуск — не покупаем старое
    if (AUTO_BUY_ONLY_NEW_LISTINGS && !user.autoBuyFeedLastId) {
      user.autoBuyFeedLastId = latestId;
      scheduleSave();
      return false;
    }

    const newItems = [];
    for (const it of r.items) {
      if (!it?.id) continue;
      if (AUTO_BUY_ONLY_NEW_LISTINGS && user.autoBuyFeedLastId && it.id === user.autoBuyFeedLastId) break;
      newItems.push(it);
      if (newItems.length >= 40) break;
    }

    if (!newItems.length) {
      user.autoBuyFeedLastId = latestId;
      scheduleSave();
      return false;
    }

    // старые -> новые
    newItems.reverse();

    let boughtCount = 0;

    for (const it of newItems) {
      if (boughtCount >= AUTO_BUY_MAX_PER_CHECK) break;

      const gift = it.gift;
      if (!gift?.id) continue;

      const priceNano = Number(it.amount ?? gift.salePrice ?? gift.salePriceWithoutFee ?? 0);
      if (!Number.isFinite(priceNano) || priceNano <= 0) continue;

      const priceTon = priceNano / 1e9;
      if (!inRange(priceTon, minP, maxP)) continue;

      // anti-repeat
      const key = `${userId}:${gift.id}`;
      const last = autoBuyRecentAttempts.get(key);
      if (last && nowMs() - last < AUTO_BUY_ATTEMPT_TTL_MS) continue;
      autoBuyRecentAttempts.set(key, nowMs());

      const title = `${gift.collectionTitle || gift.collectionName || gift.title || 'Gift'}${gift.number != null ? ` #${gift.number}` : ''}`;
      const urlTelegram = gift.name && String(gift.name).includes('-') ? `https://t.me/nft/${gift.name}` : 'https://t.me/mrkt';
      const urlMarket = mrktLotUrlFromId(gift.id);

      if (AUTO_BUY_DRY_RUN) {
        await sendMessageSafe(
          userId,
          `🤖 Автопокупка (DRY RUN)\nНовый listing:\n${title}\nЦена: ${priceTon.toFixed(3)} TON\nID: ${gift.id}\n${urlTelegram}`,
          { disable_web_page_preview: true, reply_markup: { inline_keyboard: [[{ text: 'Открыть MRKT', url: urlMarket }]] } }
        );
        boughtCount++;
        continue;
      }

      await sendMessageSafe(userId, `🤖 Покупаю (new listing): ${title} за ${priceTon.toFixed(3)} TON...`, { disable_web_page_preview: true });

      const buyRes = await mrktBuy({ id: gift.id, priceNano });

      if (buyRes.ok) {
        await sendMessageSafe(
          userId,
          `✅ Куплено!\n${title}\nЦена: ${priceTon.toFixed(3)} TON\n${urlTelegram}`,
          { disable_web_page_preview: true, reply_markup: { inline_keyboard: [[{ text: 'Открыть MRKT', url: urlMarket }]] } }
        );

        boughtCount++;

        if (AUTO_BUY_DISABLE_AFTER_SUCCESS) {
          user.autoBuyEnabled = false;
          scheduleSave();
          await sendMessageSafe(userId, `Автопокупка выключена после успешной покупки (safety).`, { disable_web_page_preview: true });
          break;
        }
      } else {
        if (isNoFundsError(buyRes)) {
          user.autoBuyEnabled = false;
          user.autoBuyPausedUntil = nowMs() + AUTO_BUY_NO_FUNDS_PAUSE_MS;
          scheduleSave();
          await sendMessageSafe(
            userId,
            `❌ Покупка не удалась: похоже, нет денег на балансе MRKT.\n` +
              `Автопокупка выключена и поставлена пауза на ${Math.round(AUTO_BUY_NO_FUNDS_PAUSE_MS / 60000)} мин.\n` +
              `Reason: ${buyRes.reason}`,
            { disable_web_page_preview: true }
          );
          break;
        }

        await sendMessageSafe(
          userId,
          `❌ Покупка не удалась: ${buyRes.reason}\n` +
            (buyRes.data?.message ? `message: ${buyRes.data.message}\n` : '') +
            (buyRes.text ? `body: ${buyRes.text}` : ''),
          { disable_web_page_preview: true }
        );
      }
    }

    // обновляем lastId на самый свежий
    user.autoBuyFeedLastId = latestId;
    scheduleSave();

    return boughtCount > 0;
  } finally {
    autoBuyLocks.delete(userId);
  }
}

// =====================
// Sellprice
// =====================
async function sendSellPriceForUser(chatId, user) {
  const gift = user.filters.gift;
  const model = user.filters.model;
  const backdrop = user.filters.backdrop;

  if (!gift) {
    return sendMessageSafe(chatId, 'Сначала выбери подарок: 🎛 Фильтры → 🎁', { reply_markup: MAIN_KEYBOARD });
  }

  let text = 'Оценка цен продажи (MRKT):\n\n';
  text += `Подарок: ${gift}\n`;
  text += `Модель: ${model || 'любая'}\n`;
  text += `Фон: ${backdrop || 'любой'}\n\n`;

  const lots = await mrktSearchLots({ gift, model, backdrop }, null, null, MRKT_PAGES);
  if (lots.ok && lots.gifts.length) {
    const best = lots.gifts[0];
    const net = best.priceTon * (1 - MRKT_FEE);
    text += `MRKT (флор): ~${best.priceTon.toFixed(3)} TON\n`;
    text += `Чистыми (комиссия ${(MRKT_FEE * 100).toFixed(1)}%): ~${net.toFixed(3)} TON\n`;
    text += `\n${best.urlTelegram}`;
  } else {
    text += `MRKT: активных лотов нет\n`;
  }

  await sendMessageSafe(chatId, text, { reply_markup: MAIN_KEYBOARD, disable_web_page_preview: false });
}

// =====================
// Deal notify
// =====================
async function sendDeal(userId, gift) {
  const lines = [];
  lines.push(`Price: ${gift.priceTon.toFixed(3)} TON`);
  lines.push(`Gift: ${gift.name}`);
  if (gift.attrs?.model) lines.push(`Model: ${gift.attrs.model}`);
  if (gift.attrs?.symbol) lines.push(`Symbol: ${gift.attrs.symbol}`);
  if (gift.attrs?.backdrop) lines.push(`Backdrop: ${gift.attrs.backdrop}`);
  lines.push(`Market: MRKT`);
  if (gift.urlTelegram) lines.push(gift.urlTelegram);

  const reply_markup = gift.urlMarket
    ? { inline_keyboard: [[{ text: 'Открыть MRKT', url: gift.urlMarket }]] }
    : undefined;

  await sendMessageSafe(userId, lines.join('\n'), { disable_web_page_preview: false, reply_markup });
}

// =====================
// Filters menu
// =====================
async function showFiltersMenu(chatId) {
  const inlineKeyboard = {
    inline_keyboard: [
      [{ text: '🎁 Выбрать подарок', callback_data: 'filter_gift' }],
      [{ text: '🔍 Поиск подарка', callback_data: 'search_gift' }],
      [{ text: '🎯 Выбрать модель (с %)', callback_data: 'filter_model' }],
      [{ text: '🔍 Поиск модели', callback_data: 'search_model' }],
      [{ text: '🎨 Выбрать фон', callback_data: 'filter_backdrop' }],
      [{ text: '🔍 Поиск фона', callback_data: 'search_backdrop' }],
      [
        { text: '♻️ Сбросить модель', callback_data: 'clear_model' },
        { text: '♻️ Сбросить фон', callback_data: 'clear_backdrop' },
      ],
      [{ text: '♻️ Сбросить всё', callback_data: 'clear_all' }],
      [{ text: 'ℹ️ Показать фильтры', callback_data: 'show_filters' }],
    ],
  };
  await sendMessageSafe(chatId, 'Фильтры MRKT:', { reply_markup: inlineKeyboard });
}

function currentFiltersText(user) {
  return (
    `Фильтры:\n` +
    `• Подарок: ${user.filters.gift || 'не выбран'}\n` +
    `• Модель: ${user.filters.model || 'любая'}\n` +
    `• Фон: ${user.filters.backdrop || 'любой'}\n` +
    `• Автопокупка (user): ${user.autoBuyEnabled ? 'ON' : 'OFF'}\n` +
    `• Автопокупка (global): ${AUTO_BUY_GLOBAL ? 'ON' : 'OFF'}\n` +
    `• DRY_RUN: ${AUTO_BUY_DRY_RUN ? 'true' : 'false'}\n` +
    `• Only new listings: ${AUTO_BUY_ONLY_NEW_LISTINGS ? 'true' : 'false'}\n`
  );
}

// =====================
// Monitor
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
      if (!user.filters.gift) continue;

      const minP = user.minPriceTon != null ? Number(user.minPriceTon) : 0;
      const maxP = Number(user.maxPriceTon);

      // 1) Автопокупка: только новые listing
      if (AUTO_BUY_GLOBAL && user.autoBuyEnabled) {
        await attemptAutoBuyFromNewListings(userId, user, minP, maxP);
      }

      // 2) Уведомления по дешёвым лотам (быстро, 1 страница)
      const lots = await mrktSearchLots(
        { gift: user.filters.gift, model: user.filters.model, backdrop: user.filters.backdrop },
        minP,
        maxP,
        MRKT_PAGES_MONITOR
      );

      if (!lots.ok || !lots.gifts.length) continue;

      const list = ONLY_CHEAPEST_PER_CHECK ? lots.gifts.slice(0, 1) : lots.gifts;

      let sent = 0;
      for (const g of list) {
        if (sent >= MAX_NOTIFICATIONS_PER_CHECK) break;

        const key = `${userId}:mrkt:${g.id}`;
        if (sentDeals.has(key)) continue;

        sentDeals.set(key, nowMs());
        await sendDeal(userId, g);
        sent++;

        if (SEND_DELAY_MS > 0) await sleep(SEND_DELAY_MS);
      }
    }
  } catch (e) {
    console.error('monitor error:', e);
  } finally {
    isChecking = false;
  }
}

// =====================
// Commands
// =====================
bot.onText(/^\/start\b/, async (msg) => {
  const user = getOrCreateUser(msg.from.id);
  await sendMessageSafe(
    msg.chat.id,
    'Бот запущен (MRKT).\n\nСначала выбери подарок: 🎛 Фильтры → 🎁',
    { reply_markup: MAIN_KEYBOARD }
  );
  if (user.autoBuyEnabled && AUTO_BUY_ONLY_NEW_LISTINGS) {
    await initAutoBuyBaseline(msg.from.id, user);
  }
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
    `• Gift: ${user.filters.gift || 'не выбран'}\n` +
    `• Model: ${user.filters.model || 'any'}\n` +
    `• Backdrop: ${user.filters.backdrop || 'any'}\n` +
    `• AutoBuy(user): ${user.autoBuyEnabled ? 'ON' : 'OFF'}\n` +
    `• AutoBuy(global): ${AUTO_BUY_GLOBAL ? 'ON' : 'OFF'}\n` +
    `• DRY_RUN: ${AUTO_BUY_DRY_RUN ? 'true' : 'false'}\n` +
    `• OnlyNewListings: ${AUTO_BUY_ONLY_NEW_LISTINGS ? 'true' : 'false'}\n` +
    `• Redis: ${redis ? '✅' : '❌'}\n\n` +
    `MRKT:\n` +
    `• MRKT_AUTH: ${MRKT_AUTH ? '✅' : '❌'}\n` +
    `• last ok: ${mrktAuthState.lastOkAt ? new Date(mrktAuthState.lastOkAt).toLocaleString() : '-'}\n` +
    `• last fail: ${mrktAuthState.lastFailAt ? `HTTP ${mrktAuthState.lastFailCode}` : '-'}\n` +
    `• MRKT_COLLECTIONS: ${MRKT_COLLECTIONS.length ? MRKT_COLLECTIONS.length : 'not set'}\n`;

  await sendMessageSafe(msg.chat.id, text, { reply_markup: MAIN_KEYBOARD });
});

// =====================
// Message handler
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

  if (user.state === 'awaiting_max') {
    user.state = null;
    const v = n(t);
    if (!Number.isFinite(v) || v <= 0) return sendMessageSafe(chatId, 'Введи MAX TON (пример: 12)', { reply_markup: MAIN_KEYBOARD });
    user.maxPriceTon = v;
    clearUserSentDeals(userId);
    scheduleSave();
    return sendMessageSafe(chatId, `Ок. MAX: ${v.toFixed(3)} TON`, { reply_markup: MAIN_KEYBOARD });
  }

  if (user.state === 'awaiting_min') {
    user.state = null;
    const v = n(t);
    if (!Number.isFinite(v) || v < 0) return sendMessageSafe(chatId, 'Введи MIN TON (0 = убрать)', { reply_markup: MAIN_KEYBOARD });
    user.minPriceTon = v === 0 ? 0 : v;
    clearUserSentDeals(userId);
    scheduleSave();
    return sendMessageSafe(chatId, `Ок. MIN: ${user.minPriceTon.toFixed(3)} TON`, { reply_markup: MAIN_KEYBOARD });
  }

  if (user.state === 'awaiting_gift_search') {
    user.state = null;
    scheduleSave();

    const src = MRKT_COLLECTIONS.length ? MRKT_COLLECTIONS : [];
    const matched = src.filter((x) => norm(x).includes(q)).slice(0, MAX_SEARCH_RESULTS);
    if (!matched.length) return sendMessageSafe(chatId, 'Ничего не нашёл. Проверь MRKT_COLLECTIONS.', { reply_markup: MAIN_KEYBOARD });

    setTmpOptions(user, 'gift', matched);
    return sendMessageSafe(chatId, 'Нашёл подарки, выбери:', { reply_markup: buildOptionsKeyboard('gift', matched) });
  }

  if (user.state === 'awaiting_model_search') {
    user.state = null;
    scheduleSave();
    if (!user.filters.gift) return sendMessageSafe(chatId, 'Сначала выбери подарок.', { reply_markup: MAIN_KEYBOARD });

    const r = await (async () => {
      const body = { collections: [user.filters.gift] };
      const res = await fetchWithTimeout(`${MRKT_API_URL}/gifts/models`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        body: JSON.stringify(body),
      }, MRKT_TIMEOUT_MS).catch(() => null);
      if (!res || !res.ok) return { ok: false, models: [] };
      const data = await res.json().catch(() => null);
      if (!Array.isArray(data)) return { ok: false, models: [] };
      const map = new Map();
      for (const it of data) {
        const name = it.modelTitle || it.modelName;
        if (!name) continue;
        const rarityPerMille = it.rarityPerMille ?? null;
        if (!map.has(name)) map.set(name, { name, rarityPerMille });
      }
      const arr = Array.from(map.values());
      arr.sort((a, b) => {
        const ra = a.rarityPerMille == null ? Infinity : Number(a.rarityPerMille);
        const rb = b.rarityPerMille == null ? Infinity : Number(b.rarityPerMille);
        if (ra !== rb) return ra - rb;
        return a.name.localeCompare(b.name);
      });
      return { ok: true, models: arr };
    })();

    if (!r.ok || !r.models.length) return sendMessageSafe(chatId, 'Не удалось получить модели.', { reply_markup: MAIN_KEYBOARD });

    const matched = r.models.filter((m) => norm(m.name).includes(q)).slice(0, MAX_SEARCH_RESULTS);
    if (!matched.length) return sendMessageSafe(chatId, 'Модель не найдена.', { reply_markup: MAIN_KEYBOARD });

    setTmpOptions(user, 'model', matched);
    return sendMessageSafe(chatId, 'Нашёл модели, выбери:', {
      reply_markup: buildOptionsKeyboard('model', matched, (m) => {
        const pct = m.rarityPerMille != null ? formatPercentFromPermille(m.rarityPerMille) : '';
        return pct ? `${m.name} (${pct})` : m.name;
      }),
    });
  }

  if (user.state === 'awaiting_backdrop_search') {
    user.state = null;
    scheduleSave();
    if (!user.filters.gift) return sendMessageSafe(chatId, 'Сначала выбери подарок.', { reply_markup: MAIN_KEYBOARD });

    const list = await (async () => {
      const uniq = new Map();
      let cursor = '';
      for (let page = 0; page < Math.min(4, MRKT_PAGES); page++) {
        const r = await mrktFetchSalingPage({ collectionName: user.filters.gift, modelName: user.filters.model || null, backdropName: null, cursor });
        if (!r.ok) break;
        for (const g of r.gifts) {
          const b = g.backdropName;
          if (!b) continue;
          const key = normTraitName(b);
          if (!uniq.has(key)) uniq.set(key, b);
          if (uniq.size >= 60) break;
        }
        cursor = r.cursor || '';
        if (!cursor || uniq.size >= 60) break;
      }
      return Array.from(uniq.values()).sort((a, b) => a.localeCompare(b));
    })();

    const matched = list.filter((b) => norm(b).includes(q)).slice(0, MAX_SEARCH_RESULTS);
    if (!matched.length) return sendMessageSafe(chatId, 'Фон не найден (по текущим лотам).', { reply_markup: MAIN_KEYBOARD });

    setTmpOptions(user, 'backdrop', matched);
    return sendMessageSafe(chatId, 'Нашёл фоны, выбери:', { reply_markup: buildOptionsKeyboard('backdrop', matched) });
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
  if (t === '💸 Цена подарка') return sendSellPriceForUser(chatId, user);

  if (t === '🤖 Автопокупка') {
    user.autoBuyEnabled = !user.autoBuyEnabled;
    if (user.autoBuyEnabled) {
      user.autoBuyPausedUntil = 0;
      if (AUTO_BUY_ONLY_NEW_LISTINGS) {
        await initAutoBuyBaseline(userId, user);
      }
    }
    scheduleSave();
    return sendMessageSafe(chatId, currentFiltersText(user), { reply_markup: MAIN_KEYBOARD });
  }

  if (t === '📌 Статус API') {
    return sendMessageSafe(
      chatId,
      `API статус:\n` +
        `• MRKT_AUTH: ${MRKT_AUTH ? '✅' : '❌'}\n` +
        `• last ok: ${mrktAuthState.lastOkAt ? new Date(mrktAuthState.lastOkAt).toLocaleString() : '-'}\n` +
        `• Redis: ${redis ? '✅' : '❌'}\n` +
        `• AUTO_BUY_GLOBAL: ${AUTO_BUY_GLOBAL ? 'ON' : 'OFF'}\n` +
        `• AUTO_BUY_DRY_RUN: ${AUTO_BUY_DRY_RUN ? 'true' : 'false'}`,
      { reply_markup: MAIN_KEYBOARD }
    );
  }

  if (t === '📡 Подписки') {
    return sendMessageSafe(chatId, 'Подписки оставим на следующем шаге (если нужно), сейчас главный фокус — автопокупка.', { reply_markup: MAIN_KEYBOARD });
  }

  if (t === '🎛 Фильтры') return showFiltersMenu(chatId);

  return sendMessageSafe(chatId, 'Используй кнопки снизу.', { reply_markup: MAIN_KEYBOARD });
});

// =====================
// Callback handler
// =====================
bot.on('callback_query', async (q) => {
  const userId = q.from.id;
  const chatId = q.message?.chat?.id;
  const data = q.data || '';
  const user = getOrCreateUser(userId);

  try {
    if (data === 'filter_gift') {
      if (!MRKT_COLLECTIONS.length) {
        await sendMessageSafe(chatId, 'MRKT_COLLECTIONS не задан. Добавь список коллекций в Railway.', { reply_markup: MAIN_KEYBOARD });
      } else {
        const list = MRKT_COLLECTIONS.slice(0, 80);
        setTmpOptions(user, 'gift', list);
        await sendMessageSafe(chatId, 'Выбери подарок:', { reply_markup: buildOptionsKeyboard('gift', list) });
      }
    } else if (data === 'search_gift') {
      user.state = 'awaiting_gift_search';
      scheduleSave();
      await sendMessageSafe(chatId, 'Напиши часть названия подарка:', { reply_markup: MAIN_KEYBOARD });
    } else if (data === 'filter_model') {
      if (!user.filters.gift) {
        await sendMessageSafe(chatId, 'Сначала выбери подарок.', { reply_markup: MAIN_KEYBOARD });
      } else {
        const r = await (async () => {
          const body = { collections: [user.filters.gift] };
          const res = await fetchWithTimeout(`${MRKT_API_URL}/gifts/models`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
            body: JSON.stringify(body),
          }, MRKT_TIMEOUT_MS).catch(() => null);
          if (!res || !res.ok) return { ok: false, models: [] };
          const data = await res.json().catch(() => null);
          if (!Array.isArray(data)) return { ok: false, models: [] };
          const map = new Map();
          for (const it of data) {
            const name = it.modelTitle || it.modelName;
            if (!name) continue;
            const rarityPerMille = it.rarityPerMille ?? null;
            if (!map.has(name)) map.set(name, { name, rarityPerMille });
          }
          const arr = Array.from(map.values());
          arr.sort((a, b) => {
            const ra = a.rarityPerMille == null ? Infinity : Number(a.rarityPerMille);
            const rb = b.rarityPerMille == null ? Infinity : Number(b.rarityPerMille);
            if (ra !== rb) return ra - rb;
            return a.name.localeCompare(b.name);
          });
          return { ok: true, models: arr };
        })();

        if (!r.ok || !r.models.length) {
          await sendMessageSafe(chatId, 'Не удалось получить модели (MRKT /gifts/models).', { reply_markup: MAIN_KEYBOARD });
        } else {
          const list = r.models.slice(0, 80);
          setTmpOptions(user, 'model', list);
          await sendMessageSafe(chatId, 'Выбери модель:', {
            reply_markup: buildOptionsKeyboard('model', list, (m) => {
              const pct = m.rarityPerMille != null ? formatPercentFromPermille(m.rarityPerMille) : '';
              return pct ? `${m.name} (${pct})` : m.name;
            }),
          });
        }
      }
    } else if (data === 'search_model') {
      user.state = 'awaiting_model_search';
      scheduleSave();
      await sendMessageSafe(chatId, 'Напиши часть названия модели:', { reply_markup: MAIN_KEYBOARD });
    } else if (data === 'filter_backdrop') {
      if (!user.filters.gift) {
        await sendMessageSafe(chatId, 'Сначала выбери подарок.', { reply_markup: MAIN_KEYBOARD });
      } else {
        const list = await (async () => {
          const uniq = new Map();
          let cursor = '';
          for (let page = 0; page < Math.min(4, MRKT_PAGES); page++) {
            const r = await mrktFetchSalingPage({ collectionName: user.filters.gift, modelName: user.filters.model || null, backdropName: null, cursor });
            if (!r.ok) break;
            for (const g of r.gifts) {
              const b = g.backdropName;
              if (!b) continue;
              const key = normTraitName(b);
              if (!uniq.has(key)) uniq.set(key, b);
              if (uniq.size >= 60) break;
            }
            cursor = r.cursor || '';
            if (!cursor || uniq.size >= 60) break;
          }
          return Array.from(uniq.values()).sort((a, b) => a.localeCompare(b));
        })();

        if (!list.length) {
          await sendMessageSafe(chatId, 'Не смог найти фоны по текущим лотам (попробуй без модели).', { reply_markup: MAIN_KEYBOARD });
        } else {
          const limited = list.slice(0, 80);
          setTmpOptions(user, 'backdrop', limited);
          await sendMessageSafe(chatId, 'Выбери фон:', { reply_markup: buildOptionsKeyboard('backdrop', limited) });
        }
      }
    } else if (data === 'search_backdrop') {
      user.state = 'awaiting_backdrop_search';
      scheduleSave();
      await sendMessageSafe(chatId, 'Напиши часть названия фона:', { reply_markup: MAIN_KEYBOARD });
    } else if (data === 'clear_model') {
      user.filters.model = '';
      clearUserSentDeals(userId);
      scheduleSave();
      await sendMessageSafe(chatId, 'Модель сброшена.', { reply_markup: MAIN_KEYBOARD });
    } else if (data === 'clear_backdrop') {
      user.filters.backdrop = '';
      clearUserSentDeals(userId);
      scheduleSave();
      await sendMessageSafe(chatId, 'Фон сброшен.', { reply_markup: MAIN_KEYBOARD });
    } else if (data === 'clear_all') {
      user.filters.gift = '';
      user.filters.model = '';
      user.filters.backdrop = '';
      clearUserSentDeals(userId);
      scheduleSave();
      await sendMessageSafe(chatId, 'Фильтры сброшены.', { reply_markup: MAIN_KEYBOARD });
    } else if (data === 'show_filters') {
      await sendMessageSafe(chatId, currentFiltersText(user), { reply_markup: MAIN_KEYBOARD });
    } else if (data.startsWith('pick:')) {
      const [, type, idx] = data.split(':');
      const picked = getTmpOption(user, type, idx);

      if (!picked) {
        await sendMessageSafe(chatId, 'Выбор устарел. Открой меню фильтров ещё раз.', { reply_markup: MAIN_KEYBOARD });
      } else {
        if (type === 'gift') {
          user.filters.gift = String(picked);
          user.filters.model = '';
          user.filters.backdrop = '';
          clearUserSentDeals(userId);
          scheduleSave();

          // если автопокупка включена — обновим baseline
          if (user.autoBuyEnabled && AUTO_BUY_ONLY_NEW_LISTINGS) {
            user.autoBuyFeedLastId = null;
            await initAutoBuyBaseline(userId, user);
          }

          await sendMessageSafe(chatId, `Подарок выбран: ${picked}`, { reply_markup: MAIN_KEYBOARD });
        } else if (type === 'model') {
          user.filters.model = typeof picked === 'string' ? picked : (picked.name || '');
          clearUserSentDeals(userId);
          scheduleSave();

          if (user.autoBuyEnabled && AUTO_BUY_ONLY_NEW_LISTINGS) {
            user.autoBuyFeedLastId = null;
            await initAutoBuyBaseline(userId, user);
          }

          await sendMessageSafe(chatId, `Модель выбрана: ${user.filters.model}`, { reply_markup: MAIN_KEYBOARD });
        } else if (type === 'backdrop') {
          user.filters.backdrop = String(picked);
          clearUserSentDeals(userId);
          scheduleSave();

          if (user.autoBuyEnabled && AUTO_BUY_ONLY_NEW_LISTINGS) {
            user.autoBuyFeedLastId = null;
            await initAutoBuyBaseline(userId, user);
          }

          await sendMessageSafe(chatId, `Фон выбран: ${picked}`, { reply_markup: MAIN_KEYBOARD });
        }
      }
    }
  } catch (e) {
    console.error('callback_query error:', e);
  }

  bot.answerCallbackQuery(q.id).catch(() => {});
});

// =====================
// Intervals + bootstrap
// =====================
setInterval(() => {
  checkMarketsForAllUsers().catch((e) => console.error('monitor error:', e));
}, CHECK_INTERVAL_MS);

setInterval(() => {
  // пока подписки не трогаем здесь, оставим как основу
  // (можно включить, если хочешь — скажешь)
}, SUBS_CHECK_INTERVAL_MS);

(async () => {
  if (REDIS_URL) {
    await initRedis();
    if (redis) await loadState();
  }
  console.log('Бот запущен. /start');
})();
