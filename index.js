const TelegramBot = require('node-telegram-bot-api');
const fs = require('fs');
const path = require('path');

const token = process.env.TELEGRAM_TOKEN;
if (!token) {
  console.error('TELEGRAM_TOKEN not set');
  process.exit(1);
}

const MODE = process.env.MODE || 'real';

// дешёвые лоты по maxPriceTon пользователя
const CHECK_INTERVAL_MS = Number(process.env.CHECK_INTERVAL_MS || 12000);
const MAX_NOTIFICATIONS_PER_CHECK = Number(process.env.MAX_NOTIFICATIONS_PER_CHECK || 3);
const SENT_TTL_MS = Number(process.env.SENT_TTL_MS || 24 * 60 * 60 * 1000);

// подписки (флор + изменение цены)
const SUBS_CHECK_INTERVAL_MS = Number(process.env.SUBS_CHECK_INTERVAL_MS || 30000);
const SUBS_MAX_NOTIFICATIONS_PER_CYCLE = Number(process.env.SUBS_MAX_NOTIFICATIONS_PER_CYCLE || 6);
const SUBS_EMPTY_CONFIRM = Number(process.env.SUBS_EMPTY_CONFIRM || 2); // сколько раз подряд “нет лотов”, чтобы считать что лоты реально пропали

// persistence
const DATA_FILE = process.env.DATA_FILE || './data/state.json';

// UI
const MAX_SEARCH_RESULTS = Number(process.env.MAX_SEARCH_RESULTS || 10);

// Portal
const PORTAL_FEE = Number(process.env.PORTAL_FEE || 0.05);
const PORTAL_HISTORY_LIMIT = Number(process.env.PORTAL_HISTORY_LIMIT || 100);
const PORTAL_HISTORY_PAGES = Number(process.env.PORTAL_HISTORY_PAGES || 10);
const PORTAL_HISTORY_PAGE_DELAY_MS = Number(process.env.PORTAL_HISTORY_PAGE_DELAY_MS || 350);

const PORTAL_LOT_URL_TEMPLATE =
  process.env.PORTAL_LOT_URL_TEMPLATE ||
  'https://t.me/portals_market_bot/market?startapp=gift_{id}';

// Portal premarket_status enum
const VALID_PORTAL_PREMARKET = new Set([
  'all',
  'only_premarket',
  'without_premarket',
  'draft',
  'listed',
  'sold',
]);

const PORTAL_PREMARKET_STATUS_RAW = String(
  process.env.PORTAL_PREMARKET_STATUS || 'without_premarket'
).trim();

const PORTAL_PREMARKET_STATUS = VALID_PORTAL_PREMARKET.has(PORTAL_PREMARKET_STATUS_RAW)
  ? PORTAL_PREMARKET_STATUS_RAW
  : 'without_premarket';

// MRKT
const MRKT_FEE = Number(process.env.MRKT_FEE || 0);
const MRKT_API_URL = 'https://api.tgmrkt.io/api/v1';

// Portal endpoints
const API_URL = 'https://portal-market.com/api/';
const SORT_PRICE_ASC = '&sort_by=price+asc';

console.log('Bot version 2026-02-18-subs-persist-max-v1');
console.log('MODE =', MODE);
console.log('CHECK_INTERVAL_MS =', CHECK_INTERVAL_MS);
console.log('SUBS_CHECK_INTERVAL_MS =', SUBS_CHECK_INTERVAL_MS);
console.log('DATA_FILE =', DATA_FILE);
console.log('PORTAL_PREMARKET_STATUS =', PORTAL_PREMARKET_STATUS);

const bot = new TelegramBot(token, { polling: true });

const MAIN_KEYBOARD = {
  keyboard: [
    [{ text: '🔍 Запустить поиск' }, { text: '⏹ Остановить поиск' }],
    [{ text: '💰 Установить цену' }, { text: '💸 Цена подарка' }],
    [{ text: '🎛 Фильтры' }, { text: '📡 Подписки' }],
    [{ text: '📌 Статус API' }],
  ],
  resize_keyboard: true,
};

// ============ state ============
const users = new Map(); // userId -> settings
const sentDeals = new Map(); // key -> ts
const subStates = new Map(); // `${userId}:${subId}:${market}` -> { floor, emptyStreak, lastNotifiedFloor, lastNotifiedAt }

let isChecking = false;
let isSubsChecking = false;

// caches
let collectionsCache = { time: 0, byLowerName: new Map() };
const COLLECTIONS_CACHE_TTL_MS = 10 * 60_000;

const filtersCache = new Map(); // shortName -> { time, data }
const FILTERS_CACHE_TTL_MS = 5 * 60_000;

const historyCache = new Map(); // key -> { time, median, count, note }
const HISTORY_CACHE_TTL_MS = 30_000;

// persistence debounce
let saveTimer = null;
function ensureDirForFile(filePath) {
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}
function scheduleSave() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    saveState().catch((e) => console.error('saveState error', e));
  }, 500);
}

async function loadState() {
  try {
    if (!fs.existsSync(DATA_FILE)) return;
    const raw = fs.readFileSync(DATA_FILE, 'utf8');
    const parsed = JSON.parse(raw);

    const objUsers = parsed?.users && typeof parsed.users === 'object' ? parsed.users : {};
    for (const [idStr, u] of Object.entries(objUsers)) {
      const userId = Number(idStr);
      if (!Number.isFinite(userId)) continue;

      const safe = {
        enabled: u?.enabled !== false,
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

      // миграция старых подписок: если у подписки нет maxPriceTon, подставим текущий user.maxPriceTon (это то, чего ты ожидал)
      for (const s of safe.subscriptions) {
        if (s && typeof s === 'object') {
          if (s.maxPriceTon == null && safe.maxPriceTon != null) s.maxPriceTon = safe.maxPriceTon;
          if (s.enabled == null) s.enabled = true;
          if (!s.id) s.id = makeId();
          if (!s.filters) s.filters = {};
          if (!Array.isArray(s.filters.markets)) s.filters.markets = ['Portal', 'MRKT'];
        }
      }

      users.set(userId, safe);
    }

    console.log('Loaded state users:', users.size);
  } catch (e) {
    console.error('loadState error:', e);
  }
}

async function saveState() {
  try {
    ensureDirForFile(DATA_FILE);

    const out = { users: {} };
    for (const [userId, u] of users.entries()) {
      out.users[String(userId)] = {
        enabled: !!u.enabled,
        maxPriceTon: typeof u.maxPriceTon === 'number' ? u.maxPriceTon : null,
        filters: u.filters,
        subscriptions: u.subscriptions || [],
      };
    }

    const tmp = DATA_FILE + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(out, null, 2), 'utf8');
    fs.renameSync(tmp, DATA_FILE);
  } catch (e) {
    console.error('saveState error:', e);
  }
}

// ============ helpers ============
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
  return String(str || '').replace(/\w+(?:'\w+)?/g, (w) => w[0].toUpperCase() + w.slice(1));
}
function quotePlus(str) {
  return encodeURIComponent(str).replace(/%20/g, '+');
}
function shorten(s, max = 32) {
  const t = String(s || '');
  return t.length <= max ? t : t.slice(0, max - 1) + '…';
}
function buildPortalLotUrl(id) {
  if (!id) return 'https://t.me/portals';
  return PORTAL_LOT_URL_TEMPLATE.replace('{id}', encodeURIComponent(String(id)));
}
function percentChange(oldV, newV) {
  if (!oldV || !Number.isFinite(oldV) || oldV <= 0) return null;
  return ((newV - oldV) / oldV) * 100;
}

function portalHeaders() {
  const auth = process.env.PORTAL_AUTH;
  return {
    ...(auth ? { Authorization: auth } : {}),
    Accept: 'application/json, text/plain, */*',
    Origin: 'https://portal-market.com',
    Referer: 'https://portal-market.com/',
  };
}

function getOrCreateUser(userId) {
  if (!users.has(userId)) {
    users.set(userId, {
      enabled: true,
      maxPriceTon: null,
      state: null, // awaiting_max_price | awaiting_gift_search | awaiting_model_search | awaiting_backdrop_search | awaiting_sub_max:<id>
      filters: { gifts: [], models: [], backdrops: [], markets: ['Portal', 'MRKT'] },
      subscriptions: [],
    });
  }
  return users.get(userId);
}

function clearUserSentDeals(userId) {
  const prefix = `${userId}:`;
  for (const k of Array.from(sentDeals.keys())) {
    if (k.startsWith(prefix)) sentDeals.delete(k);
  }
}

function pruneSentDeals() {
  const now = nowMs();
  for (const [k, ts] of sentDeals.entries()) {
    if (now - ts > SENT_TTL_MS) sentDeals.delete(k);
  }
}

function makeId() {
  return `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 7)}`;
}

function prettyGiftName(lower) {
  const hit = collectionsCache.byLowerName.get(lower);
  return hit?.name || capWords(lower);
}
function prettyModelName(lower) {
  return capWords(lower);
}
function prettyBackdropName(lower) {
  return capWords(lower);
}

// ====== RARITY: проценты только у моделей ======
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
    const key = name.toLowerCase();
    if (!map.has(key)) map.set(key, { name, rarity: rarity ?? null });
    else {
      const prev = map.get(key);
      if (prev && prev.rarity == null && rarity != null) prev.rarity = rarity;
    }
  };

  if (!block) return [];
  if (Array.isArray(block)) {
    for (const item of block) {
      if (!item) continue;
      if (typeof item === 'string') {
        const name = item.trim();
        if (name) push(name, null);
      } else {
        const name = String(item.name || item.model || item.value || item.title || '').trim();
        if (!name) continue;
        push(name, extractRarityValue(item));
      }
    }
  } else if (typeof block === 'object') {
    for (const [k, v] of Object.entries(block)) {
      const name = String(k).trim();
      if (!name) continue;
      push(name, extractRarityValue(v));
    }
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

// ====== PORTAL: collections/filters ======
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
  const res = await fetch(url, { method: 'GET', headers: portalHeaders() }).catch(() => null);
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
  const res = await fetch(url, { method: 'GET', headers: portalHeaders() }).catch(() => null);
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

function portalTgSlug(nft) {
  if (nft?.tg_id && String(nft.tg_id).includes('-')) return String(nft.tg_id);
  const name = String(nft?.name || '').trim();
  const number = nft?.external_collection_number;
  if (!name || number == null) return null;
  const slugName = name.replace(/[^a-z0-9]+/gi, '').toLowerCase();
  if (!slugName) return null;
  return `${slugName}-${number}`;
}

// ====== PORTAL: search (реальные лоты: price) ======
async function portalSearchByFilters({ giftLower, modelLower, backdropLower, maxPriceTon = null, limit = 50 }) {
  if (!process.env.PORTAL_AUTH) return { ok: false, reason: 'NO_AUTH', gifts: [], collectionId: null };
  if (!giftLower) return { ok: false, reason: 'NO_GIFT', gifts: [], collectionId: null };

  const { byLowerName } = await portalCollections(400);
  const col = byLowerName.get(giftLower);
  const giftName = col?.name || giftLower;
  const collectionId = portalCollectionId(col?.raw);

  let url = `${API_URL}nfts/search?offset=0&limit=${limit}${SORT_PRICE_ASC}`;
  if (maxPriceTon != null && Number.isFinite(maxPriceTon)) {
    url += `&min_price=0&max_price=${Number(maxPriceTon)}`;
  }

  if (collectionId) url += `&collection_ids=${encodeURIComponent(collectionId)}`;
  else url += `&filter_by_collections=${quotePlus(capWords(giftName))}`;

  if (modelLower) url += `&filter_by_models=${quotePlus(capWords(modelLower))}`;
  if (backdropLower) url += `&filter_by_backdrops=${quotePlus(capWords(backdropLower))}`;

  url += `&status=listed&exclude_bundled=true&premarket_status=${encodeURIComponent(PORTAL_PREMARKET_STATUS)}`;

  const res = await fetch(url, { method: 'GET', headers: portalHeaders() }).catch(() => null);
  if (!res) return { ok: false, reason: 'FETCH_ERROR', gifts: [], collectionId };

  if (!res.ok) {
    const reason = res.status === 401 || res.status === 403 ? 'AUTH_EXPIRED' : res.status === 429 ? 'RATE_LIMIT' : `HTTP_${res.status}`;
    return { ok: false, reason, gifts: [], collectionId };
  }

  const data = await res.json().catch(() => null);
  const results = Array.isArray(data?.results) ? data.results : Array.isArray(data) ? data : [];

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

    const slug = portalTgSlug(nft);
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
    });
  }

  gifts.sort((a, b) => a.priceTon - b.priceTon);
  return { ok: true, reason: 'OK', gifts, collectionId };
}

// ====== MRKT: search ======
async function mrktSearchByFilters({ giftLower, modelLower, backdropLower, maxPriceTon = null }) {
  const token = process.env.MRKT_AUTH;
  if (!token) return { ok: false, reason: 'NO_AUTH', gifts: [] };
  if (!giftLower) return { ok: false, reason: 'NO_GIFT', gifts: [] };

  const body = {
    count: 30,
    cursor: '',
    collectionNames: [capWords(giftLower)],
    modelNames: modelLower ? [capWords(modelLower)] : [],
    backdropNames: backdropLower ? [capWords(backdropLower)] : [],
    symbolNames: [],
    ordering: 'None',
    lowToHigh: true,
    maxPrice: maxPriceTon ?? null,
    minPrice: null,
  };

  const res = await fetch(`${MRKT_API_URL}/gifts/saling`, {
    method: 'POST',
    headers: {
      Authorization: token,
      'Content-Type': 'application/json',
      Accept: 'application/json',
    },
    body: JSON.stringify(body),
  }).catch(() => null);

  if (!res) return { ok: false, reason: 'FETCH_ERROR', gifts: [] };
  if (!res.ok) {
    const reason = res.status === 401 || res.status === 403 ? 'AUTH_EXPIRED' : `HTTP_${res.status}`;
    return { ok: false, reason, gifts: [] };
  }

  const data = await res.json().catch(() => null);
  const arr = Array.isArray(data?.gifts) ? data.gifts : [];

  const out = [];
  for (const g of arr) {
    const nano = g?.salePrice ?? g?.salePriceWithoutFee ?? null;
    if (nano == null) continue;

    const priceTon = Number(nano) / 1e9;
    if (!Number.isFinite(priceTon) || priceTon <= 0) continue;

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

    out.push({
      id: `mrkt_${g.id || g.name || displayName}`,
      market: 'MRKT',
      name: displayName,
      baseName,
      priceTon,
      urlTelegram,
      urlMarket: 'https://t.me/mrkt',
      attrs: { model, backdrop, symbol },
    });
  }

  out.sort((a, b) => a.priceTon - b.priceTon);
  return { ok: true, reason: 'OK', gifts: out };
}

// ====== send deal ======
async function sendDeal(userId, gift) {
  const lines = [];
  lines.push(`Price: ${gift.priceTon.toFixed(3)} TON`);
  lines.push(`Gift: ${gift.name}`);
  if (gift.attrs?.model) lines.push(`Model: ${gift.attrs.model}`);
  if (gift.attrs?.symbol) lines.push(`Symbol: ${gift.attrs.symbol}`);
  if (gift.attrs?.backdrop) lines.push(`Backdrop: ${gift.attrs.backdrop}`);
  lines.push(`Market: ${gift.market}`);
  if (gift.urlTelegram) lines.push(gift.urlTelegram);

  const btnText = gift.market === 'Portal' ? 'Открыть Portal' : `Открыть ${gift.market}`;
  const reply_markup = gift.urlMarket
    ? { inline_keyboard: [[{ text: btnText, url: gift.urlMarket }]] }
    : undefined;

  await bot.sendMessage(userId, lines.join('\n'), {
    disable_web_page_preview: false,
    reply_markup,
  });
}

// ====== subscriptions UI ======
function formatSubLine(sub) {
  const gift = prettyGiftName(sub.filters.gift);
  const model = sub.filters.model ? prettyModelName(sub.filters.model) : 'Любая';
  const backdrop = sub.filters.backdrop ? prettyBackdropName(sub.filters.backdrop) : 'Любой';
  const markets = (sub.filters.markets || []).join('+') || 'Portal+MRKT';
  const max = sub.maxPriceTon != null ? `${Number(sub.maxPriceTon).toFixed(3)} TON` : '—';
  return `Gift: ${gift}\nModel: ${model}\nBackdrop: ${backdrop}\nMarkets: ${markets}\nMax: ${max}`;
}

async function showSubs(chatId, user) {
  const subs = user.subscriptions || [];
  if (!subs.length) {
    return bot.sendMessage(chatId, 'Подписок нет. Создай: 📡 Подписки → ➕', { reply_markup: MAIN_KEYBOARD });
  }

  let text = 'Мои подписки:\n\n';
  subs.forEach((s, i) => {
    text += `${i + 1}) ${s.enabled ? 'ON' : 'OFF'}  id=${s.id}\n${formatSubLine(s)}\n\n`;
  });
  text = text.length > 3800 ? text.slice(0, 3800) + '\n...' : text;

  const inline_keyboard = subs.slice(0, 20).map((s) => ([
    { text: s.enabled ? `⏸ ${s.id}` : `▶️ ${s.id}`, callback_data: `sub_toggle:${s.id}` },
    { text: `💰 max`, callback_data: `sub_setmax:${s.id}` },
    { text: `🗑`, callback_data: `sub_delete:${s.id}` },
  ]));

  await bot.sendMessage(chatId, text, { reply_markup: { inline_keyboard } });
}

// ====== monitoring: cheap lots ======
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

      const found = [];

      if (markets.includes('Portal')) {
        const r = await portalSearchByFilters({ giftLower, modelLower, backdropLower, maxPriceTon: user.maxPriceTon, limit: 50 });
        if (r.ok && r.gifts.length) found.push(...r.gifts);
      }

      if (markets.includes('MRKT') && process.env.MRKT_AUTH) {
        const r = await mrktSearchByFilters({ giftLower, modelLower, backdropLower, maxPriceTon: user.maxPriceTon });
        if (r.ok && r.gifts.length) found.push(...r.gifts);
      }

      found.sort((a, b) => a.priceTon - b.priceTon);

      let sent = 0;
      for (const gift of found) {
        if (sent >= MAX_NOTIFICATIONS_PER_CHECK) break;
        if (gift.priceTon > user.maxPriceTon) continue;

        const key = `${userId}:${gift.id}`;
        if (sentDeals.has(key)) continue;

        sentDeals.set(key, nowMs());
        await sendDeal(userId, gift);
        sent++;
      }
    }
  } catch (e) {
    console.error('checkMarketsForAllUsers error:', e);
  } finally {
    isChecking = false;
  }
}

// ====== subscriptions logic ======
async function notifySub(userId, sub, market, prevFloor, newFloor, lot) {
  const gift = prettyGiftName(sub.filters.gift);
  const model = sub.filters.model ? prettyModelName(sub.filters.model) : null;
  const backdrop = sub.filters.backdrop ? prettyBackdropName(sub.filters.backdrop) : null;

  const maxLine = sub.maxPriceTon != null ? `Max: ${Number(sub.maxPriceTon).toFixed(3)} TON\n` : '';

  let text = `${gift}\n`;
  if (prevFloor == null) {
    text += `Новый лот: ${newFloor.toFixed(3)} TON\n`;
  } else {
    const pct = percentChange(prevFloor, newFloor);
    const pctTxt = pct == null ? '' : ` (${pct.toFixed(1)}%)`;
    text += `Изменение цены: ${Number(prevFloor).toFixed(3)} -> ${newFloor.toFixed(3)} TON${pctTxt}\n`;
  }

  if (model) text += `Model: ${model}\n`;
  if (backdrop) text += `Backdrop: ${backdrop}\n`;
  text += `Market: ${market}\n`;
  text += maxLine;
  if (lot?.urlTelegram) text += lot.urlTelegram;

  const reply_markup = lot?.urlMarket
    ? { inline_keyboard: [[{ text: 'Открыть', url: lot.urlMarket }]] }
    : undefined;

  await bot.sendMessage(userId, text.trim(), {
    disable_web_page_preview: false,
    reply_markup,
  });
}

async function checkSubscriptionsForAllUsers() {
  if (MODE !== 'real') return;
  if (isSubsChecking) return;

  isSubsChecking = true;
  try {
    let sent = 0;

    for (const [userId, user] of users.entries()) {
      const subs = Array.isArray(user.subscriptions) ? user.subscriptions : [];
      const active = subs.filter((s) => s && s.enabled);

      if (!active.length) continue;

      for (const sub of active) {
        if (sent >= SUBS_MAX_NOTIFICATIONS_PER_CYCLE) return;

        // миграция старых подписок “на лету”
        if (sub.maxPriceTon == null && user.maxPriceTon != null) {
          sub.maxPriceTon = user.maxPriceTon;
          scheduleSave();
        }

        const markets = Array.isArray(sub.filters?.markets) && sub.filters.markets.length
          ? sub.filters.markets
          : ['Portal', 'MRKT'];

        for (const market of markets) {
          if (sent >= SUBS_MAX_NOTIFICATIONS_PER_CYCLE) return;

          if (market === 'Portal' && !process.env.PORTAL_AUTH) continue;
          if (market === 'MRKT' && !process.env.MRKT_AUTH) continue;

          const giftLower = sub.filters.gift;
          const modelLower = sub.filters.model || null;
          const backdropLower = sub.filters.backdrop || null;
          const max = sub.maxPriceTon != null ? Number(sub.maxPriceTon) : null;

          const stateKey = `${userId}:${sub.id}:${market}`;
          const prevState = subStates.get(stateKey) || {
            floor: null,
            emptyStreak: 0,
            lastNotifiedFloor: null,
            lastNotifiedAt: 0,
          };

          let rOk = false;
          let lot = null;

          if (market === 'Portal') {
            const r = await portalSearchByFilters({ giftLower, modelLower, backdropLower, maxPriceTon: null, limit: 50 });
            rOk = r.ok;
            lot = r.ok ? (r.gifts[0] || null) : null;
            await sleep(200); // бережно к Portal
          } else if (market === 'MRKT') {
            const r = await mrktSearchByFilters({ giftLower, modelLower, backdropLower, maxPriceTon: null });
            rOk = r.ok;
            lot = r.ok ? (r.gifts[0] || null) : null;
          }

          // если запрос не ок — не портим состояние, чтобы не было повторов
          if (!rOk) {
            continue;
          }

          // применяем maxPrice к подписке: выше лимита — не уведомляем, но floor сохраняем (чтобы поймать падение ниже)
          let newFloor = lot ? lot.priceTon : null;
          if (newFloor != null && max != null && newFloor > max) {
            // не уведомляем
          }

          // empty streak логика: чтобы не было “то есть, то нет” из-за флапа
          let emptyStreak = prevState.emptyStreak || 0;
          if (newFloor == null) {
            emptyStreak++;
            if (emptyStreak < SUBS_EMPTY_CONFIRM) {
              // пока не считаем пропажу лотов реальной
              subStates.set(stateKey, { ...prevState, emptyStreak });
              continue;
            }
            // подтверждённо пусто
            newFloor = null;
          } else {
            emptyStreak = 0;
          }

          const prevFloor = prevState.floor;

          // не уведомляем выше max
          const canNotify = (newFloor != null) && (max == null || newFloor <= max);

          // NEW LOT
          if (prevFloor == null && newFloor != null && canNotify) {
            // защита от дубля: не шлём если уже шлали такой floor недавно
            if (prevState.lastNotifiedFloor == null || Number(prevState.lastNotifiedFloor) !== Number(newFloor)) {
              await notifySub(userId, sub, market, null, newFloor, lot);
              sent++;

              subStates.set(stateKey, {
                floor: newFloor,
                emptyStreak,
                lastNotifiedFloor: newFloor,
                lastNotifiedAt: nowMs(),
              });
              continue;
            }
          }

          // PRICE CHANGE
          if (prevFloor != null && newFloor != null && Number(prevFloor) !== Number(newFloor) && canNotify) {
            await notifySub(userId, sub, market, prevFloor, newFloor, lot);
            sent++;

            subStates.set(stateKey, {
              floor: newFloor,
              emptyStreak,
              lastNotifiedFloor: newFloor,
              lastNotifiedAt: nowMs(),
            });
            continue;
          }

          // просто обновляем состояние (floor сохраняем даже если выше max)
          subStates.set(stateKey, {
            floor: newFloor,
            emptyStreak,
            lastNotifiedFloor: prevState.lastNotifiedFloor,
            lastNotifiedAt: prevState.lastNotifiedAt,
          });
        }
      }
    }
  } catch (e) {
    console.error('checkSubscriptionsForAllUsers error:', e);
  } finally {
    isSubsChecking = false;
  }
}

// ============ Commands ============
bot.onText(/^\/start\b/, (msg) => {
  getOrCreateUser(msg.from.id);
  bot.sendMessage(msg.chat.id, 'Бот запущен (Portal + MRKT).', { reply_markup: MAIN_KEYBOARD });
});

bot.onText(/^\/setmaxprice\b(?:\s+(.+))?/, (msg, match) => {
  const user = getOrCreateUser(msg.from.id);
  const arg = match[1];
  if (!arg) return bot.sendMessage(msg.chat.id, 'Пример: /setmaxprice 12', { reply_markup: MAIN_KEYBOARD });

  const v = n(arg);
  if (!Number.isFinite(v) || v <= 0) return bot.sendMessage(msg.chat.id, 'Некорректно. Пример: /setmaxprice 7.5');
  user.maxPriceTon = v;
  user.state = null;
  clearUserSentDeals(msg.from.id);
  scheduleSave();

  bot.sendMessage(msg.chat.id, `Ок. Макс. цена (для мониторинга и подписок по умолчанию): ${v.toFixed(3)} TON`, { reply_markup: MAIN_KEYBOARD });
});

bot.onText(/^\/status\b/, async (msg) => {
  const user = getOrCreateUser(msg.from.id);
  await portalCollections(200).catch(() => {});

  const portalAuth = process.env.PORTAL_AUTH ? '✅' : '❌';
  const mrktAuth = process.env.MRKT_AUTH ? '✅' : '❌';

  const text =
    `Статус:\n` +
    `• Мониторинг: ${user.enabled ? 'включён' : 'выключен'}\n` +
    `• Макс. цена: ${user.maxPriceTon ? user.maxPriceTon.toFixed(3) : 'не задана'} TON\n` +
    `• Подарок: ${user.filters.gifts[0] || 'не выбран'}\n` +
    `• Модель: ${user.filters.models[0] || 'любая'}\n` +
    `• Фон: ${user.filters.backdrops[0] || 'любой'}\n` +
    `• Подписок: ${(user.subscriptions || []).length}\n\n` +
    `API:\n• Portal auth: ${portalAuth}\n• MRKT auth: ${mrktAuth}\n`;

  bot.sendMessage(msg.chat.id, text, { reply_markup: MAIN_KEYBOARD });
});

// ============ Buttons + callbacks ============
bot.on('message', async (msg) => {
  const userId = msg.from?.id;
  const chatId = msg.chat?.id;
  const text = msg.text;
  if (!userId || !chatId || !text) return;
  if (text.startsWith('/')) return;

  const user = getOrCreateUser(userId);
  const t = text.trim();

  // awaiting max price
  if (user.state === 'awaiting_max_price') {
    const v = n(t);
    if (!Number.isFinite(v) || v <= 0) return bot.sendMessage(chatId, 'Введи число TON. Пример: 12', { reply_markup: MAIN_KEYBOARD });
    user.maxPriceTon = v;
    user.state = null;
    clearUserSentDeals(userId);
    scheduleSave();
    return bot.sendMessage(chatId, `Ок. Макс. цена: ${v.toFixed(3)} TON`, { reply_markup: MAIN_KEYBOARD });
  }

  // awaiting sub max
  if (typeof user.state === 'string' && user.state.startsWith('awaiting_sub_max:')) {
    const subId = user.state.split(':')[1];
    const v = n(t);
    if (!Number.isFinite(v) || v <= 0) return bot.sendMessage(chatId, 'Введи число TON. Пример: 12', { reply_markup: MAIN_KEYBOARD });

    const sub = (user.subscriptions || []).find((s) => s.id === subId);
    if (!sub) {
      user.state = null;
      return bot.sendMessage(chatId, 'Подписка не найдена.', { reply_markup: MAIN_KEYBOARD });
    }
    sub.maxPriceTon = v;
    user.state = null;
    scheduleSave();
    return bot.sendMessage(chatId, `Ок. Для подписки ${subId} max = ${v.toFixed(3)} TON`, { reply_markup: MAIN_KEYBOARD });
  }

  // buttons
  if (t === '💰 Установить цену') {
    user.state = 'awaiting_max_price';
    return bot.sendMessage(chatId, 'Введи максимальную цену (TON). Пример: 12', { reply_markup: MAIN_KEYBOARD });
  }
  if (t === '🔍 Запустить поиск') {
    user.enabled = true;
    scheduleSave();
    return bot.sendMessage(chatId, 'Мониторинг включён.', { reply_markup: MAIN_KEYBOARD });
  }
  if (t === '⏹ Остановить поиск') {
    user.enabled = false;
    scheduleSave();
    return bot.sendMessage(chatId, 'Мониторинг остановлен.', { reply_markup: MAIN_KEYBOARD });
  }
  if (t === '📌 Статус API') {
    const portalAuth = process.env.PORTAL_AUTH ? '✅' : '❌';
    const mrktAuth = process.env.MRKT_AUTH ? '✅' : '❌';
    return bot.sendMessage(chatId, `API статус:\nPortal auth: ${portalAuth}\nMRKT auth: ${mrktAuth}`, { reply_markup: MAIN_KEYBOARD });
  }
  if (t === '📡 Подписки') {
    const inlineKeyboard = {
      inline_keyboard: [
        [{ text: '➕ Создать из текущих фильтров', callback_data: 'sub_add_current' }],
        [{ text: '📄 Мои подписки', callback_data: 'sub_list' }],
      ],
    };
    return bot.sendMessage(chatId, 'Подписки:', { reply_markup: inlineKeyboard });
  }

  // fallback
  return bot.sendMessage(chatId, 'Ок. Используй кнопки снизу.', { reply_markup: MAIN_KEYBOARD });
});

bot.on('callback_query', async (query) => {
  const userId = query.from.id;
  const chatId = query.message?.chat?.id;
  const data = query.data || '';
  const user = getOrCreateUser(userId);

  try {
    if (data === 'sub_add_current') {
      if (!user.filters.gifts.length) {
        await bot.sendMessage(chatId, 'Сначала выбери подарок в фильтрах.', { reply_markup: MAIN_KEYBOARD });
      } else {
        const sub = {
          id: makeId(),
          enabled: true,
          createdAt: new Date().toISOString(),
          filters: {
            gift: user.filters.gifts[0],
            model: user.filters.models[0] || null,
            backdrop: user.filters.backdrops[0] || null,
            markets: [...(user.filters.markets || ['Portal', 'MRKT'])],
          },
          // ВАЖНО: maxPriceTon подписки берём из текущего /setmaxprice
          maxPriceTon: user.maxPriceTon != null ? Number(user.maxPriceTon) : null,
        };

        user.subscriptions.push(sub);
        scheduleSave();

        await bot.sendMessage(
          chatId,
          `Подписка создана (id=${sub.id}):\n\n${formatSubLine(sub)}`,
          { reply_markup: MAIN_KEYBOARD }
        );
      }
    } else if (data === 'sub_list') {
      await showSubs(chatId, user);
    } else if (data.startsWith('sub_toggle:')) {
      const id = data.split(':')[1];
      const sub = (user.subscriptions || []).find((s) => s.id === id);
      if (!sub) await bot.sendMessage(chatId, 'Подписка не найдена.', { reply_markup: MAIN_KEYBOARD });
      else {
        sub.enabled = !sub.enabled;
        scheduleSave();
        await bot.sendMessage(chatId, `Подписка ${id}: ${sub.enabled ? 'ВКЛ' : 'ВЫКЛ'}`, { reply_markup: MAIN_KEYBOARD });
      }
    } else if (data.startsWith('sub_delete:')) {
      const id = data.split(':')[1];
      const before = (user.subscriptions || []).length;
      user.subscriptions = (user.subscriptions || []).filter((s) => s.id !== id);
      scheduleSave();
      await bot.sendMessage(chatId, before !== user.subscriptions.length ? `Удалил подписку ${id}.` : 'Подписка не найдена.', { reply_markup: MAIN_KEYBOARD });
    } else if (data.startsWith('sub_setmax:')) {
      const id = data.split(':')[1];
      const sub = (user.subscriptions || []).find((s) => s.id === id);
      if (!sub) {
        await bot.sendMessage(chatId, 'Подписка не найдена.', { reply_markup: MAIN_KEYBOARD });
      } else {
        user.state = `awaiting_sub_max:${id}`;
        await bot.sendMessage(chatId, `Введи max цену TON для подписки ${id}.\nПример: 12`, { reply_markup: MAIN_KEYBOARD });
      }
    }
  } catch (e) {
    console.error('callback_query error:', e);
  }

  bot.answerCallbackQuery(query.id).catch(() => {});
});

// ============ intervals ============
setInterval(() => {
  checkMarketsForAllUsers().catch((e) => console.error('monitor interval error:', e));
}, CHECK_INTERVAL_MS);

setInterval(() => {
  checkSubscriptionsForAllUsers().catch((e) => console.error('subs interval error:', e));
}, SUBS_CHECK_INTERVAL_MS);

// load persisted state and warm cache
(async () => {
  await loadState();
  try { await portalCollections(200); } catch {}
})();

console.log('Бот запущен. /start');