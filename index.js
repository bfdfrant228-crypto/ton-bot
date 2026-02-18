const TelegramBot = require('node-telegram-bot-api');

const token = process.env.TELEGRAM_TOKEN;
if (!token) {
  console.error('TELEGRAM_TOKEN not set');
  process.exit(1);
}

const MODE = process.env.MODE || 'real';

// дешёвые лоты по maxPriceTon
const CHECK_INTERVAL_MS = Number(process.env.CHECK_INTERVAL_MS || 12000);
const MAX_NOTIFICATIONS_PER_CHECK = Number(process.env.MAX_NOTIFICATIONS_PER_CHECK || 3);

// подписки (флор + изменение цены)
const SUBS_CHECK_INTERVAL_MS = Number(process.env.SUBS_CHECK_INTERVAL_MS || 30000);
const SUBS_MAX_NOTIFICATIONS_PER_CYCLE = Number(process.env.SUBS_MAX_NOTIFICATIONS_PER_CYCLE || 5);

// Portal
const PORTAL_FEE = Number(process.env.PORTAL_FEE || 0.05);
const PORTAL_HISTORY_LIMIT = Number(process.env.PORTAL_HISTORY_LIMIT || 100);
const PORTAL_HISTORY_PAGES = Number(process.env.PORTAL_HISTORY_PAGES || 10);
const PORTAL_HISTORY_PAGE_DELAY_MS = Number(process.env.PORTAL_HISTORY_PAGE_DELAY_MS || 350);

const PORTAL_LOT_URL_TEMPLATE =
  process.env.PORTAL_LOT_URL_TEMPLATE ||
  'https://t.me/portals_market_bot/market?startapp=gift_{id}';

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

console.log('Bot version 2026-02-18-subs-compact-v1');
console.log('MODE =', MODE);
console.log('CHECK_INTERVAL_MS =', CHECK_INTERVAL_MS);
console.log('SUBS_CHECK_INTERVAL_MS =', SUBS_CHECK_INTERVAL_MS);
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

// userId -> settings
const users = new Map();

// anti-spam дешёвых лотов
const sentDeals = new Map(); // key -> ts
const SENT_TTL_MS = Number(process.env.SENT_TTL_MS || 24 * 60 * 60 * 1000);

// подписки: состояние флора
const subStates = new Map(); // `${userId}:${subId}:${market}` -> { floor, ts }

let isChecking = false;
let isSubsChecking = false;

// caches
let collectionsCache = { time: 0, byLowerName: new Map() };
const COLLECTIONS_CACHE_TTL_MS = 10 * 60_000;

const filtersCache = new Map(); // shortName -> { time, data }
const FILTERS_CACHE_TTL_MS = 5 * 60_000;

const historyCache = new Map(); // key -> { time, median, count, note }
const HISTORY_CACHE_TTL_MS = 30_000;

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
function listToURL(list) {
  return list.map((s) => quotePlus(capWords(s))).join('%2C');
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
      state: null, // awaiting_max_price | awaiting_gift_search | awaiting_model_search | awaiting_backdrop_search
      filters: { gifts: [], models: [], backdrops: [], markets: ['Portal', 'MRKT'] },
      subscriptions: [], // {id, enabled, createdAt, filters:{gift,model,backdrop,markets}}
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
  return `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`;
}

// ====== RARITY: проценты ТОЛЬКО у моделей ======
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

// ====== PORTAL: search (floor lot) ======
async function portalSearchByFilters({ giftLower, modelLower, backdropLower, maxPriceTon = null, limit = 50 }) {
  if (!process.env.PORTAL_AUTH) return { ok: false, reason: 'NO_AUTH', gifts: [], collectionId: null, giftName: null };
  if (!giftLower) return { ok: false, reason: 'NO_GIFT', gifts: [], collectionId: null, giftName: null };

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
  if (!res) return { ok: false, reason: 'FETCH_ERROR', gifts: [], collectionId, giftName };

  if (!res.ok) {
    const reason = res.status === 401 || res.status === 403 ? 'AUTH_EXPIRED' : res.status === 429 ? 'RATE_LIMIT' : `HTTP_${res.status}`;
    return { ok: false, reason, gifts: [], collectionId, giftName };
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

    // strict filter protection
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
  return { ok: true, reason: 'OK', gifts, collectionId, giftName };
}

// ====== PORTAL: history median with pagination ======
async function portalHistoryMedian({ collectionId, modelLower, backdropLower }) {
  if (!process.env.PORTAL_AUTH) return { ok: false, reason: 'NO_AUTH', median: null, count: 0 };

  const key = `${collectionId || ''}|${modelLower || ''}|${backdropLower || ''}`;
  const now = nowMs();
  const cached = historyCache.get(key);
  if (cached && now - cached.time < HISTORY_CACHE_TTL_MS) return cached;

  const prices = [];
  let page = 0;

  while (page < PORTAL_HISTORY_PAGES) {
    const offset = page * PORTAL_HISTORY_LIMIT;

    // пробуем с фильтрами в query
    let url = `${API_URL}market/actions/?offset=${offset}&limit=${PORTAL_HISTORY_LIMIT}&action_types=buy`;
    if (collectionId) url += `&collection_ids=${encodeURIComponent(collectionId)}`;
    if (modelLower) url += `&filter_by_models=${quotePlus(capWords(modelLower))}`;
    if (backdropLower) url += `&filter_by_backdrops=${quotePlus(capWords(backdropLower))}`;

    let res = await fetch(url, { method: 'GET', headers: portalHeaders() }).catch(() => null);
    if (!res) break;

    if (!res.ok && res.status === 422) {
      // fallback: без query-фильтров
      url = `${API_URL}market/actions/?offset=${offset}&limit=${PORTAL_HISTORY_LIMIT}&action_types=buy`;
      res = await fetch(url, { method: 'GET', headers: portalHeaders() }).catch(() => null);
    }

    if (!res || !res.ok) break;

    const data = await res.json().catch(() => null);
    const actions = Array.isArray(data?.actions) ? data.actions : [];
    if (!actions.length) break;

    for (const act of actions) {
      const t = String(act?.type || act?.action_type || act?.actionType || '').toLowerCase();
      if (!['buy', 'purchase'].includes(t)) continue;

      const nft = act?.nft || act?.item || act?.gift;
      if (!nft) continue;
      if (collectionId && nft.collection_id !== collectionId) continue;

      let m = null, b = null;
      if (Array.isArray(nft.attributes)) {
        for (const a of nft.attributes) {
          if (!a?.type) continue;
          if (a.type === 'model') m = a.value;
          else if (a.type === 'backdrop') b = a.value;
        }
      }

      if (modelLower && norm(m) !== modelLower) continue;
      if (backdropLower && norm(b) !== backdropLower) continue;

      const amount = act.amount ?? act.price ?? act.ton_amount ?? act.tonAmount;
      const priceTon = n(amount);
      if (!Number.isFinite(priceTon) || priceTon <= 0) continue;

      prices.push(priceTon);
    }

    if (prices.length >= 15) break;

    page++;
    await sleep(PORTAL_HISTORY_PAGE_DELAY_MS);
  }

  prices.sort((a, b) => a - b);
  let median = null;
  if (prices.length) {
    const L = prices.length;
    median = L % 2 ? prices[(L - 1) / 2] : (prices[L / 2 - 1] + prices[L / 2]) / 2;
  }

  const out = {
    ok: true,
    reason: 'OK',
    median,
    count: prices.length,
    note: `pages_scanned=${Math.min(page + 1, PORTAL_HISTORY_PAGES)}`,
    time: now,
  };

  historyCache.set(key, out);
  return out;
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

// ====== send deal (как раньше) ======
async function sendDeal(userId, gift) {
  const lines = [];
  lines.push(`Price: ${gift.priceTon.toFixed(3)} TON`);
  lines.push(`Gift: ${gift.name}`);
  if (gift.attrs?.model) lines.push(`Model: ${gift.attrs.model}`);
  if (gift.attrs?.symbol) lines.push(`Symbol: ${gift.attrs.symbol}`);
  if (gift.attrs?.backdrop) lines.push(`Backdrop: ${gift.attrs.backdrop}`);
  lines.push(`Market: ${gift.market}`);
  if (gift.urlTelegram) lines.push(gift.urlTelegram); // последняя строка -> превью

  const btnText = gift.market === 'Portal' ? 'Открыть Portal' : `Открыть ${gift.market}`;
  const reply_markup = gift.urlMarket
    ? { inline_keyboard: [[{ text: btnText, url: gift.urlMarket }]] }
    : undefined;

  await bot.sendMessage(userId, lines.join('\n'), {
    disable_web_page_preview: false,
    reply_markup,
  });
}

// ====== sellprice ======
async function sendSellPriceForUser(chatId, user) {
  if (!user.filters.gifts.length) {
    return bot.sendMessage(chatId, 'Сначала выбери подарок: 🎛 Фильтры → 🎁', { reply_markup: MAIN_KEYBOARD });
  }

  const giftLower = user.filters.gifts[0];
  const modelLower = user.filters.models[0] || null;
  const backdropLower = user.filters.backdrops[0] || null;

  let text = 'Оценка цен продажи:\n\n';
  text += `Подарок: ${giftLower}\n`;
  text += `Модель: ${modelLower || 'любая'}\n`;
  text += `Фон: ${backdropLower || 'любой'}\n\n`;

  // Portal floor
  if ((user.filters.markets || []).includes('Portal')) {
    const r = await portalSearchByFilters({ giftLower, modelLower, backdropLower, maxPriceTon: null, limit: 50 });
    if (r.ok && r.gifts.length) {
      const best = r.gifts[0];
      const net = best.priceTon * (1 - PORTAL_FEE);
      text += `Portal:\n  ~${best.priceTon.toFixed(3)} TON (минимальный активный лот)\n`;
      text += `  Чистыми после комиссии ${(PORTAL_FEE * 100).toFixed(1)}%: ~${net.toFixed(3)} TON\n`;
    } else if (r.ok) {
      text += 'Portal: активных лотов по этим фильтрам нет\n';
      if (r.collectionId) {
        const h = await portalHistoryMedian({ collectionId: r.collectionId, modelLower, backdropLower });
        if (h.ok && h.median != null) {
          text += `Portal (история продаж):\n  ~${h.median.toFixed(3)} TON (медиана, выборка: ${h.count}; ${h.note})\n`;
        } else {
          text += 'Portal (история продаж): нет данных по этим фильтрам\n';
        }
      } else {
        text += 'Portal (история продаж): не могу посчитать (нет collection_id)\n';
      }
    } else {
      text += `Portal: ошибка (${r.reason})\n`;
    }
  }

  // MRKT floor
  if ((user.filters.markets || []).includes('MRKT')) {
    const r = await mrktSearchByFilters({ giftLower, modelLower, backdropLower, maxPriceTon: null });
    if (r.ok && r.gifts.length) {
      const best = r.gifts[0];
      const net = best.priceTon * (1 - MRKT_FEE);
      text += `\nMRKT:\n  ~${best.priceTon.toFixed(3)} TON (минимальный активный лот)\n`;
      text += `  Комиссия ${(MRKT_FEE * 100).toFixed(1)}%: ~${net.toFixed(3)} TON чистыми\n`;
    } else if (r.ok) {
      text += '\nMRKT: активных лотов по этим фильтрам нет\n';
    } else {
      text += `\nMRKT: ошибка (${r.reason})\n`;
    }
  }

  await bot.sendMessage(chatId, text, { reply_markup: MAIN_KEYBOARD });
}

// ====== subscriptions ======
function subTitle(sub) {
  const parts = [];
  parts.push(sub.filters.gift || '(gift?)');
  if (sub.filters.model) parts.push(`model=${sub.filters.model}`);
  if (sub.filters.backdrop) parts.push(`backdrop=${sub.filters.backdrop}`);
  parts.push(`markets=${(sub.filters.markets || []).join('+')}`);
  return parts.join(' | ');
}

async function checkSubscriptionsForAllUsers() {
  if (MODE !== 'real') return;
  if (isSubsChecking) return;

  isSubsChecking = true;
  try {
    let sent = 0;

    for (const [userId, user] of users.entries()) {
      const subs = Array.isArray(user.subscriptions) ? user.subscriptions : [];
      const active = subs.filter((s) => s.enabled);
      if (!active.length) continue;

      for (const sub of active) {
        if (sent >= SUBS_MAX_NOTIFICATIONS_PER_CYCLE) return;

        const markets = sub.filters.markets || ['Portal', 'MRKT'];
        for (const market of markets) {
          if (sent >= SUBS_MAX_NOTIFICATIONS_PER_CYCLE) return;

          if (market === 'Portal' && !process.env.PORTAL_AUTH) continue;
          if (market === 'MRKT' && !process.env.MRKT_AUTH) continue;

          const stateKey = `${userId}:${sub.id}:${market}`;
          const prev = subStates.get(stateKey) || { floor: null };

          let lot = null;
          if (market === 'Portal') {
            const r = await portalSearchByFilters({
              giftLower: sub.filters.gift,
              modelLower: sub.filters.model,
              backdropLower: sub.filters.backdrop,
              maxPriceTon: null,
              limit: 50,
            });
            lot = r.ok ? (r.gifts[0] || null) : null;
            await sleep(250); // чуть бережнее к Portal
          } else if (market === 'MRKT') {
            const r = await mrktSearchByFilters({
              giftLower: sub.filters.gift,
              modelLower: sub.filters.model,
              backdropLower: sub.filters.backdrop,
              maxPriceTon: null,
            });
            lot = r.ok ? (r.gifts[0] || null) : null;
          }

          const newFloor = lot ? lot.priceTon : null;

          // новый лот
          if (prev.floor == null && newFloor != null) {
            const text =
              `${sub.filters.gift}(${sub.filters.gift})\n` +
              `Новый лот: ${newFloor.toFixed(3)} TON\n` +
              (sub.filters.model ? `- Model: ${capWords(sub.filters.model)}\n` : '') +
              (sub.filters.backdrop ? `- Backdrop: ${capWords(sub.filters.backdrop)}\n` : '') +
              `Market: ${market}\n` +
              (lot?.urlTelegram ? lot.urlTelegram : '');

            await bot.sendMessage(userId, text.trim(), {
              disable_web_page_preview: false,
              reply_markup: lot?.urlMarket ? { inline_keyboard: [[{ text: 'Открыть', url: lot.urlMarket }]] } : undefined,
            });
            sent++;
            subStates.set(stateKey, { floor: newFloor, ts: nowMs() });
            continue;
          }

          // изменение цены
          if (prev.floor != null && newFloor != null && Number(prev.floor) !== Number(newFloor)) {
            const pct = percentChange(prev.floor, newFloor);
            const pctTxt = pct == null ? '' : ` (${pct.toFixed(1)}%)`;

            const text =
              `${sub.filters.gift}(${sub.filters.gift})\n` +
              `Изменение цены: ${Number(prev.floor).toFixed(3)} -> ${newFloor.toFixed(3)} TON${pctTxt}\n` +
              (sub.filters.model ? `- Model: ${capWords(sub.filters.model)}\n` : '') +
              (sub.filters.backdrop ? `- Backdrop: ${capWords(sub.filters.backdrop)}\n` : '') +
              `Market: ${market}\n` +
              (lot?.urlTelegram ? lot.urlTelegram : '');

            await bot.sendMessage(userId, text.trim(), {
              disable_web_page_preview: false,
              reply_markup: lot?.urlMarket ? { inline_keyboard: [[{ text: 'Открыть', url: lot.urlMarket }]] } : undefined,
            });
            sent++;
            subStates.set(stateKey, { floor: newFloor, ts: nowMs() });
            continue;
          }

          subStates.set(stateKey, { floor: newFloor, ts: nowMs() });
        }
      }
    }
  } finally {
    isSubsChecking = false;
  }
}

// ====== cheap-lots monitor ======
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
        const r = await portalSearchByFilters({
          giftLower,
          modelLower,
          backdropLower,
          maxPriceTon: user.maxPriceTon,
          limit: 50,
        });
        if (r.ok && r.gifts.length) found.push(...r.gifts);
      }

      if (markets.includes('MRKT') && process.env.MRKT_AUTH) {
        const r = await mrktSearchByFilters({
          giftLower,
          modelLower,
          backdropLower,
          maxPriceTon: user.maxPriceTon,
        });
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

// ====== Commands ======
bot.onText(/^\/start\b/, (msg) => {
  getOrCreateUser(msg.from.id);
  bot.sendMessage(msg.chat.id, 'Бот запущен (Portal + MRKT).', { reply_markup: MAIN_KEYBOARD });
});

bot.onText(/^\/setmaxprice\b(?:\s+(.+))?/, (msg, match) => {
  const user = getOrCreateUser(msg.from.id);
  const arg = match[1];
  if (!arg) return bot.sendMessage(msg.chat.id, 'Пример: /setmaxprice 80', { reply_markup: MAIN_KEYBOARD });

  const v = n(arg);
  if (!Number.isFinite(v) || v <= 0) return bot.sendMessage(msg.chat.id, 'Некорректно. Пример: /setmaxprice 7.5');
  user.maxPriceTon = v;
  user.state = null;
  clearUserSentDeals(msg.from.id);
  bot.sendMessage(msg.chat.id, `Ок. Макс. цена: ${v.toFixed(3)} TON`, { reply_markup: MAIN_KEYBOARD });
});

bot.onText(/^\/sellprice\b/, async (msg) => {
  const user = getOrCreateUser(msg.from.id);
  await sendSellPriceForUser(msg.chat.id, user);
});

bot.onText(/^\/status\b/, (msg) => {
  const user = getOrCreateUser(msg.from.id);
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

// ====== Callbacks + Menus ======
bot.on('callback_query', async (query) => {
  const userId = query.from.id;
  const chatId = query.message?.chat?.id;
  const data = query.data || '';
  const user = getOrCreateUser(userId);

  try {
    // filters menu actions
    if (data === 'filter_gift') {
      const { byLowerName } = await portalCollections(400);
      const names = Array.from(byLowerName.values()).map((x) => x.name).sort();
      const limited = names.slice(0, 60);
      await bot.sendMessage(chatId, 'Выбери подарок (первые 60):', {
        reply_markup: {
          inline_keyboard: limited.map((name) => [{ text: shorten(name, 32), callback_data: `set_gift:${name}` }]),
        },
      });
    } else if (data === 'search_gift') {
      user.state = 'awaiting_gift_search';
      await bot.sendMessage(chatId, 'Напиши часть названия подарка (поиск).', { reply_markup: MAIN_KEYBOARD });
    } else if (data === 'filter_model') {
      if (!user.filters.gifts.length) {
        await bot.sendMessage(chatId, 'Сначала выбери подарок.', { reply_markup: MAIN_KEYBOARD });
      } else {
        const giftLower = user.filters.gifts[0];
        const { byLowerName } = await portalCollections(400);
        const col = byLowerName.get(giftLower);
        const shortName = portalShortName(col?.raw);
        const f = await portalCollectionFilters(shortName);
        if (!f) return bot.sendMessage(chatId, 'Не удалось получить модели (Portal).', { reply_markup: MAIN_KEYBOARD });

        const traits = extractModelTraits(f.models).slice(0, 80);
        const inline_keyboard = traits.map((m) => [
          {
            text: `${shorten(m.name, 24)}${rarityLabelPercent(m) ? ` (${rarityLabelPercent(m)})` : ''}`,
            callback_data: `set_model:${m.name}`,
          },
        ]);

        await bot.sendMessage(chatId, 'Выбери модель:', { reply_markup: { inline_keyboard } });
      }
    } else if (data === 'search_model') {
      if (!user.filters.gifts.length) return bot.sendMessage(chatId, 'Сначала выбери подарок.', { reply_markup: MAIN_KEYBOARD });
      user.state = 'awaiting_model_search';
      await bot.sendMessage(chatId, 'Напиши часть названия модели (поиск).', { reply_markup: MAIN_KEYBOARD });
    } else if (data === 'filter_backdrop') {
      if (!user.filters.gifts.length) {
        await bot.sendMessage(chatId, 'Сначала выбери подарок.', { reply_markup: MAIN_KEYBOARD });
      } else {
        const giftLower = user.filters.gifts[0];
        const { byLowerName } = await portalCollections(400);
        const col = byLowerName.get(giftLower);
        const shortName = portalShortName(col?.raw);
        const f = await portalCollectionFilters(shortName);
        if (!f) return bot.sendMessage(chatId, 'Не удалось получить фоны (Portal).', { reply_markup: MAIN_KEYBOARD });

        // фоны: без процентов, по алфавиту
        const names = Array.isArray(f.backdrops)
          ? f.backdrops.map((x) => (typeof x === 'string' ? x : x?.name || x?.value || x?.title || '')).filter(Boolean)
          : Object.keys(f.backdrops || {});
        names.sort((a, b) => a.localeCompare(b));

        const limited = names.slice(0, 80);
        const inline_keyboard = limited.map((name) => [
          { text: shorten(name, 32), callback_data: `set_backdrop:${name}` },
        ]);
        await bot.sendMessage(chatId, 'Выбери фон:', { reply_markup: { inline_keyboard } });
      }
    } else if (data === 'search_backdrop') {
      if (!user.filters.gifts.length) return bot.sendMessage(chatId, 'Сначала выбери подарок.', { reply_markup: MAIN_KEYBOARD });
      user.state = 'awaiting_backdrop_search';
      await bot.sendMessage(chatId, 'Напиши часть названия фона (поиск).', { reply_markup: MAIN_KEYBOARD });
    } else if (data === 'set_markets_portal') {
      user.filters.markets = ['Portal'];
      clearUserSentDeals(userId);
      await bot.sendMessage(chatId, 'Теперь только Portal.', { reply_markup: MAIN_KEYBOARD });
    } else if (data === 'set_markets_mrkt') {
      user.filters.markets = ['MRKT'];
      clearUserSentDeals(userId);
      await bot.sendMessage(chatId, 'Теперь только MRKT.', { reply_markup: MAIN_KEYBOARD });
    } else if (data === 'set_markets_all') {
      user.filters.markets = ['Portal', 'MRKT'];
      clearUserSentDeals(userId);
      await bot.sendMessage(chatId, 'Теперь Portal + MRKT.', { reply_markup: MAIN_KEYBOARD });
    } else if (data === 'clear_model') {
      user.filters.models = [];
      clearUserSentDeals(userId);
      await bot.sendMessage(chatId, 'Модель сброшена (любая).', { reply_markup: MAIN_KEYBOARD });
    } else if (data === 'clear_backdrop') {
      user.filters.backdrops = [];
      clearUserSentDeals(userId);
      await bot.sendMessage(chatId, 'Фон сброшен (любой).', { reply_markup: MAIN_KEYBOARD });
    } else if (data === 'filters_clear') {
      user.filters.gifts = [];
      user.filters.models = [];
      user.filters.backdrops = [];
      clearUserSentDeals(userId);
      await bot.sendMessage(chatId, 'Все фильтры сброшены.', { reply_markup: MAIN_KEYBOARD });
    } else if (data === 'show_filters') {
      const text =
        `Фильтры:\n` +
        `• Маркеты: ${(user.filters.markets || []).join('+')}\n` +
        `• Подарок: ${user.filters.gifts[0] || 'не выбран'}\n` +
        `• Модель: ${user.filters.models[0] || 'любая'}\n` +
        `• Фон: ${user.filters.backdrops[0] || 'любой'}\n`;
      await bot.sendMessage(chatId, text, { reply_markup: MAIN_KEYBOARD });
    } else if (data.startsWith('set_gift:')) {
      const name = data.slice('set_gift:'.length).trim();
      user.filters.gifts = [name.toLowerCase()];
      user.filters.models = [];
      user.filters.backdrops = [];
      clearUserSentDeals(userId);
      await bot.sendMessage(chatId, `Подарок выбран: ${name}`, { reply_markup: MAIN_KEYBOARD });
    } else if (data.startsWith('set_model:')) {
      const name = data.slice('set_model:'.length).trim();
      user.filters.models = [name.toLowerCase()];
      clearUserSentDeals(userId);
      await bot.sendMessage(chatId, `Модель выбрана: ${name}`, { reply_markup: MAIN_KEYBOARD });
    } else if (data.startsWith('set_backdrop:')) {
      const name = data.slice('set_backdrop:'.length).trim();
      user.filters.backdrops = [name.toLowerCase()];
      clearUserSentDeals(userId);
      await bot.sendMessage(chatId, `Фон выбран: ${name}`, { reply_markup: MAIN_KEYBOARD });
    }

    // subscriptions
    else if (data === 'sub_add_current') {
      if (!user.filters.gifts.length) {
        await bot.sendMessage(chatId, 'Сначала выбери подарок (🎛 Фильтры).', { reply_markup: MAIN_KEYBOARD });
      } else {
        const sub = {
          id: makeId(),
          enabled: true,
          createdAt: new Date().toISOString(),
          filters: {
            gift: user.filters.gifts[0] || null,
            model: user.filters.models[0] || null,
            backdrop: user.filters.backdrops[0] || null,
            markets: [...(user.filters.markets || ['Portal', 'MRKT'])],
          },
        };
        user.subscriptions.push(sub);
        await bot.sendMessage(chatId, `Подписка создана:\n${subTitle(sub)}`, { reply_markup: MAIN_KEYBOARD });
      }
    } else if (data === 'sub_list') {
      const subs = user.subscriptions || [];
      if (!subs.length) {
        await bot.sendMessage(chatId, 'Подписок нет. Создай из текущих фильтров.', { reply_markup: MAIN_KEYBOARD });
      } else {
        let text = 'Мои подписки:\n\n';
        for (const s of subs) {
          text += `${s.enabled ? 'ON ' : 'OFF'} id=${s.id}\n${subTitle(s)}\n\n`;
        }
        text = text.length > 3900 ? text.slice(0, 3900) + '\n...' : text;

        const inline_keyboard = subs.slice(0, 20).map((s) => [
          { text: s.enabled ? `⏸ ${s.id}` : `▶️ ${s.id}`, callback_data: `sub_toggle:${s.id}` },
          { text: `🗑 ${s.id}`, callback_data: `sub_delete:${s.id}` },
        ]);

        await bot.sendMessage(chatId, text, { reply_markup: { inline_keyboard } });
      }
    } else if (data.startsWith('sub_toggle:')) {
      const id = data.slice('sub_toggle:'.length).trim();
      const s = (user.subscriptions || []).find((x) => x.id === id);
      if (!s) await bot.sendMessage(chatId, 'Подписка не найдена.', { reply_markup: MAIN_KEYBOARD });
      else {
        s.enabled = !s.enabled;
        await bot.sendMessage(chatId, `Подписка ${id}: ${s.enabled ? 'ВКЛ' : 'ВЫКЛ'}`, { reply_markup: MAIN_KEYBOARD });
      }
    } else if (data.startsWith('sub_delete:')) {
      const id = data.slice('sub_delete:'.length).trim();
      const before = (user.subscriptions || []).length;
      user.subscriptions = (user.subscriptions || []).filter((x) => x.id !== id);
      await bot.sendMessage(chatId, before !== user.subscriptions.length ? `Удалил подписку ${id}.` : 'Подписка не найдена.', { reply_markup: MAIN_KEYBOARD });
    }
  } catch (e) {
    console.error('callback_query error:', e);
  }

  bot.answerCallbackQuery(query.id).catch(() => {});
});

// ====== Message handler (buttons + text search states) ======
bot.on('message', async (msg) => {
  const userId = msg.from?.id;
  const chatId = msg.chat?.id;
  const text = msg.text;
  if (!userId || !chatId || !text) return;
  if (text.startsWith('/')) return;

  const user = getOrCreateUser(userId);
  const t = text.trim();
  const q = norm(t);

  if (user.state === 'awaiting_max_price') {
    const v = n(t);
    if (!Number.isFinite(v) || v <= 0) return bot.sendMessage(chatId, 'Введи число TON. Пример: 80', { reply_markup: MAIN_KEYBOARD });
    user.maxPriceTon = v;
    user.state = null;
    clearUserSentDeals(userId);
    return bot.sendMessage(chatId, `Ок. Макс. цена: ${v.toFixed(3)} TON`, { reply_markup: MAIN_KEYBOARD });
  }

  if (user.state === 'awaiting_gift_search') {
    user.state = null;
    const { byLowerName } = await portalCollections(400);
    const all = Array.from(byLowerName.values()).map((x) => x.name);
    const matched = all.filter((name) => name.toLowerCase().includes(q)).sort().slice(0, 10);
    if (!matched.length) return bot.sendMessage(chatId, 'Ничего не нашёл. Попробуй другой запрос.', { reply_markup: MAIN_KEYBOARD });

    return bot.sendMessage(chatId, 'Нашёл подарки, выбери:', {
      reply_markup: { inline_keyboard: matched.map((name) => [{ text: shorten(name, 32), callback_data: `set_gift:${name}` }]) },
    });
  }

  if (user.state === 'awaiting_model_search') {
    user.state = null;
    if (!user.filters.gifts.length) return bot.sendMessage(chatId, 'Сначала выбери подарок.', { reply_markup: MAIN_KEYBOARD });

    const giftLower = user.filters.gifts[0];
    const { byLowerName } = await portalCollections(400);
    const col = byLowerName.get(giftLower);
    const shortName = portalShortName(col?.raw);
    const f = await portalCollectionFilters(shortName);
    if (!f) return bot.sendMessage(chatId, 'Не удалось получить модели.', { reply_markup: MAIN_KEYBOARD });

    const traits = extractModelTraits(f.models);
    const matched = traits.filter((m) => m.name.toLowerCase().includes(q)).slice(0, 10);
    if (!matched.length) return bot.sendMessage(chatId, 'Модель не найдена.', { reply_markup: MAIN_KEYBOARD });

    const inline_keyboard = matched.map((m) => [
      { text: `${shorten(m.name, 24)}${rarityLabelPercent(m) ? ` (${rarityLabelPercent(m)})` : ''}`, callback_data: `set_model:${m.name}` },
    ]);
    return bot.sendMessage(chatId, 'Выбери модель:', { reply_markup: { inline_keyboard } });
  }

  if (user.state === 'awaiting_backdrop_search') {
    user.state = null;
    if (!user.filters.gifts.length) return bot.sendMessage(chatId, 'Сначала выбери подарок.', { reply_markup: MAIN_KEYBOARD });

    const giftLower = user.filters.gifts[0];
    const { byLowerName } = await portalCollections(400);
    const col = byLowerName.get(giftLower);
    const shortName = portalShortName(col?.raw);
    const f = await portalCollectionFilters(shortName);
    if (!f) return bot.sendMessage(chatId, 'Не удалось получить фоны.', { reply_markup: MAIN_KEYBOARD });

    const names = Array.isArray(f.backdrops)
      ? f.backdrops.map((x) => (typeof x === 'string' ? x : x?.name || x?.value || x?.title || '')).filter(Boolean)
      : Object.keys(f.backdrops || {});
    const matched = names.filter((name) => name.toLowerCase().includes(q)).sort().slice(0, 10);
    if (!matched.length) return bot.sendMessage(chatId, 'Фон не найден.', { reply_markup: MAIN_KEYBOARD });

    const inline_keyboard = matched.map((name) => [
      { text: shorten(name, 32), callback_data: `set_backdrop:${name}` },
    ]);
    return bot.sendMessage(chatId, 'Выбери фон:', { reply_markup: { inline_keyboard } });
  }

  // buttons
  if (t === '💰 Установить цену') {
    user.state = 'awaiting_max_price';
    return bot.sendMessage(chatId, 'Введи максимальную цену (TON). Пример: 80', { reply_markup: MAIN_KEYBOARD });
  }
  if (t === '🔍 Запустить поиск') {
    user.enabled = true;
    return bot.sendMessage(chatId, 'Мониторинг включён.', { reply_markup: MAIN_KEYBOARD });
  }
  if (t === '⏹ Остановить поиск') {
    user.enabled = false;
    return bot.sendMessage(chatId, 'Мониторинг остановлен.', { reply_markup: MAIN_KEYBOARD });
  }
  if (t === '💸 Цена подарка') {
    return sendSellPriceForUser(chatId, user);
  }
  if (t === '📌 Статус API') {
    const portalAuth = process.env.PORTAL_AUTH ? '✅' : '❌';
    const mrktAuth = process.env.MRKT_AUTH ? '✅' : '❌';
    return bot.sendMessage(chatId, `API статус:\nPortal auth: ${portalAuth}\nMRKT auth: ${mrktAuth}`, { reply_markup: MAIN_KEYBOARD });
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
          { text: '🅿 Только Portal', callback_data: 'set_markets_portal' },
          { text: '🅼 Только MRKT', callback_data: 'set_markets_mrkt' },
          { text: '🅿+🅼 Оба', callback_data: 'set_markets_all' },
        ],
        [
          { text: '♻️ Сбросить модель', callback_data: 'clear_model' },
          { text: '♻️ Сбросить фон', callback_data: 'clear_backdrop' },
        ],
        [{ text: '♻️ Сбросить всё', callback_data: 'filters_clear' }],
        [{ text: 'ℹ️ Показать фильтры', callback_data: 'show_filters' }],
      ],
    };
    return bot.sendMessage(chatId, 'Настрой фильтры:', { reply_markup: inlineKeyboard });
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

  return bot.sendMessage(chatId, 'Используй кнопки снизу.', { reply_markup: MAIN_KEYBOARD });
});

// ====== intervals ======
setInterval(() => {
  checkMarketsForAllUsers().catch((e) => console.error('monitor interval error:', e));
}, CHECK_INTERVAL_MS);

setInterval(() => {
  checkSubscriptionsForAllUsers().catch((e) => console.error('subs interval error:', e));
}, SUBS_CHECK_INTERVAL_MS);

console.log('Бот запущен. /start');