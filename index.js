const TelegramBot = require('node-telegram-bot-api');

const token = process.env.TELEGRAM_TOKEN;
const MODE = process.env.MODE || 'real';

// Мониторинг “дешёвых лотов” (по maxPriceTon пользователя)
const CHECK_INTERVAL_MS = Number(process.env.CHECK_INTERVAL_MS || 12000);

// Подписки (флор + изменения цены)
const SUBS_CHECK_INTERVAL_MS = Number(process.env.SUBS_CHECK_INTERVAL_MS || 30000);
const SUBS_MAX_NOTIFICATIONS_PER_CYCLE = Number(process.env.SUBS_MAX_NOTIFICATIONS_PER_CYCLE || 5);

// UI
const MAX_SEARCH_RESULTS = Number(process.env.MAX_SEARCH_RESULTS || 10);

// anti-spam
const MAX_NOTIFICATIONS_PER_CHECK = Number(process.env.MAX_NOTIFICATIONS_PER_CHECK || 3);
const SENT_TTL_MS = Number(process.env.SENT_TTL_MS || 24 * 60 * 60 * 1000); // 24h

// Portal
const PORTAL_FEE = Number(process.env.PORTAL_FEE || 0.05);
const PORTAL_HISTORY_LIMIT = Number(process.env.PORTAL_HISTORY_LIMIT || 100);
const PORTAL_HISTORY_PAGES = Number(process.env.PORTAL_HISTORY_PAGES || 10);
const PORTAL_HISTORY_PAGE_DELAY_MS = Number(process.env.PORTAL_HISTORY_PAGE_DELAY_MS || 350);

// Deep-link на конкретный лот Portal
const PORTAL_LOT_URL_TEMPLATE =
  process.env.PORTAL_LOT_URL_TEMPLATE ||
  'https://t.me/portals_market_bot/market?startapp=gift_{id}';

// Portal принимает только эти значения, иначе 422
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
const SORTS = {
  price_asc: '&sort_by=price+asc',
  latest: '&sort_by=listed_at+desc',
};

if (!token) {
  console.error('Ошибка: TELEGRAM_TOKEN не задан.');
  process.exit(1);
}

console.log('Bot version 2026-02-18-subs-v1');
console.log('MODE =', MODE);
console.log('CHECK_INTERVAL_MS =', CHECK_INTERVAL_MS);
console.log('SUBS_CHECK_INTERVAL_MS =', SUBS_CHECK_INTERVAL_MS);
console.log('PORTAL_PREMARKET_STATUS =', PORTAL_PREMARKET_STATUS);

const bot = new TelegramBot(token, { polling: true });

// ============ state ============
const users = new Map(); // userId -> settings
const sentDeals = new Map(); // key -> ts (для обычного мониторинга)
let isChecking = false;

// состояния подписок: key = `${userId}:${subId}:${market}`
const subStates = new Map();

// Portal caches
let collectionsCache = null; // { byLowerName: Map(lower -> {name, raw}) }
let collectionsCacheTime = 0;
const COLLECTIONS_CACHE_TTL_MS = 10 * 60_000;

const filtersCache = new Map(); // shortName -> { time, data:{models,backdrops} }
const FILTERS_CACHE_TTL_MS = 5 * 60_000;

// History cache
const historyCache = new Map(); // key -> { time, median, count, note }
const HISTORY_CACHE_TTL_MS = 30_000;

// ============ UI ============
const MAIN_KEYBOARD = {
  keyboard: [
    [{ text: '🔍 Запустить поиск' }, { text: '⏹ Остановить поиск' }],
    [{ text: '💰 Установить цену' }, { text: '💸 Цена подарка' }],
    [{ text: '🎛 Фильтры' }, { text: '📡 Подписки' }],
    [{ text: '📌 Статус API' }],
  ],
  resize_keyboard: true,
};

// ============ helpers ============
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function nowMs() {
  return Date.now();
}

function pruneSentDeals() {
  const now = nowMs();
  for (const [k, ts] of sentDeals.entries()) {
    if (now - ts > SENT_TTL_MS) sentDeals.delete(k);
  }
}

function clearUserSentDeals(userId) {
  const prefix = `${userId}:`;
  for (const key of Array.from(sentDeals.keys())) {
    if (key.startsWith(prefix)) sentDeals.delete(key);
  }
}

function makeId() {
  return `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`;
}

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

function formatMarkets(markets) {
  if (!markets || !markets.length) return 'нет';
  if (markets.length === 2) return 'Portal + MRKT';
  return markets.join(', ');
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

function portalCollectionId(raw) {
  return raw?.id || raw?.collection_id || raw?.collectionId || null;
}

function portalShortName(raw) {
  return raw?.short_name || raw?.shortName || null;
}

function safeSliceText(text, max = 3900) {
  if (text.length <= max) return text;
  return text.slice(0, max) + '\n...';
}

function buildPortalLotUrl(nftId) {
  if (!nftId) return 'https://t.me/portals';
  return PORTAL_LOT_URL_TEMPLATE.replace('{id}', encodeURIComponent(String(nftId)));
}

function shortenLabel(s, max = 26) {
  const str = String(s || '');
  if (str.length <= max) return str;
  return str.slice(0, max - 1) + '…';
}

function percentChange(oldV, newV) {
  if (!oldV || !Number.isFinite(oldV) || oldV <= 0) return null;
  return ((newV - oldV) / oldV) * 100;
}

// ============ user object ============
function getOrCreateUser(userId) {
  if (!users.has(userId)) {
    users.set(userId, {
      maxPriceTon: null,
      enabled: true,
      state: null, // awaiting_max_price | awaiting_gift_search | awaiting_model_search | awaiting_backdrop_search
      filters: {
        gifts: [],
        models: [],
        backdrops: [],
        markets: ['Portal', 'MRKT'],
      },
      subscriptions: [], // [{id, enabled, createdAt, filters:{gift, model, backdrop, markets}, maxPriceTon}]
    });
  }
  return users.get(userId);
}

// ============ rarity (% только у моделей) ============
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

function extractModelTraitsWithPercent(block) {
  // сортировка по редкости
  const map = new Map(); // lower -> { name, rarity }
  if (!block) return [];

  const push = (name, rarity) => {
    const key = name.toLowerCase();
    if (!map.has(key)) map.set(key, { name, rarity: rarity ?? null });
    else {
      const prev = map.get(key);
      if (prev && prev.rarity == null && rarity != null) prev.rarity = rarity;
    }
  };

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

// ============ Portal: collections + filters ============
async function portalCollections(limit = 300) {
  const now = nowMs();
  if (collectionsCache && now - collectionsCacheTime < COLLECTIONS_CACHE_TTL_MS) {
    return collectionsCache;
  }

  if (!process.env.PORTAL_AUTH) {
    collectionsCache = { byLowerName: new Map() };
    collectionsCacheTime = now;
    return collectionsCache;
  }

  const url = `${API_URL}collections?limit=${limit}`;
  let res;

  try {
    res = await fetch(url, { method: 'GET', headers: portalHeaders() });
  } catch (e) {
    console.error('Portal collections fetch error:', e);
    collectionsCache = { byLowerName: new Map() };
    collectionsCacheTime = now;
    return collectionsCache;
  }

  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    console.error('Portal collections HTTP error', res.status, txt.slice(0, 200));
    collectionsCache = { byLowerName: new Map() };
    collectionsCacheTime = now;
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

  collectionsCache = { byLowerName };
  collectionsCacheTime = now;
  return collectionsCache;
}

async function portalCollectionFilters(shortName) {
  if (!shortName) return null;

  const now = nowMs();
  const cached = filtersCache.get(shortName);
  if (cached && now - cached.time < FILTERS_CACHE_TTL_MS) return cached.data;

  if (!process.env.PORTAL_AUTH) return null;

  const url = `${API_URL}collections/filters?short_names=${encodeURIComponent(shortName)}`;

  let res;
  try {
    res = await fetch(url, { method: 'GET', headers: portalHeaders() });
  } catch (e) {
    console.error('Portal filters fetch error:', e);
    return null;
  }

  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    console.error('Portal filters HTTP error', res.status, txt.slice(0, 200));
    return null;
  }

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

  const out = {
    models: block.models || [],
    backdrops: block.backdrops || [],
  };

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

// ============ Portal search (реальные лоты: ТОЛЬКО nft.price) ============
async function portalSearch({ collectionId, collectionName, models = [], backdrops = [], maxPrice = null, limit = 50 }) {
  if (!process.env.PORTAL_AUTH) return { ok: false, reason: 'NO_AUTH', gifts: [] };

  let url = `${API_URL}nfts/search?offset=0&limit=${limit}${SORTS.price_asc}`;

  if (maxPrice != null && Number.isFinite(maxPrice)) {
    url += `&min_price=0&max_price=${Number(maxPrice)}`;
  }

  if (collectionId) {
    url += `&collection_ids=${encodeURIComponent(collectionId)}`;
  } else if (collectionName) {
    url += `&filter_by_collections=${quotePlus(capWords(collectionName))}`;
  }

  if (models.length) {
    url += `&filter_by_models=${models.length === 1 ? quotePlus(capWords(models[0])) : listToURL(models)}`;
  }

  if (backdrops.length) {
    url += `&filter_by_backdrops=${backdrops.length === 1 ? quotePlus(capWords(backdrops[0])) : listToURL(backdrops)}`;
  }

  url += `&status=listed&exclude_bundled=true&premarket_status=${encodeURIComponent(PORTAL_PREMARKET_STATUS)}`;

  let res;
  try {
    res = await fetch(url, { method: 'GET', headers: portalHeaders() });
  } catch (e) {
    console.error('Portal search fetch error', e);
    return { ok: false, reason: 'FETCH_ERROR', gifts: [] };
  }

  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    console.error('Portal search HTTP error', res.status, txt.slice(0, 300));
    if (res.status === 401 || res.status === 403) return { ok: false, reason: 'AUTH_EXPIRED', gifts: [] };
    if (res.status === 429) return { ok: false, reason: 'RATE_LIMIT', gifts: [] };
    return { ok: false, reason: `HTTP_${res.status}`, gifts: [] };
  }

  const data = await res.json().catch(() => null);
  const results = Array.isArray(data?.results) ? data.results : Array.isArray(data) ? data : [];

  const gifts = [];
  for (const nft of results) {
    if (!nft) continue;

    const priceTon = n(nft.price);
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
      attrs: {
        model,
        backdrop,
        symbol,
        collection_id: nft.collection_id || null,
      },
    });
  }

  gifts.sort((a, b) => a.priceTon - b.priceTon);
  return { ok: true, reason: 'OK', gifts };
}

// ============ Portal history median (pagination) ============
async function portalFetchActionsPage({ offset, limit, collectionId, model, backdrop }) {
  let url = `${API_URL}market/actions/?offset=${offset}&limit=${limit}&action_types=buy`;

  if (collectionId) url += `&collection_ids=${encodeURIComponent(collectionId)}`;
  if (model) url += `&filter_by_models=${quotePlus(capWords(model))}`;
  if (backdrop) url += `&filter_by_backdrops=${quotePlus(capWords(backdrop))}`;

  const res = await fetch(url, { method: 'GET', headers: portalHeaders() });
  return res;
}

async function portalHistoryMedian({ collectionId, model, backdrop }) {
  if (!process.env.PORTAL_AUTH) return { ok: false, reason: 'NO_AUTH', median: null, count: 0 };

  const key = `${collectionId || ''}|${norm(model)}|${norm(backdrop)}`;
  const now = nowMs();
  const cached = historyCache.get(key);
  if (cached && now - cached.time < HISTORY_CACHE_TTL_MS) return cached;

  const prices = [];
  let page = 0;

  while (page < PORTAL_HISTORY_PAGES) {
    const offset = page * PORTAL_HISTORY_LIMIT;
    let res;

    try {
      res = await portalFetchActionsPage({
        offset,
        limit: PORTAL_HISTORY_LIMIT,
        collectionId,
        model,
        backdrop,
      });
    } catch (e) {
      console.error('Portal history fetch error', e);
      break;
    }

    if (!res.ok) {
      if (res.status === 422) {
        const url = `${API_URL}market/actions/?offset=${offset}&limit=${PORTAL_HISTORY_LIMIT}&action_types=buy`;
        res = await fetch(url, { method: 'GET', headers: portalHeaders() });
        if (!res.ok) break;
      } else {
        break;
      }
    }

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

      if (model && norm(m) !== norm(model)) continue;
      if (backdrop && norm(b) !== norm(backdrop)) continue;

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

// ============ MRKT ============
async function mrktSearchLotsByFilters({ gift, model, backdrop }, maxPriceTon = null) {
  const token = process.env.MRKT_AUTH;
  if (!token) return { ok: false, reason: 'NO_AUTH', gifts: [] };

  const collectionNames = gift ? [capWords(gift)] : [];
  const modelNames = model ? [capWords(model)] : [];
  const backdropNames = backdrop ? [capWords(backdrop)] : [];

  const body = {
    count: 30,
    cursor: '',
    collectionNames,
    modelNames,
    backdropNames,
    symbolNames: [],
    ordering: 'None',
    lowToHigh: true,
    maxPrice: maxPriceTon ?? null,
    minPrice: null,
  };

  let res;
  try {
    res = await fetch(`${MRKT_API_URL}/gifts/saling`, {
      method: 'POST',
      headers: {
        Authorization: token,
        'Content-Type': 'application/json',
        Accept: 'application/json',
      },
      body: JSON.stringify(body),
    });
  } catch (e) {
    console.error('MRKT fetch error', e);
    return { ok: false, reason: 'FETCH_ERROR', gifts: [] };
  }

  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    console.error('MRKT HTTP error', res.status, txt.slice(0, 300));
    if (res.status === 401 || res.status === 403) return { ok: false, reason: 'AUTH_EXPIRED', gifts: [] };
    return { ok: false, reason: `HTTP_${res.status}`, gifts: [] };
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

    const model2 = g.modelTitle || g.modelName || null;
    const backdrop2 = g.backdropName || null;
    const symbol2 = g.symbolName || null;

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
      attrs: { model: model2, backdrop: backdrop2, symbol: symbol2 },
    });
  }

  out.sort((a, b) => a.priceTon - b.priceTon);
  return { ok: true, reason: 'OK', gifts: out };
}

// ============ sellprice ============
async function sendSellPriceForUser(chatId, user) {
  if (!user.filters.gifts.length) {
    await bot.sendMessage(chatId, 'Сначала выбери подарок: 🎛 Фильтры → 🎁 Выбрать подарок', {
      reply_markup: MAIN_KEYBOARD,
    });
    return;
  }

  const giftLower = user.filters.gifts[0];
  const modelLower = user.filters.models[0] || null;
  const backdropLower = user.filters.backdrops[0] || null;

  const { byLowerName } = await portalCollections(300);
  const col = byLowerName.get(giftLower);
  const giftName = col?.name || giftLower;
  const collectionId = portalCollectionId(col?.raw);

  let text = 'Оценка цен продажи:\n\n';
  text += `Подарок: ${giftName}\n`;
  text += `Модель: ${modelLower || 'любая'}\n`;
  text += `Фон: ${backdropLower || 'любой'}\n\n`;

  // Portal floor
  if (user.filters.markets.includes('Portal')) {
    const r = await portalSearch({
      collectionId: collectionId || null,
      collectionName: collectionId ? null : giftName,
      models: modelLower ? [modelLower] : [],
      backdrops: backdropLower ? [backdropLower] : [],
      maxPrice: null,
      limit: 50,
    });

    if (r.ok) {
      const strict = r.gifts.filter((g) => {
        if (modelLower && norm(g.attrs?.model) !== modelLower) return false;
        if (backdropLower && norm(g.attrs?.backdrop) !== backdropLower) return false;
        return true;
      });

      const best = (strict.length ? strict : r.gifts)[0] || null;

      if (best) {
        const net = best.priceTon * (1 - PORTAL_FEE);
        text += `Portal:\n  ~${best.priceTon.toFixed(3)} TON (минимальный активный лот)\n`;
        text += `  Чистыми после комиссии ${(PORTAL_FEE * 100).toFixed(1)}%: ~${net.toFixed(3)} TON\n`;
      } else {
        text += 'Portal: активных лотов по этим фильтрам нет\n';

        if (collectionId) {
          const h = await portalHistoryMedian({
            collectionId,
            model: modelLower ? capWords(modelLower) : null,
            backdrop: backdropLower ? capWords(backdropLower) : null,
          });

          if (h.ok && h.median != null) {
            text += `Portal (история продаж):\n  ~${h.median.toFixed(3)} TON (медиана, выборка: ${h.count}; ${h.note})\n`;
          } else {
            text += 'Portal (история продаж): нет данных по этим фильтрам\n';
          }
        } else {
          text += 'Portal (история продаж): не могу посчитать (нет collection_id)\n';
        }
      }
    } else {
      text += `Portal: ошибка (${r.reason})\n`;
    }
  }

  // MRKT floor
  if (user.filters.markets.includes('MRKT')) {
    const r = await mrktSearchLotsByFilters(
      { gift: giftLower, model: modelLower, backdrop: backdropLower },
      null
    );

    if (r.ok && r.gifts.length) {
      const best = r.gifts[0];
      const net = best.priceTon * (1 - MRKT_FEE);
      text += `\nMRKT:\n  ~${best.priceTon.toFixed(3)} TON (минимальный активный лот)\n`;
      text += `  Комиссия ${(MRKT_FEE * 100).toFixed(1)}%: ~${net.toFixed(3)} TON чистыми\n`;
    } else if (!r.ok) {
      text += `\nMRKT: ошибка (${r.reason})\n`;
    } else {
      text += '\nMRKT: активных лотов по этим фильтрам нет\n';
    }
  }

  await bot.sendMessage(chatId, text, { reply_markup: MAIN_KEYBOARD });
}

// ============ notifications formatting ============
async function sendDeal(userId, gift) {
  const lines = [];
  lines.push(`Price: ${gift.priceTon.toFixed(3)} TON`);
  lines.push(`Gift: ${gift.name}`);
  if (gift.attrs?.model) lines.push(`Model: ${gift.attrs.model}`);
  if (gift.attrs?.symbol) lines.push(`Symbol: ${gift.attrs.symbol}`);
  if (gift.attrs?.backdrop) lines.push(`Backdrop: ${gift.attrs.backdrop}`);
  lines.push(`Market: ${gift.market}`);
  if (gift.urlTelegram) lines.push(gift.urlTelegram); // ссылка последней строкой (для превью)

  const btnText = gift.market === 'Portal' ? 'Открыть Portal' : `Открыть ${gift.market}`;
  const reply_markup = gift.urlMarket
    ? { inline_keyboard: [[{ text: btnText, url: gift.urlMarket }]] }
    : undefined;

  await bot.sendMessage(userId, lines.join('\n'), {
    disable_web_page_preview: false,
    reply_markup,
  });
}

async function sendSubNotification(userId, text, urlMarket) {
  const reply_markup = urlMarket
    ? { inline_keyboard: [[{ text: 'Открыть', url: urlMarket }]] }
    : undefined;

  await bot.sendMessage(userId, text, {
    disable_web_page_preview: false,
    reply_markup,
  });
}

// ============ monitoring: cheap lots ============
async function checkMarketsForAllUsers() {
  if (MODE !== 'real') return;
  if (users.size === 0) return;
  if (isChecking) return;

  isChecking = true;
  try {
    pruneSentDeals();

    for (const [userId, user] of users.entries()) {
      if (!user.enabled) continue;
      if (!user.maxPriceTon) continue;

      const markets = user.filters.markets || ['Portal', 'MRKT'];
      const found = [];

      // Portal
      if (markets.includes('Portal') && user.filters.gifts.length) {
        const { byLowerName } = await portalCollections(400);
        const col = byLowerName.get(user.filters.gifts[0]);
        const giftName = col?.name || user.filters.gifts[0];
        const collectionId = portalCollectionId(col?.raw);

        const modelLower = user.filters.models[0] || null;
        const backdropLower = user.filters.backdrops[0] || null;

        const r = await portalSearch({
          collectionId: collectionId || null,
          collectionName: collectionId ? null : giftName,
          models: modelLower ? [modelLower] : [],
          backdrops: backdropLower ? [backdropLower] : [],
          maxPrice: user.maxPriceTon,
          limit: 50,
        });

        if (r.ok && r.gifts.length) {
          for (const g of r.gifts) {
            if (g.priceTon > user.maxPriceTon) continue;
            if (modelLower && norm(g.attrs?.model) !== modelLower) continue;
            if (backdropLower && norm(g.attrs?.backdrop) !== backdropLower) continue;
            found.push(g);
          }
        }
      }

      // MRKT
      if (markets.includes('MRKT') && process.env.MRKT_AUTH && user.filters.gifts.length) {
        const giftLower = user.filters.gifts[0];
        const modelLower = user.filters.models[0] || null;
        const backdropLower = user.filters.backdrops[0] || null;

        const r = await mrktSearchLotsByFilters(
          { gift: giftLower, model: modelLower, backdrop: backdropLower },
          user.maxPriceTon
        );

        if (r.ok && r.gifts.length) {
          for (const g of r.gifts) {
            if (g.priceTon > user.maxPriceTon) continue;
            if (modelLower && norm(g.attrs?.model) !== modelLower) continue;
            if (backdropLower && norm(g.attrs?.backdrop) !== backdropLower) continue;
            found.push(g);
          }
        }
      }

      found.sort((a, b) => a.priceTon - b.priceTon);

      let sentCount = 0;
      for (const gift of found) {
        if (sentCount >= MAX_NOTIFICATIONS_PER_CHECK) break;

        const key = `${userId}:${gift.id}`;
        if (sentDeals.has(key)) continue;

        sentDeals.set(key, nowMs());
        await sendDeal(userId, gift);
        sentCount++;
      }
    }
  } catch (e) {
    console.error('checkMarketsForAllUsers error:', e);
  } finally {
    isChecking = false;
  }
}

// ============ subscriptions ============
function subTitle(sub) {
  const parts = [];
  parts.push(sub.filters.gift || '(gift?)');
  if (sub.filters.model) parts.push(`model=${sub.filters.model}`);
  if (sub.filters.backdrop) parts.push(`backdrop=${sub.filters.backdrop}`);
  parts.push(`markets=${(sub.filters.markets || []).join('+')}`);
  if (sub.maxPriceTon) parts.push(`max=${sub.maxPriceTon}`);
  return parts.join(' | ');
}

async function getPortalFloorLotForFilters({ gift, model, backdrop }) {
  if (!gift) return null;

  const { byLowerName } = await portalCollections(400);
  const col = byLowerName.get(gift);
  const giftName = col?.name || gift;
  const collectionId = portalCollectionId(col?.raw);

  const r = await portalSearch({
    collectionId: collectionId || null,
    collectionName: collectionId ? null : giftName,
    models: model ? [model] : [],
    backdrops: backdrop ? [backdrop] : [],
    maxPrice: null,
    limit: 50,
  });

  if (!r.ok) return null;

  const strict = r.gifts.filter((g) => {
    if (model && norm(g.attrs?.model) !== model) return false;
    if (backdrop && norm(g.attrs?.backdrop) !== backdrop) return false;
    return true;
  });

  return (strict.length ? strict : r.gifts)[0] || null;
}

async function getMrktFloorLotForFilters({ gift, model, backdrop }) {
  if (!gift) return null;
  const r = await mrktSearchLotsByFilters({ gift, model, backdrop }, null);
  if (!r.ok) return null;
  return r.gifts[0] || null;
}

async function checkSubscriptionsForAllUsers() {
  if (MODE !== 'real') return;
  if (users.size === 0) return;

  let sentThisCycle = 0;

  for (const [userId, user] of users.entries()) {
    const subs = Array.isArray(user.subscriptions) ? user.subscriptions : [];
    const active = subs.filter((s) => s.enabled);

    if (!active.length) continue;

    for (const sub of active) {
      if (sentThisCycle >= SUBS_MAX_NOTIFICATIONS_PER_CYCLE) return;

      const markets = sub.filters.markets || ['Portal', 'MRKT'];

      for (const market of markets) {
        if (sentThisCycle >= SUBS_MAX_NOTIFICATIONS_PER_CYCLE) return;

        if (market === 'MRKT' && !process.env.MRKT_AUTH) continue;
        if (market === 'Portal' && !process.env.PORTAL_AUTH) continue;

        const stateKey = `${userId}:${sub.id}:${market}`;
        const prev = subStates.get(stateKey) || { floor: null, lotId: null };

        let lot = null;
        if (market === 'Portal') {
          lot = await getPortalFloorLotForFilters(sub.filters);
        } else if (market === 'MRKT') {
          lot = await getMrktFloorLotForFilters(sub.filters);
        }

        const newFloor = lot ? lot.priceTon : null;
        const newLotId = lot ? lot.id : null;

        // если раньше не было лотов, а сейчас появились
        if (prev.floor == null && newFloor != null) {
          const text =
            `${sub.filters.gift}(${sub.filters.gift})\n` +
            `Новый лот: ${newFloor.toFixed(3)} TON\n` +
            (sub.filters.model ? `- Model: ${capWords(sub.filters.model)}\n` : '') +
            (sub.filters.backdrop ? `- Backdrop: ${capWords(sub.filters.backdrop)}\n` : '') +
            `Market: ${market}\n` +
            (lot?.urlTelegram ? lot.urlTelegram : '');

          await sendSubNotification(userId, text.trim(), lot?.urlMarket);
          sentThisCycle++;

          subStates.set(stateKey, { floor: newFloor, lotId: newLotId, ts: nowMs() });
          continue;
        }

        // изменение цены флора
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

          await sendSubNotification(userId, text.trim(), lot?.urlMarket);
          sentThisCycle++;

          subStates.set(stateKey, { floor: newFloor, lotId: newLotId, ts: nowMs() });
          continue;
        }

        // обновляем состояние (даже если без уведомления)
        subStates.set(stateKey, { floor: newFloor, lotId: newLotId, ts: nowMs() });
      }
    }
  }
}

// ============ Commands ============
bot.onText(/^\/start\b/, (msg) => {
  getOrCreateUser(msg.from.id);
  bot.sendMessage(msg.chat.id, 'Бот запущен (Portal + MRKT).', { reply_markup: MAIN_KEYBOARD });
});

bot.onText(/^\/status\b/, (msg) => {
  const user = getOrCreateUser(msg.from.id);

  const portalAuth = process.env.PORTAL_AUTH ? '✅' : '❌';
  const mrktAuth = process.env.MRKT_AUTH ? '✅' : '❌';

  let text = 'Статус:\n';
  text += `• Мониторинг: ${user.enabled ? 'включён' : 'выключен'}\n`;
  text += `• Макс. цена: ${user.maxPriceTon ? user.maxPriceTon.toFixed(3) : 'не задана'} TON\n`;
  text += `• Маркеты: ${formatMarkets(user.filters.markets)}\n`;
  text += `• Подарок: ${user.filters.gifts[0] || 'не выбран'}\n`;
  text += `• Модель: ${user.filters.models[0] || 'любая'}\n`;
  text += `• Фон: ${user.filters.backdrops[0] || 'любой'}\n`;
  text += `• Подписок: ${(user.subscriptions || []).length}\n\n`;
  text += `API:\n• Portal auth: ${portalAuth}\n• MRKT auth: ${mrktAuth}\n`;

  bot.sendMessage(msg.chat.id, text, { reply_markup: MAIN_KEYBOARD });
});

bot.onText(/^\/setmaxprice\b(?:\s+(.+))?/, (msg, match) => {
  const arg = match[1];
  if (!arg) return bot.sendMessage(msg.chat.id, 'Пример: /setmaxprice 80', { reply_markup: MAIN_KEYBOARD });

  const v = n(arg);
  if (!Number.isFinite(v) || v <= 0) return bot.sendMessage(msg.chat.id, 'Некорректно. Пример: /setmaxprice 7.5');

  const user = getOrCreateUser(msg.from.id);
  user.maxPriceTon = v;
  user.state = null;
  clearUserSentDeals(msg.from.id);

  bot.sendMessage(msg.chat.id, `Ок. Макс. цена: ${v.toFixed(3)} TON`, { reply_markup: MAIN_KEYBOARD });
});

bot.onText(/^\/sellprice\b/, async (msg) => {
  const user = getOrCreateUser(msg.from.id);
  await sendSellPriceForUser(msg.chat.id, user);
});

// ============ Buttons + states + menus ============
bot.on('message', async (msg) => {
  const chatId = msg.chat.id;
  const userId = msg.from.id;
  const text = msg.text;

  if (!text) return;
  if (text.startsWith('/')) return;

  const user = getOrCreateUser(userId);
  const t = text.trim();
  const q = norm(t);

  // ===== states
  if (user.state === 'awaiting_max_price') {
    const v = n(t);
    if (!Number.isFinite(v) || v <= 0) {
      return bot.sendMessage(chatId, 'Введи положительное число TON. Пример: 80', { reply_markup: MAIN_KEYBOARD });
    }
    user.maxPriceTon = v;
    user.state = null;
    clearUserSentDeals(userId);
    return bot.sendMessage(chatId, `Ок. Макс. цена: ${v.toFixed(3)} TON`, { reply_markup: MAIN_KEYBOARD });
  }

  if (user.state === 'awaiting_gift_search') {
    user.state = null;
    const { byLowerName } = await portalCollections(400);
    const all = Array.from(byLowerName.values()).map((x) => x.name);
    const matched = all.filter((name) => name.toLowerCase().includes(q)).sort();

    if (!matched.length) {
      return bot.sendMessage(chatId, 'Ничего не нашёл. Попробуй другой запрос.', { reply_markup: MAIN_KEYBOARD });
    }

    const limited = matched.slice(0, MAX_SEARCH_RESULTS);
    return bot.sendMessage(chatId, 'Нашёл подарки, выбери:', {
      reply_markup: { inline_keyboard: limited.map((name) => [{ text: shortenLabel(name, 32), callback_data: `set_gift:${name}` }]) },
    });
  }

  if (user.state === 'awaiting_model_search') {
    user.state = null;
    if (!user.filters.gifts.length) {
      return bot.sendMessage(chatId, 'Сначала выбери подарок.', { reply_markup: MAIN_KEYBOARD });
    }

    const giftLower = user.filters.gifts[0];
    const { byLowerName } = await portalCollections(400);
    const col = byLowerName.get(giftLower);
    const shortName = portalShortName(col?.raw);
    const f = await portalCollectionFilters(shortName);

    if (!f) {
      return bot.sendMessage(chatId, 'Не удалось получить модели (PORTAL_AUTH/лимиты).', { reply_markup: MAIN_KEYBOARD });
    }

    const traits = extractModelTraitsWithPercent(f.models);
    const matched = traits.filter((m) => m.name.toLowerCase().includes(q)).slice(0, MAX_SEARCH_RESULTS);

    if (!matched.length) {
      return bot.sendMessage(chatId, 'Модель не найдена. Попробуй другой запрос.', { reply_markup: MAIN_KEYBOARD });
    }

    const inline_keyboard = matched.map((m) => [
      { text: `${shortenLabel(m.name, 24)}${rarityLabelPercent(m) ? ` (${rarityLabelPercent(m)})` : ''}`, callback_data: `set_model:${m.name}` },
    ]);

    return bot.sendMessage(chatId, 'Выбери модель:', { reply_markup: { inline_keyboard } });
  }

  if (user.state === 'awaiting_backdrop_search') {
    user.state = null;
    if (!user.filters.gifts.length) {
      return bot.sendMessage(chatId, 'Сначала выбери подарок.', { reply_markup: MAIN_KEYBOARD });
    }

    const giftLower = user.filters.gifts[0];
    const { byLowerName } = await portalCollections(400);
    const col = byLowerName.get(giftLower);
    const shortName = portalShortName(col?.raw);
    const f = await portalCollectionFilters(shortName);

    if (!f) {
      return bot.sendMessage(chatId, 'Не удалось получить фоны (PORTAL_AUTH/лимиты).', { reply_markup: MAIN_KEYBOARD });
    }

    // ФОНЫ: без процентов, просто по алфавиту
    const names = Array.isArray(f.backdrops)
      ? f.backdrops.map((x) => (typeof x === 'string' ? x : x?.name || x?.value || x?.title || '')).filter(Boolean)
      : Object.keys(f.backdrops || {});
    const matched = names.filter((name) => name.toLowerCase().includes(q)).sort().slice(0, MAX_SEARCH_RESULTS);

    if (!matched.length) {
      return bot.sendMessage(chatId, 'Фон не найден. Попробуй другой запрос.', { reply_markup: MAIN_KEYBOARD });
    }

    const inline_keyboard = matched.map((name) => [
      { text: shortenLabel(name, 32), callback_data: `set_backdrop:${name}` },
    ]);

    return bot.sendMessage(chatId, 'Выбери фон:', { reply_markup: { inline_keyboard } });
  }

  // ===== main buttons
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
    return bot.sendMessage(
      chatId,
      `API статус:\nPortal auth: ${portalAuth}\nMRKT auth: ${mrktAuth}\nPortal premarket_status: ${PORTAL_PREMARKET_STATUS}`,
      { reply_markup: MAIN_KEYBOARD }
    );
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

  bot.sendMessage(chatId, 'Используй кнопки снизу.', { reply_markup: MAIN_KEYBOARD });
});

// ============ callbacks ============
bot.on('callback_query', async (query) => {
  const userId = query.from.id;
  const chatId = query.message.chat.id;
  const data = query.data || '';
  const user = getOrCreateUser(userId);

  try {
    // --- filters menu
    if (data === 'filter_gift') {
      const { byLowerName } = await portalCollections(400);
      const names = Array.from(byLowerName.values()).map((x) => x.name).sort();

      if (!names.length) {
        await bot.sendMessage(chatId, 'Portal недоступен. Проверь PORTAL_AUTH.', { reply_markup: MAIN_KEYBOARD });
      } else {
        const limited = names.slice(0, 60);
        await bot.sendMessage(chatId, 'Выбери подарок (первые 60):', {
          reply_markup: { inline_keyboard: limited.map((name) => [{ text: shortenLabel(name, 32), callback_data: `set_gift:${name}` }]) },
        });
      }
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

        if (!f) {
          await bot.sendMessage(chatId, 'Не удалось получить модели (PORTAL_AUTH/лимиты).', { reply_markup: MAIN_KEYBOARD });
        } else {
          const traits = extractModelTraitsWithPercent(f.models).slice(0, 80);
          const inline_keyboard = traits.map((m) => [
            { text: `${shortenLabel(m.name, 24)}${rarityLabelPercent(m) ? ` (${rarityLabelPercent(m)})` : ''}`, callback_data: `set_model:${m.name}` },
          ]);
          await bot.sendMessage(chatId, 'Выбери модель:', { reply_markup: { inline_keyboard } });
        }
      }
    } else if (data === 'search_model') {
      if (!user.filters.gifts.length) {
        await bot.sendMessage(chatId, 'Сначала выбери подарок.', { reply_markup: MAIN_KEYBOARD });
      } else {
        user.state = 'awaiting_model_search';
        await bot.sendMessage(chatId, 'Напиши часть названия модели (поиск).', { reply_markup: MAIN_KEYBOARD });
      }
    } else if (data === 'filter_backdrop') {
      if (!user.filters.gifts.length) {
        await bot.sendMessage(chatId, 'Сначала выбери подарок.', { reply_markup: MAIN_KEYBOARD });
      } else {
        const giftLower = user.filters.gifts[0];
        const { byLowerName } = await portalCollections(400);
        const col = byLowerName.get(giftLower);
        const shortName = 