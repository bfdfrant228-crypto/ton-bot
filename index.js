const TelegramBot = require('node-telegram-bot-api');

const token = process.env.TELEGRAM_TOKEN;
const MODE = process.env.MODE || 'real'; // test | real
const CHECK_INTERVAL_MS = Number(process.env.CHECK_INTERVAL_MS || 12000);
const MAX_SEARCH_RESULTS = Number(process.env.MAX_SEARCH_RESULTS || 10);

// Portal settings
const PORTAL_FEE = Number(process.env.PORTAL_FEE || 0.05); // 5%
const PORTAL_HISTORY_LIMIT = Number(process.env.PORTAL_HISTORY_LIMIT || 200);
const PORTAL_HISTORY_CACHE_MS = Number(process.env.PORTAL_HISTORY_CACHE_MS || 30000);

// MRKT settings
const MRKT_FEE = Number(process.env.MRKT_FEE || 0);

if (!token) {
  console.error('Ошибка: TELEGRAM_TOKEN не задан.');
  process.exit(1);
}

console.log('Bot version 2026-02-18-portal-floor-fix-v2');
console.log('MODE =', MODE);
console.log('CHECK_INTERVAL_MS =', CHECK_INTERVAL_MS);

const bot = new TelegramBot(token, { polling: true });

// userId -> settings
const users = new Map();

// anti-spam (userId:giftId)
const sentDeals = new Set();

// Portal caches
let collectionsCache = null; // { list, byLowerName }
let collectionsCacheTime = 0;
const COLLECTIONS_CACHE_TTL_MS = 10 * 60_000;

const filtersCache = new Map(); // shortName -> { time, data }
const FILTERS_CACHE_TTL_MS = 5 * 60_000;

const historyCache = new Map(); // key -> { time, median, count }

// ===== Keyboard =====
const MAIN_KEYBOARD = {
  keyboard: [
    [{ text: '🔍 Запустить поиск' }, { text: '⏹ Остановить поиск' }],
    [{ text: '💰 Установить цену' }, { text: '💸 Цена подарка' }],
    [{ text: '🎛 Фильтры' }, { text: '📌 Статус API' }],
  ],
  resize_keyboard: true,
};

// ===== Helpers =====
function getOrCreateUser(userId) {
  if (!users.has(userId)) {
    users.set(userId, {
      maxPriceTon: null,
      enabled: true,
      state: null, // awaiting_max_price | awaiting_gift_search | awaiting_model_search | awaiting_backdrop_search
      filters: {
        gifts: [],     // stored as lower-case collection name (portal)
        models: [],    // lower-case
        backdrops: [], // lower-case
        markets: ['Portal', 'MRKT'],
      },
    });
  }
  return users.get(userId);
}

function clearUserSentDeals(userId) {
  const prefix = `${userId}:`;
  for (const key of Array.from(sentDeals)) {
    if (key.startsWith(prefix)) sentDeals.delete(key);
  }
}

function formatMarkets(markets) {
  if (!markets || !markets.length) return 'нет';
  if (markets.length === 2) return 'Portal + MRKT';
  return markets.join(', ');
}

function normalizeKey(s) {
  return String(s || '').toLowerCase().trim();
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

function buildInlineButtons(prefix, names) {
  const buttons = [];
  let row = [];
  for (const name of names) {
    row.push({ text: name, callback_data: `${prefix}${name}` });
    if (row.length === 2) {
      buttons.push(row);
      row = [];
    }
  }
  if (row.length) buttons.push(row);
  return buttons;
}

function buildPortalHeaders() {
  const auth = process.env.PORTAL_AUTH;
  return {
    ...(auth ? { Authorization: auth } : {}),
    Accept: 'application/json, text/plain, */*',
    Origin: 'https://portal-market.com',
    Referer: 'https://portal-market.com/',
  };
}

function getPortalCollectionId(raw) {
  if (!raw) return null;
  return raw.id || raw.collection_id || raw.collectionId || null;
}

function getPortalShortName(raw) {
  if (!raw) return null;
  return raw.short_name || raw.shortName || null;
}

// ===== Rarity parsing + sorting =====
function toFiniteNumber(x) {
  const n = Number(String(x).replace(',', '.'));
  return Number.isFinite(n) ? n : null;
}

// Возвращает rarityPermille (число) или null
function extractRarityValue(val) {
  if (val == null) return null;

  // прямое число
  if (typeof val === 'number') return Number.isFinite(val) ? val : null;

  // строка-число
  if (typeof val === 'string') {
    const n = toFiniteNumber(val);
    if (n != null) return n;
  }

  // объект
  if (typeof val === 'object') {
    const direct =
      val.rarityPermille ??
      val.rarity_per_mille ??
      val.rarityPerMille ??
      val.rarity ??
      null;

    const n = extractRarityValue(direct);
    if (n != null) return n;

    // иногда бывает { name: { rarityPermille: ... } }
    // или { rarity: { permille: ... } }
    for (const v of Object.values(val)) {
      const inner = extractRarityValue(v);
      if (inner != null) return inner;
    }
  }

  return null;
}

function extractTraitsWithRarity(block) {
  const map = new Map(); // lowerName -> { name, rarityPermille }

  if (!block) return [];

  // массив
  if (Array.isArray(block)) {
    for (const item of block) {
      if (!item) continue;

      if (typeof item === 'string') {
        const name = item.trim();
        if (!name) continue;
        map.set(name.toLowerCase(), { name, rarityPermille: null });
        continue;
      }

      const name =
        (item.name || item.model || item.value || item.title || '').toString().trim();
      if (!name) continue;

      const rarity = extractRarityValue(item);
      const lower = name.toLowerCase();
      if (!map.has(lower)) map.set(lower, { name, rarityPermille: rarity });
      else {
        // если раньше не было — заполним
        const prev = map.get(lower);
        if (prev && prev.rarityPermille == null && rarity != null) {
          prev.rarityPermille = rarity;
        }
      }
    }
  }

  // объект: { "Name": {rarityPermille:...}, ... } или { "Name": 1.5, ... }
  else if (typeof block === 'object') {
    for (const [k, v] of Object.entries(block)) {
      const name = String(k).trim();
      if (!name) continue;
      const rarity = extractRarityValue(v);
      const lower = name.toLowerCase();
      if (!map.has(lower)) map.set(lower, { name, rarityPermille: rarity });
      else {
        const prev = map.get(lower);
        if (prev && prev.rarityPermille == null && rarity != null) prev.rarityPermille = rarity;
      }
    }
  }

  const arr = Array.from(map.values());

  // Сортировка: сначала по rarityPermille (если есть), потом по имени
  arr.sort((a, b) => {
    const ra = a.rarityPermille == null ? Infinity : a.rarityPermille;
    const rb = b.rarityPermille == null ? Infinity : b.rarityPermille;
    if (ra !== rb) return ra - rb;
    return a.name.localeCompare(b.name);
  });

  return arr;
}

function formatRarityLabel(trait) {
  if (!trait) return '';
  if (trait.rarityPermille == null) return '';
  // Это именно permille (‰). В UI Portal они иногда показывают как проценты — но технически это permille.
  return `${trait.rarityPermille}‰`;
}

// ===== Portal API =====
const API_URL = 'https://portal-market.com/api/';
const SORTS = {
  price_asc: '&sort_by=price+asc',
  latest: '&sort_by=listed_at+desc',
};

async function portalCollections(limit = 200) {
  const now = Date.now();
  if (collectionsCache && now - collectionsCacheTime < COLLECTIONS_CACHE_TTL_MS) {
    return collectionsCache;
  }

  const auth = process.env.PORTAL_AUTH;
  if (!auth) {
    console.warn('PORTAL_AUTH не задан — Portal collections недоступен.');
    collectionsCache = { list: [], byLowerName: new Map() };
    collectionsCacheTime = now;
    return collectionsCache;
  }

  const url = `${API_URL}collections?limit=${limit}`;

  let res;
  try {
    res = await fetch(url, { method: 'GET', headers: buildPortalHeaders() });
  } catch (e) {
    console.error('Portal collections fetch error:', e);
    return { list: [], byLowerName: new Map() };
  }

  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    console.error('Portal collections HTTP error', res.status, txt.slice(0, 200));
    return { list: [], byLowerName: new Map() };
  }

  const data = await res.json().catch(() => null);
  const arr = Array.isArray(data?.collections) ? data.collections : Array.isArray(data) ? data : [];

  const byLowerName = new Map();
  for (const col of arr) {
    const name = String(col?.name || col?.title || '').trim();
    if (!name) continue;
    byLowerName.set(name.toLowerCase(), { name, raw: col });
  }

  collectionsCache = { list: arr, byLowerName };
  collectionsCacheTime = now;
  return collectionsCache;
}

async function portalCollectionFilters(shortName) {
  if (!shortName) return null;

  const cached = filtersCache.get(shortName);
  const now = Date.now();
  if (cached && now - cached.time < FILTERS_CACHE_TTL_MS) return cached.data;

  const auth = process.env.PORTAL_AUTH;
  if (!auth) {
    console.warn('PORTAL_AUTH не задан — Portal filters недоступен.');
    return null;
  }

  const url = `${API_URL}collections/filters?short_names=${encodeURIComponent(shortName)}`;

  let res;
  try {
    res = await fetch(url, { method: 'GET', headers: buildPortalHeaders() });
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

  // Варианты форматов
  // 1) { collections: { short: { models: [...], backdrops: [...] } } }
  if (data.collections && typeof data.collections === 'object') {
    const key =
      Object.keys(data.collections).find((k) => k.toLowerCase() === shortName.toLowerCase()) ||
      shortName;
    const block = data.collections[key];
    const out = block ? { models: block.models || [], backdrops: block.backdrops || [] } : null;
    if (out) filtersCache.set(shortName, { time: now, data: out });
    return out;
  }

  // 2) { floor_prices: { short: { models: ..., backdrops: ... } } }
  if (data.floor_prices && typeof data.floor_prices === 'object') {
    const key =
      Object.keys(data.floor_prices).find((k) => k.toLowerCase() === shortName.toLowerCase()) ||
      shortName;
    const block = data.floor_prices[key];
    const out = block ? { models: block.models || [], backdrops: block.backdrops || [] } : null;
    if (out) filtersCache.set(shortName, { time: now, data: out });
    return out;
  }

  console.error('Portal filters: неожиданный формат ответа.');
  return null;
}

// Portal search: возвращает массив лотов (listed) с price (ТОЛЬКО price, не floor_price)
async function portalSearch({
  collectionId = null,
  collectionName = null,
  models = [],
  backdrops = [],
  offset = 0,
  limit = 20,
  sort = 'price_asc',
  maxPrice = null,
}) {
  const auth = process.env.PORTAL_AUTH;
  if (!auth) return { ok: false, reason: 'NO_AUTH', gifts: [] };

  let url = `${API_URL}nfts/search?offset=${offset}&limit=${limit}`;
  url += SORTS[sort] || SORTS.price_asc;

  if (maxPrice != null && Number.isFinite(maxPrice)) {
    url += `&min_price=0&max_price=${Number(maxPrice)}`;
  }

  // главное: фильтруем по collection_id, если знаем
  if (collectionId) {
    url += `&collection_ids=${encodeURIComponent(collectionId)}`;
  } else if (collectionName) {
    url += `&filter_by_collections=${quotePlus(capWords(collectionName))}`;
  }

  if (models?.length) {
    url += `&filter_by_models=${models.length === 1 ? quotePlus(capWords(models[0])) : listToURL(models)}`;
  }
  if (backdrops?.length) {
    url += `&filter_by_backdrops=${
      backdrops.length === 1 ? quotePlus(capWords(backdrops[0])) : listToURL(backdrops)
    }`;
  }

  url += '&status=listed&exclude_bundled=true&premarket_status=exclude';

  let res;
  try {
    res = await fetch(url, { method: 'GET', headers: buildPortalHeaders() });
  } catch (e) {
    console.error('Portal search fetch error:', e);
    return { ok: false, reason: 'FETCH_ERROR', gifts: [] };
  }

  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    console.error('Portal search HTTP error', res.status, txt.slice(0, 200));
    if (res.status === 401 || res.status === 403) return { ok: false, reason: 'AUTH_EXPIRED', gifts: [] };
    if (res.status === 429) return { ok: false, reason: 'RATE_LIMIT', gifts: [] };
    return { ok: false, reason: `HTTP_${res.status}`, gifts: [] };
  }

  const data = await res.json().catch(() => null);
  const results = Array.isArray(data?.results) ? data.results : Array.isArray(data) ? data : [];

  const gifts = [];
  for (const nft of results) {
    if (!nft) continue;

    // ВАЖНО: берём только nft.price. floor_price может быть “не то”.
    const priceTon = toFiniteNumber(nft.price);
    if (!priceTon) continue;

    let model = null, symbol = null, backdrop = null;
    if (Array.isArray(nft.attributes)) {
      for (const a of nft.attributes) {
        if (!a?.type) continue;
        if (a.type === 'model') model = a.value;
        else if (a.type === 'symbol') symbol = a.value;
        else if (a.type === 'backdrop') backdrop = a.value;
      }
    }

    const baseName = nft.name || 'NFT';
    const number = nft.external_collection_number ?? null;
    const displayName = number ? `${baseName} #${number}` : baseName;

    // Telegram URL: если tg_id уже "PrettyPosy-40935" — ок. Если число — не строим “самодельный” slug (он часто ломается).
    let urlTelegram = 'https://t.me/portals';
    if (nft.tg_id && String(nft.tg_id).includes('-')) {
      urlTelegram = `https://t.me/nft/${nft.tg_id}`;
    }

    gifts.push({
      id: `portal_${nft.id || nft.tg_id || displayName}`,
      market: 'Portal',
      name: displayName,
      baseName,
      priceTon,
      urlTelegram,
      urlMarket: 'https://t.me/portals',
      attrs: {
        model,
        symbol,
        backdrop,
        collection_id: nft.collection_id || null,
      },
    });
  }

  gifts.sort((a, b) => a.priceTon - b.priceTon);
  return { ok: true, reason: 'OK', gifts };
}

// ===== Portal history (median) =====
async function portalHistoryMedian({ collectionId, model, backdrop }) {
  const auth = process.env.PORTAL_AUTH;
  if (!auth) return { ok: false, reason: 'NO_AUTH', median: null, count: 0 };

  const key = `${collectionId || ''}|${normalizeKey(model)}|${normalizeKey(backdrop)}`;
  const now = Date.now();
  const cached = historyCache.get(key);
  if (cached && now - cached.time < PORTAL_HISTORY_CACHE_MS) return cached;

  const url = `${API_URL}market/actions/?offset=0&limit=${PORTAL_HISTORY_LIMIT}&action_types=buy`;

  let res;
  try {
    res = await fetch(url, { method: 'GET', headers: buildPortalHeaders() });
  } catch (e) {
    console.error('Portal history fetch error:', e);
    return { ok: false, reason: 'FETCH_ERROR', median: null, count: 0 };
  }

  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    console.error('Portal history HTTP error', res.status, txt.slice(0, 200));
    if (res.status === 401 || res.status === 403) return { ok: false, reason: 'AUTH_EXPIRED', median: null, count: 0 };
    if (res.status === 429) return { ok: false, reason: 'RATE_LIMIT', median: null, count: 0 };
    return { ok: false, reason: `HTTP_${res.status}`, median: null, count: 0 };
  }

  const data = await res.json().catch(() => null);
  const actions = Array.isArray(data?.actions) ? data.actions : [];

  const prices = [];
  for (const act of actions) {
    const t = String(act?.type || act?.action_type || act?.actionType || '').toLowerCase();
    if (!['purchase', 'buy'].includes(t)) continue;

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

    if (model && normalizeKey(m) !== normalizeKey(model)) continue;
    if (backdrop && normalizeKey(b) !== normalizeKey(backdrop)) continue;

    const amount = act.amount ?? act.price ?? act.ton_amount ?? act.tonAmount;
    const priceTon = toFiniteNumber(amount);
    if (!priceTon) continue;

    prices.push(priceTon);
  }

  prices.sort((a, b) => a - b);

  let median = null;
  if (prices.length) {
    const n = prices.length;
    median = n % 2 ? prices[(n - 1) / 2] : (prices[n / 2 - 1] + prices[n / 2]) / 2;
  }

  const out = { ok: true, reason: 'OK', median, count: prices.length, time: now };
  historyCache.set(key, out);
  return out;
}

// ===== MRKT (auth required) =====
const MRKT_API_URL = 'https://api.tgmrkt.io/api/v1';

async function fetchMrktFloorForFilters(user) {
  const token = process.env.MRKT_AUTH;
  if (!token) return { ok: false, reason: 'NO_AUTH', floor: null };

  const collFilter = user.filters.gifts.map((x) => capWords(x));
  const modelFilter = user.filters.models.map((x) => capWords(x));
  const backdropFilter = user.filters.backdrops.map((x) => capWords(x));

  const body = {
    count: 30,
    cursor: '',
    collectionNames: collFilter,
    modelNames: modelFilter,
    backdropNames: backdropFilter,
    symbolNames: [],
    ordering: 'None',
    lowToHigh: true,
    maxPrice: null,
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
    console.error('MRKT fetch error:', e);
    return { ok: false, reason: 'FETCH_ERROR', floor: null };
  }

  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    console.error('MRKT HTTP error', res.status, txt.slice(0, 200));
    if (res.status === 401 || res.status === 403) return { ok: false, reason: 'AUTH_EXPIRED', floor: null };
    return { ok: false, reason: `HTTP_${res.status}`, floor: null };
  }

  const data = await res.json().catch(() => null);
  const gifts = Array.isArray(data?.gifts) ? data.gifts : [];
  if (!gifts.length) return { ok: true, reason: 'OK_EMPTY', floor: null };

  let min = null;
  for (const g of gifts) {
    const nano = g?.salePrice ?? g?.salePriceWithoutFee ?? null;
    if (nano == null) continue;
    const ton = Number(nano) / 1e9;
    if (!Number.isFinite(ton) || ton <= 0) continue;
    if (min == null || ton < min) min = ton;
  }

  return { ok: true, reason: 'OK', floor: min };
}

// ===== Commands =====
bot.onText(/^\/start\b/, (msg) => {
  getOrCreateUser(msg.from.id);
  bot.sendMessage(
    msg.chat.id,
    'Бот запущен.\n\n' +
      '• 💰 Установить цену — вводишь число TON\n' +
      '• 💸 Цена подарка — показывает флор (Portal/MRKT) и медиану продаж Portal\n' +
      '• 🎛 Фильтры — выбираешь подарок/модель/фон/маркеты\n',
    { reply_markup: MAIN_KEYBOARD }
  );
});

bot.onText(/^\/help\b/, (msg) => {
  bot.sendMessage(
    msg.chat.id,
    'Команды:\n' +
      '/setmaxprice 80\n' +
      '/status\n' +
      '/sellprice\n' +
      '/listgifts\n' +
      '/listmodels\n',
    { reply_markup: MAIN_KEYBOARD }
  );
});

bot.onText(/^\/setmaxprice\b(?:\s+(.+))?/, (msg, match) => {
  const arg = match[1];
  if (!arg) return bot.sendMessage(msg.chat.id, 'Пример: /setmaxprice 80');
  const v = toFiniteNumber(arg);
  if (!v || v <= 0) return bot.sendMessage(msg.chat.id, 'Некорректно. Пример: /setmaxprice 7.5');
  const user = getOrCreateUser(msg.from.id);
  user.maxPriceTon = v;
  clearUserSentDeals(msg.from.id);
  bot.sendMessage(msg.chat.id, `Ок. Макс. цена: ${v.toFixed(3)} TON`, { reply_markup: MAIN_KEYBOARD });
});

bot.onText(/^\/status\b/, async (msg) => {
  const user = getOrCreateUser(msg.from.id);

  const portalAuth = process.env.PORTAL_AUTH ? '✅ задан' : '❌ нет';
  const mrktAuth = process.env.MRKT_AUTH ? '✅ задан' : '❌ нет';

  let text = 'Твои настройки:\n';
  text += `• Макс. цена: ${user.maxPriceTon ? user.maxPriceTon.toFixed(3) + ' TON' : 'не задана'}\n`;
  text += `• Мониторинг: ${user.enabled ? 'включён' : 'выключен'}\n`;
  text += `• Маркеты: ${formatMarkets(user.filters.markets)}\n`;
  text += `• Подарок: ${user.filters.gifts[0] || 'не выбран'}\n`;
  text += `• Модель: ${user.filters.models[0] || 'любая'}\n`;
  text += `• Фон: ${user.filters.backdrops[0] || 'любой'}\n\n`;
  text += `API:\n• Portal auth: ${portalAuth}\n• MRKT auth: ${mrktAuth}\n`;

  bot.sendMessage(msg.chat.id, text, { reply_markup: MAIN_KEYBOARD });
});

bot.onText(/^\/listgifts\b/, async (msg) => {
  const { byLowerName } = await portalCollections(300);
  const names = Array.from(byLowerName.values()).map((x) => x.name).sort();
  if (!names.length) {
    return bot.sendMessage(
      msg.chat.id,
      'Не удалось получить список подарков. Проверь PORTAL_AUTH (часто он протухает).',
      { reply_markup: MAIN_KEYBOARD }
    );
  }
  let text = 'Подарки (Portal collections):\n' + names.map((n) => `- ${n}`).join('\n');
  if (text.length > 3900) text = text.slice(0, 3900) + '\n...';
  bot.sendMessage(msg.chat.id, text, { reply_markup: MAIN_KEYBOARD });
});

bot.onText(/^\/listmodels\b/, async (msg) => {
  const user = getOrCreateUser(msg.from.id);
  if (!user.filters.gifts.length) {
    return bot.sendMessage(msg.chat.id, 'Сначала выбери подарок: 🎛 Фильтры → 🎁 Выбрать подарок', {
      reply_markup: MAIN_KEYBOARD,
    });
  }

  const giftLower = user.filters.gifts[0];
  const { byLowerName } = await portalCollections(300);
  const col = byLowerName.get(giftLower);

  if (!col) {
    return bot.sendMessage(msg.chat.id, 'Не нашёл подарок в collections. Выбери заново через фильтры.', {
      reply_markup: MAIN_KEYBOARD,
    });
  }

  const shortName = getPortalShortName(col.raw);
  const f = await portalCollectionFilters(shortName);
  if (!f) {
    return bot.sendMessage(
      msg.chat.id,
      'Не удалось получить модели/фоны. Проверь PORTAL_AUTH (или лимиты Portal).',
      { reply_markup: MAIN_KEYBOARD }
    );
  }

  const models = extractTraitsWithRarity(f.models);
  const backdrops = extractTraitsWithRarity(f.backdrops);

  let text = `Подарок: ${col.name}\n\nМодели (по редкости):\n`;
  text += models.length
    ? models.map((m) => {
        const r = formatRarityLabel(m);
        return r ? `- ${m.name} (${r})` : `- ${m.name}`;
      }).join('\n')
    : '(нет данных)';

  text += `\n\nФоны (по редкости):\n`;
  text += backdrops.length
    ? backdrops.map((b) => {
        const r = formatRarityLabel(b);
        return r ? `- ${b.name} (${r})` : `- ${b.name}`;
      }).join('\n')
    : '(нет данных)';

  if (text.length > 3900) text = text.slice(0, 3900) + '\n...';
  bot.sendMessage(msg.chat.id, text, { reply_markup: MAIN_KEYBOARD });
});

bot.onText(/^\/sellprice\b/, async (msg) => {
  const user = getOrCreateUser(msg.from.id);
  await sendSellPriceForUser(msg.chat.id, msg.from.id, user);
});

// ===== Buttons / messages =====
bot.on('message', async (msg) => {
  const chatId = msg.chat.id;
  const userId = msg.from.id;
  const text = msg.text;
  if (!text) return;
  if (text.startsWith('/')) return;

  const user = getOrCreateUser(userId);
  const lower = text.toLowerCase().trim();

  if (user.state === 'awaiting_max_price') {
    const v = toFiniteNumber(text);
    if (!v || v <= 0) return bot.sendMessage(chatId, 'Введи число TON. Пример: 80', { reply_markup: MAIN_KEYBOARD });
    user.maxPriceTon = v;
    user.state = null;
    clearUserSentDeals(userId);
    return bot.sendMessage(chatId, `Ок. Макс. цена: ${v.toFixed(3)} TON`, { reply_markup: MAIN_KEYBOARD });
  }

  if (user.state === 'awaiting_gift_search') {
    user.state = null;
    const { byLowerName } = await portalCollections(300);
    const all = Array.from(byLowerName.values()).map((x) => x.name);
    const matched = all.filter((n) => n.toLowerCase().includes(lower)).sort();
    if (!matched.length) return bot.sendMessage(chatId, 'Ничего не нашёл. Попробуй другой кусок названия.', { reply_markup: MAIN_KEYBOARD });
    const limited = matched.slice(0, MAX_SEARCH_RESULTS);
    return bot.sendMessage(chatId, 'Выбери подарок:', { reply_markup: { inline_keyboard: buildInlineButtons('set_gift:', limited) } });
  }

  if (user.state === 'awaiting_model_search') {
    user.state = null;
    if (!user.filters.gifts.length) return bot.sendMessage(chatId, 'Сначала выбери подарок.', { reply_markup: MAIN_KEYBOARD });

    const giftLower = user.filters.gifts[0];
    const { byLowerName } = await portalCollections(300);
    const col = byLowerName.get(giftLower);
    const shortName = getPortalShortName(col?.raw);
    const f = await portalCollectionFilters(shortName);
    if (!f) return bot.sendMessage(chatId, 'Не удалось получить модели.', { reply_markup: MAIN_KEYBOARD });

    const models = extractTraitsWithRarity(f.models);
    const matched = models.filter((m) => m.name.toLowerCase().includes(lower)).slice(0, MAX_SEARCH_RESULTS);
    if (!matched.length) return bot.sendMessage(chatId, 'Модели не найдены по запросу.', { reply_markup: MAIN_KEYBOARD });

    const inline_keyboard = buildInlineButtons('set_model:', matched.map((m) => m.name));
    return bot.sendMessage(chatId, 'Выбери модель:', { reply_markup: { inline_keyboard } });
  }

  if (user.state === 'awaiting_backdrop_search') {
    user.state = null;
    if (!user.filters.gifts.length) return bot.sendMessage(chatId, 'Сначала выбери подарок.', { reply_markup: MAIN_KEYBOARD });

    const giftLower = user.filters.gifts[0];
    const { byLowerName } = await portalCollections(300);
    const col = byLowerName.get(giftLower);
    const shortName = getPortalShortName(col?.raw);
    const f = await portalCollectionFilters(shortName);
    if (!f) return bot.sendMessage(chatId, 'Не удалось получить фоны.', { reply_markup: MAIN_KEYBOARD });

    const backdrops = extractTraitsWithRarity(f.backdrops);
    const matched = backdrops.filter((b) => b.name.toLowerCase().includes(lower)).slice(0, MAX_SEARCH_RESULTS);
    if (!matched.length) return bot.sendMessage(chatId, 'Фоны не найдены по запросу.', { reply_markup: MAIN_KEYBOARD });

    const inline_keyboard = buildInlineButtons('set_backdrop:', matched.map((b) => b.name));
    return bot.sendMessage(chatId, 'Выбери фон:', { reply_markup: { inline_keyboard } });
  }

  // Buttons
  if (text === '💰 Установить цену') {
    user.state = 'awaiting_max_price';
    return bot.sendMessage(chatId, 'Введи максимальную цену (TON). Пример: 80', { reply_markup: MAIN_KEYBOARD });
  }

  if (text === '🔍 Запустить поиск') {
    user.enabled = true;
    return bot.sendMessage(chatId, 'Мониторинг включён.', { reply_markup: MAIN_KEYBOARD });
  }

  if (text === '⏹ Остановить поиск') {
    user.enabled = false;
    return bot.sendMessage(chatId, 'Мониторинг остановлен.', { reply_markup: MAIN_KEYBOARD });
  }

  if (text === '💸 Цена подарка') {
    return sendSellPriceForUser(chatId, userId, user);
  }

  if (text === '📌 Статус API') {
    const portalAuth = process.env.PORTAL_AUTH ? '✅' : '❌';
    const mrktAuth = process.env.MRKT_AUTH ? '✅' : '❌';
    return bot.sendMessage(chatId, `API статус:\nPortal auth: ${portalAuth}\nMRKT auth: ${mrktAuth}`, { reply_markup: MAIN_KEYBOARD });
  }

  if (text === '🎛 Фильтры') {
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

  // otherwise
  bot.sendMessage(chatId, 'Используй кнопки снизу или /help', { reply_markup: MAIN_KEYBOARD });
});

// ===== Callback queries =====
bot.on('callback_query', async (query) => {
  const userId = query.from.id;
  const chatId = query.message.chat.id;
  const data = query.data || '';
  const user = getOrCreateUser(userId);

  try {
    if (data === 'filter_gift') {
      const { byLowerName } = await portalCollections(300);
      const names = Array.from(byLowerName.values()).map((x) => x.name).sort();
      if (!names.length) {
        await bot.sendMessage(chatId, 'Portal collections пуст. Проверь PORTAL_AUTH.', { reply_markup: MAIN_KEYBOARD });
      } else {
        await bot.sendMessage(chatId, 'Выбери подарок:', {
          reply_markup: { inline_keyboard: buildInlineButtons('set_gift:', names.slice(0, 60)) },
        });
      }
    }

    else if (data === 'filter_model') {
      if (!user.filters.gifts.length) {
        await bot.sendMessage(chatId, 'Сначала выбери подарок.', { reply_markup: MAIN_KEYBOARD });
      } else {
        const giftLower = user.filters.gifts[0];
        const { byLowerName } = await portalCollections(300);
        const col = byLowerName.get(giftLower);
        const shortName = getPortalShortName(col?.raw);
        const f = await portalCollectionFilters(shortName);
        if (!f) {
          await bot.sendMessage(chatId, 'Не удалось получить модели (PORTAL_AUTH/лимиты).', { reply_markup: MAIN_KEYBOARD });
        } else {
          const traits = extractTraitsWithRarity(f.models);
          const names = traits.map((t) => t.name).slice(0, 80);
          await bot.sendMessage(chatId, 'Выбери модель:', {
            reply_markup: { inline_keyboard: buildInlineButtons('set_model:', names) },
          });
        }
      }
    }

    else if (data === 'filter_backdrop') {
      if (!user.filters.gifts.length) {
        await bot.sendMessage(chatId, 'Сначала выбери подарок.', { reply_markup: MAIN_KEYBOARD });
      } else {
        const giftLower = user.filters.gifts[0];
        const { byLowerName } = await portalCollections(300);
        const col = byLowerName.get(giftLower);
        const shortName = getPortalShortName(col?.raw);
        const f = await portalCollectionFilters(shortName);
        if (!f) {
          await bot.sendMessage(chatId, 'Не удалось получить фоны (PORTAL_AUTH/лимиты).', { reply_markup: MAIN_KEYBOARD });
        } else {
          const traits = extractTraitsWithRarity(f.backdrops);
          const names = traits.map((t) => t.name).slice(0, 80);
          await bot.sendMessage(chatId, 'Выбери фон:', {
            reply_markup: { inline_keyboard: buildInlineButtons('set_backdrop:', names) },
          });
        }
      }
    }

    else if (data === 'search_gift') {
      user.state = 'awaiting_gift_search';
      await bot.sendMessage(chatId, 'Напиши часть названия подарка.', { reply_markup: MAIN_KEYBOARD });
    }

    else if (data === 'search_model') {
      if (!user.filters.gifts.length) {
        await bot.sendMessage(chatId, 'Сначала выбери подарок.', { reply_markup: MAIN_KEYBOARD });
      } else {
        user.state = 'awaiting_model_search';
        await bot.sendMessage(chatId, 'Напиши часть названия модели.', { reply_markup: MAIN_KEYBOARD });
      }
    }

    else if (data === 'search_backdrop') {
      if (!user.filters.gifts.length) {
        await bot.sendMessage(chatId, 'Сначала выбери подарок.', { reply_markup: MAIN_KEYBOARD });
      } else {
        user.state = 'awaiting_backdrop_search';
        await bot.sendMessage(chatId, 'Напиши часть названия фона.', { reply_markup: MAIN_KEYBOARD });
      }
    }

    else if (data === 'set_markets_portal') {
      user.filters.markets = ['Portal'];
      clearUserSentDeals(userId);
      await bot.sendMessage(chatId, 'Теперь только Portal.', { reply_markup: MAIN_KEYBOARD });
    }

    else if (data === 'set_markets_mrkt') {
      user.filters.markets = ['MRKT'];
      clearUserSentDeals(userId);
      await bot.sendMessage(chatId, 'Теперь только MRKT.', { reply_markup: MAIN_KEYBOARD });
    }

    else if (data === 'set_markets_all') {
      user.filters.markets = ['Portal', 'MRKT'];
      clearUserSentDeals(userId);
      await bot.sendMessage(chatId, 'Теперь Portal + MRKT.', { reply_markup: MAIN_KEYBOARD });
    }

    else if (data === 'clear_model') {
      user.filters.models = [];
      clearUserSentDeals(userId);
      await bot.sendMessage(chatId, 'Модель сброшена (теперь любая).', { reply_markup: MAIN_KEYBOARD });
    }

    else if (data === 'clear_backdrop') {
      user.filters.backdrops = [];
      clearUserSentDeals(userId);
      await bot.sendMessage(chatId, 'Фон сброшен (теперь любой).', { reply_markup: MAIN_KEYBOARD });
    }

    else if (data === 'filters_clear') {
      user.filters.gifts = [];
      user.filters.models = [];
      user.filters.backdrops = [];
      clearUserSentDeals(userId);
      await bot.sendMessage(chatId, 'Все фильтры сброшены.', { reply_markup: MAIN_KEYBOARD });
    }

    else if (data === 'show_filters') {
      let t = 'Фильтры:\n';
      t += `• Маркеты: ${formatMarkets(user.filters.markets)}\n`;
      t += `• Подарок: ${user.filters.gifts[0] || 'не выбран'}\n`;
      t += `• Модель: ${user.filters.models[0] || 'любая'}\n`;
      t += `• Фон: ${user.filters.backdrops[0] || 'любой'}\n`;
      await bot.sendMessage(chatId, t, { reply_markup: MAIN_KEYBOARD });
    }

    else if (data.startsWith('set_gift:')) {
      const original = data.slice('set_gift:'.length);
      user.filters.gifts = [original.toLowerCase().trim()];
      user.filters.models = [];
      user.filters.backdrops = [];
      clearUserSentDeals(userId);
      await bot.sendMessage(chatId, `Подарок выбран: ${original}`, { reply_markup: MAIN_KEYBOARD });
    }

    else if (data.startsWith('set_model:')) {
      const original = data.slice('set_model:'.length);
      user.filters.models = [original.toLowerCase().trim()];
      clearUserSentDeals(userId);
      await bot.sendMessage(chatId, `Модель выбрана: ${original}`, { reply_markup: MAIN_KEYBOARD });
    }

    else if (data.startsWith('set_backdrop:')) {
      const original = data.slice('set_backdrop:'.length);
      user.filters.backdrops = [original.toLowerCase().trim()];
      clearUserSentDeals(userId);
      await bot.sendMessage(chatId, `Фон выбран: ${original}`, { reply_markup: MAIN_KEYBOARD });
    }
  } catch (e) {
    console.error('callback_query error:', e);
  }

  bot.answerCallbackQuery(query.id).catch(() => {});
});

// ===== Sell price =====
async function sendSellPriceForUser(chatId, userId, user) {
  if (!user.filters.gifts.length) {
    return bot.sendMessage(
      chatId,
      'Сначала выбери подарок: 🎛 Фильтры → 🎁 Выбрать подарок',
      { reply_markup: MAIN_KEYBOARD }
    );
  }

  const giftLower = user.filters.gifts[0];
  const { byLowerName } = await portalCollections(300);
  const col = byLowerName.get(giftLower);
  const giftName = col?.name || giftLower;

  const collectionId = getPortalCollectionId(col?.raw);
  const collectionNameFallback = giftName;

  const model = user.filters.models[0] || null;
  const backdrop = user.filters.backdrops[0] || null;

  let text = 'Оценка цен продажи:\n\n';
  text += `Подарок: ${giftName}\n`;
  text += `Модель: ${model || 'любая'}\n`;
  text += `Фон: ${backdrop || 'любой'}\n\n`;

  // Portal floor
  let portalFloor = null;
  let portalFloorReason = null;

  if (user.filters.markets.includes('Portal')) {
    const r = await portalSearch({
      collectionId: collectionId || null,
      collectionName: collectionId ? null : collectionNameFallback,
      models: model ? [model] : [],
      backdrops: backdrop ? [backdrop] : [],
      offset: 0,
      limit: 40,
      sort: 'price_asc',
      maxPrice: null,
    });

    if (!r.ok) {
      portalFloorReason = r.reason;
    } else {
      portalFloor = r.gifts.length ? r.gifts[0].priceTon : null;
    }

    if (portalFloor != null) {
      const net = portalFloor * (1 - PORTAL_FEE);
      text += `Portal:\n  ~${portalFloor.toFixed(3)} TON (минимальный активный лот)\n`;
      text += `  Чистыми после комиссии ${(PORTAL_FEE * 100).toFixed(1)}%: ~${net.toFixed(3)} TON\n`;
    } else {
      const msg =
        portalFloorReason === 'NO_AUTH'
          ? 'Portal: отключен (нет PORTAL_AUTH)\n'
          : portalFloorReason === 'AUTH_EXPIRED'
          ? 'Portal: авторизация протухла (обнови PORTAL_AUTH)\n'
          : portalFloorReason === 'RATE_LIMIT'
          ? 'Portal: лимит запросов (429), подожди чуть‑чуть\n'
          : 'Portal: активных лотов по этим фильтрам нет\n';
      text += msg;
    }

    // Portal history median (если есть collectionId)
    if (collectionId) {
      const h = await portalHistoryMedian({
        collectionId,
        model: model ? capWords(model) : null,
        backdrop: backdrop ? capWords(backdrop) : null,
      });

      if (h.ok && h.median != null) {
        text += `Portal (история продаж):\n  ~${h.median.toFixed(3)} TON (медиана, выборка: ${h.count})\n`;
      } else if (h.reason === 'NO_AUTH') {
        text += `Portal (история): отключена (нет PORTAL_AUTH)\n`;
      } else if (h.reason === 'AUTH_EXPIRED') {
        text += `Portal (история): авторизация протухла (обнови PORTAL_AUTH)\n`;
      } else if (h.reason === 'RATE_LIMIT') {
        text += `Portal (история): лимит запросов (429)\n`;
      }
    } else {
      text += `Portal (история продаж): не могу посчитать точно (нет collection_id из /collections)\n`;
    }
  }

  // MRKT floor
  if (user.filters.markets.includes('MRKT')) {
    const mr = await fetchMrktFloorForFilters(user);
    if (!mr.ok) {
      const msg =
        mr.reason === 'NO_AUTH'
          ? 'MRKT: отключен (нет MRKT_AUTH)\n'
          : mr.reason === 'AUTH_EXPIRED'
          ? 'MRKT: токен протух (обнови MRKT_AUTH)\n'
          : 'MRKT: ошибка запроса\n';
      text += `\n${msg}`;
    } else {
      if (mr.floor != null) {
        const net = mr.floor * (1 - MRKT_FEE);
        text += `\nMRKT:\n  ~${mr.floor.toFixed(3)} TON (минимальный активный лот)\n`;
        text += `  Комиссия ${(MRKT_FEE * 100).toFixed(1)}%: ~${net.toFixed(3)} TON чистыми\n`;
      } else {
        text += `\nMRKT: активных лотов по этим фильтрам нет\n`;
      }
    }
  }

  // Hint for max price
  text += '\nПодсказка для /setmaxprice:\n';
  text += '• Если хочешь чтобы бот начал показывать лоты сразу — ставь max >= флора.\n';
  text += '• Если хочешь ловить выгодные — ставь max НИЖЕ флора и жди.\n';

  await bot.sendMessage(chatId, text, { reply_markup: MAIN_KEYBOARD });
}

// ===== Monitor loop =====
async function checkMarketsForAllUsers() {
  if (MODE !== 'real') return;
  if (users.size === 0) return;

  for (const [userId, user] of users.entries()) {
    if (!user.enabled) continue;
    if (!user.maxPriceTon) continue;

    // Portal мониторинг (по активным лотам)
    if (user.filters.markets.includes('Portal')) {
      if (user.filters.gifts.length) {
        const giftLower = user.filters.gifts[0];
        const { byLowerName } = await portalCollections(300);
        const col = byLowerName.get(giftLower);
        const giftName = col?.name || giftLower;
        const collectionId = getPortalCollectionId(col?.raw);

        const model = user.filters.models[0] || null;
        const backdrop = user.filters.backdrops[0] || null;

        const r = await portalSearch({
          collectionId: collectionId || null,
          collectionName: collectionId ? null : giftName,
          models: model ? [model] : [],
          backdrops: backdrop ? [backdrop] : [],
          offset: 0,
          limit: 25,
          sort: 'price_asc',
          maxPrice: user.maxPriceTon,
        });

        if (r.ok && r.gifts.length) {
          for (const g of r.gifts) {
            if (g.priceTon > user.maxPriceTon) continue;
            const key = `${userId}:${g.id}`;
            if (sentDeals.has(key)) continue;
            sentDeals.add(key);

            const m = g.attrs?.model ? `\nМодель: ${g.attrs.model}` : '';
            const b = g.attrs?.backdrop ? `\nФон: ${g.attrs.backdrop}` : '';

            const text =
              `Найден лот (Portal):\n` +
              `${g.name}\n` +
              `Цена: ${g.priceTon.toFixed(3)} TON${m}${b}\n\n` +
              `Ссылка: ${g.urlTelegram || 'https://t.me/portals'}`;

            await bot.sendMessage(userId, text, { disable_web_page_preview: true });
          }
        }
      }
    }

    // MRKT мониторинг — только если есть токен
    if (user.filters.markets.includes('MRKT') && process.env.MRKT_AUTH) {
      // В этом файле мониторинг MRKT можно добавить позже, чтобы не усложнять и не ловить лишние ошибки.
      // Сейчас MRKT используется для sellprice (флор).
    }
  }
}

setInterval(() => {
  checkMarketsForAllUsers().catch((e) => console.error('monitor error:', e));
}, CHECK_INTERVAL_MS);

console.log('Бот запущен. /start');