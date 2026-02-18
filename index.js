const TelegramBot = require('node-telegram-bot-api');

const token = process.env.TELEGRAM_TOKEN;
const MODE = process.env.MODE || 'real';
const CHECK_INTERVAL_MS = Number(process.env.CHECK_INTERVAL_MS || 12000);
const MAX_SEARCH_RESULTS = Number(process.env.MAX_SEARCH_RESULTS || 10);

// Portal
const PORTAL_FEE = Number(process.env.PORTAL_FEE || 0.05);
const PORTAL_PREMARKET_STATUS_RAW = (process.env.PORTAL_PREMARKET_STATUS || 'without_premarket').trim();

// MRKT
const MRKT_FEE = Number(process.env.MRKT_FEE || 0);

const VALID_PORTAL_PREMARKET = new Set([
  'all',
  'only_premarket',
  'without_premarket',
  'draft',
  'listed',
  'sold',
]);

const PORTAL_PREMARKET_STATUS = VALID_PORTAL_PREMARKET.has(PORTAL_PREMARKET_STATUS_RAW)
  ? PORTAL_PREMARKET_STATUS_RAW
  : 'without_premarket';

if (!token) {
  console.error('Ошибка: TELEGRAM_TOKEN не задан.');
  process.exit(1);
}

console.log('Bot version 2026-02-18-portal-mrkt-fix-v1');
console.log('MODE =', MODE);
console.log('CHECK_INTERVAL_MS =', CHECK_INTERVAL_MS);
console.log('PORTAL_PREMARKET_STATUS =', PORTAL_PREMARKET_STATUS);

const bot = new TelegramBot(token, { polling: true });

const users = new Map(); // userId -> settings
const sentDeals = new Set(); // anti spam

// ===== UI =====
const MAIN_KEYBOARD = {
  keyboard: [
    [{ text: '🔍 Запустить поиск' }, { text: '⏹ Остановить поиск' }],
    [{ text: '💰 Установить цену' }, { text: '💸 Цена подарка' }],
    [{ text: '🎛 Фильтры' }, { text: '📌 Статус API' }],
  ],
  resize_keyboard: true,
};

// ===== helpers =====
function getOrCreateUser(userId) {
  if (!users.has(userId)) {
    users.set(userId, {
      enabled: true,
      maxPriceTon: null,
      state: null, // awaiting_max_price | awaiting_gift_search | awaiting_model_search | awaiting_backdrop_search
      filters: {
        gifts: [],     // lower-case gift name (Portal collection name)
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

function n(x) {
  const v = Number(String(x).replace(',', '.'));
  return Number.isFinite(v) ? v : NaN;
}

function normalize(s) {
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

// ===== rarity parsing (фикс сортировки по редкости) =====
function extractRarityPermille(obj) {
  if (obj == null) return null;
  if (typeof obj === 'number') return Number.isFinite(obj) ? obj : null;
  if (typeof obj === 'string') {
    const v = n(obj);
    return Number.isFinite(v) ? v : null;
  }
  if (typeof obj === 'object') {
    const direct =
      obj.rarityPermille ??
      obj.rarity_per_mille ??
      obj.rarityPerMille ??
      obj.rarity ??
      null;
    const v = extractRarityPermille(direct);
    if (v != null) return v;

    // fallback: поиск по вложенным значениям
    for (const val of Object.values(obj)) {
      const inner = extractRarityPermille(val);
      if (inner != null) return inner;
    }
  }
  return null;
}

function extractTraitsWithRarity(block) {
  const map = new Map(); // lower -> { name, rarityPermille }
  if (!block) return [];

  if (Array.isArray(block)) {
    for (const item of block) {
      if (!item) continue;

      if (typeof item === 'string') {
        const name = item.trim();
        if (!name) continue;
        const key = name.toLowerCase();
        if (!map.has(key)) map.set(key, { name, rarityPermille: null });
        continue;
      }

      const name = String(item.name || item.model || item.value || item.title || '').trim();
      if (!name) continue;
      const key = name.toLowerCase();
      const rarityPermille = extractRarityPermille(item);

      if (!map.has(key)) map.set(key, { name, rarityPermille });
      else {
        const prev = map.get(key);
        if (prev && prev.rarityPermille == null && rarityPermille != null) {
          prev.rarityPermille = rarityPermille;
        }
      }
    }
  } else if (typeof block === 'object') {
    for (const [k, v] of Object.entries(block)) {
      const name = String(k).trim();
      if (!name) continue;
      const key = name.toLowerCase();
      const rarityPermille = extractRarityPermille(v);
      if (!map.has(key)) map.set(key, { name, rarityPermille });
      else {
        const prev = map.get(key);
        if (prev && prev.rarityPermille == null && rarityPermille != null) prev.rarityPermille = rarityPermille;
      }
    }
  }

  const arr = Array.from(map.values());
  arr.sort((a, b) => {
    const ra = a.rarityPermille == null ? Infinity : a.rarityPermille;
    const rb = b.rarityPermille == null ? Infinity : b.rarityPermille;
    if (ra !== rb) return ra - rb;
    return a.name.localeCompare(b.name);
  });
  return arr;
}

function rarityLabel(t) {
  if (!t || t.rarityPermille == null) return '';
  return `${t.rarityPermille}‰`;
}

// ===== Portal API =====
const API_URL = 'https://portal-market.com/api/';
const SORTS = {
  price_asc: '&sort_by=price+asc',
  latest: '&sort_by=listed_at+desc',
};

// caches
let collectionsCache = null; // { byLowerName: Map(lower-> {name, raw}) }
let collectionsCacheTime = 0;
const COLLECTIONS_CACHE_TTL_MS = 10 * 60_000;

const filtersCache = new Map(); // shortName -> { time, data }
const FILTERS_CACHE_TTL_MS = 5 * 60_000;

function portalCollectionId(raw) {
  return raw?.id || raw?.collection_id || raw?.collectionId || null;
}
function portalShortName(raw) {
  return raw?.short_name || raw?.shortName || null;
}

async function portalCollections(limit = 300) {
  const now = Date.now();
  if (collectionsCache && now - collectionsCacheTime < COLLECTIONS_CACHE_TTL_MS) {
    return collectionsCache;
  }

  if (!process.env.PORTAL_AUTH) {
    collectionsCache = { byLowerName: new Map() };
    collectionsCacheTime = now;
    return collectionsCache;
  }

  const url = `${API_URL}collections?limit=${limit}`;
  const res = await fetch(url, { method: 'GET', headers: portalHeaders() });

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

  const now = Date.now();
  const cached = filtersCache.get(shortName);
  if (cached && now - cached.time < FILTERS_CACHE_TTL_MS) return cached.data;

  if (!process.env.PORTAL_AUTH) return null;

  const url = `${API_URL}collections/filters?short_names=${encodeURIComponent(shortName)}`;
  const res = await fetch(url, { method: 'GET', headers: portalHeaders() });

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

// Portal search: берём ТОЛЬКО nft.price (реальные лоты)
async function portalSearch({ collectionId, collectionName, models = [], backdrops = [], maxPrice = null }) {
  if (!process.env.PORTAL_AUTH) {
    return { ok: false, reason: 'NO_AUTH', gifts: [] };
  }

  let url = `${API_URL}nfts/search?offset=0&limit=50${SORTS.price_asc}`;

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

  // ВАЖНО: именно валидное значение, иначе 422 как у тебя в логах
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
    const num = nft.external_collection_number ?? null;
    const displayName = num != null ? `${baseName} #${num}` : baseName;

    const urlTelegram =
      nft.tg_id && String(nft.tg_id).includes('-') ? `https://t.me/nft/${nft.tg_id}` : 'https://t.me/portals';

    gifts.push({
      id: `portal_${nft.id || nft.tg_id || displayName}`,
      market: 'Portal',
      name: displayName,
      baseName,
      priceTon,
      urlTelegram,
      urlMarket: 'https://t.me/portals',
      attrs: { model, backdrop, symbol, collection_id: nft.collection_id || null },
    });
  }

  gifts.sort((a, b) => a.priceTon - b.priceTon);
  return { ok: true, reason: 'OK', gifts };
}

// ===== MRKT =====
const MRKT_API_URL = 'https://api.tgmrkt.io/api/v1';

async function mrktSearchLots(user) {
  const token = process.env.MRKT_AUTH;
  if (!token) return { ok: false, reason: 'NO_AUTH', gifts: [] };

  const collectionNames = user.filters.gifts.map((x) => capWords(x));
  const modelNames = user.filters.models.map((x) => capWords(x));
  const backdropNames = user.filters.backdrops.map((x) => capWords(x));

  const body = {
    count: 30,
    cursor: '',
    collectionNames,
    modelNames,
    backdropNames,
    symbolNames: [],
    ordering: 'None',
    lowToHigh: true,
    maxPrice: user.maxPriceTon ?? null,
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

    const model = g.modelTitle || g.modelName || null;
    const backdrop = g.backdropName || null;
    const symbol = g.symbolName || null;

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

// ===== SellPrice =====
async function sendSellPrice(chatId, userId, user) {
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
      // ВАЖНО: если фон выбран — всегда учитываем (иначе будут “фантомные” дешёвые цены)
      backdrops: backdropLower ? [backdropLower] : [],
      maxPrice: null,
    });

    if (!r.ok) {
      if (r.reason === 'NO_AUTH') text += 'Portal: отключен (нет PORTAL_AUTH)\n';
      else if (r.reason === 'AUTH_EXPIRED') text += 'Portal: авторизация протухла (обнови PORTAL_AUTH)\n';
      else if (r.reason === 'RATE_LIMIT') text += 'Portal: лимит запросов (429), подожди\n';
      else text += 'Portal: ошибка запроса\n';
    } else if (!r.gifts.length) {
      text += 'Portal: активных лотов по этим фильтрам нет\n';
    } else {
      // дополнительная защита: пост-фильтр по model/backdrop (на случай если API иногда игнорит фильтры)
      const filtered = r.gifts.filter((g) => {
        const m = normalize(g.attrs?.model);
        const b = normalize(g.attrs?.backdrop);
        if (modelLower && m !== modelLower) return false;
        if (backdropLower && b !== backdropLower) return false;
        return true;
      });

      const best = (filtered.length ? filtered : r.gifts)[0];
      const net = best.priceTon * (1 - PORTAL_FEE);
      text += `Portal:\n  ~${best.priceTon.toFixed(3)} TON (минимальный активный лот)\n`;
      text += `  Чистыми после комиссии ${(PORTAL_FEE * 100).toFixed(1)}%: ~${net.toFixed(3)} TON\n`;
    }
  }

  // MRKT floor
  if (user.filters.markets.includes('MRKT')) {
    const r = await mrktSearchLots(user);
    if (!r.ok) {
      if (r.reason === 'NO_AUTH') text += '\nMRKT: отключен (нет MRKT_AUTH)\n';
      else if (r.reason === 'AUTH_EXPIRED') text += '\nMRKT: токен протух (MRKT_AUTH)\n';
      else text += '\nMRKT: ошибка запроса\n';
    } else if (!r.gifts.length) {
      text += '\nMRKT: активных лотов по этим фильтрам нет\n';
    } else {
      const best = r.gifts[0];
      const net = best.priceTon * (1 - MRKT_FEE);
      text += `\nMRKT:\n  ~${best.priceTon.toFixed(3)} TON (минимальный активный лот)\n`;
      text += `  Комиссия ${(MRKT_FEE * 100).toFixed(1)}%: ~${net.toFixed(3)} TON чистыми\n`;
    }
  }

  text += '\nПодсказка:\n';
  text += '• Чтобы бот точно начал находить лоты — ставь /setmaxprice >= флора.\n';
  text += '• Чтобы ловить выгодные — ставь /setmaxprice ниже флора и жди.\n';

  await bot.sendMessage(chatId, text, { reply_markup: MAIN_KEYBOARD });
}

// ===== Commands =====
bot.onText(/^\/start\b/, (msg) => {
  getOrCreateUser(msg.from.id);
  bot.sendMessage(
    msg.chat.id,
    'Бот запущен (Portal + MRKT).\n\n' +
      'Быстрые команды (удобно с телефона):\n' +
      '/gift Victory Medal\n' +
      '/model queen bee\n' +
      '/backdrop amber\n' +
      '/clearmodel\n' +
      '/clearbackdrop\n\n' +
      'Кнопки снизу: фильтры, цена подарка, запуск поиска.',
    { reply_markup: MAIN_KEYBOARD }
  );
});

bot.onText(/^\/status\b/, async (msg) => {
  const user = getOrCreateUser(msg.from.id);
  const portalAuth = process.env.PORTAL_AUTH ? '✅' : '❌';
  const mrktAuth = process.env.MRKT_AUTH ? '✅' : '❌';

  let text = 'Статус:\n';
  text += `• Мониторинг: ${user.enabled ? 'включён' : 'выключен'}\n`;
  text += `• Макс. цена: ${user.maxPriceTon ? user.maxPriceTon.toFixed(3) : 'не задана'} TON\n`;
  text += `• Маркеты: ${formatMarkets(user.filters.markets)}\n`;
  text += `• Подарок: ${user.filters.gifts[0] || 'не выбран'}\n`;
  text += `• Модель: ${user.filters.models[0] || 'любая'}\n`;
  text += `• Фон: ${user.filters.backdrops[0] || 'любой'}\n\n`;
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
  clearUserSentDeals(msg.from.id);

  bot.sendMessage(msg.chat.id, `Ок. Макс. цена: ${v.toFixed(3)} TON`, { reply_markup: MAIN_KEYBOARD });
});

bot.onText(/^\/sellprice\b/, async (msg) => {
  const user = getOrCreateUser(msg.from.id);
  await sendSellPrice(msg.chat.id, msg.from.id, user);
});

bot.onText(/^\/gift\s+(.+)/i, async (msg, match) => {
  const q = normalize(match[1]);
  const user = getOrCreateUser(msg.from.id);

  const { byLowerName } = await portalCollections(300);
  const all = Array.from(byLowerName.values()).map((x) => x.name);
  const matched = all.filter((name) => name.toLowerCase().includes(q)).sort();

  if (!matched.length) {
    return bot.sendMessage(msg.chat.id, 'Подарок не найден. Проверь PORTAL_AUTH или попробуй другое название.', {
      reply_markup: MAIN_KEYBOARD,
    });
  }

  // если совпадение одно — ставим сразу
  if (matched.length === 1) {
    user.filters.gifts = [matched[0].toLowerCase()];
    user.filters.models = [];
    user.filters.backdrops = [];
    clearUserSentDeals(msg.from.id);
    return bot.sendMessage(msg.chat.id, `Подарок выбран: ${matched[0]}`, { reply_markup: MAIN_KEYBOARD });
  }

  // иначе — выбор кнопками
  const limited = matched.slice(0, MAX_SEARCH_RESULTS);
  return bot.sendMessage(msg.chat.id, 'Нашёл варианты, выбери:', {
    reply_markup: { inline_keyboard: buildInlineButtons('set_gift:', limited) },
  });
});

bot.onText(/^\/model\s+(.+)/i, async (msg, match) => {
  const user = getOrCreateUser(msg.from.id);
  if (!user.filters.gifts.length) {
    return bot.sendMessage(msg.chat.id, 'Сначала выбери подарок: /gift ... или 🎛 Фильтры', { reply_markup: MAIN_KEYBOARD });
  }

  const q = normalize(match[1]);
  const giftLower = user.filters.gifts[0];
  const { byLowerName } = await portalCollections(300);
  const col = byLowerName.get(giftLower);
  const shortName = portalShortName(col?.raw);
  const f = await portalCollectionFilters(shortName);

  if (!f) return bot.sendMessage(msg.chat.id, 'Не удалось получить модели (PORTAL_AUTH/лимиты).', { reply_markup: MAIN_KEYBOARD });

  const traits = extractTraitsWithRarity(f.models);
  const matched = traits.filter((t) => t.name.toLowerCase().includes(q)).slice(0, MAX_SEARCH_RESULTS);

  if (!matched.length) return bot.sendMessage(msg.chat.id, 'Модель не найдена по запросу.', { reply_markup: MAIN_KEYBOARD });

  if (matched.length === 1) {
    user.filters.models = [matched[0].name.toLowerCase()];
    clearUserSentDeals(msg.from.id);
    return bot.sendMessage(msg.chat.id, `Модель выбрана: ${matched[0].name}`, { reply_markup: MAIN_KEYBOARD });
  }

  return bot.sendMessage(msg.chat.id, 'Выбери модель:', {
    reply_markup: { inline_keyboard: buildInlineButtons('set_model:', matched.map((m) => m.name)) },
  });
});

bot.onText(/^\/backdrop\s+(.+)/i, async (msg, match) => {
  const user = getOrCreateUser(msg.from.id);
  if (!user.filters.gifts.length) {
    return bot.sendMessage(msg.chat.id, 'Сначала выбери подарок: /gift ... или 🎛 Фильтры', { reply_markup: MAIN_KEYBOARD });
  }

  const q = normalize(match[1]);
  const giftLower = user.filters.gifts[0];
  const { byLowerName } = await portalCollections(300);
  const col = byLowerName.get(giftLower);
  const shortName = portalShortName(col?.raw);
  const f = await portalCollectionFilters(shortName);

  if (!f) return bot.sendMessage(msg.chat.id, 'Не удалось получить фоны (PORTAL_AUTH/лимиты).', { reply_markup: MAIN_KEYBOARD });

  const traits = extractTraitsWithRarity(f.backdrops);
  const matched = traits.filter((t) => t.name.toLowerCase().includes(q)).slice(0, MAX_SEARCH_RESULTS);

  if (!matched.length) return bot.sendMessage(msg.chat.id, 'Фон не найден по запросу.', { reply_markup: MAIN_KEYBOARD });

  if (matched.length === 1) {
    user.filters.backdrops = [matched[0].name.toLowerCase()];
    clearUserSentDeals(msg.from.id);
    return bot.sendMessage(msg.chat.id, `Фон выбран: ${matched[0].name}`, { reply_markup: MAIN_KEYBOARD });
  }

  return bot.sendMessage(msg.chat.id, 'Выбери фон:', {
    reply_markup: { inline_keyboard: buildInlineButtons('set_backdrop:', matched.map((b) => b.name)) },
  });
});

bot.onText(/^\/clearmodel\b/i, (msg) => {
  const user = getOrCreateUser(msg.from.id);
  user.filters.models = [];
  clearUserSentDeals(msg.from.id);
  bot.sendMessage(msg.chat.id, 'Модель сброшена (теперь любая).', { reply_markup: MAIN_KEYBOARD });
});

bot.onText(/^\/clearbackdrop\b/i, (msg) => {
  const user = getOrCreateUser(msg.from.id);
  user.filters.backdrops = [];
  clearUserSentDeals(msg.from.id);
  bot.sendMessage(msg.chat.id, 'Фон сброшен (теперь любой).', { reply_markup: MAIN_KEYBOARD });
});

// ===== Buttons / generic message =====
bot.on('message', async (msg) => {
  const chatId = msg.chat.id;
  const userId = msg.from.id;
  const text = msg.text;
  if (!text) return;
  if (text.startsWith('/')) return;

  const user = getOrCreateUser(userId);

  if (text === '💰 Установить цену') {
    user.state = 'awaiting_max_price';
    return bot.sendMessage(chatId, 'Введи максимальную цену (TON). Пример: 80', { reply_markup: MAIN_KEYBOARD });
  }

  if (user.state === 'awaiting_max_price') {
    const v = n(text);
    if (!Number.isFinite(v) || v <= 0) return bot.sendMessage(chatId, 'Введи число TON. Пример: 80', { reply_markup: MAIN_KEYBOARD });
    user.maxPriceTon = v;
    user.state = null;
    clearUserSentDeals(userId);
    return bot.sendMessage(chatId, `Ок. Макс. цена: ${v.toFixed(3)} TON`, { reply_markup: MAIN_KEYBOARD });
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
    return sendSellPrice(chatId, userId, user);
  }

  if (text === '📌 Статус API') {
    const portalAuth = process.env.PORTAL_AUTH ? '✅' : '❌';
    const mrktAuth = process.env.MRKT_AUTH ? '✅' : '❌';
    return bot.sendMessage(chatId, `API статус:\nPortal auth: ${portalAuth}\nMRKT auth: ${mrktAuth}\n`, { reply_markup: MAIN_KEYBOARD });
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
          { text: '🅿 Только Portal', callback_data: 'set_markets_portal' },
          { text: '🅼 Только MRKT', callback_data: 'set_markets_mrkt' },
          { text: '🅿+🅼 Оба', callback_data: 'set_markets_all' },
        ],
        [
          { text: '♻️ Сбросить модель', callback_data: 'clear_model' },
          { text: '♻️ Сбросить фон', callback_data: 'clear_backdrop' },
        ],
        [{ text: 'ℹ️ Показать фильтры', callback_data: 'show_filters' }],
      ],
    };
    return bot.sendMessage(chatId, 'Настрой фильтры:', { reply_markup: inlineKeyboard });
  }

  // fallback
  bot.sendMessage(chatId, 'Используй /start или кнопки снизу.', { reply_markup: MAIN_KEYBOARD });
});

// ===== Callbacks =====
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
        await bot.sendMessage(chatId, 'Не удалось получить gifts. Проверь PORTAL_AUTH.', { reply_markup: MAIN_KEYBOARD });
      } else {
        await bot.sendMessage(chatId, 'Выбери подарок (первые 60):', {
          reply_markup: { inline_keyboard: buildInlineButtons('set_gift:', names.slice(0, 60)) },
        });
      }
    }

    else if (data === 'filter_model') {
      if (!user.filters.gifts.length) {
        await bot.sendMessage(chatId, 'Сначала выбери подарок.', { reply_markup: MAIN_KEYBOARD });
      } else {
        const { byLowerName } = await portalCollections(300);
        const col = byLowerName.get(user.filters.gifts[0]);
        const shortName = portalShortName(col?.raw);
        const f = await portalCollectionFilters(shortName);
        if (!f) {
          await bot.sendMessage(chatId, 'Не удалось получить модели (PORTAL_AUTH/лимиты).', { reply_markup: MAIN_KEYBOARD });
        } else {
          const traits = extractTraitsWithRarity(f.models);
          const names = traits.map((t) => {
            const r = rarityLabel(t);
            return r ? `${t.name} (${r})` : t.name;
          });
          // callback_data должен быть без “(‰)”, поэтому отправим чистые имена отдельно
          const pureNames = traits.map((t) => t.name).slice(0, 80);
          await bot.sendMessage(chatId, 'Выбери модель:', {
            reply_markup: { inline_keyboard: buildInlineButtons('set_model:', pureNames) },
          });
        }
      }
    }

    else if (data === 'filter_backdrop') {
      if (!user.filters.gifts.length) {
        await bot.sendMessage(chatId, 'Сначала выбери подарок.', { reply_markup: MAIN_KEYBOARD });
      } else {
        const { byLowerName } = await portalCollections(300);
        const col = byLowerName.get(user.filters.gifts[0]);
        const shortName = portalShortName(col?.raw);
        const f = await portalCollectionFilters(shortName);
        if (!f) {
          await bot.sendMessage(chatId, 'Не удалось получить фоны (PORTAL_AUTH/лимиты).', { reply_markup: MAIN_KEYBOARD });
        } else {
          const traits = extractTraitsWithRarity(f.backdrops);
          const pureNames = traits.map((t) => t.name).slice(0, 80);
          await bot.sendMessage(chatId, 'Выбери фон:', {
            reply_markup: { inline_keyboard: buildInlineButtons('set_backdrop:', pureNames) },
          });
        }
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
      await bot.sendMessage(chatId, 'Модель сброшена.', { reply_markup: MAIN_KEYBOARD });
    }

    else if (data === 'clear_backdrop') {
      user.filters.backdrops = [];
      clearUserSentDeals(userId);
      await bot.sendMessage(chatId, 'Фон сброшен.', { reply_markup: MAIN_KEYBOARD });
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
      const name = data.slice('set_gift:'.length).trim();
      user.filters.gifts = [name.toLowerCase()];
      user.filters.models = [];
      user.filters.backdrops = [];
      clearUserSentDeals(userId);
      await bot.sendMessage(chatId, `Подарок выбран: ${name}`, { reply_markup: MAIN_KEYBOARD });
    }

    else if (data.startsWith('set_model:')) {
      const name = data.slice('set_model:'.length).trim();
      user.filters.models = [name.toLowerCase()];
      clearUserSentDeals(userId);
      await bot.sendMessage(chatId, `Модель выбрана: ${name}`, { reply_markup: MAIN_KEYBOARD });
    }

    else if (data.startsWith('set_backdrop:')) {
      const name = data.slice('set_backdrop:'.length).trim();
      user.filters.backdrops = [name.toLowerCase()];
      clearUserSentDeals(userId);
      await bot.sendMessage(chatId, `Фон выбран: ${name}`, { reply_markup: MAIN_KEYBOARD });
    }
  } catch (e) {
    console.error('callback_query error:', e);
  }

  bot.answerCallbackQuery(query.id).catch(() => {});
});

// ===== Monitor (Portal + MRKT) =====
async function checkMarketsForAllUsers() {
  if (MODE !== 'real') return;
  if (users.size === 0) return;

  for (const [userId, user] of users.entries()) {
    if (!user.enabled) continue;
    if (!user.maxPriceTon) continue;

    // Portal monitor
    if (user.filters.markets.includes('Portal') && user.filters.gifts.length) {
      const { byLowerName } = await portalCollections(300);
      const col = byLowerName.get(user.filters.gifts[0]);
      const giftName = col?.name || user.filters.gifts[0];
      const collectionId = portalCollectionId(col?.raw);

      const model = user.filters.models[0] || null;
      const backdrop = user.filters.backdrops[0] || null;

      const r = await portalSearch({
        collectionId: collectionId || null,
        collectionName: collectionId ? null : giftName,
        models: model ? [model] : [],
        backdrops: backdrop ? [backdrop] : [],
        maxPrice: user.maxPriceTon,
      });

      if (r.ok && r.gifts.length) {
        for (const g of r.gifts) {
          if (g.priceTon > user.maxPriceTon) continue;

          // жёсткая проверка фильтров (если API вдруг вернул лишнее)
          const m = normalize(g.attrs?.model);
          const b = normalize(g.attrs?.backdrop);
          if (model && m !== model) continue;
          if (backdrop && b !== backdrop) continue;

          const key = `${userId}:${g.id}`;
          if (sentDeals.has(key)) continue;
          sentDeals.add(key);

          const lines = [];
          lines.push(`Найден лот (Portal) <= ${user.maxPriceTon} TON:`);
          lines.push(`${g.name}`);
          lines.push(`Цена: ${g.priceTon.toFixed(3)} TON`);
          if (g.attrs?.model) lines.push(`Модель: ${g.attrs.model}`);
          if (g.attrs?.backdrop) lines.push(`Фон: ${g.attrs.backdrop}`);
          lines.push(`Ссылка: ${g.urlTelegram || 'https://t.me/portals'}`);

          await bot.sendMessage(userId, lines.join('\n'), { disable_web_page_preview: true });
        }
      }
    }

    // MRKT monitor — только если есть живой токен
    if (user.filters.markets.includes('MRKT') && process.env.MRKT_AUTH) {
      const r = await mrktSearchLots(user);
      if (r.ok && r.gifts.length) {
        for (const g of r.gifts) {
          if (g.priceTon > user.maxPriceTon) continue;
          const key = `${userId}:${g.id}`;
          if (sentDeals.has(key)) continue;
          sentDeals.add(key);

          const lines = [];
          lines.push(`Найден лот (MRKT) <= ${user.maxPriceTon} TON:`);
          lines.push(`${g.name}`);
          lines.push(`Цена: ${g.priceTon.toFixed(3)} TON`);
          if (g.attrs?.model) lines.push(`Модель: ${g.attrs.model}`);
          if (g.attrs?.backdrop) lines.push(`Фон: ${g.attrs.backdrop}`);
          lines.push(`Ссылка: ${g.urlTelegram || 'https://t.me/mrkt'}`);

          await bot.sendMessage(userId, lines.join('\n'), { disable_web_page_preview: true });
        }
      }
    }
  }
}

setInterval(() => {
  checkMarketsForAllUsers().catch((e) => console.error('monitor error', e));
}, CHECK_INTERVAL_MS);

console.log('Бот запущен. /start');