const TelegramBot = require('node-telegram-bot-api');

const token = process.env.TELEGRAM_TOKEN;
const MODE = process.env.MODE || 'real'; // 'test' или 'real'
const CHECK_INTERVAL_MS = Number(process.env.CHECK_INTERVAL_MS || 5000);
const MAX_SEARCH_RESULTS = Number(process.env.MAX_SEARCH_RESULTS || 10);

// Комиссии маркетов (при желании можно менять через ENV)
const PORTAL_FEE = Number(process.env.PORTAL_FEE || 0.05); // 5%
const MRKT_FEE = Number(process.env.MRKT_FEE || 0);        // 0%

if (!token) {
  console.error('Ошибка: TELEGRAM_TOKEN не задан. Добавь токен бота в переменные окружения Railway.');
  process.exit(1);
}

console.log('Bot version 2026-02-18-sellprice-stable-v2');
console.log('Режим работы бота MODE =', MODE);

// Создаём Telegram-бота
const bot = new TelegramBot(token, { polling: true });

// настройки пользователей (userId -> {...})
const users = new Map();
// запоминаем, какие сделки уже отправляли (userId:giftId)
const sentDeals = new Set();

// кэш коллекций Portal
let collectionsCache = null; // { list: [...], byLowerName: Map(lowerName -> {name, shortName, raw}) }
let collectionsCacheTime = 0;
const COLLECTIONS_CACHE_TTL_MS = 60_000; // 60 секунд

// Основная клавиатура
const MAIN_KEYBOARD = {
  keyboard: [
    [{ text: '🔍 Запустить поиск' }, { text: '⏹ Остановить поиск' }],
    [{ text: '💰 Установить цену' }, { text: '💸 Цена подарка' }],
    [{ text: '🎛 Фильтры' }],
  ],
  resize_keyboard: true,
};

function clearUserSentDeals(userId) {
  const prefix = `${userId}:`;
  for (const key of Array.from(sentDeals)) {
    if (key.startsWith(prefix)) {
      sentDeals.delete(key);
    }
  }
}

function getOrCreateUser(userId) {
  if (!users.has(userId)) {
    users.set(userId, {
      maxPriceTon: null,
      enabled: true,
      state: null, // awaiting_max_price / awaiting_*_search
      filters: {
        gifts: [],
        models: [],
        backdrops: [],
        markets: ['Portal', 'MRKT'],
      },
    });
  }
  return users.get(userId);
}

function formatMarkets(markets) {
  if (!markets || !markets.length) return 'нет';
  if (markets.length === 2) return 'Portal + MRKT';
  return markets.join(', ');
}

function normalizeCollectionKey(name) {
  return String(name).toLowerCase().replace(/[^a-z0-9]+/g, '');
}

function buildInlineButtons(prefix, names) {
  const buttons = [];
  let row = [];
  for (const name of names) {
    row.push({
      text: name,
      callback_data: `${prefix}${name}`,
    });
    if (row.length === 2) {
      buttons.push(row);
      row = [];
    }
  }
  if (row.length) buttons.push(row);
  return buttons;
}

// =====================
// Команды
// =====================

bot.onText(/^\/start\b/, (msg) => {
  const chatId = msg.chat.id;
  getOrCreateUser(msg.from.id);

  const text =
    'Бот запущен.\n\n' +
    `Режим: ${MODE === 'test' ? 'ТЕСТОВЫЙ (случайные цены)' : 'РЕАЛЬНЫЕ ЦЕНЫ с Portal + MRKT'}\n\n` +
    'Кнопки снизу:\n' +
    '🔍 Запустить поиск — включить мониторинг\n' +
    '⏹ Остановить поиск — выключить мониторинг\n' +
    '💰 Установить цену — задать максимум в TON для уведомлений (нажми и потом просто введи число)\n' +
    '💸 Цена подарка — оценить рыночную цену продажи (Portal + MRKT)\n' +
    '🎛 Фильтры — выбрать подарки/модели/фоны/маркеты\n\n' +
    'Можно также прислать ссылку вида https://t.me/nft/..., и я посчитаю примерную цену продажи по коллекции.';

  bot.sendMessage(chatId, text, { reply_markup: MAIN_KEYBOARD });
});

bot.onText(/^\/help\b/, (msg) => {
  const chatId = msg.chat.id;
  const text =
    'Бот отслеживает NFT‑подарки в Portal и MRKT.\n\n' +
    'Кнопки:\n' +
    '🔍 Запустить поиск — начать слать найденные гифты\n' +
    '⏹ Остановить поиск — временно остановить\n' +
    '💰 Установить цену — нажми и просто введи число (макс. цена для уведомлений)\n' +
    '💸 Цена подарка — оценка цены продажи по текущим фильтрам (Portal + MRKT)\n' +
    '🎛 Фильтры — подарки / модели / фоны / маркеты\n\n' +
    'Команды:\n' +
    '/setmaxprice 0.5 — задать цену для уведомлений вручную\n' +
    '/status — показать настройки\n' +
    '/listgifts — список подарков из Portal\n' +
    '/listmodels — модели/фоны для выбранного подарка (с редкостью)\n' +
    '/sellprice — то же, что кнопка "💸 Цена подарка"\n\n' +
    'Также можно просто прислать ссылку на гифт (https://t.me/nft/...), и я оценю цену по коллекции.';

  bot.sendMessage(chatId, text, { reply_markup: MAIN_KEYBOARD });
});

// /setmaxprice <число>
bot.onText(/^\/setmaxprice\b(?:\s+(.+))?/, (msg, match) => {
  const chatId = msg.chat.id;
  const userId = msg.from.id;
  const arg = match[1];

  if (!arg) {
    bot.sendMessage(chatId, 'Укажи цену в TON.\nНапример:\n/setmaxprice 0.5');
    return;
  }

  const value = parseFloat(arg.replace(',', '.'));
  if (Number.isNaN(value) || value <= 0) {
    bot.sendMessage(chatId, 'Некорректная цена. Введи положительное число, например: 0.3');
    return;
  }

  const user = getOrCreateUser(userId);
  user.maxPriceTon = value;
  clearUserSentDeals(userId);

  bot.sendMessage(
    chatId,
    `Максимальная цена установлена: ${value.toFixed(3)} TON.`,
    { reply_markup: MAIN_KEYBOARD }
  );
});

bot.onText(/^\/status\b/, (msg) => {
  const chatId = msg.chat.id;
  const userId = msg.from.id;
  const user = getOrCreateUser(userId);

  let text = 'Твои настройки:\n';

  if (user.maxPriceTon) {
    text += `• Макс. цена для уведомлений: ${user.maxPriceTon.toFixed(3)} TON\n`;
  } else {
    text += '• Макс. цена для уведомлений: не задана (кнопка "💰 Установить цену")\n';
  }

  text += `• Мониторинг: ${user.enabled ? 'включён' : 'выключен'}\n`;
  text += `• Маркеты: ${formatMarkets(user.filters.markets)}\n`;

  if (user.filters.gifts.length) {
    text += `• Фильтр по подаркам: ${user.filters.gifts.join(', ')}\n`;
  } else {
    text += '• Фильтр по подаркам: нет\n';
  }

  if (user.filters.models.length) {
    text += `• Фильтр по моделям: ${user.filters.models.join(', ')}\n`;
  } else {
    text += '• Фильтр по моделям: нет\n';
  }

  if (user.filters.backdrops.length) {
    text += `• Фильтр по фонам: ${user.filters.backdrops.join(', ')}\n`;
  } else {
    text += '• Фильтр по фонам: нет\n';
  }

  text += `\nРежим: ${MODE === 'test' ? 'ТЕСТОВЫЙ (случайные цены)' : 'РЕАЛЬНЫЕ ЦЕНЫ (Portal + MRKT)'}.\n`;

  bot.sendMessage(chatId, text, { reply_markup: MAIN_KEYBOARD });
});

// /sellprice
bot.onText(/^\/sellprice\b/, async (msg) => {
  const chatId = msg.chat.id;
  const userId = msg.from.id;
  const user = getOrCreateUser(userId);

  try {
    await sendSellPriceForUser(chatId, user);
  } catch (e) {
    console.error('/sellprice error:', e);
    bot.sendMessage(chatId, 'Не удалось получить цены продажи. Попробуй позже.', {
      reply_markup: MAIN_KEYBOARD,
    });
  }
});

// =====================
// Portal helpers
// =====================

const API_URL = 'https://portal-market.com/api/';
const SORTS = {
  latest: '&sort_by=listed_at+desc',
  price_asc: '&sort_by=price+asc',
  price_desc: '&sort_by=price+desc',
  gift_id_asc: '&sort_by=external_collection_number+asc',
  gift_id_desc: '&sort_by=external_collection_number+desc',
  model_rarity_asc: '&sort_by=model_rarity+asc',
  model_rarity_desc: '&sort_by=model_rarity+desc',
};

const MRKT_API_URL = 'https://api.tgmrkt.io/api/v1';

function cap(text) {
  return String(text).replace(/\w+(?:'\w+)?/g, (word) => {
    return word.charAt(0).toUpperCase() + word.slice(1);
  });
}

function quotePlus(str) {
  return encodeURIComponent(str).replace(/%20/g, '+');
}

function listToURL(list) {
  return list.map((s) => quotePlus(cap(s))).join('%2C');
}

function buildPortalHeaders(auth) {
  const headers = {
    Authorization: auth,
    Accept: 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9,ru;q=0.8',
    Origin: 'https://portal-market.com',
    Referer: 'https://portal-market.com/',
    'User-Agent':
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
  };
  return headers;
}

function buildPortalTgSlug(nft, baseName, number) {
  const rawTgId = nft.tg_id ? String(nft.tg_id) : '';
  if (rawTgId && rawTgId.includes('-')) {
    return rawTgId;
  }
  if (baseName && number != null) {
    const slugName = String(baseName).replace(/['’\s-]+/g, '');
    return `${slugName}-${number}`;
  }
  return null;
}

async function portalCollections(limit = 200) {
  const now = Date.now();
  if (collectionsCache && now - collectionsCacheTime < COLLECTIONS_CACHE_TTL_MS) {
    return collectionsCache;
  }

  const authData = process.env.PORTAL_AUTH;
  if (!authData) {
    console.warn('PORTAL_AUTH не задан, Portal collections будет пропущен.');
    return { list: [], byLowerName: new Map() };
  }

  const url = `${API_URL}collections?limit=${limit}`;

  let res;
  try {
    res = await fetch(url, {
      method: 'GET',
      headers: buildPortalHeaders(authData),
    });
  } catch (e) {
    console.error('Portal collections fetch error:', e);
    return { list: [], byLowerName: new Map() };
  }

  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    console.error('Portal collections HTTP error', res.status, txt.slice(0, 200));
    return { list: [], byLowerName: new Map() };
  }

  const data = await res.json().catch((e) => {
    console.error('Portal collections JSON parse error:', e);
    return null;
  });
  if (!data) return { list: [], byLowerName: new Map() };

  const arr = Array.isArray(data.collections) ? data.collections : Array.isArray(data) ? data : [];
  const byLowerName = new Map();

  for (const col of arr) {
    const name = (col.name || col.title || '').trim();
    const shortName = (col.short_name || col.shortName || '').trim();
    if (!name) continue;
    const lower = name.toLowerCase();
    byLowerName.set(lower, { name, shortName, raw: col });
  }

  collectionsCache = { list: arr, byLowerName };
  collectionsCacheTime = now;
  return collectionsCache;
}

// filters: новый/старый формат
async function portalCollectionFilters(shortName) {
  const authData = process.env.PORTAL_AUTH;
  if (!authData) {
    console.warn('PORTAL_AUTH не задан, collection filters будет пропущен.');
    return null;
  }
  if (!shortName) return null;

  const url = `${API_URL}collections/filters?short_names=${encodeURIComponent(shortName)}`;

  let res;
  try {
    res = await fetch(url, {
      method: 'GET',
      headers: buildPortalHeaders(authData),
    });
  } catch (e) {
    console.error('Portal collection filters fetch error:', e);
    return null;
  }

  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    console.error('Portal collection filters HTTP error', res.status, txt.slice(0, 200));
    return null;
  }

  const data = await res.json().catch((e) => {
    console.error('Portal collection filters JSON parse error:', e);
    return null;
  });
  if (!data) return null;

  if (data.collections && typeof data.collections === 'object') {
    const keys = Object.keys(data.collections);
    let key = keys.find((k) => k.toLowerCase() === shortName.toLowerCase()) || shortName;
    const colBlock = data.collections[key];
    if (!colBlock) {
      console.warn('Portal collection filters: не нашёл блок для', shortName);
      return null;
    }
    return {
      models: colBlock.models || [],
      backdrops: colBlock.backdrops || [],
    };
  }

  if (data.floor_prices && typeof data.floor_prices === 'object') {
    let key = shortName;
    if (!data.floor_prices[key]) {
      const keys = Object.keys(data.floor_prices);
      const found = keys.find((k) => k.toLowerCase() === shortName.toLowerCase());
      if (!found) return null;
      key = found;
    }
    const block = data.floor_prices[key];
    if (!block) return null;
    return {
      models: block.models || [],
      backdrops: block.backdrops || [],
    };
  }

  console.error('Portal collection filters: неожиданный формат ответа.');
  return null;
}

function extractTraitsWithRarity(block) {
  const map = new Map();

  if (!block) return [];

  if (Array.isArray(block)) {
    for (const item of block) {
      if (!item) continue;
      let name = null;
      let rarityPerMille = null;
      let rarityName = null;

      if (typeof item === 'string') {
        name = item;
      } else {
        name = item.name || item.model || item.value || null;
        if (item.rarity_per_mille != null) rarityPerMille = Number(item.rarity_per_mille);
        else if (item.rarityPermille != null) rarityPerMille = Number(item.rarityPermille);
        rarityName = item.rarityName || item.rarity_name || null;
      }

      if (!name) continue;
      const lower = name.toLowerCase().trim();
      if (!map.has(lower)) {
        map.set(lower, { name, rarityPerMille, rarityName });
      }
    }
  } else if (typeof block === 'object') {
    for (const [key, val] of Object.entries(block)) {
      const name = key;
      let rarityPerMille = null;
      let rarityName = null;
      if (val && typeof val === 'object') {
        if (val.rarity_per_mille != null) rarityPerMille = Number(val.rarity_per_mille);
        else if (val.rarityPermille != null) rarityPerMille = Number(val.rarityPermille);
        rarityName = val.rarityName || val.rarity_name || null;
      }
      const lower = name.toLowerCase().trim();
      if (!map.has(lower)) {
        map.set(lower, { name, rarityPerMille, rarityName });
      }
    }
  }

  const arr = Array.from(map.values());
  arr.sort((a, b) => {
    const ra = a.rarityPerMille != null ? a.rarityPerMille : Infinity;
    const rb = b.rarityPerMille != null ? b.rarityPerMille : Infinity;
    if (ra === rb) return a.name.localeCompare(b.name);
    return ra - rb;
  });

  return arr;
}

function formatRarityLabel(trait) {
  if (!trait) return '';
  if (trait.rarityName) return trait.rarityName;
  if (trait.rarityPerMille != null) {
    const p = Number(trait.rarityPerMille);
    if (!Number.isFinite(p)) return '';
    const rounded = Number(p.toFixed(1));
    if (Number.isInteger(rounded)) return `${rounded}%`;
    return `${rounded}%`;
  }
  return '';
}

// =====================
// /listgifts и /listmodels
// =====================

bot.onText(/^\/listgifts\b/, async (msg) => {
  const chatId = msg.chat.id;

  const { byLowerName } = await portalCollections(200);
  const names = Array.from(byLowerName.values()).map((x) => x.name);

  if (!names.length) {
    bot.sendMessage(chatId, 'Подарков сейчас не найдено (Portal collections).');
    return;
  }

  const lines = names.sort().map((n) => `- ${n}`);
  let text = 'Подарки (из Portal collections):\n' + lines.join('\n');
  if (text.length > 4000) text = text.slice(0, 3990) + '\n...';

  bot.sendMessage(chatId, text, { reply_markup: MAIN_KEYBOARD });
});

bot.onText(/^\/listmodels\b/, async (msg) => {
  const chatId = msg.chat.id;
  const userId = msg.from.id;
  const user = getOrCreateUser(userId);

  if (!user.filters.gifts.length) {
    bot.sendMessage(
      chatId,
      'Сначала выбери подарок через фильтр (кнопка "🎛 Фильтры" → "🎁 Выбрать подарок" или "🔍 Подарок").',
      { reply_markup: MAIN_KEYBOARD }
    );
    return;
  }

  const giftLower = user.filters.gifts[0];
  const { byLowerName } = await portalCollections(200);
  const col = byLowerName.get(giftLower);
  if (!col) {
    bot.sendMessage(
      chatId,
      'Не нашёл такой подарок в Portal collections (возможно другое название).',
      { reply_markup: MAIN_KEYBOARD }
    );
    return;
  }

  const filters = await portalCollectionFilters(col.shortName);
  if (!filters) {
    bot.sendMessage(
      chatId,
      'Не удалось получить модели/фоны для этой коллекции (collections/filters).',
      { reply_markup: MAIN_KEYBOARD }
    );
    return;
  }

  const modelTraits = extractTraitsWithRarity(filters.models);
  const backdropTraits = extractTraitsWithRarity(filters.backdrops);

  let text = `Подарок: ${col.name}\n\nМодели (по редкости):\n`;
  if (modelTraits.length) {
    text += modelTraits
      .map((m) => {
        const r = formatRarityLabel(m);
        return r ? `- ${m.name} (${r})` : `- ${m.name}`;
      })
      .join('\n');
  } else {
    text += '(нет данных)\n';
  }

  text += '\n\nФоны:\n';
  if (backdropTraits.length) {
    text += backdropTraits
      .map((b) => {
        const r = formatRarityLabel(b);
        return r ? `- ${b.name} (${r})` : `- ${b.name}`;
      })
      .join('\n');
  } else {
    text += '(нет данных)\n';
  }

  if (text.length > 4000) text = text.slice(0, 3990) + '\n...';

  bot.sendMessage(chatId, text, { reply_markup: MAIN_KEYBOARD });
});

// =====================
// Callback-кнопки (фильтры, выбор маркетов)
// =====================

bot.on('callback_query', async (query) => {
  const userId = query.from.id;
  const chatId = query.message.chat.id;
  const data = query.data || '';
  const user = getOrCreateUser(userId);

  try {
    if (data === 'filter_gift') {
      const { byLowerName } = await portalCollections(200);
      const names = Array.from(byLowerName.values()).map((x) => x.name);
      if (!names.length) {
        await bot.sendMessage(chatId, 'Сейчас подарков не найдено (Portal collections).');
      } else {
        const inline_keyboard = buildInlineButtons('set_gift:', names.sort());
        await bot.sendMessage(chatId, 'Выбери подарок:', {
          reply_markup: { inline_keyboard },
        });
      }
    } else if (data.startsWith('set_gift:')) {
      const originalName = data.slice('set_gift:'.length);
      const key = originalName.toLowerCase().trim();
      user.filters.gifts = [key];
      user.filters.models = [];
      user.filters.backdrops = [];
      clearUserSentDeals(userId);
      await bot.sendMessage(chatId, `Фильтр по подарку установлен: ${key}`, {
        reply_markup: MAIN_KEYBOARD,
      });
    } else if (data === 'set_markets_portal') {
      user.filters.markets = ['Portal'];
      clearUserSentDeals(userId);
      await bot.sendMessage(chatId, 'Теперь поиск только в Portal.', {
        reply_markup: MAIN_KEYBOARD,
      });
    } else if (data === 'set_markets_mrkt') {
      user.filters.markets = ['MRKT'];
      clearUserSentDeals(userId);
      await bot.sendMessage(chatId, 'Теперь поиск только в MRKT.', {
        reply_markup: MAIN_KEYBOARD,
      });
    } else if (data === 'set_markets_all') {
      user.filters.markets = ['Portal', 'MRKT'];
      clearUserSentDeals(userId);
      await bot.sendMessage(chatId, 'Теперь поиск в Portal + MRKT.', {
        reply_markup: MAIN_KEYBOARD,
      });
    }
  } catch (e) {
    console.error('callback_query error:', e);
  }

  bot.answerCallbackQuery(query.id).catch(() => {});
});

// =====================
// Общий on('message') (кнопки + state + ссылка t.me/nft)
// =====================

bot.on('message', async (msg) => {
  const chatId = msg.chat.id;
  const userId = msg.from.id;
  const text = msg.text;
  if (!text) return;
  if (text.startsWith('/')) return;

  const user = getOrCreateUser(userId);
  const trimmed = text.trim();

  // 1) state: ожидание цены после "💰 Установить цену"
  if (user.state === 'awaiting_max_price') {
    const value = parseFloat(trimmed.replace(',', '.'));
    if (Number.isNaN(value) || value <= 0) {
      bot.sendMessage(chatId, 'Некорректная цена. Введи положительное число, например: 0.3');
      return;
    }
    user.maxPriceTon = value;
    user.state = null;
    clearUserSentDeals(userId);
    bot.sendMessage(
      chatId,
      `Максимальная цена установлена: ${value.toFixed(3)} TON.`,
      { reply_markup: MAIN_KEYBOARD }
    );
    return;
  }

  // 2) ссылка t.me/nft/... → оценка sellprice по коллекции
  const nftMatch = trimmed.match(/https?:\/\/t\.me\/nft\/([^\s]+)/i);
  if (nftMatch) {
    const slug = nftMatch[1];
    try {
      await handleNftLinkSellPrice(chatId, user, slug);
    } catch (e) {
      console.error('handleNftLinkSellPrice error:', e);
      bot.sendMessage(
        chatId,
        'Не удалось распознать этот подарок или получить цены продажи.\nПопробуй выбрать подарок через "🎛 Фильтры" и затем "💸 Цена подарка".',
        { reply_markup: MAIN_KEYBOARD }
      );
    }
    return;
  }

  // 3) кнопки
  if (trimmed === '💰 Установить цену') {
    user.state = 'awaiting_max_price';
    bot.sendMessage(
      chatId,
      'Введи максимальную цену в TON.\nНапример: 4.5',
      { reply_markup: MAIN_KEYBOARD }
    );
    return;
  }

  if (trimmed === '💸 Цена подарка') {
    try {
      await sendSellPriceForUser(chatId, user);
    } catch (e) {
      console.error('button 💸 error:', e);
      bot.sendMessage(
        chatId,
        'Не удалось получить цены продажи. Попробуй позже.',
        { reply_markup: MAIN_KEYBOARD }
      );
    }
    return;
  }

  if (trimmed === '🔍 Запустить поиск') {
    user.enabled = true;
    bot.sendMessage(chatId, 'Мониторинг включён. Бот будет отправлять подходящие гифты.', {
      reply_markup: MAIN_KEYBOARD,
    });
    return;
  }

  if (trimmed === '⏹ Остановить поиск') {
    user.enabled = false;
    bot.sendMessage(chatId, 'Мониторинг остановлен.', {
      reply_markup: MAIN_KEYBOARD,
    });
    return;
  }

  if (trimmed === '🎛 Фильтры') {
    const inlineKeyboard = {
      inline_keyboard: [
        [{ text: '🎁 Выбрать подарок', callback_data: 'filter_gift' }],
        [
          { text: '🅿 Только Portal', callback_data: 'set_markets_portal' },
          { text: '🅼 Только MRKT', callback_data: 'set_markets_mrkt' },
          { text: '🅿+🅼 Оба', callback_data: 'set_markets_all' },
        ],
      ],
    };

    bot.sendMessage(chatId, 'Выбери, что настроить:', {
      reply_markup: inlineKeyboard,
    });
    return;
  }

  bot.sendMessage(
    chatId,
    'Используй кнопки снизу или команды /help и /status.\nМожно также прислать ссылку на гифт (https://t.me/nft/...), чтобы оценить цену продажи по коллекции.',
    { reply_markup: MAIN_KEYBOARD }
  );
});

// =====================
// TEST-режим
// =====================

function fetchTestGifts() {
  function randomPrice() {
    return 0.1 + Math.random() * 0.9;
  }

  return [
    {
      id: 'portal_test_1',
      market: 'Portal',
      name: 'Test Gift #1',
      baseName: 'Test Gift',
      priceTon: randomPrice(),
      urlTelegram: 'https://t.me/portals',
      urlMarket: 'https://t.me/portals',
      attrs: {},
    },
  ];
}

// =====================
// REAL-режим: Portal search
// =====================

async function portalSearch({
  sort = 'price_asc',
  offset = 0,
  limit = 20,
  giftNames = [],
  models = [],
  backdrops = [],
  symbols = [],
  minPrice = 0,
  maxPrice = 100000,
}) {
  const authData = process.env.PORTAL_AUTH;
  if (!authData) {
    console.warn('PORTAL_AUTH не задан, Portal API будет пропущен.');
    return [];
  }

  let url = `${API_URL}nfts/search?offset=${offset}&limit=${limit}`;
  url += SORTS[sort] || SORTS.price_asc;

  minPrice = Number(minPrice) || 0;
  maxPrice = Number(maxPrice) || 100000;

  if (maxPrice < 100000) {
    url += `&min_price=${minPrice}&max_price=${maxPrice}`;
  }

  const g = giftNames.filter(Boolean);
  if (g.length) {
    if (g.length === 1) {
      url += `&filter_by_collections=${quotePlus(cap(g[0]))}`;
    } else {
      url += `&filter_by_collections=${listToURL(g)}`;
    }
  }

  const m = models.filter(Boolean);
  if (m.length) {
    if (m.length === 1) {
      url += `&filter_by_models=${quotePlus(cap(m[0]))}`;
    } else {
      url += `&filter_by_models=${listToURL(m)}`;
    }
  }

  const b = backdrops.filter(Boolean);
  if (b.length) {
    if (b.length === 1) {
      url += `&filter_by_backdrops=${quotePlus(cap(b[0]))}`;
    } else {
      url += `&filter_by_backdrops=${listToURL(b)}`;
    }
  }

  const s = symbols.filter(Boolean);
  if (s.length) {
    if (s.length === 1) {
      url += `&filter_by_symbols=${quotePlus(cap(s[0]))}`;
    } else {
      url += `&filter_by_symbols=${listToURL(s)}`;
    }
  }

  url += '&status=listed';

  let res;
  try {
    res = await fetch(url, {
      method: 'GET',
      headers: buildPortalHeaders(authData),
    });
  } catch (e) {
    console.error('Portal fetch error:', e);
    return [];
  }

  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    console.error('Portal HTTP error', res.status, txt.slice(0, 200));
    return [];
  }

  const data = await res.json().catch((e) => {
    console.error('Portal JSON parse error:', e);
    return null;
  });
  if (!data) return [];

  let results = [];
  if (Array.isArray(data.results)) {
    results = data.results;
  } else if (Array.isArray(data)) {
    results = data;
  } else {
    console.error('Portal: неожиданный формат ответа, ожидается массив или {results:[...]}');
    return [];
  }

  const gifts = [];
  for (const nft of results) {
    if (!nft) continue;

    const priceStr = nft.price || nft.floor_price;
    const priceTon = priceStr ? Number(priceStr) : NaN;
    if (!priceTon || Number.isNaN(priceTon)) continue;

    let model = null;
    let symbol = null;
    let backdrop = null;

    if (Array.isArray(nft.attributes)) {
      for (const attr of nft.attributes) {
        if (!attr || !attr.type) continue;
        if (attr.type === 'model') model = attr.value;
        else if (attr.type === 'symbol') symbol = attr.value;
        else if (attr.type === 'backdrop') backdrop = attr.value;
      }
    }

    const baseName = nft.name || 'NFT';

    let number = null;
    if (nft.external_collection_number) {
      number = nft.external_collection_number;
    } else if (nft.tg_id) {
      const parts = String(nft.tg_id).split('-');
      const last = parts[parts.length - 1];
      if (/^\d+$/.test(last)) {
        number = last;
      }
    }

    let displayName = baseName;
    if (number) displayName = `${displayName} #${number}`;

    const tgSlug = buildPortalTgSlug(nft, baseName, number);

    let tgUrl = 'https://t.me/portals';
    if (tgSlug) {
      tgUrl = `https://t.me/nft/${tgSlug}`;
    }

    let marketUrl = 'https://t.me/portals';
    if (nft.id) {
      marketUrl = `https://t.me/portals_market_bot/market?startapp=gift_${nft.id}`;
    }

    gifts.push({
      id: `portal_${nft.id}`,
      market: 'Portal',
      name: displayName,
      baseName,
      priceTon,
      urlTelegram: tgUrl,
      urlMarket: marketUrl,
      attrs: { model, symbol, backdrop },
    });
  }

  gifts.sort((a, b) => a.priceTon - b.priceTon);
  return gifts;
}

// =====================
// MRKT: /gifts/saling (MRKT_AUTH из ENV)
// =====================

async function fetchMrktGiftsForUser(user) {
  const token = process.env.MRKT_AUTH;
  if (!token) {
    console.warn('MRKT_AUTH не задан, MRKT будет пропущен.');
    return [];
  }

  const collFilter = user.filters.gifts.map((x) => cap(x.trim()));
  const modelFilter = user.filters.models.map((x) => cap(x.trim()));
  const backdropFilter = user.filters.backdrops.map((x) => cap(x.trim()));

  const body = {
    count: 20,
    cursor: '',
    collectionNames: collFilter,
    modelNames: modelFilter,
    backdropNames: backdropFilter,
    symbolNames: [],
    ordering: 'None',
    lowToHigh: false,
    maxPrice: null,
    minPrice: null,
    giftType: null,
    isCrafted: null,
    isNew: null,
    isPremarket: null,
    isTransferable: null,
    luckyBuy: null,
    removeSelfSales: null,
    craftable: null,
    tgCanBeCraftedFrom: null,
  };

  console.log('MRKT /gifts/saling body:', JSON.stringify(body));

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
    return [];
  }

  console.log('MRKT /gifts/saling status:', res.status);

  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    console.error('MRKT HTTP error', res.status, txt.slice(0, 200));
    return [];
  }

  const data = await res.json().catch((e) => {
    console.error('MRKT JSON parse error:', e);
    return null;
  });
  if (!data || !Array.isArray(data.gifts)) {
    console.error('MRKT: неожиданный формат ответа, ожидается {gifts:[...]}');
    return [];
  }

  const rawGifts = data.gifts;
  console.log('MRKT gifts length:', rawGifts.length);

  const gifts = [];

  for (const g of rawGifts) {
    if (!g) continue;

    let priceNano = null;
    if (g.salePrice != null) priceNano = g.salePrice;
    else if (g.salePriceWithoutFee != null) priceNano = g.salePriceWithoutFee;
    if (priceNano == null) continue;

    const priceTon = Number(priceNano) / 1e9;
    if (!priceTon || Number.isNaN(priceTon)) continue;
    if (user.maxPriceTon && priceTon > user.maxPriceTon) continue;

    const baseName = (g.collectionTitle || g.collectionName || g.title || 'MRKT Gift').trim();
    const number = g.number ?? null;
    let displayName = baseName;
    if (number) displayName += ` #${number}`;

    const model = g.modelTitle || g.modelName || null;
    const symbol = g.symbolName || null;
    const backdrop = g.backdropName || null;

    let urlTelegram = 'https://t.me/mrkt';
    if (g.name && String(g.name).includes('-')) {
      urlTelegram = `https://t.me/nft/${g.name}`;
    }

    let urlMarket = 'https://t.me/mrkt';
    if (g.id) {
      const appId = String(g.id).replace(/-/g, '');
      urlMarket = `https://t.me/mrkt/app?startapp=${appId}`;
    }

    const giftId = g.id || `${baseName}_${model || ''}_${number || ''}_${priceTon}`;

    gifts.push({
      id: `mrkt_${giftId}`,
      market: 'MRKT',
      name: displayName,
      baseName,
      priceTon,
      urlTelegram,
      urlMarket,
      attrs: {
        collection: baseName,
        model,
        symbol,
        backdrop,
      },
    });
  }

  gifts.sort((a, b) => a.priceTon - b.priceTon);
  console.log('MRKT gifts after filter:', gifts.length);
  return gifts;
}

// =====================
// Sell Price helpers
// =====================

async function getPortalFloorForUserFilters(user) {
  const markets = user.filters.markets || ['Portal', 'MRKT'];
  if (!markets.includes('Portal')) return null;

  const gifts = await portalSearch({
    sort: 'price_asc',
    offset: 0,
    limit: 50,
    giftNames: user.filters.gifts.map((x) => x.trim()),
    models: user.filters.models.map((x) => x.trim()),
    backdrops: user.filters.backdrops.map((x) => x.trim()),
    minPrice: 0,
    maxPrice: 1_000_000,
  });

  if (!gifts || !gifts.length) return null;
  return gifts[0].priceTon;
}

async function getMrktFloorForUserFilters(user) {
  const markets = user.filters.markets || ['Portal', 'MRKT'];
  if (!markets.includes('MRKT')) return null;

  const originalMax = user.maxPriceTon;
  user.maxPriceTon = null;
  try {
    const gifts = await fetchMrktGiftsForUser(user);
    if (!gifts || !gifts.length) return null;
    return gifts[0].priceTon;
  } finally {
    user.maxPriceTon = originalMax;
  }
}

async function sendSellPriceForUser(chatId, user) {
  if (!user.filters.gifts.length) {
    await bot.sendMessage(
      chatId,
      'Сначала выбери подарок (через "🎛 Фильтры" → "🎁 Выбрать подарок" или пришли ссылку https://t.me/nft/...).',
      { reply_markup: MAIN_KEYBOARD }
    );
    return;
  }

  const giftLower = user.filters.gifts[0];
  const { byLowerName } = await portalCollections(200);
  const col = byLowerName.get(giftLower);

  const giftName = col ? col.name : giftLower;
  const modelName = user.filters.models.length ? user.filters.models[0] : null;
  const backdropName = user.filters.backdrops.length ? user.filters.backdrops[0] : null;

  const markets = user.filters.markets || ['Portal', 'MRKT'];

  let portalFloor = null;
  let mrktFloor = null;

  try {
    portalFloor = await getPortalFloorForUserFilters(user);
  } catch (e) {
    console.error('getPortalFloorForUserFilters error:', e);
  }

  try {
    mrktFloor = await getMrktFloorForUserFilters(user);
  } catch (e) {
    console.error('getMrktFloorForUserFilters error:', e);
  }

  let text = 'Оценка цен продажи:\n\n';
  text += `Подарок: ${giftName}\n`;
  text += `Модель: ${modelName || 'любая'}\n`;
  text += `Фон: ${backdropName || 'любой'}\n\n`;

  const floors = [];

  if (markets.includes('Portal')) {
    if (portalFloor != null) {
      const net = portalFloor * (1 - PORTAL_FEE);
      text += `Portal:\n  ~${portalFloor.toFixed(3)} TON (минимальный активный лот)\n`;
      text += `  Чистыми после комиссии ${(PORTAL_FEE * 100).toFixed(1)}%: ~${net.toFixed(
        3
      )} TON\n`;
      floors.push({ market: 'Portal', floor: portalFloor, net });
    } else {
      text += 'Portal: нет активных лотов по этим фильтрам\n';
    }
  }

  if (markets.includes('MRKT')) {
    if (mrktFloor != null) {
      const net = mrktFloor * (1 - MRKT_FEE);
      text += `MRKT:\n  ~${mrktFloor.toFixed(3)} TON (минимальный активный лот)\n`;
      text += `  Комиссия ${(MRKT_FEE * 100).toFixed(1)}%: ~${net.toFixed(3)} TON чистыми\n`;
      floors.push({ market: 'MRKT', floor: mrktFloor, net });
    } else {
      text += 'MRKT: нет активных лотов по этим фильтрам\n';
    }
  }

  if (floors.length) {
    const minFloor = Math.min(...floors.map((f) => f.floor));
    text += `\nЕсли хочешь продать БЫСТРО — ставь цену около ${minFloor.toFixed(
      3
    )} TON (или чуть ниже минимального лота на самом дешёвом рынке).\n`;
  } else {
    text += '\nСейчас по этим фильтрам нет активных лотов — ориентироваться не на что.\n';
  }

  await bot.sendMessage(chatId, text, { reply_markup: MAIN_KEYBOARD });
}

async function handleNftLinkSellPrice(chatId, user, slug) {
  let baseSlug = slug;
  const parts = slug.split('-');
  const last = parts[parts.length - 1];
  if (/^\d+$/.test(last)) {
    parts.pop();
    baseSlug = parts.join('-');
  }

  const slugNorm = normalizeCollectionKey(baseSlug);

  const { byLowerName } = await portalCollections(200);
  const values = Array.from(byLowerName.values());

  let matched = null;
  for (const col of values) {
    const colNorm = normalizeCollectionKey(col.name);
    if (colNorm === slugNorm) {
      matched = col;
      break;
    }
  }

  if (!matched) {
    await bot.sendMessage(
      chatId,
      'Не смог распознать подарок по этой ссылке.\nПока по ссылке определяется только КОЛЛЕКЦИЯ (без модели и фона).',
      { reply_markup: MAIN_KEYBOARD }
    );
    return;
  }

  const giftLower = matched.name.toLowerCase().trim();
  user.filters.gifts = [giftLower];
  user.filters.models = [];
  user.filters.backdrops = [];
  clearUserSentDeals(chatId);

  await bot.sendMessage(
    chatId,
    `Распознал подарок по ссылке как: ${matched.name}.\nСчитаю цены продажи по Portal + MRKT для этой коллекции...`,
    { reply_markup: MAIN_KEYBOARD }
  );

  await sendSellPriceForUser(chatId, user);
}

// =====================
// Общая функция для мониторинга
// =====================

async function fetchAllGiftsForUser(user) {
  if (MODE === 'test') return fetchTestGifts();

  const markets = user.filters.markets || ['Portal', 'MRKT'];
  const wantPortal = markets.includes('Portal');
  const wantMrkt = markets.includes('MRKT');

  let portalGifts = [];
  let mrktGifts = [];

  if (wantPortal) {
    try {
      portalGifts = await portalSearch({
        sort: 'price_asc',
        offset: 0,
        limit: 50,
        giftNames: user.filters.gifts.map((x) => x.trim()),
        models: user.filters.models.map((x) => x.trim()),
        backdrops: user.filters.backdrops.map((x) => x.trim()),
        minPrice: 0,
        maxPrice: user.maxPriceTon ?? 100000,
      });
    } catch (e) {
      console.error('Ошибка в portalSearch:', e);
    }
  }

  if (wantMrkt) {
    try {
      mrktGifts = await fetchMrktGiftsForUser(user);
    } catch (e) {
      console.error('Ошибка в fetchMrktGiftsForUser:', e);
    }
  }

  const all = [...portalGifts, ...mrktGifts];
  all.sort((a, b) => a.priceTon - b.priceTon);
  return all;
}

// =====================
// Мониторинг
// =====================

async function checkMarketsForAllUsers() {
  if (users.size === 0) return;

  for (const [userId, user] of users.entries()) {
    if (!user.enabled) continue;
    if (!user.maxPriceTon) continue;

    let gifts;
    try {
      gifts = await fetchAllGiftsForUser(user);
    } catch (e) {
      console.error('Ошибка в fetchAllGiftsForUser:', e);
      continue;
    }
    if (!gifts || !gifts.length) continue;

    const markets = user.filters.markets || ['Portal', 'MRKT'];
    const wantPortal = markets.includes('Portal');
    const wantMrkt = markets.includes('MRKT');

    gifts.sort((a, b) => a.priceTon - b.priceTon);
    const chatId = userId;

    for (const gift of gifts) {
      if (!gift.priceTon || gift.priceTon > user.maxPriceTon) continue;

      if (gift.market === 'Portal' && !wantPortal) continue;
      if (gift.market === 'MRKT' && !wantMrkt) continue;

      const attrs = gift.attrs || {};

      const giftNameVal = (gift.baseName || gift.name || '').toLowerCase().trim();
      if (user.filters.gifts.length && !user.filters.gifts.includes(giftNameVal)) continue;

      const modelVal = (attrs.model || '').toLowerCase().trim();
      if (user.filters.models.length && !user.filters.models.includes(modelVal)) continue;

      const backdropVal = (attrs.backdrop || '').toLowerCase().trim();
      if (user.filters.backdrops.length && !user.filters.backdrops.includes(backdropVal)) continue;

      const key = `${userId}:${gift.id}`;
      if (sentDeals.has(key)) continue;
      sentDeals.add(key);

      let text =
        `Price: ${gift.priceTon.toFixed(3)} TON\n` +
        `Gift: ${gift.name}\n`;

      if (attrs.collection && gift.market === 'MRKT') {
        text += `Collection: ${attrs.collection}\n`;
      }
      if (attrs.model) {
        text += `Model: ${attrs.model}\n`;
      }
      if (attrs.symbol) {
        text += `Symbol: ${attrs.symbol}\n`;
      }
      if (attrs.backdrop) {
        text += `Backdrop: ${attrs.backdrop}\n`;
      }

      text += `Market: ${gift.market}\n`;

      if (gift.urlTelegram) {
        text += `${gift.urlTelegram}`;
      }

      let buttonText = 'Открыть';
      if (gift.market === 'Portal') buttonText = 'Открыть в Portal';
      else if (gift.market === 'MRKT') buttonText = 'Открыть в MRKT';

      const replyMarkup = gift.urlMarket
        ? {
            inline_keyboard: [[{ text: buttonText, url: gift.urlMarket }]],
          }
        : undefined;

      try {
        await bot.sendMessage(chatId, text, {
          disable_web_page_preview: false,
          reply_markup: replyMarkup,
        });
      } catch (e) {
        console.error('Ошибка при отправке сообщения пользователю', userId, e);
      }
    }
  }
}

setInterval(() => {
  checkMarketsForAllUsers().catch((e) =>
    console.error('Ошибка в checkMarketsForAllUsers:', e)
  );
}, CHECK_INTERVAL_MS);

console.log('Бот запущен. Ожидаю команды /start в Telegram.');
