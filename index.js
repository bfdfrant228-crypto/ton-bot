const TelegramBot = require('node-telegram-bot-api');

const token = process.env.TELEGRAM_TOKEN;
const MODE = process.env.MODE || 'real'; // 'test' или 'real'
const CHECK_INTERVAL_MS = Number(process.env.CHECK_INTERVAL_MS || 5000);
// сколько страниц Portal запрашивать для поиска (умножается на limit)
const PORTAL_PAGES = Number(process.env.PORTAL_PAGES || 3);
// максимум результатов в выдаче поиска по названию
const MAX_SEARCH_RESULTS = Number(process.env.MAX_SEARCH_RESULTS || 10);

if (!token) {
  console.error('Ошибка: TELEGRAM_TOKEN не задан. Добавь токен бота в переменные окружения Railway.');
  process.exit(1);
}

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
    [{ text: '💰 Установить цену' }],
    [{ text: '🎛 Фильтры' }],
  ],
  resize_keyboard: true,
};

// очищаем историю отправленных гифтов для конкретного пользователя
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
        gifts: [],      // подарки (Fresh Socks, Victory Medal, ...)
        models: [],     // модели (Night Bat, Genius, ...)
        backdrops: [],  // фоны (Black, Dark Green, ...)
        markets: ['Portal', 'MRKT'], // какие маркеты использовать
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
    '💰 Установить цену — задать максимум в TON\n' +
    '🎛 Фильтры — выбрать подарки/модели/фоны/маркеты';

  bot.sendMessage(chatId, text, { reply_markup: MAIN_KEYBOARD });
});

bot.onText(/^\/help\b/, (msg) => {
  const chatId = msg.chat.id;
  const text =
    'Бот отслеживает NFT‑подарки в Portal и MRKT.\n\n' +
    'Кнопки:\n' +
    '🔍 Запустить поиск — начать слать найденные гифты\n' +
    '⏹ Остановить поиск — временно остановить\n' +
    '💰 Установить цену — максимум в TON\n' +
    '🎛 Фильтры — подарки / модели / фоны / маркеты (есть поиск по названию)\n\n' +
    'Команды:\n' +
    '/setmaxprice 0.5 — задать цену\n' +
    '/status — показать настройки\n' +
    '/listgifts — список подарков из Portal\n' +
    '/listmodels — модели/фоны для выбранного подарка';

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
    text += `• Максимальная цена: ${user.maxPriceTon.toFixed(3)} TON\n`;
  } else {
    text += '• Максимальная цена: не задана (кнопка "💰 Установить цену")\n';
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

// =====================
// Работа с коллекциями Portal
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

// MRKT
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

// Получить список коллекций (подарков) Portal
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

// Получить фильтры (модели/фоны) для конкретной коллекции по short_name
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
  if (!data || !data.floor_prices) return null;

  let key = shortName;
  if (!data.floor_prices[key]) {
    const keys = Object.keys(data.floor_prices);
    const found = keys.find((k) => k.toLowerCase() === shortName.toLowerCase());
    if (!found) return null;
    key = found;
  }

  return data.floor_prices[key];
}

function extractTraitNames(block) {
  const names = new Set();
  if (!block) return [];

  if (Array.isArray(block)) {
    for (const item of block) {
      if (!item) continue;
      if (typeof item === 'string') {
        names.add(item);
      } else if (item.name) {
        names.add(item.name);
      } else if (item.model) {
        names.add(item.model);
      } else if (item.value) {
        names.add(item.value);
      }
    }
  } else if (typeof block === 'object') {
    for (const key of Object.keys(block)) {
      names.add(key);
    }
  }

  return Array.from(names).sort();
}

// =====================
// Вспомогательные для списков
// =====================

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
// /listgifts и /listmodels (Portal)
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

  const models = extractTraitNames(filters.models);
  const backdrops = extractTraitNames(filters.backdrops);

  let text = `Подарок: ${col.name}\n\nМодели:\n`;
  if (models.length) {
    text += models.map((m) => `- ${m}`).join('\n');
  } else {
    text += '(нет данных)\n';
  }

  text += '\n\nФоны:\n';
  if (backdrops.length) {
    text += backdrops.map((b) => `- ${b}`).join('\n');
  } else {
    text += '(нет данных)\n';
  }

  if (text.length > 4000) text = text.slice(0, 3990) + '\n...';

  bot.sendMessage(chatId, text, { reply_markup: MAIN_KEYBOARD });
});

// =====================
// Callback-кнопки (фильтры, выборы, поиск, выбор маркетов)
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
    } else if (data === 'search_gift') {
      user.state = 'awaiting_gift_search';
      await bot.sendMessage(
        chatId,
        'Напиши часть названия подарка.\nНапример: medal, socks, snake',
        { reply_markup: MAIN_KEYBOARD }
      );
    } else if (data === 'filter_model') {
      if (!user.filters.gifts.length) {
        await bot.sendMessage(
          chatId,
          'Сначала выбери подарок (кнопка "🎁 Выбрать подарок" или "🔍 Подарок").',
          { reply_markup: MAIN_KEYBOARD }
        );
      } else {
        const giftLower = user.filters.gifts[0];
        const { byLowerName } = await portalCollections(200);
        const col = byLowerName.get(giftLower);
        if (!col) {
          await bot.sendMessage(
            chatId,
            'Не нашёл такой подарок в Portal collections (возможно другое название).',
            { reply_markup: MAIN_KEYBOARD }
          );
        } else {
          const filters = await portalCollectionFilters(col.shortName);
          if (!filters) {
            await bot.sendMessage(
              chatId,
              'Не удалось получить модели для этой коллекции (collections/filters).',
              { reply_markup: MAIN_KEYBOARD }
            );
          } else {
            const models = extractTraitNames(filters.models);
            if (!models.length) {
              await bot.sendMessage(
                chatId,
                'Модели для этого подарка не найдены (по данным Portal).',
                { reply_markup: MAIN_KEYBOARD }
              );
            } else {
              const inline_keyboard = buildInlineButtons('set_model:', models);
              await bot.sendMessage(chatId, 'Выбери модель:', {
                reply_markup: { inline_keyboard },
              });
            }
          }
        }
      }
    } else if (data === 'search_model') {
      if (!user.filters.gifts.length) {
        await bot.sendMessage(
          chatId,
          'Сначала выбери подарок (кнопка "🎁 Выбрать подарок" или "🔍 Подарок").',
          { reply_markup: MAIN_KEYBOARD }
        );
      } else {
        user.state = 'awaiting_model_search';
        await bot.sendMessage(
          chatId,
          'Напиши часть названия модели.\nНапример: night, crab, vampire',
          { reply_markup: MAIN_KEYBOARD }
        );
      }
    } else if (data === 'filter_backdrop') {
      if (!user.filters.gifts.length) {
        await bot.sendMessage(
          chatId,
          'Сначала выбери подарок (кнопка "🎁 Выбрать подарок" или "🔍 Подарок").',
          { reply_markup: MAIN_KEYBOARD }
        );
      } else {
        const giftLower = user.filters.gifts[0];
        const { byLowerName } = await portalCollections(200);
        const col = byLowerName.get(giftLower);
        if (!col) {
          await bot.sendMessage(
            chatId,
            'Не нашёл такой подарок в Portal collections (возможно другое название).',
            { reply_markup: MAIN_KEYBOARD }
          );
        } else {
          const filters = await portalCollectionFilters(col.shortName);
          if (!filters) {
            await bot.sendMessage(
              chatId,
              'Не удалось получить фоны для этой коллекции (collections/filters).',
              { reply_markup: MAIN_KEYBOARD }
            );
          } else {
            const backdrops = extractTraitNames(filters.backdrops);
            if (!backdrops.length) {
              await bot.sendMessage(
                chatId,
                'Фоны для этого подарка не найдены (по данным Portal).',
                { reply_markup: MAIN_KEYBOARD }
              );
            } else {
              const inline_keyboard = buildInlineButtons('set_backdrop:', backdrops);
              await bot.sendMessage(chatId, 'Выбери фон:', {
                reply_markup: { inline_keyboard },
              });
            }
          }
        }
      }
    } else if (data === 'search_backdrop') {
      if (!user.filters.gifts.length) {
        await bot.sendMessage(
          chatId,
          'Сначала выбери подарок (кнопка "🎁 Выбрать подарок" или "🔍 Подарок").',
          { reply_markup: MAIN_KEYBOARD }
        );
      } else {
        user.state = 'awaiting_backdrop_search';
        await bot.sendMessage(
          chatId,
          'Напиши часть названия фона.\nНапример: black, green, gold',
          { reply_markup: MAIN_KEYBOARD }
        );
      }
    } else if (data === 'markets_menu') {
      const inline_keyboard = {
        inline_keyboard: [
          [{ text: '🅿 Только Portal', callback_data: 'set_markets_portal' }],
          [{ text: '🅼 Только MRKT', callback_data: 'set_markets_mrkt' }],
          [{ text: '🅿+🅼 Portal + MRKT', callback_data: 'set_markets_all' }],
        ],
      };
      await bot.sendMessage(chatId, 'Выбери маркеты для поиска:', {
        reply_markup: inline_keyboard,
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
    } else if (data === 'filters_clear') {
      user.filters.gifts = [];
      user.filters.models = [];
      user.filters.backdrops = [];
      user.state = null;
      clearUserSentDeals(userId);
      await bot.sendMessage(chatId, 'Фильтры подарков, моделей и фонов сброшены.', {
        reply_markup: MAIN_KEYBOARD,
      });
    } else if (data === 'list_gifts_inline') {
      const { byLowerName } = await portalCollections(200);
      const names = Array.from(byLowerName.values()).map((x) => x.name);
      if (!names.length) {
        await bot.sendMessage(chatId, 'Подарков сейчас не найдено (Portal collections).');
      } else {
        const lines = names.sort().map((n) => `- ${n}`);
        let text = 'Подарки (из Portal collections):\n' + lines.join('\n');
        if (text.length > 4000) text = text.slice(0, 3990) + '\n...';
        await bot.sendMessage(chatId, text, { reply_markup: MAIN_KEYBOARD });
      }
    } else if (data === 'list_models_inline') {
      const user2 = getOrCreateUser(query.from.id);
      if (!user2.filters.gifts.length) {
        await bot.sendMessage(
          chatId,
          'Сначала выбери подарок через фильтр (кнопка "🎛 Фильтры" → "🎁 Выбрать подарок" или "🔍 Подарок").',
          { reply_markup: MAIN_KEYBOARD }
        );
      } else {
        const giftLower = user2.filters.gifts[0];
        const { byLowerName } = await portalCollections(200);
        const col = byLowerName.get(giftLower);
        if (!col) {
          await bot.sendMessage(
            chatId,
            'Не нашёл такой подарок в Portal collections.',
            { reply_markup: MAIN_KEYBOARD }
          );
        } else {
          const filters2 = await portalCollectionFilters(col.shortName);
          if (!filters2) {
            await bot.sendMessage(
              chatId,
              'Не удалось получить модели/фоны для этой коллекции.',
              { reply_markup: MAIN_KEYBOARD }
            );
          } else {
            const models = extractTraitNames(filters2.models);
            const backdrops = extractTraitNames(filters2.backdrops);

            let text = `Подарок: ${col.name}\n\nМодели:\n`;
            if (models.length) {
              text += models.map((m) => `- ${m}`).join('\n');
            } else {
              text += '(нет данных)\n';
            }

            text += '\n\nФоны:\n';
            if (backdrops.length) {
              text += backdrops.map((b) => `- ${b}`).join('\n');
            } else {
              text += '(нет данных)\n';
            }

            if (text.length > 4000) text = text.slice(0, 3990) + '\n...';

            await bot.sendMessage(chatId, text, { reply_markup: MAIN_KEYBOARD });
          }
        }
      }
    } else if (data === 'show_filters') {
      const u = user;
      let text = 'Текущие фильтры:\n';
      text += `• Мониторинг: ${u.enabled ? 'включён' : 'выключен'}\n`;
      if (u.maxPriceTon) {
        text += `• Макс. цена: ${u.maxPriceTon.toFixed(3)} TON\n`;
      } else {
        text += '• Макс. цена: не задана\n';
      }
      text += `• Маркеты: ${formatMarkets(u.filters.markets)}\n`;
      if (u.filters.gifts.length) {
        text += `• Подарки: ${u.filters.gifts.join(', ')}\n`;
      } else {
        text += '• Подарки: нет\n';
      }
      if (u.filters.models.length) {
        text += `• Модели: ${u.filters.models.join(', ')}\n`;
      } else {
        text += '• Модели: нет\n';
      }
      if (u.filters.backdrops.length) {
        text += `• Фоны: ${u.filters.backdrops.join(', ')}\n`;
      } else {
        text += '• Фоны: нет\n';
      }
      await bot.sendMessage(chatId, text, { reply_markup: MAIN_KEYBOARD });
    } else if (data.startsWith('set_gift:')) {
      const originalName = data.slice('set_gift:'.length);
      const key = originalName.toLowerCase().trim();
      user.filters.gifts = [key];
      user.filters.models = [];
      user.filters.backdrops = [];
      clearUserSentDeals(userId);
      await bot.sendMessage(
        chatId,
        `Фильтр по подарку установлен: ${key}`,
        { reply_markup: MAIN_KEYBOARD }
      );
    } else if (data.startsWith('set_model:')) {
      const originalName = data.slice('set_model:'.length);
      const key = originalName.toLowerCase().trim();
      user.filters.models = [key];
      clearUserSentDeals(userId);
      await bot.sendMessage(
        chatId,
        `Фильтр по модели установлен: ${key}`,
        { reply_markup: MAIN_KEYBOARD }
      );
    } else if (data.startsWith('set_backdrop:')) {
      const originalName = data.slice('set_backdrop:'.length);
      const key = originalName.toLowerCase().trim();
      user.filters.backdrops = [key];
      clearUserSentDeals(userId);
      await bot.sendMessage(
        chatId,
        `Фильтр по фону установлен: ${key}`,
        { reply_markup: MAIN_KEYBOARD }
      );
    }
  } catch (e) {
    console.error('callback_query error:', e);
  }

  bot.answerCallbackQuery(query.id).catch(() => {});
});

// =====================
// Обработка обычных сообщений (кнопки + ввод цены + поиск по строке)
// =====================

bot.on('message', async (msg) => {
  const chatId = msg.chat.id;
  const userId = msg.from.id;
  if (!msg.text) return;

  const text = msg.text.trim();
  if (text.startsWith('/')) return;

  const user = getOrCreateUser(userId);

  // ввод цены
  if (user.state === 'awaiting_max_price') {
    const value = parseFloat(text.replace(',', '.'));
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

  // поиск подарка по части названия
  if (user.state === 'awaiting_gift_search') {
    user.state = null;
    const q = text.toLowerCase().trim();
    if (!q) {
      bot.sendMessage(chatId, 'Пустой запрос. Попробуй ещё раз через "🔍 Подарок".', {
        reply_markup: MAIN_KEYBOARD,
      });
      return;
    }

    const { byLowerName } = await portalCollections(200);
    const all = Array.from(byLowerName.values()).map((x) => x.name);
    const matched = all.filter((name) => name.toLowerCase().includes(q)).sort();

    if (!matched.length) {
      bot.sendMessage(
        chatId,
        'Ничего не нашёл по этому запросу. Попробуй укороченное название или другую часть слова.',
        { reply_markup: MAIN_KEYBOARD }
      );
      return;
    }

    const limited = matched.slice(0, MAX_SEARCH_RESULTS);
    const inline_keyboard = buildInlineButtons('set_gift:', limited);
    await bot.sendMessage(chatId, 'Нашёл такие подарки, выбери:', {
      reply_markup: { inline_keyboard },
    });
    return;
  }

  // поиск модели по части названия
  if (user.state === 'awaiting_model_search') {
    user.state = null;
    if (!user.filters.gifts.length) {
      bot.sendMessage(
        chatId,
        'Сначала выбери подарок (кнопка "🎁 Выбрать подарок" или "🔍 Подарок").',
        { reply_markup: MAIN_KEYBOARD }
      );
      return;
    }
    const q = text.toLowerCase().trim();
    if (!q) {
      bot.sendMessage(chatId, 'Пустой запрос. Попробуй ещё раз через "🔍 Модель".', {
        reply_markup: MAIN_KEYBOARD,
      });
      return;
    }

    const giftLower = user.filters.gifts[0];
    const { byLowerName } = await portalCollections(200);
    const col = byLowerName.get(giftLower);
    if (!col) {
      bot.sendMessage(
        chatId,
        'Не нашёл такой подарок в Portal collections.',
        { reply_markup: MAIN_KEYBOARD }
      );
      return;
    }

    const filters = await portalCollectionFilters(col.shortName);
    if (!filters) {
      bot.sendMessage(
        chatId,
        'Не удалось получить модели для этой коллекции.',
        { reply_markup: MAIN_KEYBOARD }
      );
      return;
    }

    const models = extractTraitNames(filters.models);
    const matched = models.filter((m) => m.toLowerCase().includes(q)).sort();

    if (!matched.length) {
      bot.sendMessage(
        chatId,
        'Модели по этому запросу не найдены.',
        { reply_markup: MAIN_KEYBOARD }
      );
      return;
    }

    const limited = matched.slice(0, MAX_SEARCH_RESULTS);
    const inline_keyboard = buildInlineButtons('set_model:', limited);
    await bot.sendMessage(chatId, 'Нашёл такие модели, выбери:', {
      reply_markup: { inline_keyboard },
    });
    return;
  }

  // поиск фона по части названия
  if (user.state === 'awaiting_backdrop_search') {
    user.state = null;
    if (!user.filters.gifts.length) {
      bot.sendMessage(
        chatId,
        'Сначала выбери подарок (кнопка "🎁 Выбрать подарок" или "🔍 Подарок").',
        { reply_markup: MAIN_KEYBOARD }
      );
      return;
    }
    const q = text.toLowerCase().trim();
    if (!q) {
      bot.sendMessage(chatId, 'Пустой запрос. Попробуй ещё раз через "🔍 Фон".', {
        reply_markup: MAIN_KEYBOARD,
      });
      return;
    }

    const giftLower = user.filters.gifts[0];
    const { byLowerName } = await portalCollections(200);
    const col = byLowerName.get(giftLower);
    if (!col) {
      bot.sendMessage(
        chatId,
        'Не нашёл такой подарок в Portal collections.',
        { reply_markup: MAIN_KEYBOARD }
      );
      return;
    }

    const filters = await portalCollectionFilters(col.shortName);
    if (!filters) {
      bot.sendMessage(
        chatId,
        'Не удалось получить фоны для этой коллекции.',
        { reply_markup: MAIN_KEYBOARD }
      );
      return;
    }

    const backdrops = extractTraitNames(filters.backdrops);
    const matched = backdrops.filter((b) => b.toLowerCase().includes(q)).sort();

    if (!matched.length) {
      bot.sendMessage(
        chatId,
        'Фоны по этому запросу не найдены.',
        { reply_markup: MAIN_KEYBOARD }
      );
      return;
    }

    const limited = matched.slice(0, MAX_SEARCH_RESULTS);
    const inline_keyboard = buildInlineButtons('set_backdrop:', limited);
    await bot.sendMessage(chatId, 'Нашёл такие фоны, выбери:', {
      reply_markup: { inline_keyboard },
    });
    return;
  }

  // обычные кнопки
  if (text === '💰 Установить цену') {
    user.state = 'awaiting_max_price';
    bot.sendMessage(chatId, 'Введи максимальную цену в TON, например: 4.5', {
      reply_markup: MAIN_KEYBOARD,
    });
    return;
  }

  if (text === '🔍 Запустить поиск') {
    user.enabled = true;
    bot.sendMessage(chatId, 'Мониторинг включён. Бот будет отправлять подходящие гифты.', {
      reply_markup: MAIN_KEYBOARD,
    });
    return;
  }

  if (text === '⏹ Остановить поиск') {
    user.enabled = false;
    bot.sendMessage(chatId, 'Мониторинг остановлен.', {
      reply_markup: MAIN_KEYBOARD,
    });
    return;
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
          { text: '🔍 Подарок', callback_data: 'search_gift' },
          { text: '🔍 Модель', callback_data: 'search_model' },
          { text: '🔍 Фон', callback_data: 'search_backdrop' },
        ],
        [
          { text: '🏦 Маркеты', callback_data: 'markets_menu' },
        ],
        [
          { text: '📜 Список подарков', callback_data: 'list_gifts_inline' },
          { text: '📜 Список моделей', callback_data: 'list_models_inline' },
        ],
        [
          { text: 'ℹ️ Показать фильтры', callback_data: 'show_filters' },
          { text: '♻️ Сбросить', callback_data: 'filters_clear' },
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
    'Используй кнопки снизу или команды /help и /status.',
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
// REAL-режим: Portal search (фикс ссылок tg_id)
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
    if (number) {
      displayName = `${displayName} #${number}`;
    }

    // Формируем корректный slug для t.me/nft:
    // 1) если tg_id содержит '-', используем как есть;
    // 2) если tg_id — просто число / странное, строим сами: `SnowMittens-25247`
    let tgSlug = null;
    if (nft.tg_id && String(nft.tg_id).includes('-')) {
      tgSlug = String(nft.tg_id);
    } else if (baseName && number != null) {
      const slugName = baseName.replace(/['’\s-]+/g, '');
      tgSlug = `${slugName}-${number}`;
    }

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
// MRKT: /gifts/saling (с salePrice)
// =====================

async function fetchMrktGiftsForUser(user) {
  const token = process.env.MRKT_AUTH;
  if (!token) {
    return [];
  }

  const giftsFilter = user.filters.gifts.map((x) => cap(x.trim()));
  const modelsFilter = user.filters.models.map((x) => cap(x.trim()));
  const backdropsFilter = user.filters.backdrops.map((x) => cap(x.trim()));

  const body = {
    collectionNames: giftsFilter,    // ["Hanging Star", ...]
    modelNames: modelsFilter,        // ["Cucumber", ...]
    backdropNames: backdropsFilter,  // ["Indigo Dye", ...] при необходимости
    symbolNames: [],
    ordering: 'Price',
    lowToHigh: true,
    maxPrice: user.maxPriceTon ?? null,
    minPrice: null,
    mintable: null,
    number: null,
    count: 20,       // лимит по доке = 20
    cursor: '',
    query: null,
    promotedFirst: false,
  };

  console.log('MRKT request body:', JSON.stringify(body));

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

  console.log('MRKT response status:', res.status);

  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    console.error('MRKT HTTP error', res.status, txt.slice(0, 200));
    return [];
  }

  const data = await res.json().catch((e) => {
    console.error('MRKT JSON parse error:', e);
    return null;
  });
  if (!data) return [];

  const rawGifts = Array.isArray(data.gifts) ? data.gifts : Array.isArray(data) ? data : [];
  console.log('MRKT gifts length:', rawGifts.length);

  const gifts = [];

  for (const g of rawGifts) {
    if (!g) continue;

    // MRKT JSON: цена в salePrice (наноTON)
    let priceTon = NaN;
    if (g.salePrice != null) {
      priceTon = Number(g.salePrice) / 1e9;
    } else if (g.salePriceWithoutFee != null) {
      priceTon = Number(g.salePriceWithoutFee) / 1e9;
    }

    if (!priceTon || Number.isNaN(priceTon)) continue;
    if (user.maxPriceTon && priceTon > user.maxPriceTon) continue;

    const baseName = g.collectionName || g.collectionTitle || 'MRKT Gift';
    const number = g.number ?? null;
    let displayName = baseName;
    if (number) displayName = `${displayName} #${number}`;

    const model = g.modelName || g.modelTitle || null;
    const symbol = g.symbolName || null;
    const backdrop = g.backdropName || null;

    const id = g.id || `${baseName}_${model || ''}_${number || ''}_${priceTon}`;

    gifts.push({
      id: `mrkt_${id}`,
      market: 'MRKT',
      name: displayName,
      baseName,
      priceTon,
      urlTelegram: 'https://t.me/mrkt',
      urlMarket: 'https://t.me/mrkt',
      attrs: {
        model,
        symbol,
        backdrop,
        collection: baseName,
      },
    });
  }

  gifts.sort((a, b) => a.priceTon - b.priceTon);
  console.log('MRKT gifts after filter:', gifts.length);
  return gifts;
}

// Для пользователя — поиск по его фильтрам (Portal + MRKT, с учётом выбранных маркетов)
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

      // фильтр по маркету
      if (gift.market === 'Portal' && !wantPortal) continue;
      if (gift.market === 'MRKT' && !wantMrkt) continue;

      const attrs = gift.attrs || {};

      // Жёсткий фильтр по подарку/модели/фону для ВСЕХ маркетов
      const giftNameVal = (gift.baseName || gift.name || '').toLowerCase().trim();
      if (user.filters.gifts.length && !user.filters.gifts.includes(giftNameVal)) {
        continue;
      }

      const modelVal = (attrs.model || '').toLowerCase().trim();
      if (user.filters.models.length && !user.filters.models.includes(modelVal)) {
        continue;
      }

      const backdropVal = (attrs.backdrop || '').toLowerCase().trim();
      if (user.filters.backdrops.length && !user.filters.backdrops.includes(backdropVal)) {
        continue;
      }

      const key = `${userId}:${gift.id}`;
      if (sentDeals.has(key)) {
        continue;
      }
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
            inline_keyboard: [
              [{ text: buttonText, url: gift.urlMarket }],
            ],
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