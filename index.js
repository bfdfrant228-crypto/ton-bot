const TelegramBot = require('node-telegram-bot-api');

const token = process.env.TELEGRAM_TOKEN;
const MODE = process.env.MODE || 'real'; // 'test' или 'real'
const CHECK_INTERVAL_MS = Number(process.env.CHECK_INTERVAL_MS || 5000);
// сколько страниц Portal запрашивать для поиска (умножается на limit)
const PORTAL_PAGES = Number(process.env.PORTAL_PAGES || 3);

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
      state: null,
      filters: {
        gifts: [],      // подарки (Fresh Socks, Victory Medal, ...)
        models: [],     // модели (Night Bat, Genius, ...)
        backdrops: [],  // фоны (Black, Dark Green, ...)
      },
    });
  }
  return users.get(userId);
}

// =====================
// Команды
// =====================

bot.onText(/^\/start\b/, (msg) => {
  const chatId = msg.chat.id;
  getOrCreateUser(msg.from.id);

  const text =
    'Бот запущен.\n\n' +
    `Режим: ${MODE === 'test' ? 'ТЕСТОВЫЙ (случайные цены)' : 'РЕАЛЬНЫЕ ЦЕНЫ с Portal'}\n\n` +
    'Кнопки снизу:\n' +
    '🔍 Запустить поиск — включить мониторинг\n' +
    '⏹ Остановить поиск — выключить мониторинг\n' +
    '💰 Установить цену — задать максимум в TON\n' +
    '🎛 Фильтры — выбрать подарки/модели/фоны';

  bot.sendMessage(chatId, text, { reply_markup: MAIN_KEYBOARD });
});

bot.onText(/^\/help\b/, (msg) => {
  const chatId = msg.chat.id;
  const text =
    'Бот отслеживает NFT‑подарки Portal.\n\n' +
    'Кнопки:\n' +
    '🔍 Запустить поиск — начать слать найденные гифты\n' +
    '⏹ Остановить поиск — временно остановить\n' +
    '💰 Установить цену — максимум в TON\n' +
    '🎛 Фильтры — подарки / модели / фоны\n\n' +
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

  text += `\nРежим: ${MODE === 'test' ? 'ТЕСТОВЫЙ (случайные цены)' : 'РЕАЛЬНЫЕ ЦЕНЫ (Portal)'}.\n`;

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
// /listgifts и /listmodels (через collections и collections/filters)
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
      'Сначала выбери подарок через фильтр (кнопка "🎛 Фильтры" → "Выбрать подарок").',
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
// Callback-кнопки (фильтры и выборы через collections/filters)
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
    } else if (data === 'filter_model') {
      if (!user.filters.gifts.length) {
        await bot.sendMessage(
          chatId,
          'Сначала выбери подарок (кнопка "Фильтр по подарку").',
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
    } else if (data === 'filter_backdrop') {
      if (!user.filters.gifts.length) {
        await bot.sendMessage(
          chatId,
          'Сначала выбери подарок (кнопка "Фильтр по подарку").',
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
          'Сначала выбери подарок через фильтр (кнопка "🎛 Фильтры" → "Выбрать подарок").',
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

            await bot.send