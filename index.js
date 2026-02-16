const TelegramBot = require('node-telegram-bot-api');

const token = process.env.TELEGRAM_TOKEN;
const MODE = process.env.MODE || 'real'; // 'test' или 'real'
const CHECK_INTERVAL_MS = Number(process.env.CHECK_INTERVAL_MS || 5000);
// сколько страниц Portal запрашивать (умножается на limit)
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
    '/listgifts — список доступных подарков\n' +
    '/listmodels — список моделей и фонов';

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
// Вспомогательные для списков
// =====================

async function getCurrentGifts() {
  try {
    return await fetchGiftsForListing();
  } catch (e) {
    console.error('getCurrentGifts error:', e);
    return [];
  }
}

function buildNameMapFromGifts(gifts) {
  const giftNames = new Map();
  const models = new Map();
  const backdrops = new Map();

  for (const g of gifts) {
    const base = (g.baseName || g.name || '').trim();
    if (base) {
      const k = base.toLowerCase();
      if (!giftNames.has(k)) giftNames.set(k, base);
    }

    const m = g.attrs?.model;
    if (m) {
      const k = m.toLowerCase();
      if (!models.has(k)) models.set(k, m);
    }

    const b = g.attrs?.backdrop;
    if (b) {
      const k = b.toLowerCase();
      if (!backdrops.has(k)) backdrops.set(k, b);
    }
  }

  return { giftNames, models, backdrops };
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
// /listgifts и /listmodels
// =====================

bot.onText(/^\/listgifts\b/, async (msg) => {
  const chatId = msg.chat.id;
  const gifts = await getCurrentGifts();
  if (!gifts.length) {
    bot.sendMessage(chatId, 'Подарков сейчас не найдено (по текущему запросу Portal).');
    return;
  }

  const { giftNames } = buildNameMapFromGifts(gifts);
  if (!giftNames.size) {
    bot.sendMessage(chatId, 'Не удалось выделить названия подарков.');
    return;
  }

  const lines = Array.from(giftNames.values())
    .sort()
    .map((n) => `- ${n}`);

  let text = 'Доступные подарки (по текущему запросу Portal):\n' + lines.join('\n');
  if (text.length > 4000) text = text.slice(0, 3990) + '\n...';

  bot.sendMessage(chatId, text, { reply_markup: MAIN_KEYBOARD });
});

bot.onText(/^\/listmodels\b/, async (msg) => {
  const chatId = msg.chat.id;
  const gifts = await getCurrentGifts();
  if (!gifts.length) {
    bot.sendMessage(chatId, 'Данные пока не найдены (по текущему запросу Portal).');
    return;
  }

  const { giftNames, models, backdrops } = buildNameMapFromGifts(gifts);

  let text = 'Подарки:\n';
  if (giftNames.size) {
    text += Array.from(giftNames.values())
      .sort()
      .map((n) => `- ${n}`)
      .join('\n');
  } else {
    text += '(нет данных)\n';
  }

  text += '\n\nМодели:\n';
  if (models.size) {
    text += Array.from(models.values())
      .sort()
      .map((n) => `- ${n}`)
      .join('\n');
  } else {
    text += '(нет данных)\n';
  }

  text += '\n\nФоны:\n';
  if (backdrops.size) {
    text += Array.from(backdrops.values())
      .sort()
      .map((n) => `- ${n}`)
      .join('\n');
  } else {
    text += '(нет данных)\n';
  }

  if (text.length > 4000) text = text.slice(0, 3990) + '\n...';

  bot.sendMessage(chatId, text, { reply_markup: MAIN_KEYBOARD });
});

// =====================
// Callback-кнопки (фильтры и выборы)
// =====================

bot.on('callback_query', async (query) => {
  const userId = query.from.id;
  const chatId = query.message.chat.id;
  const data = query.data || '';
  const user = getOrCreateUser(userId);

  try {
    if (data === 'filter_gift') {
      const gifts = await getCurrentGifts();
      if (!gifts.length) {
        await bot.sendMessage(chatId, 'Сейчас подарков не найдено.');
      } else {
        const giftMap = new Map(); // lowerName -> { name }
        for (const g of gifts) {
          const base = (g.baseName || g.name || '').trim();
          if (!base) continue;
          const key = base.toLowerCase();
          if (!giftMap.has(key)) {
            giftMap.set(key, { name: base });
          }
        }

        if (!giftMap.size) {
          await bot.sendMessage(chatId, 'Не удалось выделить названия подарков.');
        } else {
          const items = Array.from(giftMap.values()).sort((a, b) =>
            a.name.localeCompare(b.name)
          );

          const names = items.map((i) => i.name);
          const inline_keyboard = buildInlineButtons('set_gift:', names);
          await bot.sendMessage(chatId, 'Выбери подарок:', { reply_markup: { inline_keyboard } });
        }
      }
    } else if (data === 'filter_model') {
      if (!user.filters.gifts.length) {
        await bot.sendMessage(
          chatId,
          'Сначала выбери подарок (кнопка "Фильтр по подарку").',
          { reply_markup: MAIN_KEYBOARD }
        );
      } else {
        const selectedGift = user.filters.gifts[0]; // lower-case
        const gifts = await getCurrentGifts();
        const modelMap = new Map(); // lowerModel -> original

        for (const g of gifts) {
          const base = (g.baseName || g.name || '').toLowerCase().trim();
          if (base !== selectedGift) continue;
          const m = g.attrs?.model;
          if (!m) continue;
          const mk = m.toLowerCase().trim();
          if (!modelMap.has(mk)) {
            modelMap.set(mk, m);
          }
        }

        if (!modelMap.size) {
          await bot.sendMessage(
            chatId,
            'Не нашёл моделей для выбранного подарка (по текущему запросу Portal).',
            { reply_markup: MAIN_KEYBOARD }
          );
        } else {
          const names = Array.from(modelMap.values()).sort();
          const inline_keyboard = buildInlineButtons('set_model:', names);
          await bot.sendMessage(chatId, 'Выбери модель:', {
            reply_markup: { inline_keyboard },
          });
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
        const selectedGift = user.filters.gifts[0];
        const gifts = await getCurrentGifts();
        const backdropsSet = new Map();

        for (const g of gifts) {
          const base = (g.baseName || g.name || '').toLowerCase().trim();
          if (base !== selectedGift) continue;
          const b = g.attrs?.backdrop;
          if (b) {
            const k = b.toLowerCase().trim();
            if (!backdropsSet.has(k)) backdropsSet.set(k, b);
          }
        }

        if (!backdropsSet.size) {
          await bot.sendMessage(
            chatId,
            'Не нашёл фонов для выбранного подарка (по текущему запросу Portal).',
            { reply_markup: MAIN_KEYBOARD }
          );
        } else {
          const names = Array.from(backdropsSet.values()).sort();
          const inline_keyboard = buildInlineButtons('set_backdrop:', names);
          await bot.sendMessage(chatId, 'Выбери фон:', {
            reply_markup: { inline_keyboard },
          });
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
      const gifts = await getCurrentGifts();
      if (!gifts.length) {
        await bot.sendMessage(chatId, 'Подарков сейчас не найдено (по текущему запросу Portal).');
      } else {
        const { giftNames } = buildNameMapFromGifts(gifts);
        if (!giftNames.size) {
          await bot.sendMessage(chatId, 'Не удалось выделить названия подарков.');
        } else {
          const lines = Array.from(giftNames.values())
            .sort()
            .map((n) => `- ${n}`);
          let text = 'Доступные подарки (по текущему запросу Portal):\n' + lines.join('\n');
          if (text.length > 4000) text = text.slice(0, 3990) + '\n...';
          await bot.sendMessage(chatId, text, { reply_markup: MAIN_KEYBOARD });
        }
      }
    } else if (data === 'list_models_inline') {
      const gifts = await getCurrentGifts();
      if (!gifts.length) {
        await bot.sendMessage(chatId, 'Данные пока не найдены (по текущему запросу Portal).');
      } else {
        const { giftNames, models, backdrops } = buildNameMapFromGifts(gifts);
        let text = 'Подарки:\n';
        if (giftNames.size) {
          text += Array.from(giftNames.values())
            .sort()
            .map((n) => `- ${n}`)
            .join('\n');
        } else {
          text += '(нет данных)\n';
        }

        text += '\n\nМодели:\n';
        if (models.size) {
          text += Array.from(models.values())
            .sort()
            .map((n) => `- ${n}`)
            .join('\n');
        } else {
          text += '(нет данных)\n';
        }

        text += '\n\nФоны:\n';
        if (backdrops.size) {
          text += Array.from(backdrops.values())
            .sort()
            .map((n) => `- ${n}`)
            .join('\n');
        } else {
          text += '(нет данных)\n';
        }

        if (text.length > 4000) text = text.slice(0, 3990) + '\n...';
        await bot.sendMessage(chatId, text, { reply_markup: MAIN_KEYBOARD });
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
// Обработка обычных сообщений (кнопки + ввод цены)
// =====================

bot.on('message', (msg) => {
  const chatId = msg.chat.id;
  const userId = msg.from.id;
  if (!msg.text) return;

  const text = msg.text.trim();
  if (text.startsWith('/')) return;

  const user = getOrCreateUser(userId);

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
        [{ text: 'Выбрать подарок', callback_data: 'filter_gift' }],
        [
          { text: 'Выбрать модель', callback_data: 'filter_model' },
          { text: 'Выбрать фон', callback_data: 'filter_backdrop' },
        ],
        [
          { text: 'Список подарков', callback_data: 'list_gifts_inline' },
          { text: 'Список моделей', callback_data: 'list_models_inline' },
        ],
        [
          { text: 'Показать фильтры', callback_data: 'show_filters' },
          { text: 'Сбросить', callback_data: 'filters_clear' },
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
// REAL-режим: Portal (JS-версия portalsmp.search)
// =====================

const API_URL = 'https://portal-market.com/api/'; // ВАЖНО: без "s"
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

    let tgUrl = 'https://t.me/portals';
    if (nft.tg_id) {
      tgUrl = `https://t.me/nft/${nft.tg_id}`;
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

// Для /listgifts и /listmodels — общий запрос без фильтров
async function fetchGiftsForListing() {
  if (MODE === 'test') return fetchTestGifts();

  const all = [];
  for (let page = 0; page < PORTAL_PAGES; page++) {
    const pageGifts = await portalSearch({
      sort: 'price_asc',
      offset: page * 100,
      limit: 100,
      minPrice: 0,
      maxPrice: 100000,
    });
    all.push(...pageGifts);
    if (pageGifts.length < 100) break;
  }
  return all;
}

// Для основного мониторинга — пер-пользовательский запрос с фильтрами
async function fetchGiftsForUser(user) {
  if (MODE === 'test') return fetchTestGifts();

  const giftsFilter = user.filters.gifts.map((x) => x.trim());
  const modelsFilter = user.filters.models.map((x) => x.trim());
  const backdropsFilter = user.filters.backdrops.map((x) => x.trim());

  const gifts = await portalSearch({
    sort: 'price_asc',
    offset: 0,
    limit: 50,
    giftNames: giftsFilter,
    models: modelsFilter,
    backdrops: backdropsFilter,
    minPrice: 0,
    maxPrice: user.maxPriceTon ?? 100000,
  });

  return gifts;
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
      gifts = await fetchGiftsForUser(user);
    } catch (e) {
      console.error('Ошибка в fetchGiftsForUser:', e);
      continue;
    }
    if (!gifts || !gifts.length) continue;

    gifts.sort((a, b) => a.priceTon - b.priceTon);

    const chatId = userId;

    for (const gift of gifts) {
      if (!gift.priceTon || gift.priceTon > user.maxPriceTon) continue;

      const attrs = gift.attrs || {};

      const key = `${userId}:${gift.id}`;
      if (sentDeals.has(key)) {
        continue;
      }
      sentDeals.add(key);

      let text =
        `Price: ${gift.priceTon.toFixed(3)} TON\n` +
        `Gift: ${gift.name}\n`;

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

      const replyMarkup = gift.urlMarket
        ? {
            inline_keyboard: [
              [{ text: 'Открыть в Portal', url: gift.urlMarket }],
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
