const TelegramBot = require('node-telegram-bot-api');

const token = process.env.TELEGRAM_TOKEN;
const MODE = process.env.MODE || 'real'; // 'test' или 'real'
const CHECK_INTERVAL_MS = Number(process.env.CHECK_INTERVAL_MS || 5000);

if (!token) {
  console.error('Ошибка: TELEGRAM_TOKEN не задан. Добавь токен бота в переменные окружения Railway.');
  process.exit(1);
}

console.log('Режим работы бота MODE =', MODE);

// Создаём Telegram-бота
const bot = new TelegramBot(token, { polling: true });

// настройки пользователей в памяти (userId -> { ... })
const users = new Map();
// запоминаем, какие сделки уже отправляли (userId:giftId)
const sentDeals = new Set();

// Клавиатура с основными кнопками
const MAIN_KEYBOARD = {
  keyboard: [
    [{ text: '🔍 Запустить поиск' }, { text: '⏹ Остановить поиск' }],
    [{ text: '💰 Установить цену' }],
    [{ text: '🎛 Фильтры' }],
  ],
  resize_keyboard: true,
};

// очистка истории отправленных подарков для конкретного пользователя
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
      enabled: true, // мониторинг включён по умолчанию
      state: null,   // состояние для ввода (цена/фильтры)
      filters: {
        gifts: [],      // подарки (Victory Medal, ...)
        models: [],     // модели (Toy Joy, ...)
        backdrops: [],  // фоны (Black, ...)
      },
    });
  }
  return users.get(userId);
}

function parseListInput(text) {
  return text
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean)
    .map((s) => s.toLowerCase());
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
// Callback-кнопки (inline)
// =====================

bot.on('callback_query', async (query) => {
  const userId = query.from.id;
  const chatId = query.message.chat.id;
  const data = query.data;
  const user = getOrCreateUser(userId);

  if (data === 'filter_gift') {
    user.state = 'awaiting_gifts';
    await bot.sendMessage(
      chatId,
      'Напиши названия подарков через запятую.\nНапример:\nVictory Medal, Heart Locket',
      { reply_markup: MAIN_KEYBOARD }
    );
  } else if (data === 'filter_model') {
    user.state = 'awaiting_models';
    await bot.sendMessage(
      chatId,
      'Напиши названия моделей через запятую.\nНапример:\nToy Joy, Queen Bee',
      { reply_markup: MAIN_KEYBOARD }
    );
  } else if (data === 'filter_backdrop') {
    user.state = 'awaiting_backdrops';
    await bot.sendMessage(
      chatId,
      'Напиши названия фонов через запятую.\nНапример:\nBlack, Khaki Green',
      { reply_markup: MAIN_KEYBOARD }
    );
  } else if (data === 'filters_clear') {
    user.filters.gifts = [];
    user.filters.models = [];
    user.filters.backdrops = [];
    user.state = null;
    clearUserSentDeals(userId);
    await bot.sendMessage(chatId, 'Фильтры подарков, моделей и фонов сброшены.', {
      reply_markup: MAIN_KEYBOARD,
    });
  }

  bot.answerCallbackQuery(query.id).catch(() => {});
});

// =====================
// Обработка сообщений (состояния + кнопки)
// =====================

bot.on('message', (msg) => {
  const chatId = msg.chat.id;
  const userId = msg.from.id;
  if (!msg.text) return;

  const text = msg.text.trim();
  if (text.startsWith('/')) return; // команды уже обработаны

  const user = getOrCreateUser(userId);

  // Состояния ввода
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

  if (user.state === 'awaiting_gifts') {
    const list = parseListInput(text);
    user.filters.gifts = list;
    user.state = null;
    clearUserSentDeals(userId);
    bot.sendMessage(
      chatId,
      list.length
        ? `Фильтр по подаркам установлен: ${list.join(', ')}`
        : 'Фильтр по подаркам очищен.',
      { reply_markup: MAIN_KEYBOARD }
    );
    return;
  }

  if (user.state === 'awaiting_models') {
    const list = parseListInput(text);
    user.filters.models = list;
    user.state = null;
    clearUserSentDeals(userId);
    bot.sendMessage(
      chatId,
      list.length
        ? `Фильтр по моделям установлен: ${list.join(', ')}`
        : 'Фильтр по моделям очищен.',
      { reply_markup: MAIN_KEYBOARD }
    );
    return;
  }

  if (user.state === 'awaiting_backdrops') {
    const list = parseListInput(text);
    user.filters.backdrops = list;
    user.state = null;
    clearUserSentDeals(userId);
    bot.sendMessage(
      chatId,
      list.length
        ? `Фильтр по фонам установлен: ${list.join(', ')}`
        : 'Фильтр по фонам очищен.',
      { reply_markup: MAIN_KEYBOARD }
    );
    return;
  }

  // Кнопки
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
        [{ text: 'Фильтр по подарку', callback_data: 'filter_gift' }],
        [
          { text: 'Фильтр по модели', callback_data: 'filter_model' },
          { text: 'Фильтр по фону', callback_data: 'filter_backdrop' },
        ],
        [{ text: 'Сбросить фильтры', callback_data: 'filters_clear' }],
      ],
    };

    bot.sendMessage(chatId, 'Выбери, что настроить:', {
      reply_markup: inlineKeyboard,
    });
    return;
  }

  // Всё остальное
  bot.sendMessage(
    chatId,
    'Используй кнопки снизу или команды /help и /status.',
    { reply_markup: MAIN_KEYBOARD }
  );
});

// =====================
// TEST-режим (случайные данные)
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
// REAL-режим: Portal
// =====================

async function fetchPortalGifts() {
  const url =
    process.env.PORTAL_SEARCH_URL ||
    'https://portal-market.com/api/collections/filters/backdrops';

  const headers = {
    Accept: 'application/json',
  };

  if (process.env.PORTAL_AUTH) {
    headers['Authorization'] = process.env.PORTAL_AUTH;
  }

  const res = await fetch(url, {
    method: 'GET',
    headers,
  });

  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    console.error('Portal HTTP error', res.status, txt.slice(0, 200));
    return [];
  }

  const data = await res.json().catch((e) => {
    console.error('Portal JSON parse error:', e);
    return null;
  });

  const gifts = [];

  // Вариант 1: массив бэкдропов
  if (Array.isArray(data)) {
    for (const item of data) {
      const fp = item.floor_price || item.floorPrice;
      const priceTon = fp ? Number(fp) : NaN;
      if (!priceTon || Number.isNaN(priceTon)) continue;

      const baseName = item.name || 'Backdrop';

      gifts.push({
        id: `portal_backdrop_${item.name}`,
        market: 'Portal',
        name: `Backdrop: ${item.name}`,
        baseName,
        priceTon,
        urlTelegram: 'https://t.me/portals',
        urlMarket: 'https://t.me/portals',
        attrs: {
          backdrop: item.name || null,
        },
      });
    }
    return gifts;
  }

  // Вариант 2: поиск NFT: { results: [...] }
  if (data && Array.isArray(data.results)) {
    for (const nft of data.results) {
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

      // Номер подарка: external_collection_number или из tg_id (PrettyPosy-40935)
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

      // 1) ссылка на сам NFT в Telegram (гифт-эмодзи)
      let tgUrl = 'https://t.me/portals';
      if (nft.tg_id) {
        tgUrl = `https://t.me/nft/${nft.tg_id}`;
      }

      // 2) ссылка на этот же гифт в Portal WebApp (deep link)
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
    return gifts;
  }

  console.error('Portal: неожиданный формат ответа.');
  return [];
}

// =====================
// Общая точка получения подарков
// =====================

async function fetchGifts() {
  if (MODE === 'test') {
    return fetchTestGifts();
  }

  const all = [];

  try {
    const p = await fetchPortalGifts();
    all.push(...p);
  } catch (e) {
    console.error('Ошибка при запросе к Portal:', e);
  }

  return all;
}

// =====================
// /listgifts и /listmodels
// =====================

bot.onText(/^\/listgifts\b/, async (msg) => {
  const chatId = msg.chat.id;

  let gifts;
  try {
    gifts = await fetchGifts();
  } catch (e) {
    console.error('/listgifts fetchGifts error:', e);
    bot.sendMessage(chatId, 'Не получилось получить список подарков.');
    return;
  }

  if (!gifts || !gifts.length) {
    bot.sendMessage(chatId, 'Подарков сейчас не найдено (по текущему запросу Portal).');
    return;
  }

  const namesMap = new Map(); // lower -> original
  for (const g of gifts) {
    const base = (g.baseName || g.name || '').trim();
    if (!base) continue;
    const key = base.toLowerCase();
    if (!namesMap.has(key)) namesMap.set(key, base);
  }

  if (!namesMap.size) {
    bot.sendMessage(chatId, 'Не удалось выделить названия подарков.');
    return;
  }

  const lines = Array.from(namesMap.values())
    .sort()
    .map((n) => `- ${n}`);

  let text = 'Доступные подарки (по текущему запросу Portal):\n' + lines.join('\n');
  if (text.length > 4000) {
    text = text.slice(0, 3990) + '\n...';
  }

  bot.sendMessage(chatId, text, { reply_markup: MAIN_KEYBOARD });
});

bot.onText(/^\/listmodels\b/, async (msg) => {
  const chatId = msg.chat.id;

  let gifts;
  try {
    gifts = await fetchGifts();
  } catch (e) {
    console.error('/listmodels fetchGifts error:', e);
    bot.sendMessage(chatId, 'Не получилось получить список моделей и фонов.');
    return;
  }

  if (!gifts || !gifts.length) {
    bot.sendMessage(chatId, 'Данные пока не найдены (по текущему запросу Portal).');
    return;
  }

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

  if (text.length > 4000) {
    text = text.slice(0, 3990) + '\n...';
  }

  bot.sendMessage(chatId, text, { reply_markup: MAIN_KEYBOARD });
});

// =====================
// Мониторинг
// =====================

async function checkMarketsForAllUsers() {
  if (users.size === 0) return;

  let gifts;
  try {
    gifts = await fetchGifts();
  } catch (e) {
    console.error('Ошибка в fetchGifts:', e);
    return;
  }

  if (!gifts || !gifts.length) {
    return;
  }

  for (const [userId, user] of users.entries()) {
    if (!user.enabled) continue;
    if (!user.maxPriceTon) continue;

    const chatId = userId;

    for (const gift of gifts) {
      if (!gift.priceTon || gift.priceTon > user.maxPriceTon) 