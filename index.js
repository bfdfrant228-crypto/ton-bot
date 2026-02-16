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
      enabled: true, // мониторинг включён
      state: null,   // состояние ввода (только для цены)
      filters: {
        gifts: [],      // подарки (Victory Medal, ...)
        models: [],     // модели (Genius, ...)
        backdrops: [],  // фоны (Black, ...)
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
    return await fetchGifts();
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
      .map((n) => 