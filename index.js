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

// настройки пользователей в памяти (userId -> { maxPriceTon })
const users = new Map();

// запоминаем, какие сделки уже отправляли (userId:giftId)
const sentDeals = new Set();

// =====================
// Вспомогательные функции
// =====================

function getOrCreateUser(userId) {
  if (!users.has(userId)) {
    users.set(userId, {
      maxPriceTon: null,
    });
  }
  return users.get(userId);
}

function formatAttrs(attrs) {
  if (!attrs) return '';
  const lines = [];
  if (attrs.collection) lines.push(`Коллекция: ${attrs.collection}`);
  if (attrs.model) lines.push(`Модель: ${attrs.model}`);
  if (attrs.symbol) lines.push(`Символ: ${attrs.symbol}`);
  if (attrs.backdrop) lines.push(`Фон: ${attrs.backdrop}`);
  if (!lines.length) return '';
  return '\n' + lines.join('\n');
}

// =====================
// Команды бота
// =====================

bot.onText(/^\/start\b/, (msg) => {
  const chatId = msg.chat.id;
  getOrCreateUser(msg.from.id);

  const text =
    'Бот запущен.\n\n' +
    `Режим: ${MODE === 'test' ? 'ТЕСТОВЫЙ (случайные цены)' : 'РЕАЛЬНЫЕ ЦЕНЫ с Portal'}\n\n` +
    'Команды:\n' +
    '/setmaxprice 0.5 — установить максимальную цену в TON\n' +
    '/status — показать текущие настройки\n' +
    '/help — краткая справка';

  bot.sendMessage(chatId, text);
});

bot.onText(/^\/help\b/, (msg) => {
  const chatId = msg.chat.id;
  const text =
    'Бот отслеживает NFT‑подарки.\n\n' +
    'Сейчас:\n' +
    '• В режиме test — генерирует случайные цены (для проверки логики);\n' +
    '• В режиме real — тянет реальные цены из API Portal.\n\n' +
    'Команды:\n' +
    '/setmaxprice 0.5 — максимальная цена подарка в TON\n' +
    '/status — показать текущие настройки\n' +
    '/start — показать приветствие ещё раз';

  bot.sendMessage(chatId, text);
});

// /setmaxprice <число>
bot.onText(/^\/setmaxprice\b(?:\s+(.+))?/, (msg, match) => {
  const chatId = msg.chat.id;
  const userId = msg.from.id;
  const arg = match[1];

  if (!arg) {
    bot.sendMessage(
      chatId,
      'Укажи цену в TON.\nНапример:\n/setmaxprice 0.5'
    );
    return;
  }

  const value = parseFloat(arg.replace(',', '.'));

  if (Number.isNaN(value) || value <= 0) {
    bot.sendMessage(chatId, 'Некорректная цена. Введи положительное число, например: 0.3');
    return;
  }

  const user = getOrCreateUser(userId);
  user.maxPriceTon = value;

  bot.sendMessage(
    chatId,
    `Максимальная цена установлена: ${value.toFixed(3)} TON.\n` +
      'Когда бот найдёт подарок дешевле этой цены — пришлёт уведомление (один раз на каждый подарок).'
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
    text += '• Максимальная цена: не задана (установи через /setmaxprice)\n';
  }

  text += `\nРежим: ${MODE === 'test' ? 'ТЕСТОВЫЙ (случайные цены)' : 'РЕАЛЬНЫЕ ЦЕНЫ (Portal)'}.\n`;

  bot.sendMessage(chatId, text);
});

// Любые другие сообщения
bot.on('message', (msg) => {
  if (msg.text && !msg.text.startsWith('/')) {
    bot.sendMessage(
      msg.chat.id,
      'Я понимаю только команды.\nИспользуй /help, чтобы посмотреть список доступных команд.'
    );
  }
});

// =====================
// TEST-режим (случайные цены)
// =====================

function fetchTestGifts() {
  function randomPrice() {
    return 0.1 + Math.random() * 0.9;
  }

  return [
    {
      id: 'portal_test_1',
      market: 'Portal',
      name: 'Тестовый подарок #1',
      priceTon: randomPrice(),
      url: 'https://t.me/portals',
      attrs: {},
    },
    {
      id: 'portal_test_2',
      market: 'Portal',
      name: 'Тестовый подарок #2',
      priceTon: randomPrice(),
      url: 'https://t.me/portals',
      attrs: {},
    },
  ];
}

// =====================
// REAL-режим: Portal
// =====================

// Portal: GET URL из PORTAL_SEARCH_URL
// Поддерживает два варианта:
// 1) /api/collections/filters/backdrops  → массив бэкдропов с floor_price
// 2) /api/nfts/search?...                → объект { results: [...] } с tg_id
async function fetchPortalGifts() {
  const url =
    process.env.PORTAL_SEARCH_URL ||
    'https://portal-market.com/api/collections/filters/backdrops';

  const headers = {
    'Accept': 'application/json',
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

      gifts.push({
        id: `portal_backdrop_${item.name}`,
        market: 'Portal',
        name: `Backdrop: ${item.name}`,
        priceTon,
        url: 'https://t.me/portals', // для бэкдропа нет tg_id, оставляем общий портал
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

      // Формируем ссылку на сам NFT в Telegram, если есть tg_id
      let tgUrl = 'https://t.me/portals';
      if (nft.tg_id) {
        tgUrl = `https://t.me/nft/${nft.tg_id}`;
      }

      gifts.push({
        id: `portal_${nft.id}`,
        market: 'Portal',
        name: nft.name || 'NFT',
        priceTon,
        url: tgUrl,
        attrs: { model, symbol, backdrop },
      });
    }
    return gifts;
  }

  console.error('Portal: неожиданный формат ответа.');
  return [];
}

// =====================
// Общая точка: откуда брать подарки
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

  for (const [userId, settings] of users.entries()) {
    if (!settings.maxPriceTon) continue;

    const chatId = userId;

    for (const gift of gifts) {
      if (!gift.priceTon || gift.priceTon > settings.maxPriceTon) continue;

      const key = `${userId}:${gift.id}`;

      if (sentDeals.has(key)) {
        continue;
      }
      sentDeals.add(key);

      const text =
        `Найден подходящий подарок:\n\n` +
        `Маркет: ${gift.market}\n` +
        `Название: ${gift.name}\n` +
        `Цена: ${gift.priceTon.toFixed(3)} TON` +
        formatAttrs(gift.attrs) +
        `\nСсылка: ${gift.url}\n\n` +
        'Каждый конкретный подарок отправляется один раз, чтобы не спамить.';

      try {
        await bot.sendMessage(chatId, text, { disable_web_page_preview: false });
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