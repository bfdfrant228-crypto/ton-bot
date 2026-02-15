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

// Память для настроек пользователей (пока в ОЗУ)
// userId -> { maxPriceTon: number }
const users = new Map();

// Чтобы не спамить одинаковыми сделками
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
    'Бот запущен и работает.\n\n' +
    `Текущий режим: ${MODE === 'test' ? 'ТЕСТОВЫЙ (случайные цены)' : 'РЕАЛЬНЫЕ ЦЕНЫ с маркетов Tonnel / MRKT / Portal'}\n\n` +
    'Команды:\n' +
    '/setmaxprice 0.5 — установить максимальную цену в TON\n' +
    '/status — показать текущие настройки\n' +
    '/help — краткая справка';

  bot.sendMessage(chatId, text);
});

bot.onText(/^\/help\b/, (msg) => {
  const chatId = msg.chat.id;
  const text =
    'Этот бот отслеживает NFT‑подарки на маркетах Tonnel / MRKT / Portal.\n\n' +
    'Сейчас:\n' +
    '• В режиме test — генерирует случайные цены (для проверки логики);\n' +
    '• В режиме real — тянет реальные цены из API маркетов.\n\n' +
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
      'Когда бот найдёт подарок дешевле этой цены — пришлёт уведомление.'
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

  text += `\nТекущий режим: ${MODE === 'test' ? 'ТЕСТОВЫЙ (случайные цены)' : 'РЕАЛЬНЫЕ ЦЕНЫ (через API маркетов)'}.\n`;

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
      id: 'tonnel_test',
      market: 'Tonnel',
      name: 'Тестовый подарок Tonnel',
      priceTon: randomPrice(),
      url: 'https://t.me/Tonnel_Network_bot',
      attrs: {},
    },
    {
      id: 'portal_test',
      market: 'Portal',
      name: 'Тестовый подарок Portal',
      priceTon: randomPrice(),
      url: 'https://t.me/portals',
      attrs: {},
    },
    {
      id: 'mrkt_test',
      market: 'MRKT',
      name: 'Тестовый подарок MRKT',
      priceTon: randomPrice(),
      url: 'https://t.me/mrkt',
      attrs: {},
    },
  ];
}

// =====================
// REAL-режим: запросы к маркетам
// =====================

// Tonnel: POST https://gifts2.tonnel.network/api/pageGifts
async function fetchTonnelGifts() {
  const url = 'https://gifts2.tonnel.network/api/pageGifts';
  const userAuth = process.env.TONNEL_USER_AUTH;

  if (!userAuth) {
    console.warn('TONNEL_USER_AUTH не задан, Tonnel будет пропущен.');
    return [];
  }

  const tonnelGiftName = process.env.TONNEL_GIFT_NAME || null;

  const filter = {
    price: { $exists: true },
    buyer: { $exists: false },
    asset: 'TON',
  };
  if (tonnelGiftName) {
    filter.gift_name = tonnelGiftName;
  }

  const body = {
    page: 1,
    limit: 30,
    sort: JSON.stringify({ message_post_time: -1, gift_id: -1 }),
    filter: JSON.stringify(filter),
    price_range: null,
    ref: 0,
    user_auth: userAuth,
  };

  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Accept': 'application/json',
    },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    console.error('Tonnel HTTP error', res.status, txt);
    return [];
  }

  const data = await res.json().catch((e) => {
    console.error('Tonnel JSON parse error:', e);
    return null;
  });
  if (!Array.isArray(data)) {
    console.error('Tonnel: неожиданный формат ответа, ожидается массив.');
    return [];
  }

  const gifts = [];
  for (const item of data) {
    const price = Number(item.price);
    if (!price || Number.isNaN(price)) continue;

    gifts.push({
      id: `tonnel_${item.gift_id ?? item.gift_num}`,
      market: 'Tonnel',
      name: item.name || 'Gift',
      priceTon: price,
      url: 'https://t.me/Tonnel_Network_bot',
      attrs: {
        model: item.model || null,
        symbol: item.symbol || null,
        backdrop: item.backdrop || null,
      },
    });
  }

  return gifts;
}

// MRKT: POST https://api.tgmrkt.io/api/v1/gifts/models
async function fetchMrktGifts() {
  const url = 'https://api.tgmrkt.io/api/v1/gifts/models';
  const raw = process.env.MRKT_COLLECTIONS || '';
  const collections = raw
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);

  if (!collections.length) {
    console.warn('MRKT_COLLECTIONS не задан, MRKT будет пропущен.');
    return [];
  }

  const body = { collections };

  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Accept': 'application/json',
    },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const txt = await res.text().catch(() => '');
    console.error('MRKT HTTP error', res.status, txt);
    return [];
  }

  const data = await res.json().catch((e) => {
    console.error('MRKT JSON parse error:', e);
    return null;
  });
  if (!Array.isArray(data)) {
    console.error('MRKT: неожиданный формат ответа, ожидается массив.');
    return [];
  }

  const gifts = [];
  for (const item of data) {
    const nano = item.floorPriceNanoTons;
    const nanoNum = Number(nano);
    if (!nanoNum || Number.isNaN(nanoNum)) continue;

    const priceTon = nanoNum / 1e9;

    gifts.push({
      id: `mrkt_${item.collectionName}_${item.modelName}`,
      market: 'MRKT',
      name: `${item.collectionTitle || item.collectionName} — ${item.modelTitle || item.modelName}`,
      priceTon,
      url: 'https://t.me/mrkt',
      attrs: {
        collection: item.collectionName || null,
        model: item.modelName || null,
      },
    });
  }

  return gifts;
}

// Portal: GET URL из PORTAL_SEARCH_URL
// Поддерживает два варианта:
// 1) /api/collections/filters/backdrops  → массив бэкдропов с floor_price
// 2) /api/nfts/search?...                → объект { results: [...] }
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
    console.error('Portal HTTP error', res.status, txt);
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
        url: 'https://t.me/portals',
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

      gifts.push({
        id: `portal_${nft.id}`,
        market: 'Portal',
        name: nft.name || 'NFT',
        priceTon,
        url: 'https://t.me/portals',
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
    const t = await fetchTonnelGifts();
    all.push(...t);
  } catch (e) {
    console.error('Ошибка при запросе к Tonnel:', e);
  }

  try {
    const m = await fetchMrktGifts();
    all.push(...m);
  } catch (e) {
    console.error('Ошибка при запросе к MRKT:', e);
  }

  try {
    const p = await fetchPortalGifts();
    all.push(...p);
  } catch (e) {
    console.error('Ошибка при запросе к Portal:', e);
  }

  return all;
}

// =====================
// Мониторинг маркетов
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
        `Найден подходящий подарок (REAL):\n\n` +
        `Маркет: ${gift.market}\n` +
        `Название: ${gift.name}\n` +
        `Цена: ${gift.priceTon.toFixed(3)} TON` +
        formatAttrs(gift.attrs) +
        `\nСсылка: ${gift.url}\n\n` +
        'Дальше можно добавить автопокупку через Tonkeeper / TonConnect.';

      try {
        await bot.sendMessage(chatId, text, { disable_web_page_preview: true });
      } catch (e) {
        console.error('Ошибка при отправке сообщения пользователю', userId, e);
      }
    }
  }
}

// Запускаем периодический опрос
setInterval(() => {
  checkMarketsForAllUsers().catch((e) =>
    console.error('Ошибка в checkMarketsForAllUsers:', e)
  );
}, CHECK_INTERVAL_MS);

console.log('Бот запущен. Ожидаю команды /start в Telegram.');