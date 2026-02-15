const TelegramBot = require('node-telegram-bot-api');

const token = process.env.TELEGRAM_TOKEN;
const MODE = process.env.MODE || 'test'; // 'test' или 'real'

if (!token) {
  console.error('Ошибка: TELEGRAM_TOKEN не задан. Добавь токен бота в переменные окружения Railway.');
  process.exit(1);
}

console.log('Режим работы бота MODE =', MODE);

// Создаём Telegram-бота
const bot = new TelegramBot(token, { polling: true });

// Память для настроек пользователей (пока в ОЗУ, без базы)
// userId -> { maxPriceTon: number }
const users = new Map();

// Чтобы не спамить одинаковыми «сделками»
const sentDeals = new Set();

// Интервал проверки маркетов (в миллисекундах)
const CHECK_INTERVAL_MS = 5000;

// Получить или создать настройки пользователя
function getOrCreateUser(userId) {
  if (!users.has(userId)) {
    users.set(userId, {
      maxPriceTon: null, // максимальная цена в TON
    });
  }
  return users.get(userId);
}

// =====================
// Команды бота
// =====================

bot.onText(/^\/start\b/, (msg) => {
  const chatId = msg.chat.id;
  getOrCreateUser(msg.from.id);

  const text =
    'Бот запущен и работает.\n\n' +
    `Текущий режим: ${MODE === 'test' ? 'ТЕСТОВЫЙ (случайные цены)' : 'РЕАЛЬНЫЕ ЦЕНЫ (как только подключим API)'}\n\n` +
    'Команды:\n' +
    '/setmaxprice 0.5 — установить максимальную цену в TON\n' +
    '/status — показать текущие настройки\n' +
    '/help — краткая справка';

  bot.sendMessage(chatId, text);
});

bot.onText(/^\/help\b/, (msg) => {
  const chatId = msg.chat.id;
  const text =
    'Этот бот задуман для автоматического отслеживания и покупки NFT‑подарков ' +
    'на маркетах Tonnel / Portal / MRKT.\n\n' +
    'Сейчас:\n' +
    '• В режиме test — бот генерирует случайные цены для проверки логики.\n' +
    '• В режиме real — сюда подключим реальные API маркетов.\n\n' +
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
  text += 'Как только подключим реальные API Tonnel / Portal / MRKT — здесь будут настоящие цены с маркетов.';

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
// Работа с "подарками"
// =====================

// Унифицированный формат подарка
// { id, market, name, priceTon, url }

// Тестовая генерация подарков (как раньше)
// В реальной версии сюда не лезем, используем fetchRealGifts()
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
    },
    {
      id: 'portal_test',
      market: 'Portal',
      name: 'Тестовый подарок Portal',
      priceTon: randomPrice(),
      url: 'https://t.me/portals',
    },
    {
      id: 'mrkt_test',
      market: 'MRKT',
      name: 'Тестовый подарок MRKT',
      priceTon: randomPrice(),
      url: 'https://t.me/mrkt',
    },
  ];
}

// Заглушка для реальных маркетов.
// СЮДА МЫ ПОТОМ ДОБАВИМ ЗАПРОСЫ К Tonnel / Portal / MRKT.
async function fetchRealGifts() {
  const gifts = [];

  // Пример структуры: читаем URL-ы из переменных окружения
  // TONNEL_API_URL, PORTAL_API_URL, MRKT_API_URL
  const markets = [
    {
      name: 'Tonnel',
      env: 'TONNEL_API_URL',
      fallbackUrl: 'https://t.me/Tonnel_Network_bot',
    },
    {
      name: 'Portal',
      env: 'PORTAL_API_URL',
      fallbackUrl: 'https://t.me/portals',
    },
    {
      name: 'MRKT',
      env: 'MRKT_API_URL',
      fallbackUrl: 'https://t.me/mrkt',
    },
  ];

  for (const m of markets) {
    const url = process.env[m.env];
    if (!url) {
      continue; // для этого маркета URL ещё не настроен
    }

    try {
      // В Node 18+ fetch уже встроен
      const res = await fetch(url);
      if (!res.ok) {
        console.error(`Ошибка HTTP от ${m.name}:`, res.status);
        continue;
      }

      const data = await res.json();

      // ВАЖНО:
      // Тут нужно адаптировать под реальный формат ответа API.
      // Ниже — пример, который нужно будет подправить под конкретный JSON.

      // Предположим, API возвращает массив объектов:
      // [
      //   { "id": "123", "name": "Gift #1", "priceTon": 0.42, "url": "https://..." },
      //   ...
      // ]
      if (Array.isArray(data)) {
        for (const item of data) {
          if (typeof item.priceTon !== 'number') continue;

          gifts.push({
            id: `${m.name}_${item.id || item.name}`,
            market: m.name,
            name: item.name || 'Gift',
            priceTon: item.priceTon,
            url: item.url || m.fallbackUrl,
          });
        }
      } else {
        console.error(`Непонятный формат ответа от ${m.name}, ожидается массив.`);
      }

    } catch (e) {
      console.error(`Ошибка при запросе к ${m.name}:`, e);
    }
  }

  return gifts;
}

// Общая функция: выбирает, откуда брать данные
async function fetchGifts() {
  if (MODE === 'real') {
    return await fetchRealGifts();
  }
  // по умолчанию — тестовый режим
  return fetchTestGifts();
}

// =====================
// Мониторинг маркетов
// =====================

async function checkMarketsForAllUsers() {
  if (users.size === 0) {
    return; // никто не настроился — никого не тревожим
  }

  let gifts;
  try {
    gifts = await fetchGifts();
  } catch (e) {
    console.error('Ошибка при получении подарков:', e);
    return;
  }

  if (!gifts || gifts.length === 0) {
    // В реальном режиме это значит, что API ничего не вернуло
    return;
  }

  for (const [userId, settings] of users.entries()) {
    if (!settings.maxPriceTon) {
      continue; // у пользователя ещё нет максимальной цены
    }

    const chatId = userId; // считаем, что бот в личке

    for (const gift of gifts) {
      if (gift.priceTon <= settings.maxPriceTon) {
        const key = `${userId}:${gift.id}`;

        // Чтобы не спамить одним и тем же подарком
        if (sentDeals.has(key)) {
          continue;
        }
        sentDeals.add(key);

        const text =
          `Найден подходящий подарок (${MODE === 'test' ? 'ТЕСТОВЫЕ ДАННЫЕ' : 'РЕАЛЬНЫЙ МАРКЕТ'}):\n\n` +
          `Маркет: ${gift.market}\n` +
          `Название: ${gift.name}\n` +
          `Цена: ${gift.priceTon.toFixed(3)} TON\n` +
          `Ссылка: ${gift.url}\n\n` +
          (MODE === 'test'
            ? 'Сейчас цены генерируются случайно. Как только добавим реальные API — здесь будут настоящие сделки.'
            : 'Это реальный лот с маркета. На следующем шаге сюда добавим автоматическую покупку.');

        try {
          await bot.sendMessage(chatId, text, { disable_web_page_preview: true });
        } catch (e) {
          console.error('Ошибка при отправке сообщения пользователю', userId, e);
        }
      }
    }
  }
}

// Запускаем периодический опрос "маркетов"
setInterval(() => {
  checkMarketsForAllUsers().catch((e) =>
    console.error('Ошибка в checkMarketsForAllUsers:', e)
  );
}, CHECK_INTERVAL_MS);

console.log('Бот запущен. Ожидаю команды /start в Telegram.');