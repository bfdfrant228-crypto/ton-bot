const TelegramBot = require('node-telegram-bot-api');

const token = process.env.TELEGRAM_TOKEN;

if (!token) {
  console.error('Ошибка: TELEGRAM_TOKEN не задан. Добавь токен бота в переменные окружения Railway.');
  process.exit(1);
}

// Создаём Telegram-бота
const bot = new TelegramBot(token, { polling: true });

// Память для настроек пользователей (пока в ОЗУ, без базы)
// userId -> { maxPriceTon: number }
const users = new Map();

// Чтобы не спамить одинаковыми «сделками»
const sentDeals = new Set();

// Интервал проверки "маркетов" (в миллисекундах)
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

// Команда /start
bot.onText(/^\/start\b/, (msg) => {
  const chatId = msg.chat.id;
  getOrCreateUser(msg.from.id);

  const text =
    'Бот запущен и работает.\n\n' +
    'Сейчас он в тестовом режиме:\n' +
    '• Каждые 5 секунд он генерирует «тестовые» цены подарков на Tonnel / Portal / MRKT\n' +
    '• Если цена ниже твоей максимальной — присылает уведомление\n\n' +
    'Команды:\n' +
    '/setmaxprice 0.5 — установить максимальную цену в TON\n' +
    '/status — показать текущие настройки\n' +
    '/help — краткая справка';

  bot.sendMessage(chatId, text);
});

// Команда /help
bot.onText(/^\/help\b/, (msg) => {
  const chatId = msg.chat.id;
  const text =
    'Этот бот задуман для автоматического отслеживания и покупки NFT‑подарков ' +
    'на маркетах Tonnel / Portal / MRKT.\n\n' +
    'Сейчас включён тестовый режим (без реальных покупок).\n\n' +
    'Доступные команды:\n' +
    '/setmaxprice 0.5 — максимальная цена подарка в TON\n' +
    '/status — показать текущие настройки\n' +
    '/start — показать приветствие ещё раз';

  bot.sendMessage(chatId, text);
});

// Команда /setmaxprice <число>
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
      'Когда бот найдёт подарок дешевле этой цены (в тестовых данных) — пришлёт уведомление.'
  );
});

// Команда /status
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

  text += '\nСейчас бот работает в ТЕСТОВОМ режиме (цены генерируются случайно). ' +
          'Дальше на это место подключим реальные данные с маркетов Tonnel / Portal / MRKT.';

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

// Тестовая функция: "получение" подарков с маркетов
// В РЕАЛЬНОЙ версии здесь будут запросы к API маркетов.
async function fetchTestGifts() {
  // Генерируем случайную цену от 0.1 до 1.0 TON
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

// Периодический мониторинг "маркетов"
async function checkMarketsForAllUsers() {
  if (users.size === 0) {
    return; // никто не настроился — никого не тревожим
  }

  let gifts;
  try {
    gifts = await fetchTestGifts();
  } catch (e) {
    console.error('Ошибка при получении тестовых подарков:', e);
    return;
  }

  for (const [userId, settings] of users.entries()) {
    if (!settings.maxPriceTon) {
      continue; // у пользователя ещё нет максимальной цены
    }

    const chatId = userId; // для простоты считаем, что пишешь боту из лички

    for (const gift of gifts) {
      if (gift.priceTon <= settings.maxPriceTon) {
        const key = `${userId}:${gift.id}`;

        // Чтобы не спамить одним и тем же тестовым подарком
        if (sentDeals.has(key)) {
          continue;
        }
        sentDeals.add(key);

        const text =
          'Нашёл подарок ниже твоей максимальной цены (ТЕСТОВЫЕ ДАННЫЕ):\n\n' +
          `Маркет: ${gift.market}\n` +
          `Название: ${gift.name}\n` +
          `Цена: ${gift.priceTon.toFixed(3)} TON\n` +
          `Ссылка: ${gift.url}\n\n` +
          'Сейчас это тестовый режим без реальных покупок.\n' +
          'Когда подключим настоящий API маркетов — тут бот будет реагировать на реальные лоты.';

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