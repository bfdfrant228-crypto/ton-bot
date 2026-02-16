const TelegramBot = require('node-telegram-bot-api');

const token = process.env.TELEGRAM_TOKEN;
const SEARCH_URL = process.env.PORTAL_SEARCH_URL;

if (!token) {
  console.error("TELEGRAM_TOKEN не задан");
  process.exit(1);
}

if (!SEARCH_URL) {
  console.error("PORTAL_SEARCH_URL не задан");
  process.exit(1);
}

const bot = new TelegramBot(token, { polling: true });

let settings = {
  maxPrice: 20,
  gift: null,
  model: null,
  background: null
};

function buildMenu() {
  return {
    reply_markup: {
      inline_keyboard: [
        [{ text: "🎁 Подарки", callback_data: "list_gifts" }],
        [{ text: "🧠 Модели", callback_data: "list_models" }],
        [{ text: "🎨 Фоны", callback_data: "list_backgrounds" }],
        [{ text: "🔍 Проверить фильтры", callback_data: "check_filters" }]
      ]
    }
  };
}

async function loadNFTs() {
  try {
    const res = await fetch(SEARCH_URL);
    const data = await res.json();
    return data.items || [];
  } catch (e) {
    console.error("Ошибка загрузки Portal:", e.message);
    return [];
  }
}

function extractAttributes(nfts, typeName) {
  const result = new Set();

  nfts.forEach(nft => {
    if (!nft.attributes) return;

    nft.attributes.forEach(attr => {
      if (
        attr.trait_type &&
        attr.value &&
        attr.trait_type.toLowerCase() === typeName.toLowerCase()
      ) {
        result.add(attr.value);
      }
    });
  });

  return Array.from(result).sort();
}

bot.onText(/\/start/, (msg) => {
  bot.sendMessage(msg.chat.id, "Управление ботом:", buildMenu());
});

bot.on('callback_query', async (query) => {
  const chatId = query.message.chat.id;

  if (query.data === "list_gifts") {
    const nfts = await loadNFTs();
    const gifts = extractAttributes(nfts, "Gift");

    if (!gifts.length) {
      bot.sendMessage(chatId, "❌ Подарки не найдены");
      return;
    }

    const buttons = gifts.map(g => [
      { text: g, callback_data: "gift_" + g }
    ]);

    bot.sendMessage(chatId, "Выбери подарок:", {
      reply_markup: { inline_keyboard: buttons }
    });
  }

  if (query.data === "list_models") {
    const nfts = await loadNFTs();
    const models = extractAttributes(nfts, "Model");

    if (!models.length) {
      bot.sendMessage(chatId, "❌ Модели не найдены");
      return;
    }

    const buttons = models.map(m => [
      { text: m, callback_data: "model_" + m }
    ]);

    bot.sendMessage(chatId, "Выбери модель:", {
      reply_markup: { inline_keyboard: buttons }
    });
  }

  if (query.data === "list_backgrounds") {
    const nfts = await loadNFTs();
    const backgrounds = extractAttributes(nfts, "Background");

    if (!backgrounds.length) {
      bot.sendMessage(chatId, "❌ Фоны не найдены");
      return;
    }

    const buttons = backgrounds.map(b => [
      { text: b, callback_data: "bg_" + b }
    ]);

    bot.sendMessage(chatId, "Выбери фон:", {
      reply_markup: { inline_keyboard: buttons }
    });
  }

  if (query.data.startsWith("gift_")) {
    settings.gift = query.data.replace("gift_", "");
    bot.sendMessage(chatId, "✅ Подарок выбран: " + settings.gift);
  }

  if (query.data.startsWith("model_")) {
    settings.model = query.data.replace("model_", "");
    bot.sendMessage(chatId, "✅ Модель выбрана: " + settings.model);
  }

  if (query.data.startsWith("bg_")) {
    settings.background = query.data.replace("bg_", "");
    bot.sendMessage(chatId, "✅ Фон выбран: " + settings.background);
  }

  if (query.data === "check_filters") {
    bot.sendMessage(chatId,
      `Твои настройки:\n` +
      `• Максимальная цена: ${settings.maxPrice} TON\n` +
      `• Подарок: ${settings.gift || "не выбран"}\n` +
      `• Модель: ${settings.model || "не выбрана"}\n` +
      `• Фон: ${settings.background || "не выбран"}`
    );
  }

  bot.answerCallbackQuery(query.id);
});

console.log("Бот запущен успешно 🚀");