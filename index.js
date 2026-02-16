const TelegramBot = require('node-telegram-bot-api');
const fetch = require('node-fetch');

const token = process.env.TELEGRAM_TOKEN;
const SEARCH_URL = process.env.PORTAL_SEARCH_URL;

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

bot.onText(/\/start/, (msg) => {
  bot.sendMessage(msg.chat.id, "Управление ботом:", buildMenu());
});

bot.on('callback_query', async (query) => {
  const chatId = query.message.chat.id;

  if (query.data === "list_gifts") {
    const data = await fetch(SEARCH_URL).then(r => r.json());
    const gifts = new Set();

    data.items.forEach(nft => {
      nft.attributes?.forEach(attr => {
        if (attr.trait_type === "Gift") gifts.add(attr.value);
      });
    });

    const buttons = Array.from(gifts).map(g => [{ text: g, callback_data: "gift_" + g }]);

    bot.sendMessage(chatId, "Выбери подарок:", {
      reply_markup: { inline_keyboard: buttons }
    });
  }

  if (query.data === "list_models") {
    const data = await fetch(SEARCH_URL).then(r => r.json());
    const models = new Set();

    data.items.forEach(nft => {
      nft.attributes?.forEach(attr => {
        if (attr.trait_type === "Model") models.add(attr.value);
      });
    });

    const buttons = Array.from(models).map(m => [{ text: m, callback_data: "model_" + m }]);

    bot.sendMessage(chatId, "Выбери модель:", {
      reply_markup: { inline_keyboard: buttons }
    });
  }

  if (query.data === "list_backgrounds") {
    const data = await fetch(SEARCH_URL).then(r => r.json());
    const backgrounds = new Set();

    data.items.forEach(nft => {
      nft.attributes?.forEach(attr => {
        if (attr.trait_type === "Background") backgrounds.add(attr.value);
      });
    });

    const buttons = Array.from(backgrounds).map(b => [{ text: b, callback_data: "bg_" + b }]);

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