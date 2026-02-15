const TelegramBot = require('node-telegram-bot-api');

const token = process.env.TELEGRAM_TOKEN;

if (!token) {
  console.error("No TELEGRAM_TOKEN provided!");
  process.exit(1);
}

const bot = new TelegramBot(token, { polling: true });

bot.onText(/\/start/, (msg) => {
  bot.sendMessage(msg.chat.id, "Бот запущен и работает 🚀");
});

bot.on('message', (msg) => {
  console.log(msg.text);
});
