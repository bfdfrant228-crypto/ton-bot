const { TelegramClient } = require('telegram');
const { StringSession } = require('telegram/sessions');
const { Api } = require('telegram');

(async () => {
  const client = new TelegramClient(
    new StringSession(process.env.TG_SESSION),
    Number(process.env.TG_API_ID),
    process.env.TG_API_HASH,
    { connectionRetries: 3 }
  );
  
  await client.connect();
  console.log('Подключён:', await client.isUserAuthorized());
  
  const botPeer = await client.getInputEntity('MRKTbot');
  
  const result = await client.invoke(
    new Api.messages.RequestWebView({
      peer: botPeer,
      bot: botPeer,
      fromBotMenu: false,
      url: 'https://cdn.tgmrkt.io/',
      platform: 'android',
    })
  );
  
  const fragment = new URL(result.url).hash.replace('#', '');
  const data = new URLSearchParams(fragment).get('tgWebAppData') || '';
  console.log('initData получен, длина:', data.length);
  
  // Отправляем initData на наш сервер — он сам получит токен от MRKT
  const updateResp = await fetch(process.env.SERVER_URL + '/api/admin/refresh-via-initdata', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'X-Admin-Secret': process.env.ADMIN_SECRET,
    },
    body: JSON.stringify({ initData: data })
  });
  
  const updateJson = await updateResp.json();
  console.log('Сервер ответил:', JSON.stringify(updateJson));
  
  if (!updateJson.ok) {
    console.error('Ошибка обновления токена');
    process.exit(1);
  }
  
  console.log('Токен обновлён успешно!', updateJson.tokenMask);
  await client.disconnect();
})().catch(e => { console.error('ERR:', e.message); process.exit(1); });
