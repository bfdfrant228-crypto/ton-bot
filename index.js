const TelegramBot = require('node-telegram-bot-api');

// =====================
// ENV
// =====================
const token = process.env.TELEGRAM_TOKEN;
if (!token) {
  console.error('TELEGRAM_TOKEN not set');
  process.exit(1);
}

const MODE = process.env.MODE || 'real';

// Redis
const REDIS_URL = process.env.REDIS_URL || process.env.REDIS_PRIVATE_URL || null;

// MRKT
const MRKT_API_URL = 'https://api.tgmrkt.io/api/v1';
const MRKT_AUTH = process.env.MRKT_AUTH || null;
const MRKT_COUNT = Number(process.env.MRKT_COUNT || 50);
const MRKT_PAGES = Number(process.env.MRKT_PAGES || 6);

// monitor
const CHECK_INTERVAL_MS = Number(process.env.CHECK_INTERVAL_MS || 7000);
const MAX_NOTIFICATIONS_PER_CHECK = Number(process.env.MAX_NOTIFICATIONS_PER_CHECK || 60);
const SEND_DELAY_MS = Number(process.env.SEND_DELAY_MS || 80);

// subscriptions
const SUBS_CHECK_INTERVAL_MS = Number(process.env.SUBS_CHECK_INTERVAL_MS || 9000);
const SUBS_MAX_NOTIFICATIONS_PER_CYCLE = Number(process.env.SUBS_MAX_NOTIFICATIONS_PER_CYCLE || 8);
const SUBS_EMPTY_CONFIRM = Number(process.env.SUBS_EMPTY_CONFIRM || 2);
const SUBS_MRKT_FEED_MAX_EVENTS_PER_CYCLE = Number(process.env.SUBS_MRKT_FEED_MAX_EVENTS_PER_CYCLE || 12);

// anti-spam
const SENT_TTL_MS = Number(process.env.SENT_TTL_MS || 24 * 60 * 60 * 1000);

// MRKT feed/history
const MRKT_FEED_COUNT = Number(process.env.MRKT_FEED_COUNT || 80);
const MRKT_HISTORY_TARGET_SALES = Number(process.env.MRKT_HISTORY_TARGET_SALES || 60);
const MRKT_HISTORY_MAX_PAGES = Number(process.env.MRKT_HISTORY_MAX_PAGES || 240);
const MRKT_FEED_THROTTLE_MS = Number(process.env.MRKT_FEED_THROTTLE_MS || 110);
const MRKT_HISTORY_TIME_BUDGET_MS = Number(process.env.MRKT_HISTORY_TIME_BUDGET_MS || 20000);

const MRKT_FEED_NOTIFY_TYPES_RAW = String(process.env.MRKT_FEED_NOTIFY_TYPES || 'sale,listing,change_price');
const MRKT_FEED_NOTIFY_TYPES = new Set(
  MRKT_FEED_NOTIFY_TYPES_RAW.split(',').map((s) => s.trim().toLowerCase()).filter(Boolean)
);

const MRKT_AUTH_NOTIFY_COOLDOWN_MS = Number(process.env.MRKT_AUTH_NOTIFY_COOLDOWN_MS || 60 * 60 * 1000);

// fees
const MRKT_FEE = Number(process.env.MRKT_FEE || 0);

// AUTO BUY (safe by default)
const AUTO_BUY_DRY_RUN = String(process.env.AUTO_BUY_DRY_RUN || '1') !== '0'; // 1 = ничего не покупаем, только проверяем логику

console.log('Bot version 2026-02-28-mrkt-only-autobuy-dryrun-v1');
console.log('MODE=', MODE);
console.log('REDIS_URL=', REDIS_URL ? 'set' : 'not set');
console.log('MRKT_AUTH=', MRKT_AUTH ? 'set' : 'not set');
console.log('AUTO_BUY_DRY_RUN=', AUTO_BUY_DRY_RUN ? 'true' : 'false');

const bot = new TelegramBot(token, { polling: true });

// =====================
// UI
// =====================
const MAIN_KEYBOARD = {
  keyboard: [
    [{ text: '🔍 Запустить поиск' }, { text: '⏹ Остановить поиск' }],
    [{ text: '💰 Макс. цена' }, { text: '💵 Мин. цена' }],
    [{ text: '💸 Цена подарка' }, { text: '📡 Подписки' }],
    [{ text: '🤖 Автопокупка' }, { text: '📌 Статус API' }],
    [{ text: '🎛 Фильтры' }],
  ],
  resize_keyboard: true,
};

// =====================
// State
// =====================
const users = new Map();
const sentDeals = new Map();
const subStates = new Map(); // `${userId}:${subId}:MRKT` -> { floor, emptyStreak, lastNotifiedFloor, feedLastId }

let isChecking = false;
let isSubsChecking = false;

// MRKT auth status
const mrktAuthState = {
  ok: null,
  lastOkAt: 0,
  lastFailAt: 0,
  lastFailCode: null,
  lastNotifiedAt: 0,
};

// history cache
const historyCache = new Map();
const HISTORY_CACHE_TTL_MS = 60_000;

// =====================
// Helpers
// =====================
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const nowMs = () => Date.now();

function n(x) {
  const v = Number(String(x).replace(',', '.'));
  return Number.isFinite(v) ? v : NaN;
}
function norm(s) {
  return String(s || '').toLowerCase().trim().replace(/\s+/g, ' ');
}
function capWords(str) {
  return String(str || '').replace(/\w+(?:'\w+)?/g, (w) => (w ? w[0].toUpperCase() + w.slice(1) : w));
}
function normTraitName(s) {
  return norm(s).replace(/\s*\([^)]*%[^)]*\)\s*/g, ' ').replace(/\s+/g, ' ').trim();
}
function sameTrait(actual, expectedLower) {
  if (!expectedLower) return true;
  return normTraitName(actual) === normTraitName(expectedLower);
}
function inRange(price, minPrice, maxPrice) {
  if (!Number.isFinite(price)) return false;
  const min = minPrice != null ? Number(minPrice) : 0;
  const max = maxPrice != null ? Number(maxPrice) : null;
  if (Number.isFinite(min) && price < min) return false;
  if (max != null && Number.isFinite(max) && price > max) return false;
  return true;
}
function median(sorted) {
  if (!sorted.length) return null;
  const L = sorted.length;
  return L % 2 ? sorted[(L - 1) / 2] : (sorted[L / 2 - 1] + sorted[L / 2]) / 2;
}
function pruneSentDeals() {
  const now = nowMs();
  for (const [k, ts] of sentDeals.entries()) {
    if (now - ts > SENT_TTL_MS) sentDeals.delete(k);
  }
}
function clearUserSentDeals(userId) {
  const prefix = `${userId}:`;
  for (const k of Array.from(sentDeals.keys())) {
    if (k.startsWith(prefix)) sentDeals.delete(k);
  }
}
function mrktLotUrlFromId(id) {
  if (!id) return 'https://t.me/mrkt';
  const appId = String(id).replace(/-/g, '');
  return `https://t.me/mrkt/app?startapp=${appId}`;
}
async function sendMessageSafe(chatId, text, opts) {
  while (true) {
    try {
      return await bot.sendMessage(chatId, text, opts);
    } catch (e) {
      const retryAfter =
        e?.response?.body?.parameters?.retry_after ??
        e?.response?.parameters?.retry_after ??
        null;
      if (retryAfter) {
        await sleep((Number(retryAfter) + 1) * 1000);
        continue;
      }
      throw e;
    }
  }
}

// =====================
// Redis persistence
// =====================
let redis = null;

async function initRedis() {
  if (!REDIS_URL) return;
  try {
    const { createClient } = require('redis');
    redis = createClient({ url: REDIS_URL });
    redis.on('error', (e) => console.error('Redis error:', e));
    await redis.connect();
    console.log('Redis connected');
  } catch (e) {
    console.error('Redis init failed:', e?.message || e);
    redis = null;
  }
}

function exportState() {
  const out = { users: {} };
  for (const [userId, u] of users.entries()) {
    out.users[String(userId)] = {
      enabled: !!u.enabled,
      minPriceTon: typeof u.minPriceTon === 'number' ? u.minPriceTon : 0,
      maxPriceTon: typeof u.maxPriceTon === 'number' ? u.maxPriceTon : null,
      state: null,
      filters: u.filters,
      subscriptions: u.subscriptions || [],
      autoBuyEnabled: !!u.autoBuyEnabled,
    };
  }
  return out;
}

function renumberSubs(user) {
  const subs = Array.isArray(user.subscriptions) ? user.subscriptions : [];
  subs.forEach((s, idx) => { if (s) s.num = idx + 1; });
}

function importState(parsed) {
  const objUsers = parsed?.users && typeof parsed.users === 'object' ? parsed.users : {};
  for (const [idStr, u] of Object.entries(objUsers)) {
    const userId = Number(idStr);
    if (!Number.isFinite(userId)) continue;

    const safe = {
      enabled: u?.enabled !== false,
      minPriceTon: typeof u?.minPriceTon === 'number' ? u.minPriceTon : 0,
      maxPriceTon: typeof u?.maxPriceTon === 'number' ? u.maxPriceTon : null,
      state: null,
      autoBuyEnabled: !!u?.autoBuyEnabled,
      filters: {
        gift: typeof u?.filters?.gift === 'string' ? u.filters.gift : '',
        model: typeof u?.filters?.model === 'string' ? u.filters.model : '',
        backdrop: typeof u?.filters?.backdrop === 'string' ? u.filters.backdrop : '',
      },
      subscriptions: Array.isArray(u?.subscriptions) ? u.subscriptions : [],
    };

    for (const s of safe.subscriptions) {
      if (!s || typeof s !== 'object') continue;
      if (!s.id) s.id = `sub_${Date.now()}_${Math.random().toString(16).slice(2, 6)}`;
      if (s.enabled == null) s.enabled = true;
      if (!s.filters) s.filters = {};
      if (typeof s.filters.gift !== 'string') s.filters.gift = safe.filters.gift || '';
      if (typeof s.filters.model !== 'string') s.filters.model = safe.filters.model || '';
      if (typeof s.filters.backdrop !== 'string') s.filters.backdrop = safe.filters.backdrop || '';
      if (s.maxPriceTon != null && !Number.isFinite(Number(s.maxPriceTon))) s.maxPriceTon = null;
      if (typeof s.num !== 'number') s.num = 0;
    }

    renumberSubs(safe);
    users.set(userId, safe);
  }
}

async function loadState() {
  if (!redis) return;
  const raw = await redis.get('bot:state:mrkt:v1');
  if (!raw) return;
  importState(JSON.parse(raw));
  console.log('Loaded state from Redis. users:', users.size);
}

let saveTimer = null;
function scheduleSave() {
  if (!redis) return;
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    saveState().catch((e) => console.error('saveState error:', e));
  }, 350);
}
async function saveState() {
  if (!redis) return;
  await redis.set('bot:state:mrkt:v1', JSON.stringify(exportState()));
}

// =====================
// User
// =====================
function getOrCreateUser(userId) {
  if (!users.has(userId)) {
    users.set(userId, {
      enabled: true,
      minPriceTon: 0,
      maxPriceTon: null,

      // awaiting_max | awaiting_min | awaiting_gift | awaiting_model | awaiting_backdrop | awaiting_sub_max:<subId>
      state: null,

      autoBuyEnabled: false,

      filters: {
        gift: '',      // MRKT collectionName
        model: '',     // modelName
        backdrop: '',  // backdropName
      },

      subscriptions: [],
    });
  }
  return users.get(userId);
}

// =====================
// MRKT auth alert
// =====================
async function notifyMrktAuthExpired(statusCode) {
  const now = nowMs();
  if (now - mrktAuthState.lastNotifiedAt < MRKT_AUTH_NOTIFY_COOLDOWN_MS) return;
  mrktAuthState.lastNotifiedAt = now;

  const text =
    `⚠️ MRKT токен не работает (HTTP ${statusCode}).\n` +
    `Обнови MRKT_AUTH в Railway Variables.`;

  if (ADMIN_CHAT_ID && Number.isFinite(ADMIN_CHAT_ID)) {
    try { await sendMessageSafe(ADMIN_CHAT_ID, text, { disable_web_page_preview: true }); return; } catch {}
  }

  for (const [uid] of users.entries()) {
    try { await sendMessageSafe(uid, text, { disable_web_page_preview: true }); } catch {}
  }
}

function markMrktOk() {
  mrktAuthState.ok = true;
  mrktAuthState.lastOkAt = nowMs();
}
async function markMrktFailIfAuth(statusCode) {
  mrktAuthState.ok = false;
  mrktAuthState.lastFailAt = nowMs();
  mrktAuthState.lastFailCode = statusCode;
  if (statusCode === 401 || statusCode === 403) await notifyMrktAuthExpired(statusCode);
}

// =====================
// MRKT API
// =====================
async function mrktFetchSalingPage({ collectionName, modelName, backdropName, cursor }) {
  if (!MRKT_AUTH) return { ok: false, reason: 'NO_AUTH', gifts: [], cursor: '' };

  const body = {
    collectionNames: [collectionName],
    modelNames: modelName ? [modelName] : [],
    backdropNames: backdropName ? [backdropName] : [],
    symbolNames: [],
    ordering: 'Price',
    lowToHigh: true,
    maxPrice: null,
    minPrice: null,
    mintable: null,
    number: null,
    count: MRKT_COUNT,
    cursor: cursor || '',
    query: null,
    promotedFirst: false,
  };

  const res = await fetch(`${MRKT_API_URL}/gifts/saling`, {
    method: 'POST',
    headers: {
      Authorization: MRKT_AUTH,
      'Content-Type': 'application/json',
      Accept: 'application/json',
    },
    body: JSON.stringify(body),
  }).catch(() => null);

  if (!res) return { ok: false, reason: 'FETCH_ERROR', gifts: [], cursor: '' };
  if (!res.ok) {
    await markMrktFailIfAuth(res.status);
    return { ok: false, reason: `HTTP_${res.status}`, gifts: [], cursor: '' };
  }
  markMrktOk();

  const data = await res.json().catch(() => null);
  const gifts = Array.isArray(data?.gifts) ? data.gifts : [];
  const nextCursor = data?.cursor || '';
  return { ok: true, reason: 'OK', gifts, cursor: nextCursor };
}

async function mrktSearchLots({ gift, model, backdrop }, minPriceTon, maxPriceTon) {
  if (!gift) return { ok: false, reason: 'NO_GIFT', gifts: [] };

  let cursor = '';
  const out = [];

  for (let page = 0; page < MRKT_PAGES; page++) {
    const r = await mrktFetchSalingPage({
      collectionName: gift,
      modelName: model || null,
      backdropName: backdrop || null,
      cursor,
    });

    if (!r.ok) return { ok: false, reason: r.reason, gifts: [] };

    for (const g of r.gifts) {
      const nanoA = g?.salePriceWithoutFee ?? null;
      const nanoB = g?.salePrice ?? null;
      const nano =
        nanoA != null && nanoB != null ? Math.min(Number(nanoA), Number(nanoB)) : (nanoA ?? nanoB);

      if (nano == null) continue;
      const priceTon = Number(nano) / 1e9;
      if (!Number.isFinite(priceTon) || priceTon <= 0) continue;
      if (!inRange(priceTon, minPriceTon, maxPriceTon)) continue;

      const baseName = (g.collectionTitle || g.collectionName || g.title || gift).trim();
      const number = g.number ?? null;
      const displayName = number ? `${baseName} #${number}` : baseName;

      const modelName = g.modelTitle || g.modelName || '';
      const backdropName = g.backdropName || '';
      const symbolName = g.symbolName || '';

      if (model && !sameTrait(modelName, norm(model))) continue;
      if (backdrop && !sameTrait(backdropName, norm(backdrop))) continue;

      let urlTelegram = 'https://t.me/mrkt';
      if (g.name && String(g.name).includes('-')) urlTelegram = `https://t.me/nft/${g.name}`;

      const urlMarket = g.id ? mrktLotUrlFromId(g.id) : 'https://t.me/mrkt';

      out.push({
        id: g.id ? `mrkt_${g.id}` : `mrkt_${displayName}_${priceTon}`,
        market: 'MRKT',
        name: displayName,
        priceTon,
        urlTelegram,
        urlMarket,
        attrs: { model: modelName || null, backdrop: backdropName || null, symbol: symbolName || null },
        raw: g,
      });
    }

    cursor = r.cursor || '';
    if (!cursor) break;
    if (out.length >= MAX_PER_MARKET) break;
  }

  out.sort((a, b) => a.priceTon - b.priceTon);
  return { ok: true, reason: 'OK', gifts: out.slice(0, MAX_PER_MARKET) };
}

async function mrktFeedFetch({ gift, model, backdrop, cursor, count, types }) {
  if (!MRKT_AUTH) return { ok: false, reason: 'NO_AUTH', items: [], cursor: '' };

  const body = {
    count: Number(count || MRKT_FEED_COUNT),
    cursor: cursor || '',
    collectionNames: gift ? [gift] : [],
    modelNames: model ? [model] : [],
    backdropNames: backdrop ? [backdrop] : [],
    lowToHigh: false,
    maxPrice: null,
    minPrice: null,
    number: null,
    ordering: 'Latest',
    query: null,
    type: Array.isArray(types) ? types : [],
  };

  const res = await fetch(`${MRKT_API_URL}/feed`, {
    method: 'POST',
    headers: {
      Authorization: MRKT_AUTH,
      'Content-Type': 'application/json',
      Accept: 'application/json',
    },
    body: JSON.stringify(body),
  }).catch(() => null);

  if (!res) return { ok: false, reason: 'FETCH_ERROR', items: [], cursor: '' };
  if (!res.ok) {
    await markMrktFailIfAuth(res.status);
    return { ok: false, reason: `HTTP_${res.status}`, items: [], cursor: '' };
  }
  markMrktOk();

  const data = await res.json().catch(() => null);
  const items = Array.isArray(data?.items) ? data.items : [];
  const nextCursor = data?.cursor || '';
  return { ok: true, reason: 'OK', items, cursor: nextCursor };
}

async function mrktHistorySalesEstimate({ gift, model, backdrop }) {
  const key = `mrkt_hist|${gift}|${model || ''}|${backdrop || ''}|target=${MRKT_HISTORY_TARGET_SALES}`;
  const now = nowMs();
  const cached = historyCache.get(key);
  if (cached && now - cached.time < HISTORY_CACHE_TTL_MS) return cached;

  const started = nowMs();
  let cursor = '';
  const prices = [];
  let pages = 0;

  while (pages < MRKT_HISTORY_MAX_PAGES && prices.length < MRKT_HISTORY_TARGET_SALES) {
    if (nowMs() - started > MRKT_HISTORY_TIME_BUDGET_MS) break;

    const r = await mrktFeedFetch({ gift, model, backdrop, cursor, count: MRKT_FEED_COUNT, types: ['sale'] });
    if (!r.ok) {
      const outErr = { ok: false, median: null, count: prices.length, time: now, reason: r.reason };
      historyCache.set(key, outErr);
      return outErr;
    }
    if (!r.items.length) break;

    for (const it of r.items) {
      const type = String(it?.type || '').toLowerCase();
      if (type && type !== 'sale') continue;

      const g = it?.gift;
      if (!g) continue;

      if (model) {
        const m = g.modelTitle || g.modelName || '';
        if (!sameTrait(m, norm(model))) continue;
      }
      if (backdrop) {
        const b = g.backdropName || '';
        if (!sameTrait(b, norm(backdrop))) continue;
      }

      const amountNano = it?.amount ?? g?.salePrice ?? null;
      const ton = Number(amountNano) / 1e9;
      if (!Number.isFinite(ton) || ton <= 0) continue;

      prices.push(ton);
      if (prices.length >= MRKT_HISTORY_TARGET_SALES) break;
    }

    cursor = r.cursor || '';
    pages++;
    if (!cursor) break;
    if (MRKT_FEED_THROTTLE_MS > 0) await sleep(MRKT_FEED_THROTTLE_MS);
  }

  prices.sort((a, b) => a - b);
  const out = { ok: true, median: median(prices), count: prices.length, time: now, reason: 'OK' };
  historyCache.set(key, out);
  return out;
}

// smart fallback: gift+model+backdrop -> gift+model -> gift only
async function mrktSmartHistory({ gift, model, backdrop }) {
  let h = await mrktHistorySalesEstimate({ gift, model, backdrop });
  if (h.ok && h.median != null) return { ...h, level: 'gift+model+backdrop' };

  if (model) {
    h = await mrktHistorySalesEstimate({ gift, model, backdrop: '' });
    if (h.ok && h.median != null) return { ...h, level: 'gift+model' };
  }

  h = await mrktHistorySalesEstimate({ gift, model: '', backdrop: '' });
  if (h.ok && h.median != null) return { ...h, level: 'gift only' };

  return { ok: true, median: null, count: 0, level: null };
}

// =====================
// Send deal
// =====================
async function sendDeal(userId, gift) {
  const lines = [];
  lines.push(`Price: ${gift.priceTon.toFixed(3)} TON`);
  lines.push(`Gift: ${gift.name}`);
  if (gift.attrs?.model) lines.push(`Model: ${gift.attrs.model}`);
  if (gift.attrs?.symbol) lines.push(`Symbol: ${gift.attrs.symbol}`);
  if (gift.attrs?.backdrop) lines.push(`Backdrop: ${gift.attrs.backdrop}`);
  lines.push(`Market: MRKT`);
  if (gift.urlTelegram) lines.push(gift.urlTelegram);

  const reply_markup = gift.urlMarket
    ? { inline_keyboard: [[{ text: 'Открыть MRKT', url: gift.urlMarket }]] }
    : undefined;

  await sendMessageSafe(userId, lines.join('\n'), { disable_web_page_preview: false, reply_markup });
}

// =====================
// AUTO BUY (DRY-RUN пока без реальной покупки)
// =====================
async function attemptAutoBuyMrkt(userId, gift, user) {
  if (!user.autoBuyEnabled) return false;

  // DRY RUN — ничего не покупаем
  if (AUTO_BUY_DRY_RUN) {
    await sendMessageSafe(
      userId,
      `🤖 Автопокупка (DRY RUN):\n` +
        `Я бы попытался купить:\n` +
        `${gift.name}\n` +
        `Цена: ${gift.priceTon.toFixed(3)} TON\n\n` +
        `Чтобы включить реальную покупку, мне нужен 1 пример запроса "Buy" из DevTools MRKT (URL+Payload+Response).`,
      { disable_web_page_preview: true }
    );
    return true;
  }

  // Реальная покупка будет добавлена после того, как ты пришлёшь buy-endpoint/tx payload
  await sendMessageSafe(
    userId,
    `Автопокупка: пока не реализована.\nНужен buy-endpoint MRKT (см. инструкции).`,
    { disable_web_page_preview: true }
  );
  return false;
}

// =====================
// Sellprice
// =====================
async function sendSellPriceForUser(chatId, user) {
  const gift = user.filters.gift;
  const model = user.filters.model;
  const backdrop = user.filters.backdrop;

  if (!gift) {
    return sendMessageSafe(chatId, 'Сначала задай подарок: 🎛 Фильтры → Ввести подарок', { reply_markup: MAIN_KEYBOARD });
  }

  let text = 'Оценка цен продажи (MRKT):\n\n';
  text += `Подарок: ${gift}\n`;
  text += `Модель: ${model || 'любая'}\n`;
  text += `Фон: ${backdrop || 'любой'}\n\n`;

  const lots = await mrktSearchLots({ gift, model, backdrop }, null, null);
  if (lots.ok && lots.gifts.length) {
    const best = lots.gifts[0];
    const net = best.priceTon * (1 - MRKT_FEE);
    text += `MRKT (флор): ~${best.priceTon.toFixed(3)} TON\n`;
    text += `Чистыми (комиссия ${(MRKT_FEE * 100).toFixed(1)}%): ~${net.toFixed(3)} TON\n`;
  } else {
    text += `MRKT: активных лотов нет\n`;
    const hs = await mrktSmartHistory({ gift, model, backdrop });
    if (hs.ok && hs.median != null) {
      text += `MRKT (история продаж): ~${hs.median.toFixed(3)} TON (уровень: ${hs.level}, n=${hs.count})\n`;
    } else {
      text += `MRKT (история продаж): нет данных\n`;
    }
  }

  await sendMessageSafe(chatId, text, { reply_markup: MAIN_KEYBOARD });
}

// =====================
// Subscriptions
// =====================
async function showSubsMenu(chatId) {
  const inlineKeyboard = {
    inline_keyboard: [
      [{ text: '➕ Создать из текущих фильтров', callback_data: 'sub_add_current' }],
      [{ text: '📄 Мои подписки', callback_data: 'sub_list' }],
      [{ text: '🔄 Проверить сейчас', callback_data: 'sub_check_now' }],
    ],
  };
  await sendMessageSafe(chatId, 'Подписки:', { reply_markup: inlineKeyboard });
}

async function showSubsList(chatId, user) {
  const subs = user.subscriptions || [];
  if (!subs.length) {
    return sendMessageSafe(chatId, 'Подписок нет.\n📡 Подписки → ➕ Создать', { reply_markup: MAIN_KEYBOARD });
  }

  let text = 'Мои подписки:\n\n';
  for (const s of subs) {
    text += `#${s.num} ${s.enabled ? 'ON' : 'OFF'}\nGift: ${s.filters.gift}\nModel: ${s.filters.model || 'any'}\nBackdrop: ${s.filters.backdrop || 'any'}\nMax: ${s.maxPriceTon ?? '∞'}\n\n`;
  }

  const inline_keyboard = subs.slice(0, 25).map((s) => ([
    { text: s.enabled ? `⏸ #${s.num}` : `▶️ #${s.num}`, callback_data: `sub_toggle:${s.id}` },
    { text: `💰 Max`, callback_data: `sub_setmax:${s.id}` },
    { text: `🗑`, callback_data: `sub_delete:${s.id}` },
  ]));

  await sendMessageSafe(chatId, text.slice(0, 3900), { reply_markup: { inline_keyboard } });
}

function findSub(user, subId) {
  return (user.subscriptions || []).find((s) => s && s.id === subId) || null;
}

function makeSubFromCurrentFilters(user) {
  if (!user.filters.gift) return { ok: false, reason: 'NO_GIFT' };

  const sub = {
    id: `sub_${Date.now()}_${Math.random().toString(16).slice(2, 6)}`,
    num: (user.subscriptions?.length || 0) + 1,
    enabled: true,
    maxPriceTon: user.maxPriceTon ?? null,
    filters: {
      gift: user.filters.gift,
      model: user.filters.model || '',
      backdrop: user.filters.backdrop || '',
    },
  };
  return { ok: true, sub };
}

async function notifySubFloor(userId, sub, prevFloor, newFloor, lot) {
  let text = `${sub.filters.gift}\n`;
  if (prevFloor == null) text += `Флор: ${newFloor.toFixed(3)} TON\n`;
  else text += `Флор изменился: ${prevFloor.toFixed(3)} -> ${newFloor.toFixed(3)} TON\n`;
  if (sub.filters.model) text += `Model: ${sub.filters.model}\n`;
  if (sub.filters.backdrop) text += `Backdrop: ${sub.filters.backdrop}\n`;
  if (sub.maxPriceTon != null) text += `Max: ${Number(sub.maxPriceTon).toFixed(3)} TON\n`;
  if (lot?.urlTelegram) text += lot.urlTelegram;

  const reply_markup = lot?.urlMarket ? { inline_keyboard: [[{ text: 'Открыть MRKT', url: lot.urlMarket }]] } : undefined;
  await sendMessageSafe(userId, text.trim(), { disable_web_page_preview: false, reply_markup });
}

async function processMrktFeedForSub(userId, sub, stateKey, budgetEvents) {
  if (budgetEvents <= 0) return 0;

  const st = subStates.get(stateKey) || { feedLastId: null, floor: null, emptyStreak: 0, lastNotifiedFloor: null };

  const r = await mrktFeedFetch({
    gift: sub.filters.gift,
    model: sub.filters.model || null,
    backdrop: sub.filters.backdrop || null,
    cursor: '',
    count: MRKT_FEED_COUNT,
    types: [],
  });

  if (!r.ok || !r.items.length) return 0;

  const latestId = r.items[0]?.id || null;
  if (!latestId) return 0;

  if (!st.feedLastId) {
    subStates.set(stateKey, { ...st, feedLastId: latestId });
    return 0;
  }

  const newItems = [];
  for (const it of r.items) {
    if (!it?.id) continue;
    if (it.id === st.feedLastId) break;
    newItems.push(it);
  }

  if (!newItems.length) {
    subStates.set(stateKey, { ...st, feedLastId: latestId });
    return 0;
  }

  newItems.reverse();

  let sent = 0;
  for (const it of newItems) {
    if (sent >= budgetEvents) break;
    const type = String(it?.type || '').toLowerCase();
    if (!MRKT_FEED_NOTIFY_TYPES.has(type)) continue;
    await sendMessageSafe(userId, `MRKT событие: ${type}\n${sub.filters.gift}`, { disable_web_page_preview: true });
    sent++;
  }

  subStates.set(stateKey, { ...st, feedLastId: latestId });
  return sent;
}

async function checkSubscriptionsForAllUsers({ manual = false } = {}) {
  if (MODE !== 'real') return { processedSubs: 0, floorNotifs: 0, feedNotifs: 0 };
  if (isSubsChecking && !manual) return { processedSubs: 0, floorNotifs: 0, feedNotifs: 0 };

  isSubsChecking = true;
  try {
    let processedSubs = 0;
    let floorNotifs = 0;
    let feedNotifs = 0;

    let globalFeedBudget = SUBS_MRKT_FEED_MAX_EVENTS_PER_CYCLE;

    for (const [userId, user] of users.entries()) {
      const subs = Array.isArray(user.subscriptions) ? user.subscriptions : [];
      const active = subs.filter((s) => s && s.enabled);
      if (!active.length) continue;

      for (const sub of active) {
        processedSubs++;
        if (floorNotifs >= SUBS_MAX_NOTIFICATIONS_PER_CYCLE) break;

        const stateKey = `${userId}:${sub.id}:MRKT`;
        const prevState = subStates.get(stateKey) || { floor: null, emptyStreak: 0, lastNotifiedFloor: null, feedLastId: null };

        const lots = await mrktSearchLots({ gift: sub.filters.gift, model: sub.filters.model, backdrop: sub.filters.backdrop }, null, null);
        if (!lots.ok) continue;

        const lot = lots.gifts[0] || null;
        const newFloor = lot ? lot.priceTon : null;

        let emptyStreak = prevState.emptyStreak || 0;
        if (newFloor == null) {
          emptyStreak++;
          if (emptyStreak < SUBS_EMPTY_CONFIRM) {
            subStates.set(stateKey, { ...prevState, emptyStreak });
            continue;
          }
        } else {
          emptyStreak = 0;
        }

        const prevFloor = prevState.floor;
        const max = sub.maxPriceTon != null ? Number(sub.maxPriceTon) : null;
        const canNotify = newFloor != null && (max == null || newFloor <= max);

        if (prevFloor == null && newFloor != null && canNotify) {
          await notifySubFloor(userId, sub, null, newFloor, lot);
          floorNotifs++;
          subStates.set(stateKey, { ...prevState, floor: newFloor, emptyStreak, lastNotifiedFloor: newFloor });
        } else if (prevFloor != null && newFloor != null && Number(prevFloor) !== Number(newFloor) && canNotify) {
          await notifySubFloor(userId, sub, prevFloor, newFloor, lot);
          floorNotifs++;
          subStates.set(stateKey, { ...prevState, floor: newFloor, emptyStreak, lastNotifiedFloor: newFloor });
        } else {
          subStates.set(stateKey, { ...prevState, floor: newFloor, emptyStreak });
        }

        if (globalFeedBudget > 0) {
          const sent = await processMrktFeedForSub(userId, sub, stateKey, globalFeedBudget);
          globalFeedBudget -= sent;
          feedNotifs += sent;
        }
      }
    }

    return { processedSubs, floorNotifs, feedNotifs };
  } finally {
    isSubsChecking = false;
  }
}

// =====================
// Monitor cheap lots
// =====================
async function checkMarketsForAllUsers() {
  if (MODE !== 'real') return;
  if (isChecking) return;

  isChecking = true;
  try {
    pruneSentDeals();

    for (const [userId, user] of users.entries()) {
      if (!user.enabled) continue;
      if (!user.maxPriceTon) continue;
      if (!user.filters.gift) continue;

      const gift = user.filters.gift;
      const model = user.filters.model || '';
      const backdrop = user.filters.backdrop || '';

      const minP = user.minPriceTon != null ? Number(user.minPriceTon) : 0;
      const maxP = Number(user.maxPriceTon);

      const r = await mrktSearchLots({ gift, model, backdrop }, minP, maxP);
      if (!r.ok || !r.gifts.length) continue;

      let sent = 0;
      for (const giftObj of r.gifts) {
        if (sent >= MAX_NOTIFICATIONS_PER_CHECK) break;
        if (!inRange(giftObj.priceTon, minP, maxP)) continue;

        const key = `${userId}:${giftObj.id}`;
        if (sentDeals.has(key)) continue;

        sentDeals.set(key, nowMs());

        // Автопокупка (DRY RUN) — выполняется ДО обычного уведомления
        const autoBought = await attemptAutoBuyMrkt(userId, giftObj, user);

        // Обычное уведомление
        await sendDeal(userId, giftObj);
        sent++;

        if (SEND_DELAY_MS > 0) await sleep(SEND_DELAY_MS);

        // если авто-покупка включена — можно ограничить 1 попыткой за цикл
        if (autoBought) break;
      }
    }
  } catch (e) {
    console.error('monitor interval error:', e);
  } finally {
    isChecking = false;
  }
}

// =====================
// Commands
// =====================
bot.onText(/^\/start\b/, async (msg) => {
  getOrCreateUser(msg.from.id);
  await sendMessageSafe(msg.chat.id, 'Бот запущен (MRKT-only).', { reply_markup: MAIN_KEYBOARD });
});

bot.onText(/^\/status\b/, async (msg) => {
  const user = getOrCreateUser(msg.from.id);
  const text =
    `Настройки:\n` +
    `• Мониторинг: ${user.enabled ? 'ON' : 'OFF'}\n` +
    `• Диапазон: ${(user.minPriceTon ?? 0).toFixed(3)} .. ${user.maxPriceTon != null ? user.maxPriceTon.toFixed(3) : '∞'} TON\n` +
    `• Gift: ${user.filters.gift || 'не выбран'}\n` +
    `• Model: ${user.filters.model || 'any'}\n` +
    `• Backdrop: ${user.filters.backdrop || 'any'}\n` +
    `• Автопокупка: ${user.autoBuyEnabled ? 'ON' : 'OFF'}\n` +
    `• Подписок: ${(user.subscriptions || []).length}\n\n` +
    `API:\n` +
    `• MRKT auth: ${MRKT_AUTH ? '✅' : '❌'}\n` +
    `• MRKT last ok: ${mrktAuthState.lastOkAt ? new Date(mrktAuthState.lastOkAt).toLocaleString() : '-'}\n` +
    `• MRKT last fail: ${mrktAuthState.lastFailAt ? `HTTP ${mrktAuthState.lastFailCode}` : '-'}\n` +
    `• Redis: ${redis ? '✅' : '❌'}\n` +
    `• AUTO_BUY_DRY_RUN: ${AUTO_BUY_DRY_RUN ? 'true' : 'false'}\n`;
  await sendMessageSafe(msg.chat.id, text, { reply_markup: MAIN_KEYBOARD });
});

bot.onText(/^\/sellprice\b/, async (msg) => {
  const user = getOrCreateUser(msg.from.id);
  await sendSellPriceForUser(msg.chat.id, user);
});

// =====================
// Message handler (buttons + states)
// =====================
bot.on('message', async (msg) => {
  const userId = msg.from?.id;
  const chatId = msg.chat?.id;
  const text = msg.text;

  if (!userId || !chatId || !text) return;
  if (text.startsWith('/')) return;

  const user = getOrCreateUser(userId);
  const t = text.trim();

  // states
  if (user.state === 'awaiting_max') {
    const v = n(t);
    if (!Number.isFinite(v) || v <= 0) return sendMessageSafe(chatId, 'Введи MAX TON (пример: 12)', { reply_markup: MAIN_KEYBOARD });
    user.maxPriceTon = v;
    user.state = null;
    clearUserSentDeals(userId);
    scheduleSave();
    return sendMessageSafe(chatId, `Ок. MAX: ${v.toFixed(3)} TON`, { reply_markup: MAIN_KEYBOARD });
  }

  if (user.state === 'awaiting_min') {
    const v = n(t);
    if (!Number.isFinite(v) || v < 0) return sendMessageSafe(chatId, 'Введи MIN TON (0 = убрать)', { reply_markup: MAIN_KEYBOARD });
    user.minPriceTon = v === 0 ? 0 : v;
    user.state = null;
    clearUserSentDeals(userId);
    scheduleSave();
    return sendMessageSafe(chatId, `Ок. MIN: ${user.minPriceTon.toFixed(3)} TON`, { reply_markup: MAIN_KEYBOARD });
  }

  if (user.state === 'awaiting_gift') {
    user.state = null;
    user.filters.gift = capWords(t);
    scheduleSave();
    return sendMessageSafe(chatId, `Ок. Gift: ${user.filters.gift}`, { reply_markup: MAIN_KEYBOARD });
  }

  if (user.state === 'awaiting_model') {
    user.state = null;
    user.filters.model = capWords(t);
    scheduleSave();
    return sendMessageSafe(chatId, `Ок. Model: ${user.filters.model}`, { reply_markup: MAIN_KEYBOARD });
  }

  if (user.state === 'awaiting_backdrop') {
    user.state = null;
    user.filters.backdrop = capWords(t);
    scheduleSave();
    return sendMessageSafe(chatId, `Ок. Backdrop: ${user.filters.backdrop}`, { reply_markup: MAIN_KEYBOARD });
  }

  if (typeof user.state === 'string' && user.state.startsWith('awaiting_sub_max:')) {
    const subId = user.state.split(':')[1];
    const v = n(t);
    if (!Number.isFinite(v) || v <= 0) return sendMessageSafe(chatId, 'Введи MAX для подписки (пример: 10).', { reply_markup: MAIN_KEYBOARD });

    const sub = findSub(user, subId);
    user.state = null;
    scheduleSave();

    if (!sub) return sendMessageSafe(chatId, 'Подписка не найдена.', { reply_markup: MAIN_KEYBOARD });
    sub.maxPriceTon = v;

    return sendMessageSafe(chatId, `Ок. MAX для подписки #${sub.num}: ${v.toFixed(3)} TON`, { reply_markup: MAIN_KEYBOARD });
  }

  // buttons
  if (t === '🔍 Запустить поиск') {
    user.enabled = true; scheduleSave();
    return sendMessageSafe(chatId, 'Мониторинг включён.', { reply_markup: MAIN_KEYBOARD });
  }
  if (t === '⏹ Остановить поиск') {
    user.enabled = false; scheduleSave();
    return sendMessageSafe(chatId, 'Мониторинг остановлен.', { reply_markup: MAIN_KEYBOARD });
  }
  if (t === '💰 Макс. цена') {
    user.state = 'awaiting_max'; scheduleSave();
    return sendMessageSafe(chatId, 'Введи MAX TON:', { reply_markup: MAIN_KEYBOARD });
  }
  if (t === '💵 Мин. цена') {
    user.state = 'awaiting_min'; scheduleSave();
    return sendMessageSafe(chatId, 'Введи MIN TON (0 = убрать):', { reply_markup: MAIN_KEYBOARD });
  }
  if (t === '💸 Цена подарка') return sendSellPriceForUser(chatId, user);
  if (t === '📡 Подписки') return showSubsMenu(chatId);

  if (t === '🤖 Автопокупка') {
    user.autoBuyEnabled = !user.autoBuyEnabled;
    scheduleSave();
    return sendMessageSafe(chatId, `Автопокупка: ${user.autoBuyEnabled ? 'ON' : 'OFF'} (сейчас DRY RUN)`, { reply_markup: MAIN_KEYBOARD });
  }

  if (t === '📌 Статус API') {
    const text =
      `API статус:\n` +
      `• MRKT auth: ${MRKT_AUTH ? '✅' : '❌'}\n` +
      `• MRKT last ok: ${mrktAuthState.lastOkAt ? new Date(mrktAuthState.lastOkAt).toLocaleString() : '-'}\n` +
      `• MRKT last fail: ${mrktAuthState.lastFailAt ? `HTTP ${mrktAuthState.lastFailCode}` : '-'}\n` +
      `• Redis: ${redis ? '✅' : '❌'}\n` +
      `• AUTO_BUY_DRY_RUN: ${AUTO_BUY_DRY_RUN ? 'true' : 'false'}\n`;
    return sendMessageSafe(chatId, text, { reply_markup: MAIN_KEYBOARD });
  }

  if (t === '🎛 Фильтры') {
    const inlineKeyboard = {
      inline_keyboard: [
        [{ text: '✍️ Ввести подарок (collection)', callback_data: 'set_gift' }],
        [{ text: '✍️ Ввести модель', callback_data: 'set_model' }],
        [{ text: '✍️ Ввести фон', callback_data: 'set_backdrop' }],
        [{ text: '♻️ Сбросить модель', callback_data: 'clear_model' }],
        [{ text: '♻️ Сбросить фон', callback_data: 'clear_backdrop' }],
        [{ text: '♻️ Сбросить всё', callback_data: 'clear_all' }],
        [{ text: 'ℹ️ Показать', callback_data: 'show_filters' }],
      ],
    };
    return sendMessageSafe(chatId, 'Фильтры MRKT:', { reply_markup: inlineKeyboard });
  }

  return sendMessageSafe(chatId, 'Используй кнопки снизу или /status', { reply_markup: MAIN_KEYBOARD });
});

// callbacks
bot.on('callback_query', async (q) => {
  const userId = q.from.id;
  const chatId = q.message?.chat?.id;
  const data = q.data || '';
  const user = getOrCreateUser(userId);

  try {
    if (data === 'set_gift') {
      user.state = 'awaiting_gift'; scheduleSave();
      await sendMessageSafe(chatId, 'Введи название коллекции MRKT (например: Victory Medal):', { reply_markup: MAIN_KEYBOARD });
    } else if (data === 'set_model') {
      user.state = 'awaiting_model'; scheduleSave();
      await sendMessageSafe(chatId, 'Введи модель (например: Queen Bee):', { reply_markup: MAIN_KEYBOARD });
    } else if (data === 'set_backdrop') {
      user.state = 'awaiting_backdrop'; scheduleSave();
      await sendMessageSafe(chatId, 'Введи фон (например: Old Gold):', { reply_markup: MAIN_KEYBOARD });
    } else if (data === 'clear_model') {
      user.filters.model = ''; scheduleSave();
      await sendMessageSafe(chatId, 'Модель сброшена.', { reply_markup: MAIN_KEYBOARD });
    } else if (data === 'clear_backdrop') {
      user.filters.backdrop = ''; scheduleSave();
      await sendMessageSafe(chatId, 'Фон сброшен.', { reply_markup: MAIN_KEYBOARD });
    } else if (data === 'clear_all') {
      user.filters.gift = ''; user.filters.model = ''; user.filters.backdrop = '';
      scheduleSave();
      await sendMessageSafe(chatId, 'Фильтры сброшены.', { reply_markup: MAIN_KEYBOARD });
    } else if (data === 'show_filters') {
      await sendMessageSafe(
        chatId,
        `Текущие:\nGift: ${user.filters.gift || '-'}\nModel: ${user.filters.model || '-'}\nBackdrop: ${user.filters.backdrop || '-'}\nAutoBuy: ${user.autoBuyEnabled ? 'ON' : 'OFF'}`,
        { reply_markup: MAIN_KEYBOARD }
      );
    }

    // subs
    else if (data === 'sub_add_current') {
      const r = makeSubFromCurrentFilters(user);
      if (!r.ok) {
        await sendMessageSafe(chatId, 'Сначала задай Gift в фильтрах.', { reply_markup: MAIN_KEYBOARD });
      } else {
        user.subscriptions.push(r.sub);
        renumberSubs(user);
        scheduleSave();
        await sendMessageSafe(chatId, `Подписка создана: #${r.sub.num}`, { reply_markup: MAIN_KEYBOARD });
      }
    } else if (data === 'sub_list') {
      await showSubsList(chatId, user);
    } else if (data === 'sub_check_now') {
      await sendMessageSafe(chatId, 'Проверяю подписки...', { reply_markup: MAIN_KEYBOARD });
      const st = await checkSubscriptionsForAllUsers({ manual: true });
      await sendMessageSafe(chatId, `Готово.\nSubs: ${st.processedSubs}\nFloor notifs: ${st.floorNotifs}\nFeed notifs: ${st.feedNotifs}`, { reply_markup: MAIN_KEYBOARD });
    } else if (data.startsWith('sub_toggle:')) {
      const subId = data.split(':')[1];
      const sub = findSub(user, subId);
      if (!sub) return sendMessageSafe(chatId, 'Подписка не найдена.', { reply_markup: MAIN_KEYBOARD });
      sub.enabled = !sub.enabled;
      scheduleSave();
      await sendMessageSafe(chatId, `Подписка #${sub.num}: ${sub.enabled ? 'ON' : 'OFF'}`, { reply_markup: MAIN_KEYBOARD });
    } else if (data.startsWith('sub_delete:')) {
      const subId = data.split(':')[1];
      user.subscriptions = (user.subscriptions || []).filter((s) => s && s.id !== subId);
      renumberSubs(user);
      scheduleSave();
      await sendMessageSafe(chatId, 'Подписка удалена.', { reply_markup: MAIN_KEYBOARD });
    } else if (data.startsWith('sub_setmax:')) {
      const subId = data.split(':')[1];
      const sub = findSub(user, subId);
      if (!sub) return sendMessageSafe(chatId, 'Подписка не найдена.', { reply_markup: MAIN_KEYBOARD });
      user.state = `awaiting_sub_max:${subId}`;
      scheduleSave();
      await sendMessageSafe(chatId, `Введи MAX TON для подписки #${sub.num}:`, { reply_markup: MAIN_KEYBOARD });
    }
  } catch (e) {
    console.error('callback_query error:', e);
  }

  bot.answerCallbackQuery(q.id).catch(() => {});
});

// intervals
setInterval(() => {
  checkMarketsForAllUsers().catch((e) => console.error('monitor interval error:', e));
}, CHECK_INTERVAL_MS);

setInterval(() => {
  checkSubscriptionsForAllUsers().catch((e) => console.error('subs interval error:', e));
}, SUBS_CHECK_INTERVAL_MS);

// bootstrap
(async () => {
  if (REDIS_URL) {
    await initRedis();
    if (redis) await loadState();
  }
  console.log('Бот запущен. /start');
})();
