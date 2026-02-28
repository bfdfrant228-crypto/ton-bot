const tg = window.Telegram?.WebApp;
tg?.ready();

const initData = tg?.initData || '';

function showErr(msg) {
  const box = document.getElementById('err');
  box.style.display = 'block';
  box.textContent = msg;
}
function hideErr() {
  const box = document.getElementById('err');
  box.style.display = 'none';
  box.textContent = '';
}

async function api(path, opts = {}) {
  const res = await fetch(path, {
    ...opts,
    headers: {
      'Content-Type': 'application/json',
      'X-Tg-Init-Data': initData,
      ...(opts.headers || {})
    }
  });
  const txt = await res.text();
  let data = null;
  try { data = txt ? JSON.parse(txt) : null; } catch { data = { raw: txt }; }
  if (!res.ok) throw new Error(`HTTP ${res.status}: ${JSON.stringify(data).slice(0, 400)}`);
  return data;
}

const el = (id) => document.getElementById(id);

function renderStatus(st) {
  const okAuth = st.api?.mrktAuthSet ? 'ok' : 'bad';
  el('status').innerHTML =
    `<b>Статус</b><br>` +
    `MRKT_AUTH: <span class="${okAuth}">${st.api?.mrktAuthSet ? '✅' : '❌'}</span><br>` +
    `Мониторинг: <b>${st.user.enabled ? 'ON' : 'OFF'}</b><br>` +
    `Автопокупка: <b>${st.user.autoBuyEnabled ? 'ON' : 'OFF'}</b><br>` +
    (st.user.autoBuyPausedUntil && st.user.autoBuyPausedUntil > Date.now()
      ? `Пауза автопокупки до: <b>${new Date(st.user.autoBuyPausedUntil).toISOString()}</b><br>`
      : '');
}

function renderSubs(st) {
  const subs = st.user.subscriptions || [];
  if (!subs.length) {
    el('subs').innerHTML = '<i>Подписок нет</i>';
    return;
  }
  el('subs').innerHTML = subs.map(s => `
    <div class="card">
      <div><b>#${s.num}</b> ${s.enabled ? 'ON' : 'OFF'}</div>
      <div>Gift: ${s.filters.gift}</div>
      <div>Model: ${s.filters.model || 'any'}</div>
      <div>Backdrop: ${s.filters.backdrop || 'any'}</div>
      <div>Max: ${s.maxPriceTon ?? '∞'}</div>
      <div class="row" style="margin-top:8px">
        <button data-act="subToggle" data-id="${s.id}">${s.enabled ? '⏸' : '▶️'} Toggle</button>
        <button data-act="subMax" data-id="${s.id}">💰 Set Max</button>
        <button data-act="subDel" data-id="${s.id}">🗑 Delete</button>
      </div>
    </div>
  `).join('');
}

function renderLogs(st) {
  const logs = st.user.logs || [];
  if (!logs.length) {
    el('logs').innerHTML = '<i>Логов нет</i>';
    return;
  }
  el('logs').innerHTML = logs.map(l => `
    <pre>${l.tsIso} | ${l.type}\n${l.text}</pre>
  `).join('');
}

async function loadCollections() {
  const meta = await api('/api/meta/collections');
  const sel = el('gift');
  sel.innerHTML = (meta.collections || []).map(c => `<option value="${c}">${c}</option>`).join('');
}

async function loadState() {
  const st = await api('/api/state');
  renderStatus(st);

  el('gift').value = st.user.filters.gift || (el('gift').options[0]?.value || '');
  el('model').value = st.user.filters.model || '';
  el('backdrop').value = st.user.filters.backdrop || '';
  el('minPrice').value = st.user.minPriceTon ?? 0;
  el('maxPrice').value = st.user.maxPriceTon ?? '';

  el('toggleMonitor').textContent = `Мониторинг: ${st.user.enabled ? 'ON' : 'OFF'}`;
  el('toggleAutobuy').textContent = `Автопокупка: ${st.user.autoBuyEnabled ? 'ON' : 'OFF'}`;

  renderSubs(st);
  renderLogs(st);
}

el('save').onclick = async () => {
  hideErr();
  try {
    await api('/api/state/patch', {
      method: 'POST',
      body: JSON.stringify({
        filters: {
          gift: el('gift').value,
          model: el('model').value,
          backdrop: el('backdrop').value,
        },
        minPriceTon: Number(el('minPrice').value || 0),
        maxPriceTon: el('maxPrice').value === '' ? null : Number(el('maxPrice').value),
      })
    });
    await loadState();
  } catch (e) { showErr(String(e.message || e)); }
};

el('toggleMonitor').onclick = async () => {
  hideErr();
  try {
    await api('/api/action', { method: 'POST', body: JSON.stringify({ action: 'toggleMonitor' }) });
    await loadState();
  } catch (e) { showErr(String(e.message || e)); }
};

el('toggleAutobuy').onclick = async () => {
  hideErr();
  try {
    await api('/api/action', { method: 'POST', body: JSON.stringify({ action: 'toggleAutoBuy' }) });
    await loadState();
  } catch (e) { showErr(String(e.message || e)); }
};

el('subCreate').onclick = async () => {
  hideErr();
  try {
    await api('/api/sub/create', { method: 'POST' });
    await loadState();
  } catch (e) { showErr(String(e.message || e)); }
};

el('refresh').onclick = async () => { hideErr(); await loadState().catch(e => showErr(e.message)); };
el('logsRefresh').onclick = async () => { hideErr(); await loadState().catch(e => showErr(e.message)); };

document.body.addEventListener('click', async (e) => {
  const btn = e.target.closest('button[data-act]');
  if (!btn) return;

  hideErr();
  const act = btn.dataset.act;
  const id = btn.dataset.id;

  try {
    if (act === 'subToggle') {
      await api('/api/sub/toggle', { method: 'POST', body: JSON.stringify({ id }) });
    } else if (act === 'subDel') {
      await api('/api/sub/delete', { method: 'POST', body: JSON.stringify({ id }) });
    } else if (act === 'subMax') {
      const v = prompt('MAX TON для подписки:', '5');
      if (v == null) return;
      await api('/api/sub/setmax', { method: 'POST', body: JSON.stringify({ id, maxPriceTon: Number(v) }) });
    }
    await loadState();
  } catch (e2) {
    showErr(String(e2.message || e2));
  }
});

(async () => {
  try {
    await loadCollections();
    await loadState();
  } catch (e) {
    showErr('Панель не может подключиться к API. ' + (e.message || e));
  }
})();
