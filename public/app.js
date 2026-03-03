(() => {
  const tg = window.Telegram && window.Telegram.WebApp;
  try { tg && tg.ready && tg.ready(); tg && tg.expand && tg.expand(); } catch {}

  const initData = (tg && tg.initData) ? tg.initData : '';
  const el = (id) => document.getElementById(id);

  if (!initData) {
    document.body.innerHTML =
      '<div style="padding:16px;font-family:system-ui;color:#fff;background:#0b0f14">' +
      '<h3>Открой панель из Telegram</h3></div>';
    return;
  }

  const lotsLoading = el('lotsLoading');
  const subsLoading = el('subsLoading');
  const profileLoading = el('profileLoading');
  const maxOfferBox = el('maxOfferBox');

  function setLoading(which, on) {
    if (which === 'lots' && lotsLoading) lotsLoading.style.display = on ? 'flex' : 'none';
    if (which === 'subs' && subsLoading) subsLoading.style.display = on ? 'flex' : 'none';
    if (which === 'profile' && profileLoading) profileLoading.style.display = on ? 'flex' : 'none';
  }

  function showErr(msg) {
    const box = el('err');
    box.style.display = 'block';
    box.textContent = String(msg || '');
  }
  function hideErr() {
    const box = el('err');
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

    if (!res.ok) throw new Error((data && data.reason) ? String(data.reason) : ('HTTP ' + res.status));
    return data;
  }

  // open links
  document.body.addEventListener('click', (e) => {
    const b = e.target.closest('button[data-open]');
    if (!b) return;
    const url = b.getAttribute('data-open');
    if (!url) return;
    try {
      if (tg && tg.openTelegramLink) tg.openTelegramLink(url);
      else window.open(url, '_blank');
    } catch {
      window.open(url, '_blank');
    }
  });

  // tabs
  function setTab(name) {
    ['market', 'subs', 'profile', 'admin'].forEach((x) => {
      const node = el(x);
      if (node) node.style.display = (x === name ? 'block' : 'none');
    });
    document.querySelectorAll('.tabbtn').forEach((b) => {
      b.classList.toggle('active', b.dataset.tab === name);
    });
  }
  document.querySelectorAll('.tabbtn').forEach((b) => {
    b.onclick = async () => {
      setTab(b.dataset.tab);
      if (b.dataset.tab === 'profile') await refreshProfile().catch(() => {});
      if (b.dataset.tab === 'admin') await refreshAdmin().catch(() => {});
    };
  });

  // bottom sheet
  const sheetWrap = el('sheetWrap');
  const sheetTitle = el('sheetTitle');
  const sheetSub = el('sheetSub');
  const sheetTop = el('sheetTop');
  const sheetKV = el('sheetKV');
  const sheetBtns = el('sheetBtns');
  const sheetSales = el('sheetSales');
  const sheetImg = el('sheetImg');

  el('sheetClose').onclick = () => sheetWrap.classList.remove('show');
  sheetWrap.addEventListener('click', (e) => { if (e.target === sheetWrap) sheetWrap.classList.remove('show'); });

  function openSheet(title, sub) {
    sheetTitle.textContent = title || '';
    sheetSub.textContent = sub || '';
    sheetTop.textContent = '';
    sheetKV.innerHTML = '';
    sheetBtns.innerHTML = '';
    sheetSales.innerHTML = '';
    sheetImg.style.display = 'none';
    sheetImg.src = '';
    sheetWrap.classList.add('show');
  }

  function pill(txt) { return '<div class="p">' + txt + '</div>'; }

  function wrap(which, fn) {
    return async () => {
      hideErr();
      setLoading(which, true);
      try { await fn(); }
      catch (e) { showErr(e.message || String(e)); }
      finally { setLoading(which, false); }
    };
  }

  // -------- multi-select state ----------
  let sel = { gifts: [], giftLabels: {}, models: [], backdrops: [], numberPrefix: '' };

  function norm(s) { return String(s || '').toLowerCase().trim(); }
  function isSelected(arr, v) {
    const k = norm(v);
    return (arr || []).some((x) => norm(x) === k);
  }
  function toggleIn(arr, v) {
    const k = norm(v);
    const out = [];
    let removed = false;
    for (const x of (arr || [])) {
      if (norm(x) === k) { removed = true; continue; }
      out.push(x);
    }
    if (!removed) out.push(v);
    return out;
  }
  function giftsInputText() {
    if (!sel.gifts.length) return '';
    const labels = sel.gifts.map((v) => sel.giftLabels[v] || v);
    return labels.join(', ');
  }
  function listInputText(arr) { return (arr || []).join(', '); }

  // -------- suggest UI ----------
  function hideSug(id) {
    const b = el(id);
    b.style.display = 'none';
    b.innerHTML = '';
  }

  function renderSug(id, title, items, isSelFn, onToggleFn) {
    const b = el(id);
    if (!items || !items.length) { hideSug(id); return; }

    const head =
      '<div class="sugHead">' +
        '<b>' + title + '</b>' +
        '<span class="muted">тап = выбрать</span>' +
      '</div>';

    b.innerHTML = head + items.map((x) => {
      const selected = !!isSelFn(x.value);
      const mark = selected ? '<span class="selMark on">✓</span>' : '<span class="selMark"></span>';

      const thumb = x.imgUrl
        ? '<img class="thumb" src="' + x.imgUrl + '" referrerpolicy="no-referrer"/>'
        : (x.colorHex
            ? '<div class="thumb color"><div class="colorFill" style="background:' + x.colorHex + '"></div></div>'
            : '<div class="thumb"></div>');

      const right =
        '<div style="min-width:0;flex:1">' +
          '<div class="ellipsis"><b>' + x.label + '</b></div>' +
          (x.sub ? '<div class="muted ellipsis">' + x.sub + '</div>' : '') +
        '</div>';

      return '<button type="button" class="item" data-v="' + String(x.value).replace(/"/g, '&quot;') + '">' +
        thumb + right + mark +
      '</button>';
    }).join('');

    b.style.display = 'block';
    b.onclick = (e) => {
      const btn = e.target.closest('button[data-v]');
      if (!btn) return;
      onToggleFn(btn.getAttribute('data-v'));
    };
  }

  async function patchFilters() {
    await api('/api/state/patch', {
      method: 'POST',
      body: JSON.stringify({
        filters: {
          gifts: sel.gifts,
          giftLabels: sel.giftLabels,
          models: sel.models,
          backdrops: sel.backdrops,
          numberPrefix: el('number').value.trim()
        }
      })
    });
  }

  async function showGiftSug() {
    const q = el('gift').value.trim();
    const r = await api('/api/mrkt/collections?q=' + encodeURIComponent(q));
    const items = r.items || [];

    renderSug('giftSug', 'Gift', items,
      (v) => isSelected(sel.gifts, v),
      (v) => {
        const next = toggleIn(sel.gifts, v);

        if (next.length !== 1) { sel.models = []; sel.backdrops = []; }
        sel.gifts = next;

        const lbl = (r.mapLabel || {})[v] || v;
        sel.giftLabels[v] = lbl;

        el('gift').value = giftsInputText();
        el('model').value = listInputText(sel.models);
        el('backdrop').value = listInputText(sel.backdrops);
      }
    );
  }

  async function showModelSug() {
    if (sel.gifts.length !== 1) { hideSug('modelSug'); return; }
    const gift = sel.gifts[0];
    const q = el('model').value.trim();
    const r = await api('/api/mrkt/suggest?kind=model&gift=' + encodeURIComponent(gift) + '&q=' + encodeURIComponent(q));
    const items = r.items || [];

    renderSug('modelSug', 'Model', items,
      (v) => isSelected(sel.models, v),
      (v) => {
        sel.models = toggleIn(sel.models, v);
        el('model').value = listInputText(sel.models);
      }
    );
  }

  async function showBackdropSug() {
    if (sel.gifts.length !== 1) { hideSug('backdropSug'); return; }
    const gift = sel.gifts[0];
    const q = el('backdrop').value.trim();
    const r = await api('/api/mrkt/suggest?kind=backdrop&gift=' + encodeURIComponent(gift) + '&q=' + encodeURIComponent(q));
    const items = r.items || [];

    renderSug('backdropSug', 'Backdrop', items,
      (v) => isSelected(sel.backdrops, v),
      (v) => {
        sel.backdrops = toggleIn(sel.backdrops, v);
        el('backdrop').value = listInputText(sel.backdrops);
      }
    );
  }

  el('gift').addEventListener('focus', wrap('lots', showGiftSug));
  el('gift').addEventListener('click', wrap('lots', showGiftSug));
  el('gift').addEventListener('input', wrap('lots', showGiftSug));

  el('model').addEventListener('focus', wrap('lots', showModelSug));
  el('model').addEventListener('click', wrap('lots', showModelSug));
  el('model').addEventListener('input', wrap('lots', showModelSug));

  el('backdrop').addEventListener('focus', wrap('lots', showBackdropSug));
  el('backdrop').addEventListener('click', wrap('lots', showBackdropSug));
  el('backdrop').addEventListener('input', wrap('lots', showBackdropSug));

  document.addEventListener('click', (e) => {
    if (!e.target.closest('#gift,#model,#backdrop,.sug')) {
      hideSug('giftSug'); hideSug('modelSug'); hideSug('backdropSug');
    }
  });

  // clear buttons
  document.body.addEventListener('click', (e) => {
    const b = e.target.closest('button[data-clear]');
    if (!b) return;
    const what = b.getAttribute('data-clear');

    wrap('lots', async () => {
      if (what === 'gift') {
        sel.gifts = [];
        sel.giftLabels = {};
        sel.models = [];
        sel.backdrops = [];
        el('gift').value = '';
        el('model').value = '';
        el('backdrop').value = '';
        el('number').value = '';
      }
      if (what === 'model') { sel.models = []; el('model').value = ''; }
      if (what === 'backdrop') { sel.backdrops = []; el('backdrop').value = ''; }
      if (what === 'number') { el('number').value = ''; }

      await patchFilters();
      await refreshAll();
    })();
  });

  // -------- render lots ----------
  function renderLots(resp) {
    const box = el('lots');
    if (resp.ok === false) { box.innerHTML = '<div style="color:#ef4444"><b>' + resp.reason + '</b></div>'; return; }
    const lots = resp.lots || [];
    if (!lots.length) { box.innerHTML = '<i class="muted">Лотов не найдено</i>'; return; }

    box.innerHTML = lots.map((x) => {
      const img = x.imgUrl
        ? '<img src="' + x.imgUrl + '" referrerpolicy="no-referrer" loading="lazy"/>'
        : '<div style="aspect-ratio:1/1;border:1px solid rgba(255,255,255,.10);border-radius:14px"></div>';
      const num = (x.number != null) ? ('<span class="badge">#' + x.number + '</span>') : '';
      return '<div class="lot" data-id="' + x.id + '">' +
        img +
        '<div class="price">' + x.priceTon.toFixed(3) + ' TON</div>' +
        '<div style="display:flex;justify-content:space-between;gap:10px;align-items:center">' +
          '<b class="ellipsis">' + x.name + '</b>' + num +
        '</div>' +
        (x.model ? '<div class="muted ellipsis">Model: ' + x.model + '</div>' : '') +
        (x.backdrop ? '<div class="muted ellipsis" style="margin-top:6px">Backdrop: ' + x.backdrop + '</div>' : '') +
      '</div>';
    }).join('');

    const map = new Map(lots.map((l) => [String(l.id), l]));
    box.querySelectorAll('.lot').forEach((node) => {
      node.onclick = wrap('lots', async () => {
        const id = node.getAttribute('data-id');
        const lot = map.get(String(id));
        if (!lot) return;

        openSheet('Лот', lot.name);

        if (lot.imgUrl) {
          sheetImg.style.display = 'block';
          sheetImg.src = lot.imgUrl;
          sheetImg.referrerPolicy = 'no-referrer';
        }

        sheetTop.innerHTML =
          '<div><b>Цена:</b> ' + lot.priceTon.toFixed(3) + ' TON</div>' +
          (lot.model ? '<div class="muted">Model: ' + lot.model + '</div>' : '') +
          (lot.backdrop ? '<div class="muted">Backdrop: ' + lot.backdrop + '</div>' : '') +
          (lot.listedMsk ? '<div class="muted">Listed: ' + lot.listedMsk + '</div>' : '') +
          '<div class="muted" style="margin-top:6px">ID: ' + lot.id + '</div>';

        const det = await api('/api/lot/details', { method: 'POST', body: JSON.stringify({ id: lot.id }) });

        sheetKV.innerHTML = [
          (det.offers && det.offers.exact && det.offers.exact.maxOfferTon != null)
            ? pill('Max offer (точно): <b>' + det.offers.exact.maxOfferTon.toFixed(3) + ' TON</b>')
            : pill('Max offer (точно): —'),
          (det.offers && det.offers.collection && det.offers.collection.maxOfferTon != null)
            ? pill('Max offer (колл.): <b>' + det.offers.collection.maxOfferTon.toFixed(3) + ' TON</b>')
            : pill('Max offer (колл.): —'),
          (det.floors && det.floors.exact && det.floors.exact.priceTon != null)
            ? pill('Floor (точно): <b>' + det.floors.exact.priceTon.toFixed(3) + ' TON</b>')
            : pill('Floor (точно): —'),
          (det.floors && det.floors.collection && det.floors.collection.priceTon != null)
            ? pill('Floor (колл.): <b>' + det.floors.collection.priceTon.toFixed(3) + ' TON</b>')
            : pill('Floor (колл.): —'),
        ].join('');

        sheetBtns.innerHTML = '';

        const mkBtn = (label, action, strong) => {
          const b = document.createElement('button');
          b.className = 'small';
          b.textContent = label;
          if (strong) {
            b.style.borderColor = 'var(--accent)';
            b.style.background = 'var(--accent)';
            b.style.color = '#052e16';
            b.style.fontWeight = '900';
          }
          b.onclick = action;
          sheetBtns.appendChild(b);
        };

        mkBtn('Buy', async () => {
          const ok = confirm('Купить?\\n' + lot.name + '\\nЦена: ' + lot.priceTon.toFixed(3) + ' TON');
          if (!ok) return;
          const r = await api('/api/mrkt/buy', { method: 'POST', body: JSON.stringify({ id: lot.id, priceNano: lot.priceNano }) });
          alert('Куплено: ' + r.title + ' за ' + r.priceTon.toFixed(3) + ' TON');
        }, true);

        mkBtn('MRKT', () => (tg && tg.openTelegramLink) ? tg.openTelegramLink(lot.urlMarket) : window.open(lot.urlMarket, '_blank'), false);
        mkBtn('NFT', () => {
          const url = lot.urlTelegram || 'https://t.me/mrkt';
          (tg && tg.openTelegramLink) ? tg.openTelegramLink(url) : window.open(url, '_blank');
        }, false);

        renderSales(det.salesHistory);
      });
    });
  }

  function renderSales(resp) {
    if (!resp || resp.ok === false) {
      sheetSales.innerHTML = '<i class="muted">Нет данных</i>';
      return;
    }
    const approx = resp.approxPriceTon;
    const head = (approx != null)
      ? ('Примерная цена продажи: <b>' + approx.toFixed(3) + ' TON</b>')
      : 'Примерная цена продажи: <b>нет данных</b>';

    let html = '<div class="muted" style="margin-bottom:10px">' + head + '</div>';

    const items = resp.sales || [];
    if (!items.length) {
      html += '<i class="muted">Нет продаж</i>';
      sheetSales.innerHTML = html;
      return;
    }

    html += items.slice(0, 20).map((x) => {
      const img = x.imgUrl ? '<img src="' + x.imgUrl + '" referrerpolicy="no-referrer" loading="lazy" style="width:60px;height:60px;border-radius:14px;border:1px solid var(--border);object-fit:cover;flex:0 0 auto"/>' : '';
      const btns =
        '<div class="row" style="margin-top:8px">' +
          (x.urlTelegram ? '<button class="small" data-open="' + x.urlTelegram + '">NFT</button>' : '') +
          (x.urlMarket ? '<button class="small" data-open="' + x.urlMarket + '">MRKT</button>' : '') +
        '</div>';

      return '<div class="sale">' +
        '<div style="display:flex;gap:10px;align-items:center">' +
          (img || '<div class="thumb"></div>') +
          '<div style="min-width:0;flex:1">' +
            '<div style="display:flex;justify-content:space-between;gap:10px">' +
              '<b>' + x.priceTon.toFixed(3) + ' TON</b>' +
              '<span class="muted">' + (x.tsMsk || '') + '</span>' +
            '</div>' +
            (x.name ? '<div class="muted ellipsis">' + x.name + '</div>' : '') +
            (x.model ? '<div class="muted">Model: ' + x.model + '</div>' : '') +
            (x.backdrop ? '<div class="muted">Backdrop: ' + x.backdrop + '</div>' : '') +
          '</div>' +
        '</div>' +
        btns +
      '</div>';
    }).join('<div style="height:8px"></div>');

    sheetSales.innerHTML = html;
  }

  function renderMaxOfferBox(resp) {
    if (!maxOfferBox) return;
    if (!resp || !resp.ok) { maxOfferBox.style.display = 'none'; return; }
    maxOfferBox.style.display = 'block';
    maxOfferBox.innerHTML =
      '<b>Max offer</b> · ' +
      'точно: <b>' + (resp.exactMaxOfferTon != null ? resp.exactMaxOfferTon.toFixed(3) + ' TON' : '—') + '</b>' +
      ' · коллекция: <b>' + (resp.collectionMaxOfferTon != null ? resp.collectionMaxOfferTon.toFixed(3) + ' TON' : '—') + '</b>' +
      (resp.note ? ('<div class="muted" style="margin-top:6px">' + resp.note + '</div>') : '');
  }

  // buttons
  el('apply').onclick = wrap('lots', async () => { await patchFilters(); await refreshAll(); });
  el('refresh').onclick = wrap('lots', async () => { await refreshAll(); });

  el('toProfile').onclick = async () => { setTab('profile'); await refreshProfile().catch(() => {}); };

  el('salesBtn').onclick = wrap('lots', async () => {
    openSheet('Продажи', 'по текущим фильтрам');
    const r = await api('/api/mrkt/sales_history_current');
    renderSales(r);
  });

  // profile
  async function refreshProfile() {
    setLoading('profile', true);
    try {
      const r = await api('/api/profile');
      const u = r.user || {};
      el('profileBox').textContent = (u.username ? ('@' + u.username + ' ') : '') + 'id: ' + (u.id || '-');

      const pfp = el('pfp');
      if (pfp) {
        if (u.photo_url) {
          pfp.style.display = 'block';
          pfp.src = u.photo_url;
          pfp.referrerPolicy = 'no-referrer';
        } else {
          pfp.style.display = 'none';
          pfp.src = '';
        }
      }

      const list = r.purchases || [];
      const box = el('purchases');

      box.innerHTML = list.length ? list.map((p) => {
        const img = p.imgUrl ? '<img src="' + p.imgUrl + '" referrerpolicy="no-referrer" loading="lazy"/>' : '';
        const btns =
          '<div class="row" style="margin-top:8px">' +
            (p.urlTelegram ? '<button class="small" data-open="' + p.urlTelegram + '">NFT</button>' : '') +
            (p.urlMarket ? '<button class="small" data-open="' + p.urlMarket + '">MRKT</button>' : '') +
          '</div>';

        return '<div class="card">' +
          '<div class="purchRow">' +
            (img || '<div class="thumb"></div>') +
            '<div style="min-width:0">' +
              '<div class="ellipsis"><b>' + p.title + '</b></div>' +
              (p.model ? '<div class="muted ellipsis">Model: ' + p.model + '</div>' : '') +
              (p.backdrop ? '<div class="muted ellipsis">Backdrop: ' + p.backdrop + '</div>' : '') +
            '</div>' +
          '</div>' +
          '<div class="muted" style="margin-top:8px">Listed: ' + (p.listedMsk || '-') + '</div>' +
          '<div class="muted">Found: ' + (p.foundMsk || '-') + '</div>' +
          '<div class="muted">Buy: ' + (p.boughtMsk || '-') + (p.latencyMs != null ? (' · Latency ' + p.latencyMs + ' ms') : '') + '</div>' +
          '<div class="muted">Price: ' + Number(p.priceTon).toFixed(3) + ' TON</div>' +
          btns +
        '</div>';
      }).join('') : '<i class="muted">Покупок пока нет</i>';
    } finally {
      setLoading('profile', false);
    }
  }

  // admin
  async function refreshAdmin() {
    const r = await api('/api/admin/status');
    el('adminStatus').textContent =
      'MRKT fail: ' + (r.mrktLastFailMsg || '-') + '\\n' +
      'endpoint: ' + (r.mrktLastFailEndpoint || '-') + '\\n' +
      'status: ' + (r.mrktLastFailStatus || '-') + '\\n' +
      'AutoBuy: eligible=' + r.autoBuy.eligible +
      ', scanned=' + r.autoBuy.scanned +
      ', candidates=' + r.autoBuy.candidates +
      ', buys=' + r.autoBuy.buys +
      '\\nreason: ' + (r.autoBuy.lastReason || '-');

    const m = el('tokMask');
    if (m) m.value = r.mrktAuthMask || '';
  }

  const tokSave = el('tokSave');
  if (tokSave) {
    tokSave.onclick = wrap('subs', async () => {
      const t = el('tokNew').value.trim();
      if (!t) throw new Error('Вставь токен');
      await api('/api/admin/mrkt_auth', { method: 'POST', body: JSON.stringify({ token: t }) });
      el('tokNew').value = '';
      await refreshAdmin();
    });
  }

  // state refresh
  async function refreshState() {
    const st = await api('/api/state');

    sel.gifts = st.user.filters.gifts || [];
    sel.giftLabels = st.user.filters.giftLabels || {};
    sel.models = st.user.filters.models || [];
    sel.backdrops = st.user.filters.backdrops || [];

    el('number').value = st.user.filters.numberPrefix || '';

    el('gift').value = sel.gifts.map((v) => sel.giftLabels[v] || v).join(', ');
    el('model').value = listInputText(sel.models);
    el('backdrop').value = listInputText(sel.backdrops);

    el('status').textContent = 'MRKT_AUTH: ' + (st.api.mrktAuthSet ? 'YES' : 'NO');

    if (st.api.isAdmin) el('adminTabBtn').style.display = 'inline-block';

    // max offer
    try {
      const mo = await api('/api/mrkt/maxoffer_current');
      renderMaxOfferBox(mo);
    } catch {
      if (maxOfferBox) maxOfferBox.style.display = 'none';
    }
  }

  async function refreshMarketData() {
    const lots = await api('/api/mrkt/lots');
    renderLots(lots);
  }

  async function refreshAll() {
    await refreshState();
    await refreshMarketData();
  }

  // init
  wrap('lots', async () => { await refreshAll(); })();
})();`;