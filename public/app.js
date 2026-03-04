(() => {
  try {
    const tg = window.Telegram?.WebApp;
    tg?.ready?.();
    tg?.expand?.();

    const initData = tg?.initData || '';
    const el = (id) => document.getElementById(id);

    if (!initData) {
      document.body.innerHTML = '<div style="padding:16px;font-family:system-ui;color:#fff;background:#0b0f14"><h3>Открой панель из Telegram</h3></div>';
      return;
    }

    const lotsLoading = el('lotsLoading');
    const subsLoading = el('subsLoading');
    const profileLoading = el('profileLoading');

    function setLoading(which, on){
      if(which==='lots') lotsLoading.style.display = on ? 'flex' : 'none';
      if(which==='subs') subsLoading.style.display = on ? 'flex' : 'none';
      if(which==='profile') profileLoading.style.display = on ? 'flex' : 'none';
    }

    function showErr(msg){ const box=el('err'); box.style.display='block'; box.textContent=String(msg||''); }
    function hideErr(){ const box=el('err'); box.style.display='none'; box.textContent=''; }

    async function api(path, opts = {}) {
      const res = await fetch(path, {
        ...opts,
        headers: { 'Content-Type':'application/json', 'X-Tg-Init-Data': initData, ...(opts.headers||{}) }
      });
      const txt = await res.text();
      let data=null;
      try{ data = txt?JSON.parse(txt):null; }catch{ data={raw:txt}; }
      if(!res.ok) throw new Error((data && data.reason) ? String(data.reason) : ('HTTP '+res.status));
      return data;
    }

    // open links
    document.body.addEventListener('click', (e) => {
      const b = e.target.closest('button[data-open]');
      if(!b) return;
      const url = b.getAttribute('data-open');
      if(!url) return;
      if (tg?.openTelegramLink) tg.openTelegramLink(url);
      else window.open(url,'_blank');
    });

    // tabs
    function setTab(name){
      ['market','subs','profile','admin'].forEach(x => el(x).style.display = (x===name?'block':'none'));
      document.querySelectorAll('.tabbtn').forEach(b => b.classList.toggle('active', b.dataset.tab===name));
    }
    document.querySelectorAll('.tabbtn').forEach(b => b.onclick = async () => {
      setTab(b.dataset.tab);
      if (b.dataset.tab === 'profile') await refreshProfile().catch(()=>{});
      if (b.dataset.tab === 'admin') await refreshAdmin().catch(()=>{});
    });

    function wrap(which, fn){
      return async () => {
        hideErr();
        setLoading(which, true);
        try { await fn(); }
        catch(e){ showErr(e.message || String(e)); }
        finally { setLoading(which, false); }
      };
    }

    // selection state
    let sel = { gifts: [], giftLabels: {}, models: [], backdrops: [], numberPrefix: '' };

    function isSelected(arr, v){
      const k = String(v||'').toLowerCase().trim();
      return (arr||[]).some(x => String(x).toLowerCase().trim() === k);
    }
    function toggleIn(arr, v){
      const k = String(v||'').toLowerCase().trim();
      const out = [];
      let removed = false;
      for (const x of arr || []) {
        if (String(x).toLowerCase().trim() === k) { removed = true; continue; }
        out.push(x);
      }
      if (!removed) out.push(v);
      return out;
    }
    function giftsInputText(){
      if (!sel.gifts.length) return '';
      return sel.gifts.map(v => sel.giftLabels[v] || v).join(', ');
    }
    function listInputText(arr){ return (arr||[]).join(', '); }

    // dropdown
    function hideSug(id){ const b=el(id); b.style.display='none'; b.innerHTML=''; }
    function renderSug(id, title, items, isSelFn, onToggleFn){
      const box = el(id);
      if (!items || !items.length) { hideSug(id); return; }

      box.innerHTML =
        `<div class="sugHead">
          <b>${title}</b>
          <button type="button" class="small" data-done="${id}">Готово</button>
        </div>` +
        items.map(x => {
          const selected = isSelFn(x.value);
          const mark = selected ? '<span class="selMark on">✓</span>' : '<span class="selMark"></span>';

          const thumb = x.imgUrl
            ? `<img class="thumb" src="${x.imgUrl}" referrerpolicy="no-referrer"/>`
            : (x.colorHex
                ? `<div class="thumb color"><div class="colorFill" style="background:${x.colorHex}"></div></div>`
                : `<div class="thumb"></div>`);

          return `<button type="button" class="item" data-v="${String(x.value).replace(/"/g,'&quot;')}">
            ${thumb}
            <div style="min-width:0;flex:1">
              <div class="ellipsis"><b>${x.label}</b></div>
              ${x.sub ? `<div class="muted ellipsis">${x.sub}</div>` : ''}
            </div>
            ${mark}
          </button>`;
        }).join('');

      box.style.display = 'block';

      box.onclick = (e) => {
        const done = e.target.closest('button[data-done]');
        if (done) { hideSug(id); return; }

        const btn = e.target.closest('button[data-v]');
        if (!btn) return;
        onToggleFn(btn.getAttribute('data-v'));
        renderSug(id, title, items, isSelFn, onToggleFn);
      };
    }

    async function patchFilters(){
      await api('/api/state/patch', {
        method:'POST',
        body: JSON.stringify({ filters:{
          gifts: sel.gifts,
          giftLabels: sel.giftLabels,
          models: sel.models,
          backdrops: sel.backdrops,
          numberPrefix: el('number').value.trim()
        }})
      });
    }

    async function showGiftSug(){
      const q = el('gift').value.trim();
      const r = await api('/api/mrkt/collections?q='+encodeURIComponent(q));
      const items = r.items || [];

      renderSug('giftSug', 'Gift', items,
        (v)=> isSelected(sel.gifts, v),
        (v)=>{
          const next = toggleIn(sel.gifts, v);
          if (next.length !== 1) { sel.models = []; sel.backdrops = []; }
          sel.gifts = next;

          const lbl = (r.mapLabel||{})[v] || v;
          sel.giftLabels[v] = lbl;

          el('gift').value = giftsInputText();
          el('model').value = listInputText(sel.models);
          el('backdrop').value = listInputText(sel.backdrops);
        }
      );
    }

    async function showModelSug(){
      if (sel.gifts.length !== 1) { hideSug('modelSug'); return; }
      const gift = sel.gifts[0];
      const q = el('model').value.trim();
      const r = await api('/api/mrkt/suggest?kind=model&gift='+encodeURIComponent(gift)+'&q='+encodeURIComponent(q));
      renderSug('modelSug', 'Model', r.items||[],
        (v)=> isSelected(sel.models, v),
        (v)=>{ sel.models = toggleIn(sel.models, v); el('model').value = listInputText(sel.models); }
      );
    }

    async function showBackdropSug(){
      if (sel.gifts.length !== 1) { hideSug('backdropSug'); return; }
      const gift = sel.gifts[0];
      const q = el('backdrop').value.trim();
      const r = await api('/api/mrkt/suggest?kind=backdrop&gift='+encodeURIComponent(gift)+'&q='+encodeURIComponent(q));
      renderSug('backdropSug', 'Backdrop', r.items||[],
        (v)=> isSelected(sel.backdrops, v),
        (v)=>{ sel.backdrops = toggleIn(sel.backdrops, v); el('backdrop').value = listInputText(sel.backdrops); }
      );
    }

    // open sug
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
      if(!e.target.closest('#gift,#model,#backdrop,.sug')) {
        hideSug('giftSug'); hideSug('modelSug'); hideSug('backdropSug');
      }
    });

    // clear buttons
    document.body.addEventListener('click', (e) => {
      const b = e.target.closest('button[data-clear]');
      if(!b) return;
      const what = b.getAttribute('data-clear');

      wrap('lots', async()=>{
        if(what==='gift'){ sel.gifts=[]; sel.giftLabels={}; sel.models=[]; sel.backdrops=[]; el('gift').value=''; el('model').value=''; el('backdrop').value=''; el('number').value=''; }
        if(what==='model'){ sel.models=[]; el('model').value=''; }
        if(what==='backdrop'){ sel.backdrops=[]; el('backdrop').value=''; }
        if(what==='number'){ el('number').value=''; }
        await patchFilters();
        await refreshAll();
      })();
    });

    // bottom sheet
    const sheetWrap = el('sheetWrap');
    const sheetTitle = el('sheetTitle');
    const sheetSub = el('sheetSub');
    const sheetSales = el('sheetSales');
    el('sheetClose').onclick = ()=>sheetWrap.classList.remove('show');
    sheetWrap.addEventListener('click',(e)=>{ if(e.target===sheetWrap) sheetWrap.classList.remove('show'); });

    function openSheet(title, sub){
      sheetTitle.textContent=title||'';
      sheetSub.textContent=sub||'';
      sheetSales.innerHTML='';
      sheetWrap.classList.add('show');
    }

    function renderSales(resp){
      if(!resp || resp.ok===false){
        sheetSales.innerHTML = '<i class="muted">Нет данных</i>';
        return;
      }
      const approx = resp.approxPriceTon;
      let html = `<div class="muted" style="margin-bottom:10px">
        Примерная цена продажи: <b>${approx!=null ? approx.toFixed(3)+' TON' : 'нет данных'}</b>
      </div>`;
      if (resp.note) html += `<div class="muted" style="margin-bottom:10px">${resp.note}</div>`;

      const items = resp.sales || [];
      if(!items.length){
        sheetSales.innerHTML = html + '<i class="muted">Нет продаж</i>';
        return;
      }

      html += items.slice(0, 20).map(x=>{
        const img = x.imgUrl ? `<img src="${x.imgUrl}" referrerpolicy="no-referrer" loading="lazy" style="width:60px;height:60px;border-radius:14px;border:1px solid var(--border);object-fit:cover;flex:0 0 auto"/>` : `<div class="thumb"></div>`;
        return `<div class="sale" style="margin-bottom:8px">
          <div style="display:flex;gap:10px;align-items:center">
            ${img}
            <div style="min-width:0;flex:1">
              <div style="display:flex;justify-content:space-between;gap:10px">
                <b>${x.priceTon.toFixed(3)} TON</b>
                <span class="muted">${x.tsMsk||''}</span>
              </div>
              ${x.model ? `<div class="muted">Model: ${x.model}</div>` : ''}
              ${x.backdrop ? `<div class="muted">Backdrop: ${x.backdrop}</div>` : ''}
            </div>
          </div>
          <div class="row" style="margin-top:8px">
            <button class="small" data-open="${x.urlTelegram}">NFT</button>
            <button class="small" data-open="${x.urlMarket}">MRKT</button>
          </div>
        </div>`;
      }).join('');

      sheetSales.innerHTML = html;
    }

    function renderLots(resp){
      const box=el('lots');
      if(resp.ok===false){ box.innerHTML='<div style="color:#ef4444"><b>'+resp.reason+'</b></div>'; return; }
      const lots=resp.lots||[];
      if(!lots.length){ box.innerHTML='<i class="muted">Лотов не найдено</i>'; return; }

      box.innerHTML = lots.map(x=>{
        const img = x.imgUrl ? `<img src="${x.imgUrl}" referrerpolicy="no-referrer" loading="lazy"/>` : '<div style="aspect-ratio:1/1;border:1px solid rgba(255,255,255,.10);border-radius:14px"></div>';
        const num = (x.number!=null)?(`<span class="badge">#${x.number}</span>`):'';
        return `<div class="lot">
          ${img}
          <div class="price">${x.priceTon.toFixed(3)} TON</div>
          <div style="display:flex;justify-content:space-between;gap:10px;align-items:center">
            <b class="ellipsis">${x.name}</b>${num}
          </div>
          ${x.model?`<div class="muted ellipsis">Model: ${x.model}</div>`:''}
          ${x.backdrop?`<div class="muted ellipsis" style="margin-top:6px">Backdrop: ${x.backdrop}</div>`:''}
        </div>`;
      }).join('');
    }

    function renderSubs(list){
      const box = el('subsList');
      if(!list || !list.length){ box.innerHTML = '<i class="muted">Подписок нет</i>'; return; }
      box.innerHTML = list.map(s => {
        return `<div class="card">
          <div style="display:flex;justify-content:space-between;gap:10px;align-items:center">
            <div style="min-width:0;flex:1">
              <b>#${s.num} ${s.enabled?'ON':'OFF'}</b>
              <div class="muted ellipsis">Gifts: ${(s.filters.gifts||[]).join(', ') || '-'}</div>
            </div>
            <button class="small" data-act="subToggle" data-id="${s.id}">${s.enabled?'Disable':'Enable'}</button>
          </div>
        </div>`;
      }).join('');
    }

    async function refreshState(){
      const st = await api('/api/state');

      sel.gifts = st.user.filters.gifts || [];
      sel.giftLabels = st.user.filters.giftLabels || {};
      sel.models = st.user.filters.models || [];
      sel.backdrops = st.user.filters.backdrops || [];
      el('number').value = st.user.filters.numberPrefix || '';

      el('gift').value = giftsInputText();
      el('model').value = listInputText(sel.models);
      el('backdrop').value = listInputText(sel.backdrops);

      renderSubs(st.user.subscriptions || []);
      if (st.api.isAdmin) el('adminTabBtn').style.display = 'inline-block';
    }

    async function refreshLots(){
      const r = await api('/api/mrkt/lots');
      renderLots(r);
    }

    async function refreshAll(){
      await refreshState();
      await refreshLots();
    }

    async function refreshProfile(){
      setLoading('profile', true);
      try{
        const r = await api('/api/profile');
        const u=r.user||{};
        el('profileBox').textContent = (u.username?('@'+u.username+' '):'') + 'id: '+(u.id||'-');
        const pfp = el('pfp');
        if (u.photo_url) { pfp.style.display='block'; pfp.src=u.photo_url; pfp.referrerPolicy='no-referrer'; }
        else { pfp.style.display='none'; pfp.src=''; }
        el('purchases').innerHTML = '<i class="muted">Профиль в этой сборке минимальный</i>';
      } finally {
        setLoading('profile', false);
      }
    }

    async function refreshAdmin(){
      const r = await api('/api/admin/status');
      el('adminStatus').textContent =
        'haveSession: ' + (r.haveSession?'YES':'NO') + '\n' +
        'mrktAuthSet: ' + (r.mrktAuthSet?'YES':'NO') + '\n' +
        'pauseUntil(ms): ' + (r.pauseUntil||0) + '\n' +
        'MRKT fail: ' + (r.lastFailMsg || '-') + '\n';
    }

    // buttons
    el('apply').onclick = wrap('lots', async()=>{ await patchFilters(); await refreshAll(); });
    el('refresh').onclick = wrap('lots', async()=>{ await refreshAll(); });

    el('historyBtn').onclick = wrap('lots', async()=>{
      openSheet('История продаж', 'по текущим фильтрам');
      renderSales(await api('/api/mrkt/history_current'));
    });

    el('subCreate').onclick = wrap('subs', async()=>{ await api('/api/sub/create',{method:'POST'}); await refreshState(); });
    el('subRefresh').onclick = wrap('subs', async()=>{ await refreshState(); });

    document.body.addEventListener('click', (e) => {
      const btn = e.target.closest('button[data-act]');
      if(!btn) return;
      const act = btn.dataset.act;
      const id = btn.dataset.id;

      wrap('subs', async()=>{
        if(act==='subToggle') await api('/api/sub/toggle',{method:'POST',body:JSON.stringify({id})});
        await refreshState();
      })();
    });

    el('adminRefresh').onclick = wrap('subs', async()=>{
      await api('/api/admin/mrkt_refresh', { method:'POST' });
      await refreshAdmin();
      await refreshAll();
    });

    // initial
    wrap('lots', async()=>{ await refreshAll(); })();

  } catch (e) {
    const box = document.getElementById('err');
    if (box) { box.style.display = 'block'; box.textContent = 'JS crash: ' + (e?.message || String(e)); }
  }
})();
