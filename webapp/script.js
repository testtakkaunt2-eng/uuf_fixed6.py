const tg = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;
if (tg) { tg.expand(); tg.setHeaderColor('#0d1117'); }
const initData = tg ? tg.initData : "";
const urlToken = new URLSearchParams(location.search).get("token") || "";

async function api(path, body=null) {
  const opts = { headers: { "Content-Type":"application/json", "X-Telegram-Init-Data": initData, "X-Dashboard-Token": urlToken } };
  if (body) { opts.method = "POST"; opts.body = JSON.stringify(body); }
  const r = await fetch(path, opts);
  return await r.json();
}

async function loadState() {
  try {
    const d = await api("/api/state");
    if (!d.ok) return;
    document.getElementById("price").innerText = (d.price !== null && d.price !== undefined) ? Number(d.price).toFixed(2) : "---";
    document.getElementById("brk_lbl").innerText = "XAUUSD (" + (d.broker || "EXNESS").toUpperCase() + ") | $" + Number(d.balance || 20).toFixed(2);
    document.getElementById("bias").innerText = "15m: " + (d.b15 || "NEUTRAL").toUpperCase() + " | 1m: " + (d.trend_1m || "NEUTRAL").toUpperCase();
    document.getElementById("offset").innerText = "Offset: " + (d.offset >= 0 ? "+" : "") + Number(d.offset || 0).toFixed(2);

    const tb = document.getElementById("trade_box");
    if (d.trade) {
      const color = d.trade.signal === 'LONG' ? 'var(--green)' : 'var(--red)';
      const label = d.trade.signal === 'LONG' ? 'BUY' : 'SELL';
      tb.innerHTML = `<div class="row"><span>Yo'nalish:</span><b style="color:${color}; font-size: 15px;">${label} @ ${d.trade.entry}</b></div>
                      <div class="row"><span>SL / TP:</span><b>${d.trade.sl} / ${d.trade.tp1}</b></div>`;
    } else {
      tb.innerHTML = `<div class="row"><span>Holat:</span><span style="color:var(--muted)">15m Sniper zona kutilmoqda...</span></div>
                      <div class="row"><span>Win Rate:</span><b style="color:var(--gold)">${d.wr}</b></div>`;
    }

    const ab = document.getElementById("ake_box");
    const ast = (d.ake_stats && d.ake_stats.day) ? d.ake_stats.day : {total:0, wins:0, wr:0, total_r:0};
    if (d.ake_trade) {
      const aColor = d.ake_trade.signal === 'LONG' ? 'var(--green)' : 'var(--red)';
      const aLabel = d.ake_trade.signal === 'LONG' ? 'BUY' : 'SELL';
      ab.innerHTML = `<div class="row"><span>Ochiq 1m Skalp:</span><b style="color:${aColor}">${aLabel} @ ${d.ake_trade.entry} (TP hit: ${d.ake_trade.max_r_hit || 0}R)</b></div>
                      <div class="row"><span>SL / TP1..TP5:</span><b>${d.ake_trade.sl} / ${d.ake_trade.tp1} → ${d.ake_trade.tp5}</b></div>
                      <div class="row"><span>Bugungi R:</span><b style="color:var(--gold)">${ast.total} bitim | TP: ${ast.wins} (${ast.wr}%) | ${ast.total_r >= 0 ? '+' : ''}${ast.total_r}R</b></div>`;
    } else {
      ab.innerHTML = `<div class="row"><span>Holat:</span><span style="color:var(--muted)">1m Ake impuls kutilmoqda...</span></div>
                      <div class="row"><span>Bugungi R:</span><b style="color:var(--gold)">${ast.total} bitim | TP: ${ast.wins} (${ast.wr}%) | ${ast.total_r >= 0 ? '+' : ''}${ast.total_r}R</b></div>`;
    }

    const el = document.getElementById("exness_list");
    el.innerHTML = (d.exness || []).map(a =>
      `<div class="row">
         <div>
           <div style="font-weight:700; font-size:15px; margin-bottom:4px;">#${a.slot} - ${a.login}</div>
           <div style="font-size:12px; color:var(--muted);"><i class="fa-solid fa-server"></i> ${a.server} | <i class="fa-solid fa-coins"></i> ${a.lot} lot</div>
         </div>
         <a href="#" class="delete-btn" onclick="delEx(${a.slot})"><i class="fa-solid fa-trash"></i></a>
       </div>`
    ).join("") || "<div style='color:var(--muted);text-align:center;padding:10px 0;'>Hali hisob ulanmagan</div>";
  } catch (e) {}
}

async function act(cmd) {
  if (tg && tg.HapticFeedback) tg.HapticFeedback.impactOccurred('medium');
  await api("/api/action", { action: cmd });
  if (tg) tg.showAlert("Buyruq bajarildi: " + cmd);
  loadState();
}

async function syncPrice() {
  if (tg && tg.HapticFeedback) tg.HapticFeedback.impactOccurred('light');
  const v = parseFloat(document.getElementById("sync_p").value);
  if (!v) return;
  await api("/api/action", { action: "sync", price: v });
  loadState();
  document.getElementById("sync_p").value = "";
}

async function addExness() {
  if (tg && tg.HapticFeedback) tg.HapticFeedback.impactOccurred('heavy');
  const login = document.getElementById("ex_login").value.trim();
  const password = document.getElementById("ex_pass").value.trim();
  const server = document.getElementById("ex_srv").value.trim() || "AUTO";
  const lot = parseFloat(document.getElementById("ex_lot").value) || 0.01;
  if (!login || !password) {
    if(tg) tg.showAlert("Login va Parol kiritish shart!"); else alert("Login va Parol kiritish shart!");
    return;
  }
  const r = await api("/api/action", { action: "exness_add", login, password, server, lot });
  if(tg) tg.showAlert(r.msg || "Bajarildi"); else alert(r.msg || "Bajarildi");
  document.getElementById("ex_pass").value = "";
  loadState();
}

async function delEx(slot) {
  if (tg && tg.HapticFeedback) tg.HapticFeedback.notificationOccurred('warning');
  await api("/api/action", { action: "exness_del", slot });
  loadState();
}

loadState();
setInterval(loadState, 3000);
