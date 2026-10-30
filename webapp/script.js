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
    document.getElementById("price").innerText = d.price ? d.price.toFixed(2) : "---";
    document.getElementById("bias").innerText = "15m: " + (d.b15 || "NEUTRAL").toUpperCase();
    document.getElementById("offset").innerText = "Offset: " + (d.offset >= 0 ? "+" : "") + d.offset;
    const tb = document.getElementById("trade_box");
    if (d.trade) {
      const color = d.trade.signal === 'LONG' ? 'var(--green)' : 'var(--red)';
      const icon = d.trade.signal === 'LONG' ? 'fa-arrow-trend-up' : 'fa-arrow-trend-down';
      tb.innerHTML = `<div class="row"><span><i class="fa-solid ${icon}"></i> Yo'nalish:</span><b style="color:${color}; font-size: 15px;">${d.trade.signal} @ ${d.trade.entry}</b></div>
                      <div class="row"><span><i class="fa-solid fa-shield"></i> SL / TP1 / TP2:</span><b style="letter-spacing:0.5px">${d.trade.sl} / ${d.trade.tp1} / ${d.trade.tp2}</b></div>`;
    } else {
      tb.innerHTML = `<div class="row"><span><i class="fa-solid fa-eye"></i> Holat:</span><span style="color:var(--muted)">Sniper zona kutilmoqda...</span></div>
                      <div class="row"><span><i class="fa-solid fa-scale-balanced"></i> Win Rate:</span><b style="color:var(--gold)">${d.wr}</b></div>`;
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
    ).join("") || "<div style='color:var(--muted);text-align:center;padding:10px 0;'><i class='fa-solid fa-box-open mb-3'></i><br>Hali hisob ulanmagan</div>";
  } catch (e) {}
}

async function act(cmd) {
  if (tg && tg.HapticFeedback) tg.HapticFeedback.impactOccurred('medium');
  await api("/api/action", { action: cmd });
  if (tg) tg.showAlert("Buyruq yuborildi: " + cmd);
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
