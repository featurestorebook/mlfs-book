// No build step and no CDN: the app runs air-gapped. Every URL is relative so
// the Hopsworks proxy mount works without the app knowing its prefix.

async function api(path, options = {}) {
  const response = await fetch(path, {
    ...options,
    headers: { Accept: "application/json", ...(options.body ? { "Content-Type": "application/json" } : {}) },
  });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    let detail = body.detail;
    if (Array.isArray(detail)) detail = detail.map((d) => `${(d.loc || []).slice(-1)[0]}: ${d.msg}`).join("; ");
    throw new Error(detail || `${response.status} ${response.statusText}`);
  }
  return response.json();
}

const $ = (selector) => document.querySelector(selector);
const int = new Intl.NumberFormat();
const money = new Intl.NumberFormat(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const pct = (v) => (v === null || v === undefined ? "–" : `${(v * 100).toFixed(1)}%`);
const fmtInt = (v) => (v === null || v === undefined ? "–" : int.format(v));
const fmtMoney = (v) => (v === null || v === undefined ? "–" : `$${money.format(v)}`);
const fmtTime = (iso) => {
  if (!iso) return "–";
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleString(undefined, {
    year: "numeric", month: "short", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit",
  }) + `.${String(d.getMilliseconds()).padStart(3, "0")}`;
};
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined && text !== null) node.textContent = text;
  return node;
}

function notice(kind, text) {
  return el("p", `notice ${kind}`, text);
}

// ---------------------------------------------------------------- status ----

let statusTimer = null;

function setService(id, { state, label, title, message }) {
  const row = document.getElementById(id);
  if (!row) return;
  row.querySelector(".dot").className = `dot ${state}`;
  const badge = row.querySelector(".badge");
  badge.className = `badge ${state === "ok" ? "" : state === "bad" ? "high" : state === "wait" ? "warn" : "plain"}`;
  badge.textContent = label;
  if (title) badge.title = title;
  const msg = row.querySelector(".msg");
  if (msg) {
    msg.textContent = message || "";
    msg.classList.toggle("hidden", !message);
  }
}

function renderStatus(s) {
  $("#project-band").textContent = `Project ${s.project}`;
  $("#s-project").textContent = s.project;
  $("#s-project").nextElementSibling.textContent = "connected";

  const e = s.entities;
  const entMsg = $("#entities-msg");
  if (e.state === "ready") {
    $("#s-merchants").textContent = fmtInt(e.merchants);
    $("#s-accounts").textContent = fmtInt(e.accounts);
    $("#s-cards").textContent = fmtInt(e.cards);
    entMsg.replaceChildren();
  } else if (e.state === "error") {
    for (const id of ["#s-merchants", "#s-accounts", "#s-cards"]) $(id).textContent = "–";
    entMsg.replaceChildren(notice("error", `Could not load the feature groups: ${e.error}. Run the backfill first.`));
  }

  const d = s.deployment;
  const startBtn = $("#start-deployment");
  if (d.running) {
    setService("svc-deployment", { state: "ok", label: d.status, message: "" });
    startBtn.classList.add("hidden");
  } else if (d.starting || /start|creat|updat|pending/i.test(d.status)) {
    setService("svc-deployment", { state: "wait", label: d.starting ? `Starting (${d.status})` : d.status,
      message: "Starting the deployment can take a few minutes; this panel updates on its own." });
    startBtn.classList.add("hidden");
  } else {
    setService("svc-deployment", { state: "bad", label: d.status,
      message: d.error ? `Error: ${d.error}` : "Predictions need a running deployment." });
    startBtn.classList.toggle("hidden", d.status === "Unavailable");
    startBtn.disabled = false;
  }

  for (const job of s.jobs) {
    const running = job.running;
    const waiting = /INITIALIZING|ACCEPTED|SUBMITTED|NEW|STARTING/.test(job.state);
    setService(`svc-${job.name}`, {
      state: running ? "ok" : waiting ? "wait" : "bad",
      label: job.state.replaceAll("_", " ").toLowerCase().replace(/^\w/, (c) => c.toUpperCase()),
      title: job.error || "",
    });
  }
  $("#status-updated").textContent = `updated ${new Date().toLocaleTimeString()}`;
  return e.state === "loading" || (!d.running && (d.starting || /start|creat|updat|pending/i.test(d.status)));
}

async function loadStatus() {
  clearTimeout(statusTimer);
  const panel = $("#status");
  panel.setAttribute("aria-busy", "true");
  let busy = false;
  try {
    busy = renderStatus(await api("api/status"));
    $("#status-error").replaceChildren();
  } catch (error) {
    busy = true;
    $("#status-error").replaceChildren(notice("error", `Could not load the status: ${error.message}`));
    if ($("#s-project .loading")) {
      for (const id of ["#s-project", "#s-merchants", "#s-accounts", "#s-cards"]) $(id).textContent = "–";
    }
  } finally {
    panel.removeAttribute("aria-busy");
    statusTimer = setTimeout(loadStatus, busy ? 4000 : 30000);
  }
}

$("#start-deployment").addEventListener("click", async (event) => {
  const button = event.currentTarget;
  button.disabled = true;
  try {
    const d = await api("api/deployment/start", { method: "POST" });
    setService("svc-deployment", { state: "wait", label: `Starting (${d.status})`,
      message: "Starting the deployment can take a few minutes; this panel updates on its own." });
    button.classList.add("hidden");
  } catch (error) {
    setService("svc-deployment", { state: "bad", label: "Start failed", message: error.message });
    button.disabled = false;
  }
  clearTimeout(statusTimer);
  statusTimer = setTimeout(loadStatus, 3000);
});

// ------------------------------------------------------------------ form ----

const form = $("#run-form");

function updateFraudHint() {
  const n = Number(form.elements.n.value);
  const rate = Number(form.elements.fraud_rate_pct.value);
  const hint = $("#fraud-hint");
  if (!Number.isFinite(n) || !Number.isFinite(rate)) {
    hint.textContent = " ";
    return;
  }
  const count = Math.max(0, Math.floor((n * rate) / 100 + 0.5));
  hint.textContent = `${int.format(count)} injected chain-attack transaction${count === 1 ? "" : "s"}`;
}
form.addEventListener("input", updateFraudHint);
updateFraudHint();

function readForm() {
  const n = Number(form.elements.n.value);
  const rate = Number(form.elements.fraud_rate_pct.value);
  const seed = Number(form.elements.seed.value);
  if (!Number.isInteger(n) || n < 10 || n > 500) throw new Error("Number of transactions must be a whole number from 10 to 500.");
  if (!Number.isFinite(rate) || rate < 0 || rate > 10) throw new Error("Fraud rate must be between 0 and 10 %.");
  if (!Number.isInteger(seed) || seed < 0) throw new Error("Random seed must be a whole number of 0 or more.");
  return { n, fraud_rate_pct: rate, seed, write: form.elements.write.checked };
}

function setProgress(percent, text) {
  const bar = $("#progress");
  bar.classList.remove("idle");
  bar.setAttribute("aria-valuenow", String(percent));
  bar.querySelector(".fill").style.width = `${percent}%`;
  bar.querySelector(".text").textContent = `${percent}% · ${text}`;
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = $("#run-button");
  const formError = $("#form-error");
  formError.replaceChildren();
  let request;
  try {
    request = readForm();
  } catch (error) {
    formError.replaceChildren(notice("error", error.message));
    return;
  }
  button.disabled = true;
  const results = $("#results");
  results.setAttribute("aria-busy", "true");
  setProgress(0, "Starting");
  try {
    const { id } = await api("api/runs", { method: "POST", body: JSON.stringify(request) });
    let run;
    let failures = 0;
    for (;;) {
      await sleep(800);
      try {
        run = await api(`api/runs/${id}`);
        failures = 0;
      } catch (error) {
        if (++failures >= 5) throw error;
        continue;
      }
      setProgress(run.percent, run.stage);
      if (run.status !== "running") break;
    }
    if (run.status === "error") {
      formError.replaceChildren(notice("error", run.error));
    } else {
      renderResults(run);
    }
    if (run.status === "error") loadStatus();
  } catch (error) {
    formError.replaceChildren(notice("error", `The run failed: ${error.message}`));
  } finally {
    button.disabled = false;
    results.removeAttribute("aria-busy");
  }
});

// --------------------------------------------------------------- results ----

function stat(label, value, cls = "", foot = "") {
  const box = el("div", "card stat");
  box.append(el("div", "label", label), el("div", `value ${cls}`, value), el("div", "foot", foot));
  return box;
}

const COLUMNS = [
  { title: "ID", cls: "mono", get: (r) => String(r.t_id) },
  { title: "Card", cls: "mono", get: (r) => r.cc_num },
  { title: "Merchant", cls: "mono", get: (r) => r.merchant_id },
  { title: "Amount ($)", cls: "num", get: (r) => money.format(r.amount) },
  { title: "IP", cls: "mono", get: (r) => r.ip_address, tip: (r) => `${r.ip_country} (home: ${r.home_country})` },
  { title: "Card present", get: (r) => (r.card_present ? "Yes" : "No") },
  { title: "Timestamp (local)", get: (r) => fmtTime(r.ts) },
  { title: "Txns last 10 min", cls: "num", get: (r) => fmtInt(r.num_trans_last_10_mins),
    tip: (r) => (r.aggs_event_time ? `window end ${r.aggs_event_time} UTC · sum ${fmtMoney(r.sum_trans_last_10_mins)}` : "no live aggregates for this card") },
  { title: "Txns last hour", cls: "num", get: (r) => fmtInt(r.num_trans_last_hour),
    tip: (r) => (r.aggs_event_time ? `sum ${fmtMoney(r.sum_trans_last_hour)} · max ${fmtMoney(r.max_trans_last_hour)} · ${fmtInt(r.num_ip_addresses_last_hour)} IPs` : "no live aggregates for this card") },
  { title: "Injected fraud", node: (r) => (r.injected_fraud ? el("span", "badge warn", "Injected") : el("span", "muted", "–")) },
  { title: "Prediction", node: (r) => {
    if (r.prediction === "fraud") return el("span", "badge high", "Fraud");
    if (r.prediction === "legit") return el("span", "badge", "Legitimate");
    const b = el("span", "badge plain", "Error");
    b.title = r.error || "";
    return b;
  } },
];

function table(rows) {
  const wrap = el("div", "table-wrap");
  const t = el("table");
  const head = el("tr");
  for (const c of COLUMNS) head.append(el("th", c.cls === "num" ? "num" : "", c.title));
  const thead = el("thead");
  thead.append(head);
  const tbody = el("tbody");
  for (const r of rows) {
    const tr = el("tr", r.prediction === "fraud" ? "flag" : "");
    for (const c of COLUMNS) {
      const td = el("td", c.cls || "");
      if (c.node) td.append(c.node(r));
      else td.textContent = c.get(r);
      if (c.tip) td.title = c.tip(r);
      tr.append(td);
    }
    tbody.append(tr);
  }
  t.append(thead, tbody);
  wrap.append(t);
  return wrap;
}

function renderResults(run) {
  const { summary: s, rows } = run.result;
  const body = $("#results-body");
  const parts = [];
  for (const w of run.warnings || []) parts.push(notice("warn", w));

  const tiles = el("div", "grid");
  tiles.append(
    stat("Total", fmtInt(s.total), "", s.written ? "written to credit_card_transactions" : "not written"),
    stat("Predicted fraud", fmtInt(s.predicted_fraud), s.predicted_fraud ? "danger" : ""),
    stat("Legitimate", fmtInt(s.legitimate), "accent", s.errors ? `${fmtInt(s.errors)} errors` : ""),
    stat("Predicted fraud rate", pct(s.predicted_fraud_rate)),
    stat("Injected fraud caught", s.injected_fraud ? `${fmtInt(s.injected_caught)} / ${fmtInt(s.injected_fraud)}` : "–",
      s.injected_fraud && s.injected_caught === s.injected_fraud ? "accent" : s.injected_fraud ? "danger" : "",
      s.injected_fraud ? pct(s.injected_caught / s.injected_fraud) : "no fraud injected"),
  );
  parts.push(tiles);

  const all = el("div");
  const head = el("div", "section-head");
  head.append(el("h3", "", "Transactions"),
    el("span", "muted", `Live aggregates for ${fmtInt(s.cards_with_aggregates)} of ${fmtInt(s.cards)} cards (cc_trans_aggs_fg v2, online)`));
  all.append(head, table(rows));
  parts.push(all);

  const flagged = rows.filter((r) => r.prediction === "fraud");
  const sus = el("div");
  const sHead = el("div", "section-head");
  sHead.append(el("h3", "", "Suspected fraud"), el("span", "muted", `${fmtInt(flagged.length)} transaction${flagged.length === 1 ? "" : "s"}`));
  sus.append(sHead, flagged.length ? table(flagged) : el("p", "empty", "The model flagged none of these transactions."));
  parts.push(sus);

  body.replaceChildren(...parts);
  const r = run.request;
  $("#results-meta").textContent = `${fmtInt(r.n)} transactions · fraud rate ${r.fraud_rate_pct}% · seed ${r.seed} · ${fmtTime(run.finished_at)}`;
}

loadStatus();
