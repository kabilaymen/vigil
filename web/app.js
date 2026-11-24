// Drives the fleet simulation (window.vigil, from main.wasm) and draws it.
"use strict";

const $ = (id) => document.getElementById(id);
const VIEW = { "healthy": "healthy", "suspected": "suspected", "connection lost": "lost", "warming up": "warming" };
const PHI_MAX = 20; // the chart is capped here, as on the production dashboard
const THRESHOLD = 8;
let state = null;
let selected = "m-0000";
let paused = false;
let speed = 1;
let carry = 0;
let lastFrame = 0;

const secs = (ms) => (ms / 1000).toFixed(1) + " s";
const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();

// The page redraws about 60 times a second. These only touch the DOM when the
// content really changes, so text stays selectable and nothing flickers.
function setText(node, text) { if (node.textContent !== text) node.textContent = text; }
function setHTML(node, html) { if (node._html !== html) { node.innerHTML = html; node._html = html; } }

// A canvas drawn in fixed logical units (its width/height attributes) but
// rendered at the screen's real pixel density, so lines stay sharp.
function sharp(canvas) {
  if (!canvas._w) { canvas._w = canvas.width; canvas._h = canvas.height; }
  const dpr = window.devicePixelRatio || 1;
  const cssW = canvas.getBoundingClientRect().width || canvas._w;
  const w = Math.round(cssW * dpr), h = Math.round(cssW * dpr * canvas._h / canvas._w);
  if (canvas.width !== w || canvas.height !== h) { canvas.width = w; canvas.height = h; }
  const ctx = canvas.getContext("2d");
  ctx.setTransform(w / canvas._w, 0, 0, w / canvas._w, 0, 0);
  return { ctx, W: canvas._w, H: canvas._h };
}

function renderFleet() {
  const box = $("fleet");
  // The tiles are created once: replacing them would swallow clicks.
  if (box.children.length !== state.machines.length) {
    box.innerHTML = "";
    for (const m of state.machines) {
      const b = document.createElement("button");
      b.textContent = Number(m.id.slice(2));
      b.addEventListener("click", () => { selected = m.id; refresh(); });
      box.appendChild(b);
    }
  }
  state.machines.forEach((m, i) => {
    const b = box.children[i];
    const cls = `m ${VIEW[m.view] || "warming"}${m.truth !== "running" ? " truth" : ""}${m.id === selected ? " selected" : ""}`;
    if (b.className !== cls) b.className = cls;
    const title = `${m.id}, ${m.type}: really ${m.truth === "stalled" ? "frozen" : m.truth}, the detector says ${m.view}`;
    if (b.title !== title) b.title = title;
  });
  const count = (v) => state.machines.filter((m) => m.view === v).length;
  const parts = [`${count("healthy")} healthy`];
  if (count("suspected")) parts.push(`${count("suspected")} suspected`);
  if (count("connection lost")) parts.push(`${count("connection lost")} disconnected`);
  if (count("warming up")) parts.push(`${count("warming up")} learning their rhythm`);
  setText($("fleetLine"), `${parts.join(", ")}. A dot marks a machine that is really crashed or frozen, even before the detector has noticed. ${secs(state.ms)} simulated.`);
}

// One plain sentence about the selected machine, from the truth and the detector's view.
function caption(m) {
  const phi = m.phi > PHI_MAX ? `above ${PHI_MAX}` : m.phi.toFixed(1);
  if (m.truth === "crashed") {
    return m.view === "connection lost"
      ? `${m.id} has crashed. Its connection dropped, the broker published its last will, and Vigil reported it immediately. It will reconnect on its own.`
      : `${m.id} has crashed.`;
  }
  if (m.truth === "stalled") {
    return m.view === "suspected"
      ? `${m.id} is frozen and has been reported. Its last message was ${secs(m.silentMs)} ago and φ is ${phi}, past the threshold of 8.`
      : `${m.id} is frozen. Its connection is still open, so the broker notices nothing. Its last message was ${secs(m.silentMs)} ago; φ is ${phi} and the alert fires at 8.`;
  }
  if (m.view === "warming up") return `${m.id} just connected. The detector waits for ten intervals before judging it, so it has a rhythm to compare against.`;
  return `${m.id} is healthy: a message about every 0.5 s, the last one ${secs(m.silentMs)} ago. φ is ${phi}. Running at ${m.temp} °C, ${m.throughput} parts a minute.`;
}

function drawChart(points) {
  const { ctx, W, H } = sharp($("chart"));
  const padL = 30, padB = 22, padT = 10, padR = 10;
  const y = (v) => padT + (H - padT - padB) * (1 - Math.min(v, PHI_MAX) / PHI_MAX);
  const x = (i) => padL + (W - padL - padR) * (i / 149);
  ctx.clearRect(0, 0, W, H);
  ctx.font = "12px system-ui, sans-serif";
  for (const v of [0, 5, 10, 15, 20]) {
    ctx.strokeStyle = css("--rule"); ctx.lineWidth = 1;
    ctx.beginPath(); ctx.moveTo(padL, y(v)); ctx.lineTo(W - padR, y(v)); ctx.stroke();
    ctx.fillStyle = css("--soft"); ctx.fillText(String(v), 6, y(v) + 4);
  }
  ctx.fillText("30 s ago", padL, H - 6);
  ctx.fillText("now", W - padR - 24, H - 6);
  ctx.strokeStyle = css("--lost"); ctx.setLineDash([5, 4]);
  ctx.beginPath(); ctx.moveTo(padL, y(THRESHOLD)); ctx.lineTo(W - padR, y(THRESHOLD)); ctx.stroke();
  ctx.setLineDash([]);
  const off = 150 - points.length;
  ctx.strokeStyle = css("--link"); ctx.lineWidth = 2;
  ctx.beginPath();
  points.forEach((v, i) => { const px = x(i + off), py = y(v); i ? ctx.lineTo(px, py) : ctx.moveTo(px, py); });
  ctx.stroke();
}

function renderAlerts() {
  const k = state.counts, med = state.detectionMedianMs;
  const crashes = k.connection_lost || 0, freezes = k.suspected || 0;
  setText($("tally"), (crashes || freezes)
    ? `So far: ${crashes} crash${crashes === 1 ? "" : "es"} reported` + (crashes ? ` (median ${secs(med.connection_lost)} after the last message)` : "") +
      `, ${freezes} freeze${freezes === 1 ? "" : "s"} reported` + (freezes ? ` (median ${secs(med.suspected)} of silence)` : "") + "."
    : "No alert yet. The detector first learns each machine's rhythm.");
  const what = {
    suspected: (a) => `<b>suspected</b> after ${secs(a.silentMs)} of silence, φ = ${a.phi}`,
    connection_lost: () => `<b>disconnected</b>, reported by the broker`,
    recovered: (a) => `<b>back</b> after ${secs(a.silentMs)} of silence`,
    reconnected: () => `<b>reconnected</b>`,
  };
  setHTML($("alerts"), state.alerts.slice().reverse()
    .map((a) => `<li class="a-${a.type}"><time>${secs(a.ms)}</time><span>${a.machine} ${what[a.type](a)}</span></li>`).join(""));
}

// A button that cannot act on the selected machine is disabled, with the
// reason as a tooltip, instead of silently doing nothing.
function renderButtons(m) {
  const stall = $("stall"), crash = $("crash");
  stall.disabled = m.truth !== "running";
  stall.title = m.truth === "crashed" ? "This machine is down; it will reconnect on its own"
    : m.truth === "stalled" ? "This machine is already frozen" : "";
  crash.disabled = m.truth === "crashed";
  crash.title = m.truth === "crashed" ? "This machine is already down" : "";
}

function refresh() {
  state = JSON.parse(vigil.state());
  renderFleet();
  const m = state.machines.find((x) => x.id === selected);
  setText($("selId"), m.id);
  setText($("selType"), m.type.replace("-", " "));
  setText($("caption"), caption(m));
  renderButtons(m);
  drawChart(JSON.parse(vigil.phi(selected)) || []);
  renderAlerts();
}

function frame(t) {
  if (!lastFrame) lastFrame = t;
  const dt = Math.min(t - lastFrame, 250); // after a hidden tab comes back, do not run minutes at once
  lastFrame = t;
  if (!paused) {
    carry += (dt / state.tickMs) * speed;
    const n = Math.floor(carry);
    carry -= n;
    if (n > 0) vigil.step(n);
  }
  refresh();
  requestAnimationFrame(frame);
}

window.addEventListener("vigil-ready", () => {
  $("crash").onclick = () => { vigil.crash(selected); refresh(); };
  $("stall").onclick = () => { vigil.stall(selected); refresh(); };
  $("chaos").onchange = (e) => vigil.setChaos(e.target.checked);
  $("speed").onchange = (e) => { speed = parseFloat(e.target.value); };
  $("pause").onclick = (e) => { paused = !paused; e.target.textContent = paused ? "Resume" : "Pause"; };
  refresh();
  requestAnimationFrame(frame);
});

const go = new Go();
WebAssembly.instantiateStreaming(fetch("main.wasm"), go.importObject)
  .then((r) => go.run(r.instance))
  .catch((err) => { $("caption").textContent = "The simulation could not load in this browser (" + err + ")."; });
