"use strict";

// Real inline autocomplete code, synthetic DOM/clock only: no HTTP or browser.
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");
const html = fs.readFileSync(path.join(__dirname,
  "../src/nyc_taxi/dashboard/address_frontend/index.html"), "utf8");
const source = html.match(/<script>\s*([\s\S]*?)\s*<\/script>/)[1];
const SESSION = "11111111-1111-4111-8111-111111111111";
const NEXT_SESSION = "22222222-2222-4222-8222-222222222222";
const candidate = (index) => ({mapbox_id: "synthetic-" + index,
  label: "Synthetic place " + index + ", synthetic street, NYC"});
const defaults = (overrides = {}) => ({label: "Synthetic pickup", online: true,
  session_token: SESSION, query: "", version: 0, status: "idle", selected: null,
  suggestions: [], suggestions_query: null, suggestions_version: null, ...overrides});

function harness() {
  let now = 0, nextTimer = 0, eventCount = 0;
  const timers = new Map(), messages = [], listeners = {};
  const style = {setProperty(name, value) { this[name] = value; }};
  const nodes = {};
  const element = (tagName) => ({tagName, attributes: {}, children: [], textContent: "",
    value: "", hidden: false, selectionStart: 0, selectionEnd: 0,
    setAttribute(name, value) { this.attributes[name] = String(value); },
    removeAttribute(name) { delete this.attributes[name]; },
    replaceChildren() { this.children = []; }, appendChild(child) { this.children.push(child); },
    focus() { document.activeElement = this; },
    setSelectionRange(start, end) { this.selectionStart = start; this.selectionEnd = end; }});
  for (const id of ["query", "choices", "status", "label"]) nodes[id] = element(id);
  const document = {documentElement: {style}, activeElement: null,
    getElementById: (id) => nodes[id], createElement: element,
    body: {get scrollHeight() { return 96 + nodes.choices.children.length * 54; }}};
  const parent = {postMessage: (message) => messages.push(message)};
  const window = {parent,
    addEventListener: (type, callback) => { listeners[type] = callback; },
    setTimeout: (callback, delay) => { timers.set(++nextTimer, {callback, at: now + delay}); return nextTimer; },
    clearTimeout: (id) => timers.delete(id)};
  const context = vm.createContext({window, document,
    crypto: {randomUUID: () => "00000000-0000-4000-8000-" + String(++eventCount).padStart(12, "0")},
    console: {log: () => assert.fail("No address logging allowed"),
      warn: () => assert.fail("No address logging allowed"), error: () => assert.fail("No address logging allowed")}});
  vm.runInContext(source, context, {filename: "address_frontend/index.html"});
  return {nodes, messages, timers, document, style,
    events: (kind) => messages.filter((item) => item.type === "streamlit:setComponentValue")
      .map((item) => item.value).filter((item) => !kind || item.kind === kind),
    render: (args = defaults(), theme = {base: "light"}) => listeners.message({source: parent,
      data: {type: "streamlit:render", args, theme}}),
    foreignRender: (args) => listeners.message({source: {}, data: {type: "streamlit:render", args}}),
    type: (value) => { nodes.query.value = value; nodes.query.oninput(); },
    key: (key) => { let prevented = false; nodes.query.onkeydown({key,
      preventDefault() { prevented = true; }}); return prevented; },
    advance: (milliseconds) => {
      now += milliseconds;
      for (const [id, timer] of [...timers]) if (timer.at <= now) { timers.delete(id); timer.callback(); }
    },
  };
}
function results(ui, query, version, candidates = [candidate(1), candidate(2)], overrides = {}) {
  ui.render(defaults({query, version, status: candidates.length ? "results" : "empty",
    suggestions_query: query, suggestions_version: version, suggestions: candidates, ...overrides}));
}

test("a single valid character schedules suggest after 400ms without a search button", () => {
  const ui = harness(); ui.render(); ui.type("m");
  assert.equal(ui.events("edit").length, 1);
  assert.equal(ui.events("suggest").length, 0);
  ui.advance(399); assert.equal(ui.events("suggest").length, 0);
  ui.advance(1); assert.equal(ui.events("suggest").length, 1);
  assert.deepEqual(JSON.parse(JSON.stringify(ui.events("suggest")[0])), {
    event_id: "00000000-0000-4000-8000-000000000002", session_token: SESSION,
    kind: "suggest", query: "m", version: 1});
  assert.equal(/id="search"|Dùng địa chỉ/.test(html), false);
  assert.match(ui.nodes.status.textContent, /Đang tìm/);
});

test("rapid typing debounces and clearing cancels pending requests and suggestions", () => {
  const ui = harness(); ui.render();
  ui.type("m"); ui.advance(200); ui.type("me"); ui.advance(200); ui.type("met");
  ui.advance(399); assert.equal(ui.events("suggest").length, 0);
  ui.advance(1); assert.equal(ui.events("suggest").length, 1);
  assert.equal(ui.events("suggest")[0].query, "met");
  results(ui, "met", 3); assert.equal(ui.nodes.choices.children.length, 2);
  ui.type("meta"); ui.type(""); ui.advance(500);
  assert.equal(ui.events("suggest").length, 1);
  assert.equal(ui.nodes.choices.hidden, true);
  assert.equal(ui.nodes.query.attributes["aria-expanded"], "false");
  assert.equal(ui.events("edit").at(-1).query, "");
});

test("late query/version responses never replace newer query or suggestions", () => {
  const ui = harness(); ui.render(); ui.type("m"); ui.advance(400);
  ui.type("me"); ui.advance(400); results(ui, "me", 2, [candidate(2)]);
  results(ui, "m", 1, [candidate(1)]);
  assert.equal(ui.nodes.query.value, "me");
  assert.equal(ui.nodes.choices.children[0].textContent, candidate(2).label);
  results(ui, "me", 1, [candidate(3)]);
  assert.equal(ui.nodes.choices.children[0].textContent, candidate(2).label);
  assert.equal(ui.events("suggest").length, 2);
});

test("rerenders preserve draft text focus and caret and never replay suggest", () => {
  const ui = harness(); ui.render(); ui.nodes.query.focus(); ui.type("met");
  ui.nodes.query.setSelectionRange(1, 2); ui.advance(400);
  for (let index = 0; index < 4; index++) ui.render(defaults());
  assert.equal(ui.nodes.query.value, "met");
  assert.equal(ui.document.activeElement, ui.nodes.query);
  assert.equal(ui.nodes.query.selectionStart, 1); assert.equal(ui.nodes.query.selectionEnd, 2);
  assert.equal(ui.events("suggest").length, 1);
  assert.equal(ui.timers.size, 0);
});

test("click retrieves exactly once and selected response completes selection and fills input", () => {
  const ui = harness(); ui.render(); ui.type("m"); ui.advance(400); results(ui, "m", 1);
  ui.nodes.query.focus(); const option = ui.nodes.choices.children[0];
  let prevented = false; option.onmousedown({preventDefault() { prevented = true; }});
  option.onclick(); option.onclick();
  assert.equal(prevented, true); assert.equal(ui.document.activeElement, ui.nodes.query);
  assert.equal(ui.events("retrieve").length, 1);
  assert.equal(ui.events("retrieve")[0].mapbox_id, "synthetic-1");
  assert.equal("selection" in ui.events("retrieve")[0], false);
  assert.equal("latitude" in ui.events("retrieve")[0], false);
  const selected = defaults({query: "m", version: 1, status: "selected",
    session_token: NEXT_SESSION, selected: candidate(1)});
  ui.render(selected); ui.render(selected);
  assert.equal(ui.nodes.query.value, candidate(1).label);
  assert.equal(ui.nodes.status.textContent, "Đã chọn địa chỉ.");
  assert.equal(ui.nodes.status.textContent.includes(candidate(1).label), false);
  assert.equal(ui.events("retrieve").length, 1); assert.equal(ui.events("select").length, 0);
  assert.equal(ui.events("suggest").length, 1);
  ui.type("new"); assert.doesNotMatch(ui.nodes.status.textContent, /Đã chọn/);
  assert.equal(ui.events("edit").at(-1).session_token, NEXT_SESSION);
  ui.advance(400); assert.equal(ui.events("suggest").at(-1).query, "new");
});

test("Arrow keys and Enter select once; Escape closes results across rerenders", () => {
  const ui = harness(); ui.render(); ui.type("m"); ui.advance(400); results(ui, "m", 1);
  assert.equal(ui.key("ArrowDown"), true); assert.equal(ui.key("ArrowDown"), true);
  assert.equal(ui.key("ArrowUp"), true);
  assert.equal(ui.nodes.query.attributes["aria-activedescendant"], "address-option-0");
  results(ui, "m", 1);
  assert.equal(ui.nodes.query.attributes["aria-activedescendant"], "address-option-0");
  assert.equal(ui.key("Enter"), true); ui.key("Enter");
  assert.equal(ui.events("retrieve").length, 1);
  const dismissed = harness(); dismissed.render(); dismissed.type("m"); dismissed.advance(400);
  results(dismissed, "m", 1); dismissed.key("Escape"); results(dismissed, "m", 1);
  assert.equal(dismissed.nodes.choices.hidden, true);
  assert.equal(dismissed.events("retrieve").length, 0);
});

test("pickup and dropoff iframe instances have independent queries sessions and timers", () => {
  const pickup = harness(), dropoff = harness(); pickup.render();
  dropoff.render(defaults({session_token: NEXT_SESSION, label: "Synthetic dropoff"}));
  pickup.type("m"); dropoff.type("e"); pickup.advance(400);
  assert.equal(pickup.events("suggest").length, 1); assert.equal(dropoff.events("suggest").length, 0);
  dropoff.advance(400); assert.equal(dropoff.events("suggest")[0].session_token, NEXT_SESSION);
  results(pickup, "m", 1); pickup.nodes.choices.children[0].onclick();
  assert.equal(dropoff.nodes.query.value, "e"); assert.equal(dropoff.events("retrieve").length, 0);
});

test("empty/error/offline and invalid queries are safe without retries", () => {
  const ui = harness(); ui.render();
  for (const value of ["", "  ", "x\n", "x".repeat(251)]) { ui.type(value); ui.advance(500); }
  assert.equal(ui.events("suggest").length, 0);
  ui.type("m"); ui.advance(400); results(ui, "m", 5, []);
  assert.match(ui.nodes.status.textContent, /Không có kết quả/);
  ui.render(defaults({query: "m", version: 5, status: "error", error: "safe error"}));
  assert.match(ui.nodes.status.textContent, /Không thể/); ui.advance(5000);
  assert.equal(ui.events("suggest").length, 1);
  const offline = harness(); offline.render(defaults({online: false}));
  offline.type("m"); offline.advance(500); assert.equal(offline.events("suggest").length, 0);
  assert.equal(offline.events("edit").length, 1);
  assert.match(offline.nodes.status.textContent, /chưa được bật/);
});

test("results are capped at five safe text options with theme-aware nonclipped height", () => {
  const ui = harness(); ui.render(); ui.type("m"); ui.advance(400);
  const unsafe = {mapbox_id: "synthetic", label: '<img src=x onerror="fail()">, synthetic street'};
  results(ui, "m", 1, [unsafe, ...Array.from({length: 6}, (_, index) => candidate(index))]);
  assert.equal(ui.nodes.choices.children.length, 5);
  assert.equal(ui.nodes.choices.children[0].textContent, unsafe.label);
  assert.equal("innerHTML" in ui.nodes.choices.children[0], false);
  assert.equal(ui.messages.at(-1).height, 378);
  ui.render(defaults({query: "m", version: 1}), {base: "dark"});
  assert.equal(ui.style.colorScheme, "dark"); assert.equal(ui.style["--bg"], "#0e1117");
  assert.equal(ui.style["--fg"], "#fafafa");
  ui.render(defaults({query: "m", version: 1}), {base: "light"});
  assert.equal(ui.style.colorScheme, "light");
  ui.render(defaults({query: "m", version: 1}), {backgroundColor: "#0e1117", textColor: "#fafafa"});
  assert.equal(ui.style.colorScheme, "dark");
  assert.equal(/overflow:\s*hidden|<script[^>]+src=|fetch\(|localStorage|sessionStorage/.test(html), false);
});

test("rotated retrieve session does not overwrite newer text or accept retired-session responses", () => {
  const ui = harness(); ui.render(); ui.type("m"); ui.advance(400); results(ui, "m", 1);
  ui.nodes.choices.children[0].onclick(); ui.type("me");
  ui.render(defaults({query: "m", version: 1, status: "selected",
    session_token: NEXT_SESSION, selected: candidate(1)}));
  assert.equal(ui.nodes.query.value, "me"); assert.doesNotMatch(ui.nodes.status.textContent, /Đã chọn/);
  assert.equal(ui.events("edit").at(-1).session_token, NEXT_SESSION);
  results(ui, "m", 1); assert.equal(ui.nodes.query.value, "me");
  ui.advance(400); assert.equal(ui.events("suggest").at(-1).session_token, NEXT_SESSION);
  assert.equal(ui.events("retrieve").length, 1);
});

test("rotation schedules an unanswered newer draft whose old-token debounce already fired", () => {
  const ui = harness(); ui.render(); ui.type("m"); ui.advance(400); results(ui, "m", 1);
  ui.nodes.choices.children[0].onclick(); ui.type("me"); ui.advance(400);
  assert.equal(ui.events("suggest").at(-1).session_token, SESSION);
  const finished = defaults({query: "m", version: 1, status: "selected",
    session_token: NEXT_SESSION, selected: candidate(1)});
  ui.render(finished); ui.render(finished); ui.advance(399);
  assert.equal(ui.events("suggest").length, 2);
  ui.advance(1);
  assert.equal(ui.events("suggest").length, 3);
  assert.equal(ui.events("suggest").at(-1).query, "me");
  assert.equal(ui.events("suggest").at(-1).session_token, NEXT_SESSION);
  ui.render(finished); ui.advance(1000); assert.equal(ui.events("suggest").length, 3);
  results(ui, "me", 2, [candidate(2)], {session_token: NEXT_SESSION});
  assert.equal(ui.nodes.choices.children[0].textContent, candidate(2).label);
});

test("composition input waits for completion and foreign render messages are ignored", () => {
  const ui = harness(); ui.render(); ui.foreignRender(defaults({online: false}));
  ui.nodes.query.oncompositionstart(); ui.type("m"); ui.advance(500);
  assert.equal(ui.events("suggest").length, 0);
  ui.nodes.query.oncompositionend(); ui.advance(400);
  assert.equal(ui.events("suggest").length, 1);
});

test("address labels input choices and status share readable local typography and spacing", () => {
  const styles = html.match(/<style>\s*([\s\S]*?)\s*<\/style>/)[1];
  assert.match(styles, /--font-family:\s*"Segoe UI",\s*Arial,\s*sans-serif/);
  assert.match(styles, /body\s*\{[^}]*font:\s*16px\/1\.5\s+var\(--font-family\)/);
  assert.match(styles, /input\s*\{[^}]*min-height:\s*48px[^}]*font:\s*inherit/);
  assert.match(styles, /#choices li\s*\{[^}]*min-height:\s*48px/);
  assert.match(styles, /#status\s*\{[^}]*min-height:\s*24px[^}]*color:\s*var\(--fg\)/);
  assert.match(styles, /input:focus\s*\{[^}]*outline:\s*2px solid var\(--accent\)/);
  assert.doesNotMatch(styles, /opacity\s*:|@import|url\(/);
});

test("address font inherits the native theme stack with a safe local fallback", () => {
  const ui = harness();
  ui.render(defaults(), {font: '"Source Sans Pro", sans-serif'});
  assert.equal(ui.style["--font-family"], '"Source Sans Pro", sans-serif');
  for (const font of [undefined, " ", "x".repeat(201), "Arial; color: red", "url(https://invalid.example/font)", "Arial\nserif"]) {
    ui.render(defaults(), {font});
    assert.equal(ui.style["--font-family"], '"Segoe UI", Arial, sans-serif');
  }
  assert.equal(ui.events().length, 0);
});

test("light and dark address text keep strong contrast without dimming status", () => {
  const luminance = (hex) => {
    const digits = hex.length === 4 ? hex.slice(1).split("").map((digit) => digit + digit).join("") : hex.slice(1);
    const components = [0, 2, 4].map((offset) => {
      const value = parseInt(digits.slice(offset, offset + 2), 16) / 255;
      return value <= .04045 ? value / 12.92 : ((value + .055) / 1.055) ** 2.4;
    });
    return components[0] * .2126 + components[1] * .7152 + components[2] * .0722;
  };
  const contrast = (first, second) => {
    const a = luminance(first), b = luminance(second);
    return (Math.max(a, b) + .05) / (Math.min(a, b) + .05);
  };
  const ui = harness();
  for (const base of ["light", "dark"]) {
    ui.render(defaults(), {base});
    assert.ok(contrast(ui.style["--fg"], ui.style["--bg"]) >= 7, base + " body/status contrast");
    assert.ok(contrast(ui.style["--muted"], ui.style["--bg"]) >= 4.5, base + " secondary text contrast");
    assert.ok(contrast(ui.style["--fg"], ui.style["--active"]) >= 7, base + " selected row contrast");
    assert.equal(ui.events().length, 0);
  }
});
