"use strict";

// Native Node tests execute the real inline renderer against a DOM/Mapbox stub.
// No external package, browser, token, style tile, or network access is used.
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");
const html = fs.readFileSync(path.join(__dirname,
  "../src/nyc_taxi/dashboard/route_map_frontend/index.html"), "utf8");
const source = html.match(/<script>\s*([\s\S]*?)\s*<\/script>/)[1];
const geometry = {type: "LineString", coordinates: [[-73.980123456789, 40.75], [-73.97, 40.76]]};
const args = (overrides = {}) => ({online: true, access_token: "pk.synthetic_public_value",
  submission_id: "submitted-1", geometry: structuredClone(geometry), ...overrides});
const plain = (value) => JSON.parse(JSON.stringify(value));
const tick = async () => { await Promise.resolve(); await Promise.resolve(); await Promise.resolve(); };

function harness({libraryError = false, deferredLibrary = false} = {}) {
  const appended = [];
  const messages = [];
  const instances = [];
  const listeners = {};
  const timers = new globalThis.Map();
  let timerId = 0;
  const nodes = {map: {style: {}}, status: {textContent: ""}};
  const parent = {postMessage: (message) => messages.push(message)};
  const document = {
    body: {scrollHeight: 410}, getElementById: (id) => nodes[id],
    createElement: (tagName) => ({tagName}),
    head: {appendChild: (node) => {
      appended.push(node);
      if (node.tagName === "script" && !deferredLibrary)
        queueMicrotask(() => libraryError ? node.onerror() : node.onload());
    }},
  };
  class Map {
    constructor(options) {
      this.options = options; this.events = {}; this.sources = new globalThis.Map();
      this.added = []; this.updated = []; this.layers = []; this.bounds = [];
      this.removed = false; instances.push(this);
    }
    on(name, callback) { this.events[name] = callback; }
    getSource(id) { return this.sources.get(id); }
    addSource(id, value) {
      this.added.push({id, value: plain(value)});
      this.sources.set(id, {setData: (data) => this.updated.push(plain(data))});
    }
    addLayer(layer) { this.layers.push(plain(layer)); }
    fitBounds(bounds, options) { this.bounds.push({bounds: plain(bounds), options: plain(options)}); }
    remove() { this.removed = true; }
  }
  const window = {parent, mapboxgl: {Map}, addEventListener: (type, callback) => { listeners[type] = callback; },
    setTimeout: (callback, milliseconds) => { timers.set(++timerId, {callback, milliseconds}); return timerId; },
    clearTimeout: (id) => timers.delete(id)};
  const context = vm.createContext({window, document, console: {log: () => assert.fail("Unexpected log"),
    warn: () => assert.fail("Unexpected warning"), error: () => assert.fail("Unexpected error log")}});
  vm.runInContext(source, context, {filename: "route_map_frontend/index.html"});
  return {appended, messages, instances, nodes, timers,
    render: async (value) => {
      listeners.message({source: parent, data: {type: "streamlit:render", args: value}});
      await tick();
    },
    foreignRender: async (value) => {
      listeners.message({source: {}, data: {type: "streamlit:render", args: value}});
      await tick();
    },
    completeLibrary: async () => { appended.find((node) => node.tagName === "script").onload(); await tick(); },
  };
}

test("offline default and invalid routes/tokens do not load any external resource", async () => {
  const ui = harness();
  for (const value of [undefined, {}, args({online: false}), args({online: "true"}),
    args({access_token: ""}), args({access_token: "sk.synthetic_secret"}),
    args({access_token: "pk.bad\nvalue"}), args({geometry: null}),
    args({geometry: {type: "LineString", coordinates: [[0, 0]]}}),
    args({geometry: {type: "LineString", coordinates: [[NaN, 0], [0, 0]]}}),
    args({geometry: {type: "LineString", coordinates: [[181, 0], [0, 0]]}})]) {
    await ui.render(value);
  }
  assert.equal(ui.appended.length, 0);
  assert.equal(ui.instances.length, 0);
  assert.equal(ui.nodes.map.style.display, "none");
  assert.equal(ui.messages[0].type, "streamlit:componentReady");
});

test("only explicit online render loads pinned JS/CSS and keeps standard attribution/logo", async () => {
  const ui = harness();
  await ui.foreignRender(args());
  assert.equal(ui.appended.length, 0);
  await ui.render(args());
  assert.deepEqual(ui.appended.map((node) => node.src || node.href), [
    "https://api.mapbox.com/mapbox-gl-js/v3.15.0/mapbox-gl.css",
    "https://api.mapbox.com/mapbox-gl-js/v3.15.0/mapbox-gl.js",
  ]);
  assert.equal(ui.instances.length, 1);
  const map = ui.instances[0];
  assert.equal(map.options.attributionControl, true);
  assert.equal(map.options.logoPosition, "bottom-left");
  assert.equal(map.options.style, "mapbox://styles/mapbox/streets-v12");
  assert.equal(map.options.performanceMetricsCollection, false);
  assert.equal(map.options.collectResourceTiming, false);
  map.events.load();
  assert.deepEqual(map.added[0].value.data.geometry, geometry);
  assert.equal(map.layers[0].type, "line");
  assert.equal(map.bounds.length, 1);
  assert.equal(ui.nodes.map.style.display, "block");
  assert.equal(/\.mapboxgl-(?:ctrl-logo|ctrl-attrib)\s*\{/.test(html), false);
  assert.equal(/<script[^>]+src=|<link[^>]+href=/.test(html), false);
});

test("same submitted route survives reruns/draft inputs without new map/source or requests", async () => {
  const ui = harness();
  await ui.render(args());
  const map = ui.instances[0]; map.events.load();
  for (let index = 0; index < 4; index++) {
    await ui.render(args({draft_address: "Changed synthetic draft " + index}));
  }
  assert.equal(ui.appended.length, 2);
  assert.equal(ui.instances.length, 1);
  assert.equal(map.added.length, 1);
  assert.equal(map.updated.length, 0);
  assert.equal(map.bounds.length, 1);
  assert.equal(ui.messages.some((message) => message.type === "streamlit:setComponentValue"), false);
  assert.equal(/fetch\s*\(|XMLHttpRequest|\/directions\/|\/predict/.test(source), false);
  assert.equal(/localStorage|sessionStorage|console\./.test(source), false);
});

test("new submission reuses the map and updates the existing source from received geometry", async () => {
  const ui = harness();
  await ui.render(args());
  const map = ui.instances[0]; map.events.load();
  const next = {type: "LineString", coordinates: [[-73.9, 40.7], [-73.8, 40.8]]};
  await ui.render(args({submission_id: "submitted-2", geometry: next}));
  assert.equal(ui.instances.length, 1);
  assert.equal(map.added.length, 1);
  assert.equal(map.updated.length, 1);
  assert.deepEqual(map.updated[0].geometry, next);
  assert.equal(map.bounds.length, 2);
});

test("same submission cannot be relabelled with mutated draft geometry", async () => {
  const ui = harness();
  await ui.render(args());
  const map = ui.instances[0]; map.events.load();
  await ui.render(args({geometry: {type: "LineString", coordinates: [[0, 0], [1, 1]]}}));
  assert.equal(map.updated.length, 0);
  assert.match(ui.nodes.status.textContent, /Không thể hiển thị bản đồ/);
});

test("rerun during library loading creates only one map for the latest submission", async () => {
  const ui = harness({deferredLibrary: true});
  await ui.render(args());
  await ui.render(args({submission_id: "submitted-2"}));
  await ui.completeLibrary();
  assert.equal(ui.appended.length, 2);
  assert.equal(ui.instances.length, 1);
  ui.instances[0].events.load();
  assert.equal(ui.instances[0].added.length, 1);
});

test("load failure is safe and never retries or exposes upstream errors on rerun", async () => {
  const ui = harness({libraryError: true});
  await ui.render(args());
  await ui.render(args());
  assert.equal(ui.appended.length, 2);
  assert.equal(ui.instances.length, 0);
  assert.match(ui.nodes.status.textContent, /Không thể hiển thị bản đồ/);
  const ready = harness();
  await ready.render(args());
  ready.instances[0].events.error({error: new Error("pk.synthetic_secret_url")});
  assert.equal(ready.nodes.status.textContent.includes("synthetic_secret"), false);
});

test("switching offline removes the map without loading resources again", async () => {
  const ui = harness();
  await ui.render(args());
  const map = ui.instances[0]; map.events.load();
  await ui.render({online: false});
  assert.equal(map.removed, true);
  assert.equal(ui.nodes.map.style.display, "none");
  assert.equal(ui.appended.length, 2);
});

test("CDN loading has a bounded deadline and timeout cannot retry on rerun", async () => {
  const ui = harness({deferredLibrary: true});
  await ui.render(args());
  assert.equal(ui.timers.size, 1);
  const timer = [...ui.timers.values()][0];
  assert.equal(timer.milliseconds, 15000);
  timer.callback();
  await tick();
  await ui.render(args());
  assert.equal(ui.appended.length, 2);
  assert.equal(ui.instances.length, 0);
  assert.match(ui.nodes.status.textContent, /Không thể hiển thị bản đồ/);
});
