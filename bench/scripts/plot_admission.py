#!/usr/bin/env python3
"""Admission control A/B chart: lateness p99 per second with and without backpressure.

Reads the loadgen CSVs that bench/scripts/admission.sh collects (-csv) and writes ONE self-contained HTML file:
no external libraries, light and dark theme, hover tooltips, a table view of every number.

    python3 bench/scripts/plot_admission.py bench/2026-11-18-r2 -o bench/2026-11-18-r2-p99.html

PREFIX (bench/2026-11-18-r2) is the common start of the run directories; the script expects
PREFIX-off, PREFIX-default-a, PREFIX-default-b and PREFIX-qw50 next to each other (edit CONFIGS for other runs).
Steady state is the same window as bench/scripts/late.awk: seconds 41..175.
"""
import argparse
import csv
import json
import math
import statistics as st
import sys
from pathlib import Path

SLO_MS = 150          # BENCHMARKS.md: lateness p99 <= W + 50 ms with the 100 ms flush window
RAMP_S = 40           # 200 new streams a second: the ramp-up is over after 40 s
STEADY = (40, 175)    # seconds in (40, 175]
T_MAX = 180

# slot = categorical colour slot (fixed order), runs = (directory suffix, loadgen mode in the file name, tag)
CONFIGS = [
    dict(id="off", name="No admission", slot=1, runs=[("off", "off", "")]),
    dict(id="def", name="Defaults, queue 20 ms", slot=2, runs=[("default-a", "on", "run A"), ("default-b", "on", "run B")]),
    dict(id="qw50", name="Queue 50 ms", slot=3, runs=[("qw50", "on", "")]),
]


def num(s):
    return float(s) if s != "" else None


def load(path):
    rows = {}
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            rows[int(float(r["elapsed_s"]))] = r
    return rows


def nice_axis(vmax):
    """The smallest round upper bound for 0..vmax that takes 3 to 5 grid steps, and its ticks."""
    best = None
    for exp in range(-2, 8):
        for m in (1, 2, 2.5, 5):
            step = m * 10 ** exp
            steps = math.ceil(vmax / step)
            if 3 <= steps <= 5 and (best is None or steps * step < best[0]):
                best = (steps * step, step, steps)
    top, step, steps = best
    return top, [int(round(i * step)) for i in range(steps + 1)]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("prefix", help="common start of the run directories, e.g. bench/2026-11-18-r2")
    ap.add_argument("-o", "--out", required=True, help="HTML file to write")
    args = ap.parse_args()

    ts = list(range(1, T_MAX))
    runs, tiles = [], []
    for cfg in CONFIGS:
        stats = []
        for sub, mode, tag in cfg["runs"]:
            path = Path(f"{args.prefix}-{sub}") / f"run-{mode}.csv"
            if not path.exists():
                sys.exit(f"missing {path}")
            rows = load(path)
            late = [num(rows[t]["late_p99_ms"]) if t in rows else None for t in ts]
            r503 = [num(rows[t]["rej_503_s"]) if t in rows else None for t in ts]
            acc = [num(rows[t]["accepted_s"]) or 0 for t in ts]
            runs.append(dict(cfg=cfg["id"], tag=tag, late=late, r503=r503))

            steady = [v for t, v in zip(ts, late) if STEADY[0] < t <= STEADY[1] and v is not None]
            total503 = sum(v or 0 for v in r503)
            stats.append(dict(
                mean=st.mean(steady), worst=max(steady), over=sum(v > SLO_MS for v in steady), n=len(steady),
                share503=100 * total503 / (total503 + sum(acc)) if total503 else 0.0, tag=tag,
            ))
        tiles.append(dict(cfg=cfg["id"], stats=stats))

    def vmax(metric, cfgs):
        return max(v for r in runs if r["cfg"] in cfgs for v in r[metric] if v is not None)

    panels = {}
    for key, metric, cfgs in (("all", "late", ["off", "def", "qw50"]), ("zoom", "late", ["def", "qw50"]),
                              ("r503", "r503", ["off", "def", "qw50"])):
        top, ticks = nice_axis(vmax(metric, cfgs) * 1.02)
        panels[key] = dict(yMax=top, ticks=ticks)

    # table view: 10 second windows, so the whole run fits on one screen
    bins = []
    for k in range(0, T_MAX // 10):
        lo, hi = k * 10 + 1, min(k * 10 + 10, T_MAX - 1)
        cells = []
        for r in runs:
            def mean(metric):
                xs = [v for t, v in zip(ts, r[metric]) if lo <= t <= hi and v is not None]
                return st.mean(xs) if xs else None
            cells.append(dict(late=mean("late"), r503=mean("r503")))
        bins.append(dict(lo=lo, hi=hi, cells=cells))

    data = dict(
        t=ts, tmax=T_MAX, slo=SLO_MS, ramp=RAMP_S, steady=[STEADY[0] + 1, STEADY[1]],
        cfgs=[dict(id=c["id"], name=c["name"], slot=c["slot"]) for c in CONFIGS],
        runs=runs, tiles=tiles, panels=panels, bins=bins,
    )
    html = TEMPLATE.replace("__DATA__", json.dumps(data, separators=(",", ":")))
    Path(args.out).write_text(html, encoding="utf-8")
    print(f"wrote {args.out} ({len(html) // 1024} KB)")


TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<title>Tensorgate admission control, lateness p99</title>
<style>
  :root {
    color-scheme: light;
    --page: #f9f9f7; --surface: #fcfcfb;
    --text-primary: #0b0b0b; --text-secondary: #52514e; --muted: #898781;
    --grid: #e1e0d9; --axis: #c3c2b7; --border: rgba(11,11,11,0.10);
    --s1: #2a78d6; --s2: #eb6834; --s3: #1baf7a;
    --good: #006300; --bad: #d03b3b;
  }
  @media (prefers-color-scheme: dark) {
    :root:where(:not([data-theme="light"])) {
      color-scheme: dark;
      --page: #0d0d0d; --surface: #1a1a19;
      --text-primary: #f0efec; --text-secondary: #c3c2b7; --muted: #898781;
      --grid: #2c2c2a; --axis: #383835; --border: rgba(255,255,255,0.10);
      --s1: #3987e5; --s2: #d95926; --s3: #199e70;
      --good: #0ca30c; --bad: #d03b3b;
    }
  }
  :root[data-theme="dark"] {
    color-scheme: dark;
    --page: #0d0d0d; --surface: #1a1a19;
    --text-primary: #f0efec; --text-secondary: #c3c2b7; --muted: #898781;
    --grid: #2c2c2a; --axis: #383835; --border: rgba(255,255,255,0.10);
    --s1: #3987e5; --s2: #d95926; --s3: #199e70;
    --good: #0ca30c; --bad: #d03b3b;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0; background: var(--page); color: var(--text-primary);
    font: 14px/1.45 system-ui, -apple-system, "Segoe UI", sans-serif;
  }
  .wrap { max-width: 1000px; margin: 0 auto; padding-inline: 16px; padding-block: 24px 40px; }
  header { display: flex; gap: 16px; align-items: flex-start; justify-content: space-between; flex-wrap: wrap; }
  h1 { font-size: 20px; line-height: 1.25; margin: 0 0 4px; font-weight: 600; }
  .sub { margin: 0; color: var(--text-secondary); max-width: 62ch; }
  button.toggle {
    font: inherit; color: var(--text-primary); background: var(--surface); border: 1px solid var(--border);
    border-radius: 8px; padding: 6px 12px; cursor: pointer;
  }
  button.toggle[aria-pressed="true"] { border-color: var(--text-secondary); }
  button:focus-visible, svg:focus-visible { outline: 2px solid var(--text-secondary); outline-offset: 2px; }

  .tiles { display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 12px; margin-block: 20px 8px; }
  .tile { background: var(--surface); border: 1px solid var(--border); border-radius: 12px; padding: 14px 16px; }
  .tile .lab { display: flex; align-items: center; gap: 8px; color: var(--text-secondary); }
  .key { display: inline-block; width: 14px; height: 2px; border-radius: 1px; flex: none; }
  .tile .val { font-size: 30px; line-height: 1.2; font-weight: 600; margin-block: 6px 0; }
  .tile .val small { font-size: 14px; font-weight: 400; color: var(--text-secondary); }
  .tile .what { color: var(--muted); font-size: 12px; }
  .verdict { margin-top: 8px; font-weight: 600; display: flex; gap: 6px; align-items: baseline; }
  .verdict.pass { color: var(--good); } .verdict.fail { color: var(--bad); }
  .tile .facts { margin: 8px 0 0; padding: 0; list-style: none; color: var(--text-secondary); font-size: 12px; }

  .legend { display: flex; flex-wrap: wrap; gap: 6px 8px; margin-block: 16px 12px; }
  .legend button {
    font: inherit; color: var(--text-primary); background: none; border: 1px solid transparent; border-radius: 8px;
    padding: 4px 10px; cursor: pointer; display: inline-flex; align-items: center; gap: 8px;
  }
  .legend button:hover { background: var(--surface); }
  .legend button[aria-pressed="false"] { color: var(--muted); }
  .legend button[aria-pressed="false"] .key { opacity: 0.3; }
  .legend .hint { color: var(--muted); font-size: 12px; align-self: center; }

  figure { margin: 0 0 12px; background: var(--surface); border: 1px solid var(--border); border-radius: 12px; padding: 14px 8px 6px; }
  figcaption { padding-inline: 8px; margin-bottom: 4px; }
  figcaption b { font-weight: 600; }
  figcaption span { display: block; color: var(--text-secondary); font-size: 12px; }
  .chart { position: relative; width: 100%; }
  .chart svg { display: block; touch-action: pan-y; }
  .tip {
    position: absolute; pointer-events: none; background: var(--surface); color: var(--text-primary);
    border: 1px solid var(--axis); border-radius: 8px; padding: 6px 10px; font-size: 12px; white-space: nowrap;
    box-shadow: 0 2px 8px rgba(0,0,0,0.12);
  }
  .tip .th { color: var(--text-secondary); margin-bottom: 2px; }
  .tip .tr { display: flex; align-items: center; gap: 8px; }
  .tip .tr b { font-weight: 600; min-width: 5.5ch; text-align: right; font-variant-numeric: tabular-nums; }
  .tip .tr i { font-style: normal; color: var(--text-secondary); }

  .tableview { display: none; }
  body.tv .charts { display: none; }
  body.tv .tableview { display: block; }
  .tablewrap { overflow-x: auto; background: var(--surface); border: 1px solid var(--border); border-radius: 12px; margin-bottom: 12px; }
  table { border-collapse: collapse; width: 100%; font-variant-numeric: tabular-nums; }
  caption { text-align: left; padding: 12px 12px 4px; font-weight: 600; }
  th, td { padding: 6px 12px; text-align: right; white-space: nowrap; border-top: 1px solid var(--grid); }
  th:first-child, td:first-child { text-align: left; }
  thead th { color: var(--text-secondary); font-weight: 500; border-top: 0; vertical-align: bottom; }

  .notes { color: var(--text-secondary); font-size: 12px; margin-top: 16px; }
  .notes p { margin: 0 0 6px; max-width: 90ch; }

  svg text { font: 11px system-ui, -apple-system, "Segoe UI", sans-serif; }
  .ax { fill: var(--muted); font-variant-numeric: tabular-nums; }
  .lbl { fill: var(--text-secondary); }
</style>
</head>
<body>
<div class="wrap">
  <header>
    <div>
      <h1>Admission control: lateness p99 under overload</h1>
      <p class="sub">8000 streams offered to a 2 vCPU server that is allowed 5000. Per-second p99 of token lateness at the client, three-minute runs, 1000 API keys.</p>
    </div>
    <button class="toggle" id="tv" aria-pressed="false">Table view</button>
  </header>

  <section class="tiles" id="tiles" aria-label="Steady-state summary"></section>

  <div class="charts">
    <div class="legend" id="legend" role="group" aria-label="Series"></div>

    <figure>
      <figcaption><b>All runs</b><span>Lateness p99 per second, ms. Without admission it stays far above the SLO for the whole run.</span></figcaption>
      <div class="chart" id="chart-all"></div>
    </figure>
    <figure>
      <figcaption><b>Zoom on the runs with admission</b><span>The same data from 0 to 250 ms. The 20 ms default stays under the SLO, the 50 ms queue does not.</span></figcaption>
      <div class="chart" id="chart-zoom"></div>
    </figure>
    <figure>
      <figcaption><b>Requests answered 503</b><span>Per second. The rejected clients retry after their Retry-After, so this is the load the rejections themselves put on the server.</span></figcaption>
      <div class="chart" id="chart-503"></div>
    </figure>
  </div>

  <div class="tableview" id="tableview"></div>

  <div class="notes">
    <p>Steady state is seconds 41 to 175; the shaded first 40 s is the ramp-up at 200 new streams per second and is not in the summary. The SLO is lateness p99 up to 150 ms (BENCHMARKS.md, 100 ms flush window), judged on the mean of the per-second p99.</p>
    <p>Lateness is measured from the moment the request is sent, so the time a stream waited for a free slot is part of it: the 50 ms queue adds up to 50 ms to every accepted stream, the 20 ms queue up to 20 ms.</p>
    <p>One three-minute run per line on shared vCPUs. The two default runs differ by about 2 ms in mean p99, which is the noise to compare against.</p>
  </div>
</div>

<script type="application/json" id="data">__DATA__</script>
<script>
(() => {
  const D = JSON.parse(document.getElementById('data').textContent);
  const NS = 'http://www.w3.org/2000/svg';
  const hidden = new Set();                         // configurations switched off in the legend
  const cfgs = Object.fromEntries(D.cfgs.map(c => [c.id, c]));
  const colour = c => `var(--s${cfgs[c].slot})`;
  const ms = v => (v >= 100 ? Math.round(v) : v.toFixed(1)) + ' ms';
  const legendName = c => c.id === 'def' ? c.name + ' (2 runs)' : c.name;

  const mk = (tag, attrs, parent) => {
    const e = document.createElementNS(NS, tag);
    for (const k in attrs) e.setAttribute(k, attrs[k]);
    if (parent) parent.appendChild(e);
    return e;
  };
  const div = (cls, parent, text) => {
    const e = document.createElement('div');
    if (cls) e.className = cls;
    if (text != null) e.textContent = text;
    if (parent) parent.appendChild(e);
    return e;
  };
  const keyEl = (c, parent) => {
    const k = document.createElement('span');
    k.className = 'key'; k.style.background = colour(c);
    parent.appendChild(k);
    return k;
  };

  // ---------- summary tiles
  const tilesHost = document.getElementById('tiles');
  for (const t of D.tiles) {
    const c = cfgs[t.cfg];
    const mean = t.stats.reduce((a, s) => a + s.mean, 0) / t.stats.length;
    const pass = mean <= D.slo;
    const tile = div('tile', tilesHost);
    const lab = div('lab', tile); keyEl(t.cfg, lab); lab.append(c.name);
    const val = div('val', tile, Math.round(mean) + ' ms');
    div('what', tile, 'lateness p99, mean of seconds ' + D.steady[0] + ' to ' + D.steady[1]);
    div('verdict ' + (pass ? 'pass' : 'fail'), tile, (pass ? '✓ ' : '✕ ') + (pass ? 'Within' : 'Over') + ' the ' + D.slo + ' ms SLO');
    const ul = document.createElement('ul'); ul.className = 'facts'; tile.appendChild(ul);
    const li = text => { const e = document.createElement('li'); e.textContent = text; ul.appendChild(e); };
    const each = f => t.stats.map(f).join(' / ');
    if (t.stats.length > 1) li('runs ' + t.stats.map(s => s.tag.replace('run ', '')).join(' / ') + ': ' + each(s => s.mean.toFixed(1)) + ' ms');
    li('seconds over the SLO: ' + each(s => s.over) + ' of ' + t.stats[0].n);
    li('worst second: ' + each(s => Math.round(s.worst)) + ' ms');
    const sh = t.stats.reduce((a, s) => a + s.share503, 0) / t.stats.length;
    if (sh > 0) li(Math.round(sh) + '% of requests rejected with 503');
  }

  // ---------- legend (click to hide a configuration everywhere)
  const legendHost = document.getElementById('legend');
  for (const c of D.cfgs) {
    const b = document.createElement('button');
    b.type = 'button'; b.setAttribute('aria-pressed', 'true');
    keyEl(c.id, b); b.append(legendName(c));
    b.addEventListener('click', () => {
      const on = hidden.has(c.id);
      on ? hidden.delete(c.id) : hidden.add(c.id);
      b.setAttribute('aria-pressed', String(on));
      drawAll();
    });
    legendHost.appendChild(b);
  }
  div('hint', legendHost, 'Click a name to hide a run.');

  // ---------- charts
  const PANELS = [
    { host: 'chart-all',  metric: 'late', cfgs: ['off', 'def', 'qw50'], slo: true, unit: 'ms', aria: 'Lateness p99 per second for all runs' },
    { host: 'chart-zoom', metric: 'late', cfgs: ['def', 'qw50'],        slo: true, unit: 'ms', aria: 'Lateness p99 per second, runs with admission, 0 to 250 ms' },
    { host: 'chart-503',  metric: 'r503', cfgs: ['off', 'def', 'qw50'], slo: false, unit: '/s', aria: 'Requests answered 503 per second' },
  ];
  PANELS[0].axis = D.panels.all; PANELS[1].axis = D.panels.zoom; PANELS[2].axis = D.panels.r503;
  const fmt = (p, v) => p.unit === 'ms' ? ms(v) : Math.round(v) + ' /s';

  function draw(p) {
    const host = document.getElementById(p.host);
    host.textContent = '';
    const W = Math.max(280, host.clientWidth), H = 264;
    const narrow = W < 600;
    const m = { l: 48, r: narrow ? 14 : 168, t: 22, b: 36 };
    const pw = W - m.l - m.r, ph = H - m.t - m.b;
    const xs = t => m.l + t / D.tmax * pw;
    const ys = v => m.t + (1 - Math.min(v, p.axis.yMax) / p.axis.yMax) * ph;
    const svg = mk('svg', { width: W, height: H, viewBox: `0 0 ${W} ${H}`, role: 'img', 'aria-label': p.aria + '. Table view has every value.', tabindex: 0 }, host);

    // ramp-up band
    mk('rect', { x: xs(0), y: m.t, width: xs(D.ramp) - xs(0), height: ph, fill: 'var(--text-primary)', opacity: 0.045 }, svg);
    mk('text', { x: xs(0) + 6, y: m.t + 12, class: 'ax' }, svg).textContent = 'ramp-up';

    // grid and y axis
    for (const v of p.axis.ticks) {
      const y = ys(v);
      mk('line', { x1: m.l, x2: W - m.r, y1: y, y2: y, stroke: v === 0 ? 'var(--axis)' : 'var(--grid)', 'stroke-width': 1 }, svg);
      const tx = mk('text', { x: m.l - 8, y: y + 4, 'text-anchor': 'end', class: 'ax' }, svg);
      tx.textContent = v;
    }
    mk('text', { x: m.l - 8, y: 10, 'text-anchor': 'end', class: 'ax' }, svg).textContent = p.unit === 'ms' ? 'ms' : 'per s';
    for (let t = 0; t <= D.tmax; t += 30) {
      mk('text', { x: xs(t), y: H - m.b + 16, 'text-anchor': t === 0 ? 'start' : t === D.tmax ? 'end' : 'middle', class: 'ax' }, svg).textContent = t;
    }
    mk('text', { x: W - m.r, y: H - 4, 'text-anchor': 'end', class: 'ax' }, svg).textContent = 'seconds since the start';

    // SLO line
    if (p.slo) {
      const y = ys(D.slo);
      mk('line', { x1: m.l, x2: W - m.r, y1: y, y2: y, stroke: 'var(--text-secondary)', 'stroke-width': 1 }, svg);
      // at the left edge: there every run is still below the line, on any width
      mk('text', { x: xs(0) + 6, y: y - 5, class: 'lbl' }, svg).textContent = narrow ? 'SLO' : 'SLO ' + D.slo + ' ms';   // short on a phone: the full text would touch the rising line
    }

    // lines
    const runs = D.runs.filter(r => p.cfgs.includes(r.cfg) && !hidden.has(r.cfg));
    for (const r of runs) {
      let d = '', pen = false;
      r[p.metric].forEach((v, i) => {
        if (v == null) { pen = false; return; }
        d += (pen ? 'L' : 'M') + xs(D.t[i]).toFixed(1) + ' ' + ys(v).toFixed(1);
        pen = true;
      });
      mk('path', { d, fill: 'none', stroke: colour(r.cfg), 'stroke-width': 2, 'stroke-linejoin': 'round', 'stroke-linecap': 'round' }, svg);
    }

    // end dots and direct labels; a label that would touch another one is left to the legend
    const lastOf = r => { for (let i = r[p.metric].length - 1; i >= 0; i--) if (r[p.metric][i] != null) return [D.t[i], r[p.metric][i]]; };
    for (const r of runs) {
      const [t, v] = lastOf(r);
      mk('circle', { cx: xs(t), cy: ys(v), r: 4, fill: colour(r.cfg), stroke: 'var(--surface)', 'stroke-width': 2 }, svg);
    }
    if (!narrow) {
      const labels = [];
      for (const id of p.cfgs) {
        const rs = runs.filter(r => r.cfg === id);
        if (rs.length) labels.push({ id, y: ys(rs.reduce((a, r) => a + lastOf(r)[1], 0) / rs.length) });
      }
      for (const l of labels) {
        if (labels.some(o => o !== l && Math.abs(o.y - l.y) < 16)) continue;
        mk('text', { x: W - m.r + 12, y: l.y + 4, class: 'lbl' }, svg).textContent = cfgs[l.id].name;
      }
    }

    // hover and keyboard: a crosshair that snaps to the nearest second, one tooltip for every series
    const cross = mk('line', { y1: m.t, y2: m.t + ph, stroke: 'var(--text-secondary)', 'stroke-width': 1, visibility: 'hidden' }, svg);
    const dots = runs.map(r => mk('circle', { r: 4, fill: colour(r.cfg), stroke: 'var(--surface)', 'stroke-width': 2, visibility: 'hidden' }, svg));
    const tip = div('tip', host); tip.style.display = 'none';
    let idx = 0;

    function show(i) {
      idx = Math.max(0, Math.min(D.t.length - 1, i));
      const t = D.t[idx], x = xs(t);
      cross.setAttribute('x1', x); cross.setAttribute('x2', x); cross.setAttribute('visibility', 'visible');
      tip.textContent = '';
      div('th', tip, t + ' s' + (t <= D.ramp ? ' (ramp-up)' : ''));
      runs.forEach((r, k) => {
        const v = r[p.metric][idx];
        if (v == null) { dots[k].setAttribute('visibility', 'hidden'); return; }
        dots[k].setAttribute('cx', x); dots[k].setAttribute('cy', ys(v)); dots[k].setAttribute('visibility', 'visible');
        const row = div('tr', tip); keyEl(r.cfg, row);
        const b = document.createElement('b'); b.textContent = fmt(p, v); row.appendChild(b);
        const nm = document.createElement('i'); nm.textContent = cfgs[r.cfg].name + (r.tag ? ', ' + r.tag : ''); row.appendChild(nm);
      });
      tip.style.display = 'block';
      const tw = tip.offsetWidth;
      // flip to the left of the crosshair before the tooltip would cover the direct labels beside the plot
      tip.style.left = Math.max(0, x + 14 + tw > m.l + pw ? x - 14 - tw : x + 14) + 'px';
      tip.style.top = (m.t + 6) + 'px';
    }
    function hide() {
      cross.setAttribute('visibility', 'hidden'); dots.forEach(d => d.setAttribute('visibility', 'hidden'));
      tip.style.display = 'none';
    }
    const area = mk('rect', { x: m.l, y: m.t, width: pw, height: ph, fill: 'transparent', style: 'cursor:crosshair' }, svg);
    area.addEventListener('pointermove', e => {
      const box = svg.getBoundingClientRect();
      show(Math.round((e.clientX - box.left - m.l) / pw * D.tmax) - 1);
    });
    area.addEventListener('pointerleave', hide);
    svg.addEventListener('focus', () => show(idx));
    svg.addEventListener('blur', hide);
    svg.addEventListener('keydown', e => {
      const step = e.shiftKey ? 10 : 1;
      if (e.key === 'ArrowRight') { show(idx + step); e.preventDefault(); }
      else if (e.key === 'ArrowLeft') { show(idx - step); e.preventDefault(); }
      else if (e.key === 'Escape') hide();
    });
  }

  let queued = false;
  function drawAll() {
    if (queued) return;
    queued = true;
    requestAnimationFrame(() => { queued = false; if (!document.body.classList.contains('tv')) PANELS.forEach(draw); });
  }
  const ro = new ResizeObserver(drawAll);
  PANELS.forEach(p => ro.observe(document.getElementById(p.host)));

  // ---------- table view: every number behind the charts
  const tv = document.getElementById('tableview');
  function table(caption, head, rows) {
    const wrap = div('tablewrap', tv);
    const t = document.createElement('table'); wrap.appendChild(t);
    t.createCaption().textContent = caption;
    const tr = t.createTHead().insertRow();
    head.forEach(h => { const th = document.createElement('th'); th.scope = 'col'; th.textContent = h; tr.appendChild(th); });
    const tb = t.createTBody();
    rows.forEach(r => { const row = tb.insertRow(); r.forEach(c => { row.insertCell().textContent = c; }); });
  }
  const runName = r => cfgs[r.cfg].name + (r.tag ? ' ' + r.tag : '');
  table('Steady state, seconds ' + D.steady[0] + ' to ' + D.steady[1],
    ['Configuration', 'Mean p99 of lateness', 'Worst second', 'Seconds over ' + D.slo + ' ms', 'Requests 503'],
    D.tiles.flatMap(t => t.stats.map(s => [cfgs[t.cfg].name + (s.tag ? ' ' + s.tag : ''), s.mean.toFixed(1) + ' ms', Math.round(s.worst) + ' ms', s.over + ' of ' + s.n, Math.round(s.share503) + '%'])));
  table('Lateness p99, ms, mean over 10 second windows', ['Seconds'].concat(D.runs.map(runName)),
    D.bins.map(b => [b.lo + ' to ' + b.hi].concat(b.cells.map(c => c.late == null ? '-' : c.late.toFixed(1)))));
  table('Requests answered 503 per second, mean over 10 second windows', ['Seconds'].concat(D.runs.map(runName)),
    D.bins.map(b => [b.lo + ' to ' + b.hi].concat(b.cells.map(c => c.r503 == null ? '-' : Math.round(c.r503)))));

  const btn = document.getElementById('tv');
  btn.addEventListener('click', () => {
    const on = document.body.classList.toggle('tv');
    btn.setAttribute('aria-pressed', String(on));
    btn.textContent = on ? 'Show charts' : 'Table view';
    if (!on) drawAll();
  });
  drawAll();
})();
</script>
</body>
</html>
"""

if __name__ == "__main__":
    main()
