#!/usr/bin/env python3
import argparse
import csv
import html
import json
import shutil
from datetime import datetime
from pathlib import Path


THROUGHPUT = "Throughput (requests/second)"


def parse_args():
    script_dir = Path(__file__).resolve().parent
    repo_root = script_dir.parent.parent
    parser = argparse.ArgumentParser(description="Build a GitHub Pages dashboard for BenchBase results.")
    parser.add_argument("--results", type=Path, default=script_dir / "results")
    parser.add_argument("--out", type=Path, default=repo_root / "pages")
    return parser.parse_args()


def read_json(path):
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as file:
        return json.load(file)


def read_csv(path):
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))


def number(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def fmt(value):
    value = number(value)
    if abs(value) >= 100:
        return f"{value:.1f}"
    if abs(value) >= 10:
        return f"{value:.2f}"
    return f"{value:.3f}"


def latency_ms(summary, label):
    return number(summary.get("Latency Distribution", {}).get(f"{label} Latency (microseconds)")) / 1000


def first_summary(run_dir):
    summaries = sorted(run_dir.glob("*.summary.json"), key=lambda path: path.stat().st_mtime, reverse=True)
    return summaries[0] if summaries else None


def report_link_for(run_dir, copied_root):
    reports = sorted(run_dir.glob("*.report.html"), key=lambda path: path.stat().st_mtime, reverse=True)
    if not reports:
        return ""
    return str(copied_root / run_dir.name / reports[0].name).replace("\\", "/")


def copied_link(results_root, path):
    rel = path.relative_to(results_root)
    return str(Path("benchmarks") / rel).replace("\\", "/")


def query_label_from_name(name):
    marker = "-q"
    if marker not in name:
        return "all"
    tail = name.split(marker, 1)[1]
    parts = tail.rsplit("-", 2)
    return f"q{parts[0]}" if parts else "all"


def metadata_for(run_dir):
    for path in [run_dir / "benchmark-metadata.json", run_dir.parent / "benchmark-metadata.json"]:
        if path.exists():
            return read_json(path)
    return {}


def run_timestamp(path):
    try:
        return datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds")
    except OSError:
        return ""


def load_single_run(results_root, run_dir):
    summary_path = first_summary(run_dir)
    if not summary_path:
        return None
    summary = read_json(summary_path)
    metadata = metadata_for(run_dir)
    base = summary_path.name.removesuffix(".summary.json")
    report = run_dir / f"{base}.report.html"
    rows = read_csv(run_dir / f"{base}.results.csv")
    last = rows[-1] if rows else {}
    return {
        "kind": "run",
        "id": str(run_dir.relative_to(results_root)).replace("\\", "/"),
        "title": run_dir.name,
        "benchmark": summary.get("Benchmark Type", metadata.get("dataset", "n/a")),
        "path": run_dir.name,
        "query": query_label_from_name(run_dir.name),
        "scale": metadata.get("scale_factor", summary.get("scalefactor", "n/a")),
        "timestamp": run_timestamp(run_dir),
        "throughput": number(summary.get(THROUGHPUT)),
        "lastThroughput": number(last.get(THROUGHPUT)),
        "avgMs": latency_ms(summary, "Average"),
        "p95Ms": latency_ms(summary, "95th Percentile"),
        "measuredRequests": summary.get("Measured Requests", 0),
        "report": copied_link(results_root, report) if report.exists() else "",
        "files": copied_link(results_root, run_dir),
    }


def load_compare_run(results_root, run_dir):
    report = run_dir / "compare.report.html"
    if not report.exists():
        return None

    flight = load_single_run(results_root, run_dir / "flight")
    direct = load_single_run(results_root, run_dir / "direct")
    metadata = metadata_for(run_dir)
    benchmark = metadata.get("dataset", "tpch")
    return {
        "kind": "compare",
        "id": str(run_dir.relative_to(results_root)).replace("\\", "/"),
        "title": run_dir.name,
        "benchmark": benchmark,
        "path": "flight vs direct",
        "query": query_label_from_name(run_dir.name),
        "scale": metadata.get("scale_factor", "n/a"),
        "timestamp": run_timestamp(run_dir),
        "report": copied_link(results_root, report),
        "files": copied_link(results_root, run_dir),
        "flight": flight,
        "direct": direct,
        "flightNodes": metadata.get("cluster_nodes", "n/a"),
        "flightHosts": metadata.get("flight_hosts", "n/a"),
    }


def collect_runs(results_root):
    runs = []
    if not results_root.exists():
        return runs

    compare_dirs = set()
    for report in results_root.rglob("compare.report.html"):
        compare_dirs.add(report.parent)
        run = load_compare_run(results_root, report.parent)
        if run:
            runs.append(run)

    for summary in results_root.rglob("*.summary.json"):
        run_dir = summary.parent
        if run_dir.name in {"flight", "direct"} and run_dir.parent in compare_dirs:
            continue
        if run_dir in compare_dirs:
            continue
        run = load_single_run(results_root, run_dir)
        if run:
            runs.append(run)

    runs.sort(key=lambda item: item.get("timestamp", ""), reverse=True)
    return runs


def copy_results(results_root, out_dir):
    target = out_dir / "benchmarks"
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True, exist_ok=True)

    if not results_root.exists():
        return

    for path in results_root.iterdir():
        destination = target / path.name
        if path.is_dir():
            shutil.copytree(path, destination)
        elif path.is_file():
            shutil.copy2(path, destination)


def metric_cell(run, side, field, suffix=""):
    nested = run.get(side) or {}
    value = nested.get(field)
    if value in (None, ""):
        return "-"
    return f"{fmt(value)}{suffix}"


def build_index(runs):
    payload = json.dumps(runs, ensure_ascii=False)
    rows = []
    for run in runs:
        if run["kind"] == "compare":
            flight = run.get("flight") or {}
            direct = run.get("direct") or {}
            links = [
                f'<a href="{html.escape(run["report"])}">compare</a>',
            ]
            if flight.get("report"):
                links.append(f'<a href="{html.escape(flight["report"])}">flight</a>')
            if direct.get("report"):
                links.append(f'<a href="{html.escape(direct["report"])}">direct</a>')
            links.append(f'<a href="{html.escape(run["files"])}">files</a>')
            rows.append(
                f"""
      <tr data-benchmark="{html.escape(str(run["benchmark"]))}" data-kind="compare" data-query="{html.escape(str(run["query"]))}">
        <td><strong>{html.escape(run["title"])}</strong><span>compare</span></td>
        <td>{html.escape(str(run["benchmark"]))}</td>
        <td>{html.escape(str(run["query"]))}</td>
        <td>{html.escape(str(run["scale"]))}</td>
        <td>{metric_cell(run, "flight", "throughput", " req/s")}</td>
        <td>{metric_cell(run, "direct", "throughput", " req/s")}</td>
        <td>{metric_cell(run, "flight", "avgMs", " ms")}</td>
        <td>{metric_cell(run, "direct", "avgMs", " ms")}</td>
        <td>{html.escape(str(run.get("flightNodes", "-")))}</td>
        <td>{' '.join(links)}</td>
      </tr>
"""
            )
        else:
            links = [f'<a href="{html.escape(run["report"])}">report</a>' if run.get("report") else ""]
            links.append(f'<a href="{html.escape(run["files"])}">files</a>')
            rows.append(
                f"""
      <tr data-benchmark="{html.escape(str(run["benchmark"]))}" data-kind="run" data-query="{html.escape(str(run["query"]))}">
        <td><strong>{html.escape(run["title"])}</strong><span>{html.escape(run["path"])}</span></td>
        <td>{html.escape(str(run["benchmark"]))}</td>
        <td>{html.escape(str(run["query"]))}</td>
        <td>{html.escape(str(run["scale"]))}</td>
        <td>{fmt(run["throughput"])} req/s</td>
        <td>-</td>
        <td>{fmt(run["avgMs"])} ms</td>
        <td>-</td>
        <td>-</td>
        <td>{' '.join(link for link in links if link)}</td>
      </tr>
"""
            )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Arrow Flight Benchmarks</title>
  <style>
    body {{ margin: 0; font: 14px/1.45 system-ui, -apple-system, Segoe UI, sans-serif; background: #f7f8fb; color: #111827; }}
    main {{ max-width: 1220px; margin: 0 auto; padding: 28px; }}
    h1 {{ margin: 0 0 6px; font-size: 30px; }}
    p {{ color: #5b6472; }}
    .toolbar {{ display: flex; flex-wrap: wrap; gap: 10px; margin: 22px 0; }}
    select, input {{ border: 1px solid #d1d5db; border-radius: 7px; padding: 9px 10px; background: #fff; min-width: 150px; }}
    input {{ flex: 1; min-width: 230px; }}
    .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(210px, 1fr)); gap: 12px; margin: 18px 0; }}
    .card {{ background: #fff; border: 1px solid #e5e7eb; border-radius: 8px; padding: 16px; }}
    .label {{ color: #6b7280; font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }}
    .value {{ font-size: 25px; font-weight: 750; margin-top: 3px; }}
    table {{ width: 100%; border-collapse: collapse; background: #fff; border: 1px solid #e5e7eb; border-radius: 8px; overflow: hidden; }}
    th, td {{ padding: 10px 12px; border-bottom: 1px solid #edf0f3; text-align: right; vertical-align: top; }}
    th:first-child, td:first-child {{ text-align: left; }}
    th {{ color: #5b6472; font-size: 12px; text-transform: uppercase; letter-spacing: .04em; background: #fbfcfe; }}
    td strong {{ display: block; }}
    td span {{ display: block; color: #6b7280; font-size: 12px; margin-top: 2px; }}
    a {{ color: #2563eb; margin-right: 10px; white-space: nowrap; }}
    .empty {{ display: none; background: #fff; border: 1px solid #e5e7eb; border-radius: 8px; padding: 18px; }}
  </style>
</head>
<body>
<main>
  <h1>Arrow Flight Benchmarks</h1>
  <p>Single entry point for BenchBase Spark reports. Choose a benchmark, query, or path and open compare/direct/flight details.</p>

  <div class="cards">
    <div class="card"><div class="label">Reports</div><div class="value" id="visible-count">0</div></div>
    <div class="card"><div class="label">Compare Runs</div><div class="value" id="compare-count">0</div></div>
    <div class="card"><div class="label">Latest Run</div><div class="value" id="latest-run">-</div></div>
  </div>

  <div class="toolbar">
    <select id="benchmark-filter">
      <option value="">All benchmarks</option>
    </select>
    <select id="kind-filter">
      <option value="">All paths</option>
      <option value="compare">Flight vs Direct</option>
      <option value="run">Single run</option>
    </select>
    <select id="query-filter">
      <option value="">All queries</option>
    </select>
    <input id="search" type="search" placeholder="Search run name">
  </div>

  <div class="empty" id="empty">No reports match the selected filters.</div>
  <table>
    <thead>
      <tr>
        <th>Run</th>
        <th>Benchmark</th>
        <th>Query</th>
        <th>SF</th>
        <th>Flight thr.</th>
        <th>Direct thr.</th>
        <th>Flight avg</th>
        <th>Direct avg</th>
        <th>Nodes</th>
        <th>Open</th>
      </tr>
    </thead>
    <tbody id="runs">
      {''.join(rows)}
    </tbody>
  </table>
</main>
<script type="application/json" id="runs-json">{html.escape(payload)}</script>
<script>
  const runs = JSON.parse(document.getElementById('runs-json').textContent);
  const rows = [...document.querySelectorAll('#runs tr')];
  const benchmarkFilter = document.getElementById('benchmark-filter');
  const kindFilter = document.getElementById('kind-filter');
  const queryFilter = document.getElementById('query-filter');
  const search = document.getElementById('search');

  function addOptions(select, values) {{
    values.filter(Boolean).sort().forEach((value) => {{
      const option = document.createElement('option');
      option.value = value;
      option.textContent = value;
      select.appendChild(option);
    }});
  }}

  addOptions(benchmarkFilter, [...new Set(runs.map((run) => String(run.benchmark)))]);
  addOptions(queryFilter, [...new Set(runs.map((run) => String(run.query)))]);
  document.getElementById('compare-count').textContent = runs.filter((run) => run.kind === 'compare').length;
  document.getElementById('latest-run').textContent = runs[0]?.title || '-';

  function applyFilters() {{
    const benchmark = benchmarkFilter.value;
    const kind = kindFilter.value;
    const query = queryFilter.value;
    const term = search.value.trim().toLowerCase();
    let visible = 0;
    rows.forEach((row) => {{
      const text = row.textContent.toLowerCase();
      const show = (!benchmark || row.dataset.benchmark === benchmark)
        && (!kind || row.dataset.kind === kind)
        && (!query || row.dataset.query === query)
        && (!term || text.includes(term));
      row.style.display = show ? '' : 'none';
      if (show) visible += 1;
    }});
    document.getElementById('visible-count').textContent = visible;
    document.getElementById('empty').style.display = visible ? 'none' : 'block';
  }}

  [benchmarkFilter, kindFilter, queryFilter, search].forEach((element) => {{
    element.addEventListener('input', applyFilters);
  }});
  applyFilters();
</script>
</body>
</html>
"""


def main():
    args = parse_args()
    results_root = args.results.resolve()
    out_dir = args.out.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    copy_results(results_root, out_dir)

    runs = collect_runs(results_root)
    (out_dir / "benchmarks.json").write_text(json.dumps(runs, indent=2, ensure_ascii=False), encoding="utf-8")
    (out_dir / "index.html").write_text(build_index(runs), encoding="utf-8")
    (out_dir / ".nojekyll").write_text("", encoding="utf-8")
    print(out_dir / "index.html")


if __name__ == "__main__":
    main()
