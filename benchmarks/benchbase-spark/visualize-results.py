#!/usr/bin/env python3
import argparse
import csv
import html
import json
import xml.etree.ElementTree as ET
from pathlib import Path


LAT_AVG = "Average Latency (millisecond)"
LAT_P95 = "95th Percentile Latency (millisecond)"
LAT_MAX = "Maximum Latency (millisecond)"
THROUGHPUT = "Throughput (requests/second)"
TIME = "Time (seconds)"


def parse_args():
    script_dir = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description="Build HTML report for BenchBase CSV results.")
    parser.add_argument("--results", type=Path, default=script_dir / "results")
    parser.add_argument("--base", help="Result prefix, for example tpch_2026-07-09_07-49-48.")
    parser.add_argument("--out", type=Path)
    return parser.parse_args()


def latest_summary(results_dir):
    summaries = sorted(results_dir.rglob("*.summary.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not summaries:
        raise SystemExit(f"No *.summary.json files in {results_dir}")
    return summaries[0]


def find_summary(results_dir, base):
    direct = results_dir / f"{base}.summary.json"
    if direct.exists():
        return direct

    matches = sorted(results_dir.rglob(f"{base}.summary.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not matches:
        raise SystemExit(f"No {base}.summary.json under {results_dir}")
    return matches[0]


def read_csv(path):
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))


def read_json(path):
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as file:
        return json.load(file)


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


def metric_from_last(rows, column):
    if not rows:
        return "n/a"
    return fmt(rows[-1].get(column))


def summary_latency_ms(summary, name):
    latency = summary.get("Latency Distribution", {})
    return fmt(number(latency.get(f"{name} Latency (microseconds)")) / 1000)


def summary_metric(summary, name):
    if not summary:
        return "n/a"
    return fmt(summary.get(name))


def read_config(path):
    if not path.exists():
        return {}
    try:
        root = ET.parse(path).getroot()
    except ET.ParseError:
        return {}

    def text(expr, default="n/a"):
        value = root.findtext(expr)
        return value.strip() if value and value.strip() else default

    weights = text("./works/work/weights", "")
    active = []
    for idx, raw in enumerate(weights.split(","), start=1):
        try:
            if float(raw) > 0:
                active.append(f"Q{idx}")
        except ValueError:
            pass

    return {
        "scale": text("./scalefactor"),
        "terminals": text("./terminals"),
        "time": text("./works/work/time", "serial"),
        "warmup": text("./works/work/warmup", "0"),
        "rate": text("./works/work/rate"),
        "queries": ", ".join(active) if active else "all configured",
    }


def run_context(summary, config, rows):
    elapsed = number(summary.get("Elapsed Time (nanoseconds)")) / 1_000_000_000
    items = [
        ("Benchmark", summary.get("Benchmark Type", "n/a")),
        ("Scale factor", config.get("scale", summary.get("scalefactor", "n/a"))),
        ("Queries", config.get("queries", "n/a")),
        ("Workers", config.get("terminals", summary.get("terminals", "n/a"))),
        ("Duration", f"{fmt(elapsed)} s" if elapsed else config.get("time", "n/a")),
        ("Measured requests", str(summary.get("Measured Requests", "n/a"))),
        ("Last window", f"{rows[-1].get(TIME, 'n/a')} s" if rows else "n/a"),
    ]
    body = "".join(
        f"<tr><th>{html.escape(label)}</th><td>{html.escape(str(value))}</td></tr>"
        for label, value in items
    )
    return f"""
<section>
  <h2>Run Context</h2>
  <table class="kv"><tbody>{body}</tbody></table>
</section>
"""


def svg_line_chart(rows, columns, title, unit):
    width = 920
    height = 320
    pad_left = 58
    pad_right = 24
    pad_top = 44
    pad_bottom = 46
    plot_w = width - pad_left - pad_right
    plot_h = height - pad_top - pad_bottom
    colors = ["#2563eb", "#dc2626", "#16a34a"]

    points_by_column = {}
    values = []
    for column in columns:
        points = []
        for index, row in enumerate(rows):
            x_value = number(row.get(TIME), index)
            y_value = number(row.get(column))
            points.append((x_value, y_value))
            values.append(y_value)
        points_by_column[column] = points

    if not rows or not values:
        return f"<section><h2>{html.escape(title)}</h2><p>No data.</p></section>"

    min_x = min(point[0] for points in points_by_column.values() for point in points)
    max_x = max(point[0] for points in points_by_column.values() for point in points)
    min_y = 0
    max_y = max(values) or 1

    def sx(value):
        if max_x == min_x:
            return pad_left + plot_w / 2
        return pad_left + ((value - min_x) / (max_x - min_x)) * plot_w

    def sy(value):
        return pad_top + plot_h - ((value - min_y) / (max_y - min_y)) * plot_h

    lines = []
    legends = []
    for idx, column in enumerate(columns):
        color = colors[idx % len(colors)]
        points = " ".join(f"{sx(x):.1f},{sy(y):.1f}" for x, y in points_by_column[column])
        lines.append(f'<polyline points="{points}" fill="none" stroke="{color}" stroke-width="3"/>')
        legends.append(f'<span><i style="background:{color}"></i>{html.escape(column)} ({unit})</span>')

    y_labels = []
    for step in range(5):
        value = min_y + (max_y - min_y) * step / 4
        y = sy(value)
        y_labels.append(
            f'<line x1="{pad_left}" y1="{y:.1f}" x2="{width-pad_right}" y2="{y:.1f}" stroke="#e5e7eb"/>'
            f'<text x="{pad_left-10}" y="{y+4:.1f}" text-anchor="end">{fmt(value)}</text>'
        )

    x_labels = []
    for step in range(4):
        value = min_x + (max_x - min_x) * step / 3
        x = sx(value)
        x_labels.append(
            f'<line x1="{x:.1f}" y1="{pad_top}" x2="{x:.1f}" y2="{height-pad_bottom}" stroke="#f3f4f6"/>'
            f'<text x="{x:.1f}" y="{height-pad_bottom+20}" text-anchor="middle">{fmt(value)}</text>'
        )

    return f"""
<section>
  <h2>{html.escape(title)}</h2>
  <div class="legend">{''.join(legends)}</div>
  <svg viewBox="0 0 {width} {height}" role="img">
    <rect x="0" y="0" width="{width}" height="{height}" fill="#fff"/>
    {''.join(y_labels)}
    {''.join(x_labels)}
    <line x1="{pad_left}" y1="{height-pad_bottom}" x2="{width-pad_right}" y2="{height-pad_bottom}" stroke="#9ca3af"/>
    <line x1="{pad_left}" y1="{pad_top}" x2="{pad_left}" y2="{height-pad_bottom}" stroke="#9ca3af"/>
    {''.join(lines)}
    <text x="{width/2:.1f}" y="{height-10}" text-anchor="middle">time, seconds</text>
  </svg>
</section>
"""


def query_number(path):
    suffix = path.name.split(".results.Q", 1)[-1].split(".csv", 1)[0]
    try:
        return int(suffix)
    except ValueError:
        return 999


def per_query_rows(results_dir, base):
    rows = []
    for path in sorted(results_dir.glob(f"{base}.results.Q*.csv"), key=query_number):
        data = read_csv(path)
        if not data:
            continue
        last = data[-1]
        rows.append(
            {
                "query": f"Q{query_number(path)}",
                "throughput": last.get(THROUGHPUT, "0"),
                "avg": last.get(LAT_AVG, "0"),
                "p95": last.get(LAT_P95, "0"),
                "max": last.get(LAT_MAX, "0"),
            }
        )
    return rows


def query_table(rows):
    if not rows:
        return "<section><h2>Per Query</h2><p>No per-query CSV files.</p></section>"

    body = []
    for row in rows:
        active = number(row["throughput"]) > 0 or number(row["avg"]) > 0
        cls = "active" if active else "muted"
        body.append(
            "<tr class=\"%s\"><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"
            % (
                cls,
                html.escape(row["query"]),
                fmt(row["throughput"]),
                fmt(row["avg"]),
                fmt(row["p95"]),
                fmt(row["max"]),
            )
        )

    return f"""
<section>
  <h2>Per Query</h2>
  <table>
    <thead>
      <tr><th>Query</th><th>Throughput req/s</th><th>Avg ms</th><th>P95 ms</th><th>Max ms</th></tr>
    </thead>
    <tbody>{''.join(body)}</tbody>
  </table>
</section>
"""


def summary_block(summary):
    if not summary:
        return ""
    pretty = html.escape(json.dumps(summary, indent=2, ensure_ascii=False))
    return f"""
<section>
  <h2>Summary JSON</h2>
  <pre>{pretty}</pre>
</section>
"""


def build_report(results_dir, base, output):
    rows = read_csv(results_dir / f"{base}.results.csv")
    summary = read_json(results_dir / f"{base}.summary.json")
    config = read_config(results_dir / f"{base}.config.xml")
    query_rows = per_query_rows(results_dir, base)

    html_text = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>BenchBase Spark Report - {html.escape(base)}</title>
  <style>
    body {{ margin: 0; font: 14px/1.45 system-ui, -apple-system, Segoe UI, sans-serif; background: #f8fafc; color: #111827; }}
    main {{ max-width: 1100px; margin: 0 auto; padding: 28px; }}
    h1 {{ margin: 0 0 8px; font-size: 28px; }}
    h2 {{ margin: 0 0 14px; font-size: 19px; }}
    section {{ background: #fff; border: 1px solid #e5e7eb; border-radius: 8px; padding: 18px; margin: 16px 0; }}
    .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin-top: 18px; }}
    .card {{ background: #fff; border: 1px solid #e5e7eb; border-radius: 8px; padding: 14px; }}
    .label {{ color: #6b7280; font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }}
    .value {{ font-size: 25px; font-weight: 700; margin-top: 4px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border-bottom: 1px solid #e5e7eb; padding: 8px 10px; text-align: right; }}
    th:first-child, td:first-child {{ text-align: left; }}
    .kv th {{ color: #6b7280; width: 190px; font-weight: 600; }}
    .kv td, .kv th {{ text-align: left; }}
    .subtle {{ color: #6b7280; margin: 4px 0 0; }}
    tr.muted {{ color: #9ca3af; }}
    tr.active {{ background: #eff6ff; }}
    pre {{ overflow: auto; background: #0f172a; color: #e5e7eb; padding: 14px; border-radius: 8px; }}
    svg {{ width: 100%; height: auto; }}
    svg text {{ fill: #4b5563; font-size: 12px; }}
    .legend {{ display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 8px; color: #4b5563; }}
    .legend i {{ display: inline-block; width: 11px; height: 11px; border-radius: 2px; margin-right: 6px; vertical-align: -1px; }}
  </style>
</head>
<body>
<main>
  <h1>BenchBase Spark Report</h1>
  <p>{html.escape(base)}</p>
  <div class="cards">
    <div class="card"><div class="label">Overall Throughput</div><div class="value">{summary_metric(summary, THROUGHPUT)} req/s</div><p class="subtle">last window: {metric_from_last(rows, THROUGHPUT)} req/s</p></div>
    <div class="card"><div class="label">Overall Avg Latency</div><div class="value">{summary_latency_ms(summary, "Average")} ms</div><p class="subtle">last window: {metric_from_last(rows, LAT_AVG)} ms</p></div>
    <div class="card"><div class="label">Overall P95 Latency</div><div class="value">{summary_latency_ms(summary, "95th Percentile")} ms</div><p class="subtle">last window: {metric_from_last(rows, LAT_P95)} ms</p></div>
    <div class="card"><div class="label">Measured Requests</div><div class="value">{html.escape(str(summary.get("Measured Requests", "n/a")))}</div><p class="subtle">completed SQL transactions</p></div>
  </div>
  {run_context(summary, config, rows)}
  {svg_line_chart(rows, [LAT_AVG, LAT_P95, LAT_MAX], "Latency", "ms")}
  {svg_line_chart(rows, [THROUGHPUT], "Throughput", "req/s")}
  {query_table(query_rows)}
  {summary_block(summary)}
</main>
</body>
</html>
"""
    output.write_text(html_text, encoding="utf-8")


def main():
    args = parse_args()
    results_dir = args.results.resolve()
    summary_path = find_summary(results_dir, args.base) if args.base else latest_summary(results_dir)
    run_dir = summary_path.parent
    base = summary_path.name.removesuffix(".summary.json")
    output = args.out or run_dir / f"{base}.report.html"
    build_report(run_dir, base, output)
    print(output)


if __name__ == "__main__":
    main()
