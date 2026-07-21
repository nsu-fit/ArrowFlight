#!/usr/bin/env python3
"""Analyze ArrowFlight timing logs and produce HTML waterfall report.

Usage:
  docker compose logs server-node-1 server-node-2 | python tools/analyze_logs.py > report.html
  python tools/analyze_logs.py --input server.log > report.html
  python tools/analyze_logs.py --input node1.log --input node2.log > report.html
"""

import argparse
import os
import re
import sys
from collections import defaultdict, OrderedDict
from html import escape
from typing import Optional

LOG_LINE_RE = re.compile(
    r'^(?:(\S+)\s+\|\s*)?'   # docker container prefix (optional)
    r'.*?'                     # skip timestamp, thread, level, MDC
    r'\b\w+\s*-\s+'           # logger name + " - "
    r'(.*)$'                   # message
)

KV_RE = re.compile(r'(\w[\w.]*)=(\S+)')

EVENT_COLORS = {
    'schema': '#4a90d9',
    'planning': '#5b6abf',
    'execution': '#9b59b6',
    'parseQuery': '#3f51b5',
    'files': '#00897b',
    'resolve': '#00897b',
    'filter': '#7cb342',
    'engine': '#e67e22',
    'duckdb': '#d32f2f',
    'acero': '#fbc02d',
    'parquet': '#43a047',
    'hazelcast': '#78909c',
    'cluster': '#78909c',
    'default': '#757575',
}

EVENT_ORDER = [
    'cluster', 'schema', 'planning', 'execution',
    'parseQuery', 'filter', 'files', 'resolve',
    'engine', 'duckdb', 'acero', 'parquet',
    'hazelcast',
]


def get_event_type(event: str) -> str:
    for prefix in EVENT_ORDER:
        if event.startswith(prefix):
            return prefix
    return 'default'


def get_event_color(event: str) -> str:
    return EVENT_COLORS.get(get_event_type(event), EVENT_COLORS['default'])


def parse_nanos(s: str) -> int:
    try:
        return int(s)
    except (ValueError, TypeError):
        return 0


def fmt_nanos(ns: int) -> str:
    if ns < 1_000:
        return f'{ns}ns'
    if ns < 1_000_000:
        return f'{ns / 1_000:.1f}µs'
    if ns < 1_000_000_000:
        return f'{ns / 1_000_000:.2f}ms'
    return f'{ns / 1_000_000_000:.3f}s'


def fmt_nanos_short(ns: int) -> str:
    if ns < 1_000:
        return f'{ns}ns'
    if ns < 1_000_000:
        return f'{ns / 1_000:.0f}µs'
    if ns < 1_000_000_000:
        return f'{ns / 1_000_000:.0f}ms'
    return f'{ns / 1_000_000_000:.1f}s'


def parse_log_line(line: str) -> Optional[dict]:
    m = LOG_LINE_RE.match(line)
    if not m:
        return None
    container = m.group(1)
    msg = m.group(2)
    kv = {}
    for k, v in KV_RE.findall(msg):
        kv[k] = v
    if 'timing' not in kv:
        return None
    node = kv.get('node') or container or 'unknown'
    return {
        'node': node,
        'container': container or '',
        'event': kv['timing'],
        'elapsed_ns': parse_nanos(kv.get('elapsedNs', '0')),
        'qid': kv.get('qid', ''),
        'extra': ' '.join(
            f'{k}={v}' for k, v in kv.items()
            if k not in ('timing', 'elapsedNs', 'elapsed', 'node', 'qid')
        ),
    }


def read_logs(input_files: list[str]) -> list[str]:
    lines: list[str] = []
    if input_files:
        for path in input_files:
            with open(path, 'r', errors='replace') as f:
                lines.extend(f.readlines())
    else:
        lines = sys.stdin.readlines()
    return lines


def build_report(timing_events: list[dict]) -> dict:
    by_node: dict[str, dict[str, list[dict]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for ev in timing_events:
        by_node[ev['node']][ev['qid']].append(ev)

    node_stats: dict[str, dict] = {}
    node_queries: dict[str, list[dict]] = {}

    for node, queries in by_node.items():
        qlist: list[dict] = []
        for qid, events in sorted(queries.items()):
            total_ns = 0
            for ev in events:
                if 'total' in ev['event'] or ev['event'].endswith('.total'):
                    total_ns = max(total_ns, ev['elapsed_ns'])
            if total_ns == 0 and events:
                total_ns = max(ev['elapsed_ns'] for ev in events)
            qlist.append({
                'qid': qid or '(no qid)',
                'events': events,
                'total_ns': total_ns,
                'count': len(events),
            })
        qlist.sort(key=lambda q: q['total_ns'], reverse=True)
        node_queries[node] = qlist

        times = sorted(q['total_ns'] for q in qlist)
        n = len(times)
        avg = sum(times) / n if n else 0
        p50 = percentile(times, 50)
        p95 = percentile(times, 95)
        p99 = percentile(times, 99)
        node_stats[node] = {
            'count': n,
            'avg_ns': avg,
            'p50_ns': p50,
            'p95_ns': p95,
            'p99_ns': p99,
        }

    return {
        'nodes': sorted(by_node.keys()),
        'node_stats': node_stats,
        'node_queries': node_queries,
        'total_events': len(timing_events),
    }


def percentile(sorted_times: list[int], p: int) -> int:
    if not sorted_times:
        return 0
    idx = max(0, min(len(sorted_times) - 1, len(sorted_times) * p // 100))
    return sorted_times[idx]


def render_html(report: dict, title: str = 'ArrowFlight Timing Report') -> str:
    nodes = report['nodes']
    node_stats = report['node_stats']
    node_queries = report['node_queries']

    def stat_row(stats: dict) -> str:
        return (
            f'<tr>'
            f'<td>{stats["count"]}</td>'
            f'<td>{fmt_nanos(int(stats["avg_ns"]))}</td>'
            f'<td>{fmt_nanos(stats["p50_ns"])}</td>'
            f'<td>{fmt_nanos(stats["p95_ns"])}</td>'
            f'<td>{fmt_nanos(stats["p99_ns"])}</td>'
            f'</tr>'
        )

    def render_query(q: dict, max_total: int) -> str:
        events = q['events']
        total_ns = q['total_ns']
        if total_ns == 0 or max_total == 0:
            scale = 1
        else:
            scale = 600 / max_total

        rows_html = ''
        for ev in events:
            ev_type = get_event_type(ev['event'])
            color = EVENT_COLORS[ev_type]
            bar_width = max(4, ev['elapsed_ns'] * scale)
            if bar_width > 600:
                bar_width = 600
            rows_html += (
                f'<div class="event-row">'
                f'<span class="event-label">{escape(ev["event"])}</span>'
                f'<div class="bar-wrapper">'
                f'<div class="bar" style="width:{bar_width:.0f}px;background:{color}"></div>'
                f'</div>'
                f'<span class="event-time">{fmt_nanos_short(ev["elapsed_ns"])}</span>'
                f'<span class="event-extra">{escape(ev["extra"])}</span>'
                f'</div>\n'
            )

        return (
            f'<div class="query-block">'
            f'<div class="query-header">'
            f'<span class="query-qid">{escape(q["qid"])}</span>'
            f'<span class="query-total">{fmt_nanos(total_ns)}</span>'
            f'<span class="query-count">{q["count"]} events</span>'
            f'</div>'
            f'<div class="events-container">{rows_html}</div>'
            f'</div>'
        )

    max_totals: dict[str, int] = {}
    for node in nodes:
        times = [q['total_ns'] for q in node_queries.get(node, [])]
        max_totals[node] = max(times) if times else 1

    tab_links = ''
    tab_contents = ''
    for i, node in enumerate(nodes):
        active = ' active' if i == 0 else ''
        stats = node_stats[node]
        queries_html = ''
        max_total = max_totals[node]
        for q in node_queries.get(node, []):
            queries_html += render_query(q, max_total)

        if not queries_html:
            queries_html = '<p class="no-data">No timing events for this node.</p>'

        tab_links += (
            f'<button class="tab-link{active}" onclick="openTab(event,{i!r})">'
            f'{escape(node)}'
            f'<span class="badge">{stats["count"]}</span>'
            f'</button>\n'
        )
        tab_contents += (
            f'<div id="tab-{i}" class="tab-content{active}">'
            f'<table class="stats-table">'
            f'<thead><tr><th>Queries</th><th>Avg</th><th>P50</th><th>P95</th><th>P99</th></tr></thead>'
            f'<tbody>{stat_row(stats)}</tbody>'
            f'</table>'
            f'<div class="queries-list">{queries_html}</div>'
            f'</div>\n'
        )

    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{escape(title)}</title>
<style>
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Oxygen,Ubuntu,sans-serif;background:#f5f5f5;color:#333;padding:20px;line-height:1.4}}
h1{{font-size:1.5rem;margin-bottom:4px;color:#111}}
.subtitle{{font-size:.85rem;color:#666;margin-bottom:20px}}
.tab-bar{{display:flex;gap:4px;flex-wrap:wrap;margin-bottom:0}}
.tab-link{{padding:8px 16px;border:none;background:#e0e0e0;cursor:pointer;border-radius:6px 6px 0 0;font-size:.85rem;transition:background .15s}}
.tab-link.active{{background:#fff;font-weight:600;box-shadow:0 -1px 3px rgba(0,0,0,.08)}}
.tab-link:hover:not(.active){{background:#d0d0d0}}
.badge{{display:inline-block;background:#1565c0;color:#fff;border-radius:10px;padding:0 7px;font-size:.7rem;margin-left:6px;vertical-align:middle;line-height:1.5}}
.tab-content{{display:none;background:#fff;border-radius:0 6px 6px 6px;padding:20px;box-shadow:0 1px 4px rgba(0,0,0,.08)}}
.tab-content.active{{display:block}}
.stats-table{{width:100%;border-collapse:collapse;margin-bottom:20px;font-size:.85rem}}
.stats-table th,.stats-table td{{text-align:left;padding:6px 12px;border-bottom:1px solid #eee}}
.stats-table th{{color:#666;font-weight:600;text-transform:uppercase;font-size:.75rem}}
.stats-table td:not(:first-child){{font-variant-numeric:tabular-nums;text-align:right}}
.query-block{{margin-bottom:24px;border:1px solid #e8e8e8;border-radius:6px;overflow:hidden}}
.query-header{{display:flex;align-items:center;gap:12px;padding:8px 12px;background:#fafafa;border-bottom:1px solid #eee;font-size:.85rem}}
.query-qid{{font-weight:600;color:#1565c0;font-family:'SF Mono','Fira Code','Consolas',monospace}}
.query-total{{margin-left:auto;font-weight:600;font-variant-numeric:tabular-nums}}
.query-count{{color:#888;font-size:.75rem}}
.events-container{{padding:6px 12px 8px}}
.event-row{{display:flex;align-items:center;gap:8px;padding:3px 0;font-size:.78rem}}
.event-label{{width:200px;flex-shrink:0;text-align:right;color:#555;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.bar-wrapper{{flex:1;min-width:40px}}
.bar{{height:14px;border-radius:3px;min-width:4px;transition:width .1s}}
.event-time{{width:60px;flex-shrink:0;font-variant-numeric:tabular-nums;color:#333;font-weight:500}}
.event-extra{{color:#999;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:300px}}
.legend{{display:flex;gap:16px;flex-wrap:wrap;margin-bottom:20px;font-size:.78rem;color:#555}}
.legend-item{{display:flex;align-items:center;gap:4px}}
.legend-swatch{{width:12px;height:12px;border-radius:2px;display:inline-block}}
.no-data{{color:#999;font-style:italic;padding:20px;text-align:center}}
footer{{text-align:center;color:#aaa;font-size:.75rem;margin-top:30px}}
</style>
</head>
<body>
<h1>{escape(title)}</h1>
<p class="subtitle">{report["total_events"]} timing events across {len(nodes)} nodes</p>
<div class="legend">
{[f'<span class="legend-item"><span class="legend-swatch" style="background:{c}"></span>{k}</span>' for k,c in EVENT_COLORS.items()]}
</div>
<div class="tab-bar">{tab_links}</div>
{tab_contents}
<script>
function openTab(e,i){{document.querySelectorAll('.tab-link').forEach(t=>t.classList.remove('active'));document.querySelectorAll('.tab-content').forEach(t=>t.classList.remove('active'));e.currentTarget.classList.add('active');document.getElementById('tab-'+i).classList.add('active')}}
</script>
<footer>Generated by analyze_logs.py — ArrowFlight Timing Analyzer</footer>
</body>
</html>'''
    return html


def main():
    parser = argparse.ArgumentParser(
        description='Analyze ArrowFlight timing logs and produce HTML report.'
    )
    parser.add_argument(
        '--input', '-i', action='append', dest='inputs',
        help='Input log file (repeatable). Reads from stdin if omitted.'
    )
    parser.add_argument(
        '--title', default='ArrowFlight Timing Report',
        help='Report title (default: ArrowFlight Timing Report)'
    )
    args = parser.parse_args()

    lines = read_logs(args.inputs)
    timing_events = [ev for ev in (parse_log_line(l) for l in lines) if ev]

    if not timing_events:
        print('No timing events found in input.', file=sys.stderr)
        sys.exit(1)

    report = build_report(timing_events)
    html = render_html(report, title=args.title)
    sys.stdout.write(html)


if __name__ == '__main__':
    main()
