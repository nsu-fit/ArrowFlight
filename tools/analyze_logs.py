#!/usr/bin/env python3
"""
Parse ArrowFlight timing + INFO logs into a sorted flame-chart view or HTML report.
Robustly handles Docker Compose prefixes and variable field ordering.

Usage:
  python tools/analyze_logs.py < logfile.log
  python tools/analyze_logs.py /path/to/arrowflight.log
  python tools/analyze_logs.py --html /path/to/arrowflight.log > report.html
"""

import sys
import re
import argparse
from html import escape
from collections import OrderedDict

# ── colour palette (256-colour friendly for terminal) ─────────────────
PALETTE = [
    '\033[38;5;39m', '\033[38;5;46m', '\033[38;5;226m', '\033[38;5;201m',
    '\033[38;5;51m', '\033[38;5;214m', '\033[38;5;198m', '\033[38;5;118m',
    '\033[38;5;99m', '\033[38;5;82m',
]
RESET = '\033[0m'
DIM = '\033[2m'
BOLD = '\033[1m'

# ── helpers ───────────────────────────────────────────────────────────

def colour_for(s):
    return PALETTE[abs(hash(s)) % len(PALETTE)]

def color_for_css(s):
    hues = ['#3b82f6', '#22c55e', '#eab308', '#ec4899', '#06b6d4', '#f97316', '#ef4444', '#8b5cf6', '#14b8a6', '#64748b']
    return hues[abs(hash(s)) % len(hues)]

def fmt_us(us):
    if us < 1000: return f'{us:>7} µs'
    elif us < 1_000_000: return f'{us/1000:>7.1f} ms'
    else: return f'{us/1_000_000:>7.2f} s'

def fmt_bar(us, max_us, width=30):
    if max_us == 0: return ' ' * width
    filled = max(1, int(us / max_us * width))
    bar = '█' * filled + '░' * (width - filled)
    return f'\033[38;5;240m{bar[:filled]}\033[38;5;245m{bar[filled:]}{RESET}'

ANSI_RE = re.compile(r'\033\[[0-9;]*m')
def visible_len(s): return len(ANSI_RE.sub('', s))

# ── Robust Parsers ────────────────────────────────────────────────────

def parse_timing_line(line):
    if 'TIMING' not in line:
        return None

    # Search for key-value pairs anywhere in the line
    qid_m = re.search(r'qid=(\S+)', line)
    node_m = re.search(r'node=(\S+)', line)
    dur_m = re.search(r'durationUs=(\d+)', line)
    tag_m = re.search(r'tag=(\S+)', line)
    thread_m = re.search(r'thread=(\S+)', line)

    # Extract standard log metadata
    log_thread_m = re.search(r'\[([^\]]+)\]', line)
    ts_m = re.search(r'(\d{2}:\d{2}:\d{2}\.\d{3})', line)

    if not (dur_m and tag_m and thread_m):
        return None

    # Extract detail (everything after the tag=... part)
    detail = ''
    if tag_m:
        tag_end = line.find(tag_m.group(0)) + len(tag_m.group(0))
        if tag_end < len(line):
            detail = line[tag_end:].strip()

    return {
        'ts': ts_m.group(1) if ts_m else '',
        'thread': log_thread_m.group(1) if log_thread_m else '',
        'timingThread': thread_m.group(1),
        'qid': qid_m.group(1) if qid_m else 'unknown_qid',
        'node': node_m.group(1) if node_m else 'unknown_node',
        'tag': tag_m.group(1),
        'durationUs': int(dur_m.group(1)),
        'detail': detail,
        'level': 'INFO'
    }

def parse_info_line(line):
    if 'execution=start' not in line:
        return None

    qid_m = re.search(r'qid=(\S+)', line)
    node_m = re.search(r'node=(\S+)', line)
    query_m = re.search(r"query='([^']*)'", line)
    ts_m = re.search(r'(\d{2}:\d{2}:\d{2}\.\d{3})', line)

    if not qid_m or not query_m:
        return None

    return {
        'ts': ts_m.group(1) if ts_m else '',
        'qid': qid_m.group(1),
        'node': node_m.group(1) if node_m else 'unknown_node',
        'query': query_m.group(1)
    }

# ── HTML Generation ───────────────────────────────────────────────────

def generate_html(node_data):
    node_max_us = {}
    total_events = 0
    for node, qids in node_data.items():
        max_us = max((ev['durationUs'] for data in qids.values() for ev in data['events']), default=1)
        node_max_us[node] = max_us if max_us > 0 else 1
        total_events += sum(len(data['events']) for data in qids.values())

    html = [f'''<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ArrowFlight Timing Report</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #f8fafc; color: #334155; padding: 20px; line-height: 1.5; }}
  h1 {{ font-size: 1.5rem; margin-bottom: 4px; color: #0f172a; }}
  .subtitle {{ font-size: 0.9rem; color: #64748b; margin-bottom: 24px; }}
  .tabs {{ display: flex; border-bottom: 2px solid #e2e8f0; margin-bottom: 20px; flex-wrap: wrap; gap: 4px; }}
  .tab-btn {{ padding: 10px 20px; border: none; background: #f1f5f9; cursor: pointer; border-radius: 8px 8px 0 0; font-weight: 600; color: #64748b; transition: all 0.2s; display: flex; align-items: center; gap: 8px; }}
  .tab-btn:hover {{ background: #e2e8f0; }}
  .tab-btn.active {{ background: #fff; color: #2563eb; border-bottom: 2px solid #2563eb; margin-bottom: -2px; }}
  .badge {{ background: #2563eb; color: #fff; border-radius: 12px; padding: 2px 8px; font-size: 0.75rem; }}
  .tab-content {{ display: none; animation: fadeIn 0.3s; }}
  .tab-content.active {{ display: block; }}
  @keyframes fadeIn {{ from {{ opacity: 0; }} to {{ opacity: 1; }} }}
  .query-block {{ background: #fff; border: 1px solid #e2e8f0; border-radius: 8px; margin-bottom: 24px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.05); }}
  .query-header {{ background: #f8fafc; padding: 12px 16px; border-bottom: 1px solid #e2e8f0; display: flex; flex-wrap: wrap; gap: 12px; align-items: center; }}
  .query-qid {{ font-weight: 600; color: #2563eb; font-family: monospace; font-size: 0.9rem; }}
  .query-sql {{ width: 100%; font-family: monospace; font-size: 0.85rem; color: #475569; background: #f1f5f9; padding: 8px; border-radius: 4px; margin-top: 8px; white-space: pre-wrap; word-break: break-all; }}
  .query-meta {{ font-size: 0.8rem; color: #64748b; margin-left: auto; }}
  .events-table {{ width: 100%; border-collapse: collapse; font-size: 0.85rem; }}
  .events-table th {{ text-align: left; padding: 8px 16px; background: #f8fafc; border-bottom: 1px solid #e2e8f0; color: #64748b; font-weight: 600; text-transform: uppercase; font-size: 0.75rem; }}
  .events-table td {{ padding: 6px 16px; border-bottom: 1px solid #f1f5f9; vertical-align: middle; }}
  .events-table tr:hover {{ background: #f8fafc; }}
  .bar-container {{ width: 100%; max-width: 300px; background: #e2e8f0; border-radius: 4px; height: 8px; overflow: hidden; }}
  .bar-fill {{ height: 100%; border-radius: 4px; transition: width 0.3s ease; }}
  .tag-badge {{ display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 0.75rem; font-weight: 600; color: #fff; }}
  .detail-text {{ color: #64748b; font-family: monospace; font-size: 0.8rem; }}
  .thread-text {{ font-family: monospace; font-size: 0.8rem; font-weight: 500; }}
</style>
</head>
<body>
<h1>ArrowFlight Timing Report</h1>
<p class="subtitle">Всего событий: {total_events} | Нод: {len(node_data)}</p>
<div class="tabs">
''']

    tab_contents = []
    for i, (node, qids) in enumerate(node_data.items()):
        is_active = "active" if i == 0 else ""
        node_events = sum(len(data['events']) for data in qids.values())
        html.append(f'  <button class="tab-btn {is_active}" onclick="openTab(event, \'tab-{i}\')">{escape(node)} <span class="badge">{node_events}</span></button>\n')

        content = [f'<div id="tab-{i}" class="tab-content {is_active}">']
        sorted_qids = sorted(qids.keys(), key=lambda q: sum(ev['durationUs'] for ev in qids[q]['events']), reverse=True)

        for qid in sorted_qids:
            data = qids[qid]
            query_text = data['query']
            block_ts = data['ts']
            events = data['events']
            max_us = node_max_us[node]

            content.append('<div class="query-block">')
            content.append('  <div class="query-header">')
            content.append(f'    <span class="query-qid">qid: {escape(qid)}</span>')
            if query_text:
                content.append(f'    <div class="query-sql">{escape(query_text)}</div>')
            content.append(f'    <span class="query-meta">ts: {escape(block_ts)} | событий: {len(events)}</span>')
            content.append('  </div>')
            content.append('  <table class="events-table">')
            content.append('    <thead><tr><th>Время</th><th>Поток</th><th>Длительность</th><th>Диаграмма</th><th>Тег</th><th>Детали</th></tr></thead>')
            content.append('    <tbody>')

            for ev in events:
                color = color_for_css(ev['tag'])
                duration_str = fmt_us(ev['durationUs']).strip()
                pct = (ev['durationUs'] / max_us * 100) if max_us > 0 else 0

                content.append('    <tr>')
                content.append(f'      <td>{escape(ev["ts"])}</td>')
                content.append(f'      <td><span class="thread-text" style="color:{color_for_css(ev["thread"])}">{escape(ev["thread"])}</span></td>')
                content.append(f'      <td style="font-variant-numeric: tabular-nums; text-align: right; font-weight: 500;">{duration_str}</td>')
                content.append(f'      <td><div class="bar-container"><div class="bar-fill" style="width:{pct:.1f}%; background:{color}" title="{duration_str}"></div></div></td>')
                content.append(f'      <td><span class="tag-badge" style="background:{color}">{escape(ev["tag"])}</span></td>')
                content.append(f'      <td><span class="detail-text">{escape(ev["detail"])}</span></td>')
                content.append('    </tr>')

            content.append('    </tbody>')
            content.append('  </table>')
            content.append('</div>')

        content.append('</div>')
        tab_contents.append('\n'.join(content))

    html.append('</div>\n')
    html.extend(tab_contents)
    html.append('''
<script>
function openTab(evt, tabName) {
    document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
    document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));
    document.getElementById(tabName).classList.add('active');
    evt.currentTarget.classList.add('active');
}
</script>
</body>
</html>''')
    return '\n'.join(html)

# ── main ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Parse ArrowFlight timing logs.')
    parser.add_argument('logfile', nargs='?', help='Path to log file (reads from stdin if omitted)')
    parser.add_argument('--html', action='store_true', help='Output as interactive HTML report with node tabs')
    args = parser.parse_args()

    if args.logfile:
        with open(args.logfile, 'r', encoding='utf-8', errors='replace') as f:
            lines = f.readlines()
    else:
        lines = sys.stdin.readlines()

    node_data = {}
    timing_events_flat = []

    for line in lines:
        ev = parse_timing_line(line)
        if ev:
            timing_events_flat.append(ev)
            node = ev['node']
            qid = ev['qid']

            if node not in node_data:
                node_data[node] = {}
            if qid not in node_data[node]:
                node_data[node][qid] = {'query': '', 'ts': ev['ts'], 'events': []}

            node_data[node][qid]['events'].append(ev)
            continue

        info = parse_info_line(line)
        if info:
            node = info['node']
            qid = info['qid']
            if node not in node_data:
                node_data[node] = {}
            if qid not in node_data[node]:
                node_data[node][qid] = {'query': '', 'ts': info['ts'], 'events': []}
            node_data[node][qid]['query'] = info['query']
            node_data[node][qid]['ts'] = info['ts']

    if not timing_events_flat:
        if args.html:
            print("<html><body><h1>No TIMING events found</h1></body></html>")
        else:
            print(f'{DIM}No TIMING events found (parsed {len(lines)} lines){RESET}')
        sys.exit(1)

    if args.html:
        print(generate_html(node_data))
        return

    # ── Terminal Output ───────────────────────────────────────────────
    by_qid = OrderedDict()
    info_blocks_global = {}
    for node, qids in node_data.items():
        for qid, data in qids.items():
            if qid not in by_qid:
                by_qid[qid] = []
                info_blocks_global[qid] = {'query': data['query'], 'ts': data['ts']}
            by_qid[qid].extend(data['events'])

    max_us = max(e['durationUs'] for e in timing_events_flat)
    if max_us == 0: max_us = 1

    for qid, events in by_qid.items():
        block = info_blocks_global.get(qid, {})
        query_text = block.get('query', '')
        block_ts = block.get('ts', events[0]['ts'])

        if query_text:
            hdr = f'  SQL: {query_text}'
            print()
            print(f'{DIM}{"─" * min(visible_len(hdr) + 2,100)}{RESET}')
            print(f'{BOLD}{hdr}{RESET}')
            print(f'{DIM}{"─" * min(visible_len(hdr) + 2,100)}{RESET}')
            print(f'  {DIM}qid={qid}  ts={block_ts}{RESET}')
        print()
        print(f'  {"TIMESTAMP":17} {"THREAD":20} {"DURATION":10} {"BAR":30}  {"TAG":30} {"DETAIL":}')
        print(f'  {"─"*16}  {"─"*19}  {"─"*9}  {"─"*30}  {"─"*30}  {"─"*30}')

        for ev in events:
            tc = colour_for(ev['thread'])
            tag_col = colour_for(ev['tag'])
            duration = f'{fmt_us(ev["durationUs"])}'
            bar = fmt_bar(ev['durationUs'], max_us)
            tag = f'{tag_col}{ev["tag"]:30}{RESET}'
            detail = ev['detail']
            duration_fmt = f'{duration:>10}'
            print(f'  {ev["ts"]:17} {tc}{ev["thread"]:20}{RESET} {duration_fmt}  {bar}  {tag} {DIM}{detail}{RESET if detail else ""}')

    print()
    print(f'{DIM}── {len(timing_events_flat)} events  max={max_us/1000:.1f}ms{RESET}')

if __name__ == '__main__':
    main()

