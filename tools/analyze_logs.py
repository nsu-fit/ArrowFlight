#!/usr/bin/env python3
"""
Parse ArrowFlight timing + INFO logs into a sorted flame-chart view.

Usage:  python tools/analyze_logs.py < logfile.log
        python tools/analyze_logs.py /path/to/arrowflight.log

Columns:
  TIMESTAMP  THREAD          DURATION  TAG  BLOCK  DETAIL
  10:15:30.1 main             8320 µs  ─── planning.getHandle
  10:15:30.1 main            12450 µs  ─── parquet.schemaRead      table=tpch.lineitem fields=16
"""

import sys
import re
import math
from datetime import datetime, timedelta
from collections import OrderedDict

# ── colour palette (256-colour friendly) ──────────────────────────────
PALETTE = [
    '\033[38;5;39m',   # dodger blue
    '\033[38;5;46m',   # lime
    '\033[38;5;226m',  # yellow
    '\033[38;5;201m',  # pink
    '\033[38;5;51m',   # cyan
    '\033[38;5;214m',  # orange
    '\033[38;5;198m',  # hot pink
    '\033[38;5;118m',  # chartreuse
    '\033[38;5;99m',   # purple
    '\033[38;5;82m',   # spring green
]
RESET = '\033[0m'
DIM = '\033[2m'
BOLD = '\033[1m'

# ── parse TIMING_LOG and INFO lines ───────────────────────────────────
PAT_TIMING = re.compile(
    r'^'
    r'(?P<ts>\d{2}:\d{2}:\d{2}\.\d{3})\s+'
    r'\[(?P<thread>[^\]]+)\]\s+'
    r'(?P<level>[A-Z]+)\s+'
    r'qid=(?P<qid>\S+)?\s*'
    r'node=(?P<node>\S+)?\s*'
    r'TIMING\s+'
    r'thread=(?P<timingThread>\S+)\s+'
    r'durationUs=(?P<durationUs>\d+)\s+'
    r'tag=(?P<tag>\S+)\s*'
    r'(?P<detail>.*)'
)

PAT_INFO = re.compile(
    r'^'
    r'(?P<ts>\d{2}:\d{2}:\d{2}\.\d{3})\s+'
    r'\[(?P<thread>[^\]]+)\]\s+'
    r'(?P<level>[A-Z]+)\s+'
    r'qid=(?P<qid>\S+)?\s*'
    r'node=(?P<node>\S+)?\s*'
    r'.*?'
    r"query='(?P<query>[^']*)'"
)

# ── helpers ───────────────────────────────────────────────────────────

def colour_for(s):
    h = hash(s)
    return PALETTE[abs(h) % len(PALETTE)]

def fmt_us(us):
    if us < 1000:
        return f'{us:>7} µs'
    elif us < 1_000_000:
        return f'{us/1000:>7.1f} ms'
    else:
        return f'{us/1_000_000:>7.2f} s'

def fmt_bar(us, max_us, width=30):
    if max_us == 0:
        return ' ' * width
    filled = max(1, int(us / max_us * width))
    bar = '█' * filled + '░' * (width - filled)
    idx = 0
    # colour the filled portion
    return f'\033[38;5;240m{bar[:filled]}\033[38;5;245m{bar[filled:]}{RESET}'

ANSI_RE = re.compile(r'\033\[[0-9;]*m')

def visible_len(s):
    return len(ANSI_RE.sub('', s))

# ── main ──────────────────────────────────────────────────────────────

def main():
    lines = sys.stdin.readlines() if len(sys.argv) < 2 else open(sys.argv[1]).readlines()

    timing_events = []
    info_blocks = OrderedDict()  # qid -> {'query': str, 'ts': str, 'events': list}

    for line in lines:
        m = PAT_TIMING.match(line)
        if m:
            d = m.groupdict()
            qid = d['qid'] if d['qid'] else ''
            detail = d['detail'].strip()
            timing_events.append({
                'ts': d['ts'],
                'thread': d['timingThread'],
                'qid': qid,
                'tag': d['tag'],
                'durationUs': int(d['durationUs']),
                'detail': detail,
                'level': d['level'],
            })
            # group by qid for block headers
            if qid not in info_blocks:
                info_blocks[qid] = OrderedDict(query='', ts=d['ts'], events=[])
            info_blocks[qid]['events'].append(timing_events[-1])
            continue

        m = PAT_INFO.match(line)
        if m:
            d = m.groupdict()
            qid = d['qid'] if d['qid'] else ''
            if qid and 'execution=start' in line:
                info_blocks.setdefault(qid, OrderedDict(query='', ts=d['ts'], events=[]))
                info_blocks[qid]['query'] = d['query']
                info_blocks[qid]['ts'] = d['ts']

    if not timing_events:
        # fallback: show raw matching lines
        print(f'{DIM}No TIMING events found (parsed {len(lines)} lines){RESET}')
        for line in lines:
            if 'qid=' in line and ('TIMING' in line or 'execution=start' in line):
                print(line.rstrip())
        sys.exit(0 if timing_events else 1)

    max_us = max(e['durationUs'] for e in timing_events)
    if max_us == 0:
        max_us = 1

    # Group events by qid for block display
    by_qid = OrderedDict()
    for ev in timing_events:
        by_qid.setdefault(ev['qid'], []).append(ev)

    # Print each qid block
    for qid, events in by_qid.items():
        block = info_blocks.get(qid, {})
        query_text = block.get('query', '')
        block_ts = block.get('ts', events[0]['ts'])

        # Block header
        if query_text:
            hdr = f'  SQL: {query_text}'
            print()
            print(f'{DIM}{"─" * min(visible_len(hdr) + 2, 100)}{RESET}')
            print(f'{BOLD}{hdr}{RESET}')
            print(f'{DIM}{"─" * min(visible_len(hdr) + 2, 100)}{RESET}')
            print(f'  {DIM}qid={qid}  ts={block_ts}{RESET}')
        print()

        # Column header
        print(f'  {"TIMESTAMP":17} {"THREAD":20} {"DURATION":10} {"BAR":30}  {"TAG":30} {"DETAIL":}')
        print(f'  {"─"*16}  {"─"*19}  {"─"*9}  {"─"*30}  {"─"*30}  {"─"*30}')

        for ev in events:
            tc = colour_for(ev['thread'])
            tag_col = colour_for(ev['tag'])

            ts = ev['ts']
            thread = f'{tc}{ev["thread"]:20}{RESET}'
            duration = f'{fmt_us(ev["durationUs"])}'
            bar = fmt_bar(ev['durationUs'], max_us)
            tag = f'{tag_col}{ev["tag"]:30}{RESET}'
            detail = ev['detail']

            dur_pad = max(0, 10 - len(ANSI_RE.sub('', fmt_us(0)))) + len(ANSI_RE.sub('', duration)) - len(duration)
            duration_fmt = f'{duration:>10}'

            print(f'  {ts:17} {thread} {duration_fmt:>10}  {bar}  {tag} {DIM}{detail}{RESET if detail else ""}')

    print()
    print(f'{DIM}── {len(timing_events)} events  max={max_us/1000:.1f}ms{RESET}')

if __name__ == '__main__':
    main()
