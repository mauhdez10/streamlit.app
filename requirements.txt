"""
Broadcast Playlist Checker - Core Logic
Checks JSON playlist against XLS traffic log and Grilla schedule.
"""
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from html.parser import HTMLParser


# ─── HELPERS ─────────────────────────────────────────────────────────────────

def parse_timecode(tc):
    """Parse Vipe timecode: '2026-03-27 10:00:00;00@2997d' → datetime"""
    try:
        tc = tc.split(';')[0].split('@')[0].strip()
        return datetime.strptime(tc, '%Y-%m-%d %H:%M:%S')
    except:
        try:
            tc = tc.split(';')[0]
            return datetime.strptime(tc, '%H:%M:%S')
        except:
            return None

def parse_duration(dur):
    """Parse '00:08:00;02@2997d' → seconds (int)"""
    try:
        dur = dur.split(';')[0].split('@')[0]
        h, m, s = dur.split(':')
        return int(h)*3600 + int(m)*60 + int(s)
    except:
        return 0

def fmt_time(dt):
    if dt is None:
        return '??:??:??'
    return dt.strftime('%H:%M:%S')

def normalize_episode_id(ep_id):
    """Normalize episode IDs for fuzzy matching.
    COSA00326 -> COSA0326 (remove extra zeros before 4-digit date)
    TMPN0326_1 -> TMPN0326 (strip segment suffix)
    """
    if not ep_id:
        return ''
    ep_id = str(ep_id).strip()
    # Strip segment suffix _N
    ep_id = re.sub(r'_\d+$', '', ep_id)
    # Normalize extra zeros: letters + 5+ digits → letters + last 4 digits
    ep_id = re.sub(r'([A-Za-z]+)0+(\d{4})$', r'\g<1>\g<2>', ep_id)
    return ep_id.upper()


# ─── JSON PARSER ─────────────────────────────────────────────────────────────

def parse_json_playlist(data):
    """
    Returns dict with:
      type: 'full' | 'current'
      date: datetime.date
      events: list of all events
      programs: ordered list of {episode_id, seg_num, start, duration, name, ref}
      commercials: list of {asset_ref, start, duration, name}
      promos: list of {asset_ref, start, name}
      breaks: list of breaks, each = {after_program, items: [{type,ref,start}]}
      cue_tones: list of {ref, name, start, ct_id}
    """
    events = data.get('events', [])

    # Detect type: full day has a marker event at start
    has_marker = any(
        a.get('type') == 'marker'
        for ev in events[:3]
        for a in ev.get('assets', [])
    )
    playlist_type = 'full' if has_marker else 'current'

    # Get date from first non-marker event
    date = None
    for ev in events:
        st = ev.get('startTime', '')
        dt = parse_timecode(st)
        if dt:
            date = dt.date()
            break

    programs = []
    commercials = []
    promos = []
    cue_tones = []
    breaks = []
    current_break = []
    last_program = None

    for ev in events:
        ev_assets = ev.get('assets', [])
        ev_start = parse_timecode(ev.get('startTime', ''))
        ev_dur = parse_duration(ev.get('duration', ''))
        ev_name = ev.get('name', '')
        ev_ref = ev.get('reference', '')
        behaviors = ev.get('behaviors', [])

        # Cue tones: active CUEON behavior
        for b in behaviors:
            if b.get('name') == 'CUEON' and not b.get('disabled', True):
                ct_asset = ev_assets[0].get('reference', ev_name) if ev_assets else ev_name
                cue_tones.append({
                    'ref': ev_ref,
                    'name': ev_name,
                    'ct_id': ct_asset,
                    'start': ev_start
                })

        for asset in ev_assets:
            atype = asset.get('type', '')
            aref = asset.get('reference', '')

            if atype in ('Program', 'live'):
                # Flush current break
                if current_break:
                    breaks.append({
                        'after_program': last_program,
                        'items': current_break[:]
                    })
                    current_break = []

                ep_id_raw = aref  # e.g. TMPN0326_1
                seg_match = re.search(r'_(\d+)$', ep_id_raw)
                seg_num = int(seg_match.group(1)) if seg_match else 1
                ep_id = normalize_episode_id(ep_id_raw)

                prog = {
                    'episode_id': ep_id,
                    'episode_id_raw': ep_id_raw,
                    'seg_num': seg_num,
                    'start': ev_start,
                    'duration': ev_dur,
                    'name': ev_name,
                    'ref': ev_ref,
                    'asset_type': atype
                }
                programs.append(prog)
                last_program = ep_id

            elif atype == 'Commercial':
                commercials.append({
                    'asset_ref': aref,
                    'name': ev_name,
                    'start': ev_start,
                    'duration': ev_dur,
                    'ref': ev_ref
                })
                current_break.append({'type': 'Commercial', 'ref': aref, 'start': ev_start})

            elif atype == 'Promotion':
                promos.append({
                    'asset_ref': aref,
                    'name': ev_name,
                    'start': ev_start,
                    'ref': ev_ref
                })
                current_break.append({'type': 'Promotion', 'ref': aref, 'start': ev_start})

    # Flush final break
    if current_break:
        breaks.append({'after_program': last_program, 'items': current_break})

    return {
        'type': playlist_type,
        'date': date,
        'events': events,
        'programs': programs,
        'commercials': commercials,
        'promos': promos,
        'breaks': breaks,
        'cue_tones': cue_tones
    }


# ─── XLS LOG PARSER (HTML-format .xls) ───────────────────────────────────────

class _HTMLTableParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.rows = []
        self.current_row = []
        self.current_cell = ''
        self.in_td = False

    def handle_starttag(self, tag, attrs):
        if tag in ('td', 'th'):
            self.in_td = True
            self.current_cell = ''
        elif tag == 'tr':
            self.current_row = []

    def handle_endtag(self, tag):
        if tag in ('td', 'th'):
            self.current_row.append(self.current_cell.strip())
            self.in_td = False
        elif tag == 'tr':
            if self.current_row:
                self.rows.append(self.current_row)

    def handle_data(self, data):
        if self.in_td:
            self.current_cell += data

def parse_xls_log(filepath):
    """Parse HTML-format XLS traffic log. Returns list of dicts."""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    parser = _HTMLTableParser()
    parser.feed(content)
    if not parser.rows:
        return []
    headers = parser.rows[0]
    rows = [dict(zip(headers, row)) for row in parser.rows[1:]]
    return rows

def xls_commercials(rows):
    """Extract commercial rows from XLS log."""
    return [r for r in rows if r.get('Type', '').upper() == 'COMMERCIAL']

def xls_programs(rows):
    """Extract program/live rows from XLS log."""
    return [r for r in rows if r.get('Type', '').upper() in ('PROGRAM', 'LIVE')]


# ─── GRILLA PARSER ───────────────────────────────────────────────────────────

def parse_grilla(filepath, target_date):
    """
    Parse Grilla XLSX. Returns ordered list of episode IDs for target_date.
    Handles standalone IDs (TMPN0326) and IDs embedded in show names
    e.g. 'Teledos T2SV0327' or 'Este es el Salvador ESSV0321'.
    target_date: datetime.date
    """
    from openpyxl import load_workbook
    wb = load_workbook(filepath, read_only=True)
    ws = wb.active
    all_rows = list(ws.iter_rows(values_only=True))
    if len(all_rows) < 2:
        return []

    # Find target column from header row (row index 1, cols 2-8 = Mon-Sun)
    header_row = all_rows[1]
    target_col = None
    try:
        monday_val = header_row[2]
        monday = monday_val.date() if hasattr(monday_val, 'date') else None
        if monday:
            for col_offset in range(7):
                if monday + timedelta(days=col_offset) == target_date:
                    target_col = 2 + col_offset
                    break
    except:
        pass

    if target_col is None:
        target_col = 2 + target_date.weekday()

    def extract_ids_from_cell(val):
        if not val or not isinstance(val, str) or '=' in val:
            return []
        val = val.strip()
        # Standalone: all-caps alphanumeric ending in 3+ digits, short
        if re.match(r'^[A-Z0-9]{2,}[0-9]{3,}$', val) and len(val) <= 15:
            return [normalize_episode_id(val)]
        # Embedded in show name: e.g. "Teledos T2SV0327"
        matches = re.findall(r'\b([A-Z][A-Z0-9]{1,}[0-9]{4})\b', val)
        return [normalize_episode_id(m) for m in matches]

    seen = set()
    episode_ids = []
    for row in all_rows[2:]:
        val = row[target_col] if target_col < len(row) else None
        for ep_id in extract_ids_from_cell(val):
            if ep_id and ep_id not in seen:
                seen.add(ep_id)
                episode_ids.append(ep_id)

    return episode_ids


# ─── CHECKS ──────────────────────────────────────────────────────────────────

def check_programs_vs_grilla(playlist, grilla_ids, current_start=None):
    """
    Compare program order in playlist against grilla.
    If current_start is set, only check programs from that time onward.
    Returns list of issues.
    """
    issues = []

    # Get unique ordered episode IDs from playlist (in order of first appearance)
    seen = set()
    playlist_eps = []
    for p in playlist['programs']:
        if current_start and p['start'] and p['start'] < current_start:
            continue
        ep = p['episode_id']
        if ep not in seen:
            seen.add(ep)
            playlist_eps.append({'id': ep, 'start': p['start'], 'name': p['name']})

    # Build sets for comparison
    playlist_set = set(p['id'] for p in playlist_eps)
    grilla_set = set(grilla_ids)

    # Missing from playlist (in grilla but not in playlist)
    for gid in grilla_ids:
        if gid not in playlist_set:
            # Try fuzzy: maybe extra zero
            close = [pid for pid in playlist_set if pid.replace('0','') == gid.replace('0','')]
            if close:
                issues.append(f'  ⚠  ID MISMATCH: Grilla={gid} | Playlist={close[0]}')
            else:
                issues.append(f'  ✗  MISSING FROM PLAYLIST: {gid} (in grilla, not in playlist)')

    # Extra in playlist (not in grilla)
    for p in playlist_eps:
        if p['id'] not in grilla_set:
            close = [gid for gid in grilla_set if gid.replace('0','') == p['id'].replace('0','')]
            if close:
                pass  # Already caught above as mismatch
            else:
                issues.append(f'  ✗  EXTRA IN PLAYLIST: {p["id"]} @ {fmt_time(p["start"])} (not in grilla)')

    # Order check
    # Get playlist IDs that ARE in grilla (in playlist order)
    pl_ordered = [p['id'] for p in playlist_eps if p['id'] in grilla_set]
    gr_ordered = [gid for gid in grilla_ids if gid in playlist_set]

    if pl_ordered != gr_ordered:
        # Find specific mismatches
        for i, (pl, gr) in enumerate(zip(pl_ordered, gr_ordered)):
            if pl != gr:
                issues.append(f'  ⚠  ORDER MISMATCH at position {i+1}: Grilla expects {gr}, Playlist has {pl}')

    return issues

def check_commercials_vs_log(playlist, xls_rows, current_start=None):
    """Compare commercials between playlist and XLS log."""
    issues = []

    # Playlist commercials
    pl_comms = playlist['commercials']
    if current_start:
        pl_comms = [c for c in pl_comms if c['start'] and c['start'] >= current_start]

    pl_counter = Counter(c['asset_ref'] for c in pl_comms)

    # XLS log commercials
    xls_comms = xls_commercials(xls_rows)
    xls_counter = Counter(r.get('Media Id', '') for r in xls_comms)

    all_refs = set(pl_counter.keys()) | set(xls_counter.keys())
    for ref in sorted(all_refs):
        pl_cnt = pl_counter.get(ref, 0)
        xl_cnt = xls_counter.get(ref, 0)
        if pl_cnt != xl_cnt:
            if pl_cnt == 0:
                issues.append(f'  ✗  IN LOG NOT IN PLAYLIST: {ref} ({xl_cnt}x in log)')
            elif xl_cnt == 0:
                issues.append(f'  ✗  IN PLAYLIST NOT IN LOG: {ref} ({pl_cnt}x in playlist)')
            else:
                issues.append(f'  ⚠  COUNT MISMATCH: {ref} | Log={xl_cnt}x | Playlist={pl_cnt}x')

    return issues

def check_promo_repeats(playlist, current_start=None):
    """Check if any promo repeats within the same break."""
    issues = []
    for i, brk in enumerate(playlist['breaks']):
        if current_start:
            # Skip breaks before current time
            break_times = [item['start'] for item in brk['items'] if item.get('start')]
            if break_times and break_times[0] and break_times[0] < current_start:
                continue

        promo_refs = [item['ref'] for item in brk['items'] if item['type'] == 'Promotion']
        counts = Counter(promo_refs)
        dups = {ref: cnt for ref, cnt in counts.items() if cnt > 1}
        if dups:
            break_time = None
            for item in brk['items']:
                if item.get('start'):
                    break_time = item['start']
                    break
            after = brk.get('after_program', 'unknown')
            for ref, cnt in dups.items():
                issues.append(f'  ⚠  PROMO REPEAT in break after [{after}] @ {fmt_time(break_time)}: {ref} appears {cnt}x')
    return issues

def check_cue_tones(playlist):
    """Return cue tone summary."""
    lines = []
    cts = playlist['cue_tones']
    ct_counter = Counter(ct['ct_id'] for ct in cts)
    lines.append(f'  Total cue tones: {len(cts)}')
    for ct_id, count in sorted(ct_counter.items()):
        times = [fmt_time(ct['start']) for ct in cts if ct['ct_id'] == ct_id]
        lines.append(f'  {ct_id}: {count}x | First: {times[0]} | Last: {times[-1]}')
    return lines

def check_breaks(playlist, current_start=None):
    """Check each break has at least one commercial."""
    issues = []
    for i, brk in enumerate(playlist['breaks']):
        commercials_in_break = [item for item in brk['items'] if item['type'] == 'Commercial']
        if not commercials_in_break:
            break_time = None
            for item in brk['items']:
                if item.get('start'):
                    break_time = item['start']
                    break
            if current_start and break_time and break_time < current_start:
                continue
            after = brk.get('after_program', 'unknown')
            issues.append(f'  ⚠  EMPTY BREAK (no commercials) after [{after}] @ {fmt_time(break_time)}')
    return issues


# ─── REPORT GENERATOR ────────────────────────────────────────────────────────

def generate_report(channel, playlist, xls_rows, grilla_ids):
    """Generate full plain-text report for one channel."""
    lines = []
    sep = '═' * 60

    pt = playlist['type']
    date_str = str(playlist['date']) if playlist['date'] else 'Unknown'
    start_str = fmt_time(playlist['programs'][0]['start']) if playlist['programs'] else '??:??:??'

    lines.append(sep)
    lines.append(f'CHANNEL: {channel.upper()}')
    lines.append(f'DATE: {date_str}')
    lines.append(f'PLAYLIST TYPE: {"FULL DAY" if pt == "full" else "CURRENT (partial)"}')
    if pt == 'current':
        lines.append(f'CHECKING FROM: {start_str} UTC to end of day')
    lines.append(sep)

    # Determine cut-off for partial playlist
    current_start = None
    if pt == 'current' and playlist['programs']:
        current_start = playlist['programs'][0]['start']

    total_programs = len(set(p['episode_id'] for p in playlist['programs']))
    total_commercials = len(playlist['commercials'])
    total_breaks = len(playlist['breaks'])
    lines.append(f'SUMMARY: {total_programs} programs | {total_commercials} commercials | {total_breaks} breaks')
    lines.append('')

    # ── 1. PROGRAM vs GRILLA ──
    lines.append('── [1] PROGRAM CHECK (Playlist vs Grilla) ──')
    if not grilla_ids:
        lines.append('  ! Grilla not provided or date not found')
    else:
        prog_issues = check_programs_vs_grilla(playlist, grilla_ids, current_start)
        if not prog_issues:
            lines.append('  ✓ All programs match grilla (order and IDs correct)')
        else:
            for issue in prog_issues:
                lines.append(issue)
    lines.append('')

    # ── 2. COMMERCIAL CHECK ──
    lines.append('── [2] COMMERCIAL CHECK (Playlist vs XLS Log) ──')
    if not xls_rows:
        lines.append('  ! XLS log not provided')
    else:
        comm_issues = check_commercials_vs_log(playlist, xls_rows, current_start)
        if not comm_issues:
            lines.append(f'  ✓ All commercials match ({total_commercials} spots verified)')
        else:
            for issue in comm_issues:
                lines.append(issue)
    lines.append('')

    # ── 3. BREAK CHECK ──
    lines.append('── [3] BREAK CHECK (Commercial presence per break) ──')
    break_issues = check_breaks(playlist, current_start)
    if not break_issues:
        lines.append(f'  ✓ All {total_breaks} breaks contain commercials')
    else:
        for issue in break_issues:
            lines.append(issue)
    lines.append('')

    # ── 4. PROMO REPEATS ──
    lines.append('── [4] PROMO REPEAT CHECK ──')
    promo_issues = check_promo_repeats(playlist, current_start)
    if not promo_issues:
        lines.append('  ✓ No repeated promos within same break')
    else:
        for issue in promo_issues:
            lines.append(issue)
    lines.append('')

    # ── 5. CUE TONES (full playlist only) ──
    if pt == 'full':
        lines.append('── [5] CUE TONE REPORT ──')
        ct_lines = check_cue_tones(playlist)
        lines.extend(ct_lines)
        lines.append('')

    # ── 6. MISSING ITEMS SUMMARY ──
    lines.append('── [6] MISSING ITEMS SUMMARY ──')
    missing_programs = []
    if grilla_ids:
        playlist_set = set(p['episode_id'] for p in playlist['programs'])
        for gid in grilla_ids:
            if gid not in playlist_set:
                close = [pid for pid in playlist_set if pid.replace('0','') == gid.replace('0','')]
                if not close:
                    missing_programs.append(gid)

    missing_commercials = []
    if xls_rows:
        pl_refs = set(c['asset_ref'] for c in playlist['commercials'])
        xls_comms = xls_commercials(xls_rows)
        for r in xls_comms:
            ref = r.get('Media Id', '')
            if ref and ref not in pl_refs:
                missing_commercials.append(ref)

    if not missing_programs and not missing_commercials:
        lines.append('  ✓ Nothing missing')
    else:
        if missing_programs:
            lines.append(f'  Programs missing from playlist ({len(missing_programs)}):')
            for ep in missing_programs:
                lines.append(f'    - {ep}')
        if missing_commercials:
            lines.append(f'  Commercials missing from playlist ({len(Counter(missing_commercials))} unique):')
            for ref, cnt in Counter(missing_commercials).items():
                lines.append(f'    - {ref} ({cnt}x in log)')

    lines.append('')
    lines.append(sep)
    return '\n'.join(lines)
