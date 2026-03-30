"""
Broadcast Playlist Checker — Core Logic v4
"""
import json
import re
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from datetime import datetime, timedelta


# ── HELPERS ───────────────────────────────────────────────────────────────────

def parse_timecode(tc):
    try:
        tc = tc.split(';')[0].split('@')[0].strip()
        return datetime.strptime(tc, '%Y-%m-%d %H:%M:%S')
    except:
        return None

def parse_duration(dur):
    try:
        dur = dur.split(';')[0].split('@')[0]
        h, m, s = dur.split(':')
        return int(h)*3600 + int(m)*60 + int(s)
    except:
        return 0

def fmt_time(dt):
    if dt is None: return '??:??:??'
    return dt.strftime('%H:%M:%S')

def is_episode_id(val):
    """
    Detect episode IDs: starts with letter, contains 3+ digits, no spaces, len 3-16.
    Handles: COSA0327, INM197R, T2SV0327, RYM816, NF0328, VD0329.
    """
    if not val or not isinstance(val, str): return False
    val = val.strip()
    if ' ' in val or len(val) < 3 or len(val) > 16: return False
    if not re.match(r'^[A-Z]', val): return False
    return len(re.findall(r'\d', val)) >= 3

def normalize_id(ep_id):
    """Strip segment suffix _N. Normalize extra leading zeros."""
    if not ep_id: return ''
    ep_id = re.sub(r'_\d+$', '', str(ep_id).strip())
    ep_id = re.sub(
        r'([A-Za-z][A-Za-z0-9]*)0{2,}(\d{3,})',
        lambda m: m.group(1) + (m.group(2)[-4:] if len(m.group(2)) > 4 else m.group(2)),
        ep_id
    )
    return ep_id.upper()

def show_prefix(ep_id):
    m = re.match(r'^([A-Za-z]{2,})', ep_id)
    return m.group(1).upper() if m else ''


# ── JSON PLAYLIST PARSER ──────────────────────────────────────────────────────

def parse_json_playlist(data):
    events = data.get('events', [])

    has_marker = any(
        a.get('type') == 'marker'
        for ev in events[:3]
        for a in ev.get('assets', [])
    )
    playlist_type = 'full' if has_marker else 'current'

    date = None
    for ev in events:
        dt = parse_timecode(ev.get('startTime', ''))
        if dt:
            date = dt.date()
            break

    programs     = []
    commercials  = []
    promos       = []
    cue_tones    = []
    not_ingested = []
    breaks       = []
    current_break = []
    last_program  = None

    for ev in events:
        ev_assets  = ev.get('assets', [])
        ev_start   = parse_timecode(ev.get('startTime', ''))
        ev_dur     = parse_duration(ev.get('duration', ''))
        ev_name    = ev.get('name', '')
        ev_ref     = ev.get('reference', '')
        behaviors  = ev.get('behaviors', [])

        # Cue tones
        for b in behaviors:
            if b.get('name') == 'CUEON' and not b.get('disabled', True):
                ct = ev_assets[0].get('reference', ev_name) if ev_assets else ev_name
                cue_tones.append({'ref': ev_ref, 'name': ev_name,
                                  'ct_id': ct, 'start': ev_start})

        for asset in ev_assets:
            atype = asset.get('type', '')
            aref  = asset.get('reference', '')
            tcin  = asset.get('tcIn', '')

            # Not ingested: 07: tcIn on any non-live asset
            if tcin.startswith('07:') and atype != 'live':
                not_ingested.append({
                    'asset_ref': aref, 'name': ev_name,
                    'type': atype, 'start': ev_start, 'ref': ev_ref
                })

            if atype in ('Program', 'live'):
                if current_break:
                    breaks.append({'after_program': last_program,
                                   'items': current_break[:]})
                    current_break = []

                seg_m = re.search(r'_(\d+)$', aref)
                seg   = int(seg_m.group(1)) if seg_m else 1
                ep_id = normalize_id(aref)

                programs.append({
                    'episode_id': ep_id, 'episode_id_raw': aref,
                    'seg_num': seg, 'start': ev_start, 'duration': ev_dur,
                    'name': ev_name, 'ref': ev_ref,
                    'asset_type': atype,
                    'is_missing': (atype == 'Program' and tcin.startswith('07:'))
                })
                last_program = ep_id

            elif atype == 'Commercial':
                commercials.append({
                    'asset_ref': aref, 'name': ev_name,
                    'start': ev_start, 'duration': ev_dur,
                    'ref': ev_ref
                })
                current_break.append({'type': 'Commercial', 'ref': aref,
                                      'start': ev_start, 'event_ref': ev_ref})

            elif atype == 'Promotion':
                promos.append({'asset_ref': aref, 'name': ev_name,
                               'start': ev_start, 'ref': ev_ref})
                current_break.append({'type': 'Promotion', 'ref': aref,
                                      'start': ev_start})

    if current_break:
        breaks.append({'after_program': last_program, 'items': current_break})

    return {
        'type': playlist_type, 'date': date, 'events': events,
        'programs': programs, 'commercials': commercials,
        'promos': promos, 'breaks': breaks,
        'cue_tones': cue_tones, 'not_ingested': not_ingested
    }


def build_show_sequence(programs, from_start=None):
    """
    Build ordered list of show blocks as they appear in playlist.
    Each entry = one show block (consecutive segments collapsed to one).
    Preserves re-airs (same show appearing multiple times = multiple entries).
    """
    seq = []
    prev = None
    for p in programs:
        if from_start and p['start'] and p['start'] < from_start:
            continue
        ep = p['episode_id']
        if ep != prev:
            seq.append({'id': ep, 'start': p['start']})
            prev = ep
    return seq


# ── XML TRAFFIC LOG PARSER ────────────────────────────────────────────────────

def parse_xml_log(filepath_or_bytes):
    try:
        if hasattr(filepath_or_bytes, 'read'):
            content = filepath_or_bytes.read()
        elif isinstance(filepath_or_bytes, bytes):
            content = filepath_or_bytes
        else:
            with open(filepath_or_bytes, 'rb') as f:
                content = f.read()

        root = ET.fromstring(content)
        traffic = root.find('traffic')
        if traffic is None:
            traffic = root

        items = []
        for item in traffic.findall('item'):
            items.append({
                'mediaid':     item.get('mediaid', ''),
                'name':        item.findtext('n', '').strip(),
                'description': item.findtext('description', '').strip(),
                'contenttype': item.findtext('contenttype', '').strip().upper(),
                'startat':     item.findtext('startat', '').strip(),
                'duration':    item.findtext('duration', '').strip(),
                'externalid':  item.findtext('externalid', '').strip(),
            })
        return items
    except Exception:
        return []

def xml_commercials(rows):
    return [r for r in rows if r.get('contenttype') == 'COMMERCIAL']


# ── GRILLA PARSER (no deduplication — preserves re-airs) ────────────────────

def parse_grilla(filepath_or_bytes, target_date):
    from openpyxl import load_workbook
    import io

    if isinstance(filepath_or_bytes, str):
        wb = load_workbook(filepath_or_bytes, read_only=True)
    else:
        raw = filepath_or_bytes.read() if hasattr(filepath_or_bytes, 'read') else filepath_or_bytes
        wb = load_workbook(io.BytesIO(raw), read_only=True)

    ws = wb.active
    all_rows = list(ws.iter_rows(values_only=True))
    if len(all_rows) < 2:
        return []

    # Find target column
    header_row = all_rows[1]
    target_col = None
    try:
        monday_val = header_row[2]
        monday = monday_val.date() if hasattr(monday_val, 'date') else None
        if monday:
            for offset in range(7):
                if monday + timedelta(days=offset) == target_date:
                    target_col = 2 + offset
                    break
    except:
        pass
    if target_col is None:
        target_col = 2 + target_date.weekday()

    def resolve_cell(val):
        """If val is a simple cell-ref formula like =G21, resolve it. Otherwise return val."""
        if not val or not isinstance(val, str): return val
        m = re.match(r'^=([A-Z]+)(\d+)$', val.strip())
        if not m: return val
        col_str, row_num = m.group(1), int(m.group(2))
        col_idx = 0
        for ch in col_str:
            col_idx = col_idx * 26 + (ord(ch) - ord('A') + 1)
        col_idx -= 1  # 0-based
        row_idx  = row_num - 1  # 0-based
        if row_idx < len(all_rows) and col_idx < len(all_rows[row_idx]):
            return all_rows[row_idx][col_idx]
        return val

    def extract_ids(val):
        val = resolve_cell(val)
        if not val or not isinstance(val, str) or val.startswith('='):
            return []
        val = val.strip()
        if is_episode_id(val):
            return [normalize_id(val)]
        tokens = re.findall(r'[A-Z0-9]+', val.upper())
        return [normalize_id(t) for t in tokens if is_episode_id(t)]

    # NO deduplication — keep all occurrences including re-airs from formulas
    episode_ids = []
    for row in all_rows[2:]:
        val = row[target_col] if target_col < len(row) else None
        for ep in extract_ids(val):
            if ep:
                episode_ids.append(ep)

    return episode_ids


# ── CHECKS ────────────────────────────────────────────────────────────────────

def check_programs_vs_grilla(playlist, grilla_ids, current_start=None):
    """
    Compare show sequence in playlist vs grilla (order-based, preserves re-airs).
    For partial: anchor grilla to where playlist starts using externalid match.
    """
    issues = []
    is_partial = (current_start is not None)

    # Full show sequence from playlist (blocks, preserving re-airs)
    full_seq  = build_show_sequence(playlist['programs'])
    # Sequence from current position
    part_seq  = build_show_sequence(playlist['programs'], from_start=current_start)

    # For partial: find anchor in grilla
    if is_partial and part_seq:
        anchor = 0
        first_id = part_seq[0]['id']
        first_pfx = show_prefix(first_id)
        for i, gid in enumerate(grilla_ids):
            if gid == first_id or (first_pfx and show_prefix(gid) == first_pfx):
                anchor = i
                issues.append(f'  ℹ  Anchored at grilla position {i+1}: {gid}')
                break
        grilla_slice = grilla_ids[anchor:]
    else:
        grilla_slice = grilla_ids

    pl_seq = part_seq

    # Build sets for missing/extra detection
    grilla_set = set(grilla_slice)
    pl_set     = set(p['id'] for p in pl_seq)

    # Check each grilla entry (sequentially, not as set)
    reported_missing = set()
    for gid in grilla_slice:
        if gid not in pl_set and gid not in reported_missing:
            pfx = show_prefix(gid)
            pl_same = [p for p in pl_seq if show_prefix(p['id']) == pfx and p['id'] != gid]
            if pl_same:
                issues.append(
                    f'  ⚠  WRONG EPISODE: Grilla={gid} | '
                    f'Playlist={pl_same[0]["id"]} @ {fmt_time(pl_same[0]["start"])} UTC'
                )
            else:
                if is_partial and any(p['episode_id'] == gid for p in playlist['programs']):
                    issues.append(f'  ℹ  ALREADY AIRED: {gid}')
                else:
                    issues.append(f'  ✗  NOT IN PLAYLIST: {gid}')
            reported_missing.add(gid)

    # Extra in playlist not in grilla
    for p in pl_seq:
        if p['id'] not in grilla_set:
            pfx = show_prefix(p['id'])
            grilla_same = [g for g in grilla_slice if show_prefix(g) == pfx and g != p['id']]
            if not grilla_same:
                issues.append(
                    f'  ✗  EXTRA: {p["id"]} @ {fmt_time(p["start"])} UTC (not in grilla)'
                )

    # Order comparison — direct sequence zip (handles duplicates)
    # Only compare shows present in both
    pl_ord = [p['id'] for p in pl_seq if p['id'] in grilla_set]
    gr_ord = [g for g in grilla_slice if g in pl_set]

    mismatch_count = 0
    for i, (pl, gr) in enumerate(zip(pl_ord, gr_ord)):
        if pl != gr and mismatch_count < 6:
            issues.append(f'  ⚠  ORDER pos {i+1}: Grilla={gr} | Playlist={pl}')
            mismatch_count += 1

    if not [x for x in issues if x.strip().startswith(('✗','⚠  O','⚠  W'))]:
        issues.append('  ✓ All programs match grilla')

    return issues


def check_commercials_vs_xml(playlist, xml_rows, current_start=None):
    """
    Compare commercial counts: playlist vs XML log.
    For partial: anchor to first JSON event reference in XML externalid.
    """
    issues = []
    all_comms = playlist['commercials']

    if current_start:
        # Build index of XML externalid -> position
        ext_idx = {row['externalid']: i for i, row in enumerate(xml_rows)}
        # Find first JSON event at or after current_start in XML
        anchor_xml = 0
        for ev in playlist['events']:
            ev_start = parse_timecode(ev.get('startTime', ''))
            if ev_start and ev_start >= current_start:
                ref = ev.get('reference', '')
                if ref in ext_idx:
                    anchor_xml = ext_idx[ref]
                    break
        xml_rows_use = xml_rows[anchor_xml:]
        pl_comms = [c for c in all_comms if c['start'] and c['start'] >= current_start]
    else:
        xml_rows_use = xml_rows
        pl_comms = all_comms

    pl_ids  = [c['asset_ref'] for c in pl_comms]
    xml_ids = [r['mediaid'] for r in xml_commercials(xml_rows_use)]

    pl_set  = Counter(pl_ids)
    xml_set = Counter(xml_ids)

    diffs = []
    for ref in sorted(set(pl_set.keys()) | set(xml_set.keys())):
        pc = pl_set.get(ref, 0)
        xc = xml_set.get(ref, 0)
        if pc != xc:
            if pc == 0:
                diffs.append(f'  ✗  IN XML, NOT IN PLAYLIST: {ref} ({xc}x in XML)')
            elif xc == 0:
                diffs.append(f'  ✗  IN PLAYLIST, NOT IN XML: {ref} ({pc}x in playlist)')
            else:
                diffs.append(f'  ⚠  COUNT DIFF: {ref} | XML={xc}x | Playlist={pc}x')

    issues += diffs if diffs else [f'  ✓ All {len(pl_ids)} commercials match XML log']
    return issues


def check_promo_repeats(playlist, current_start=None):
    issues = []
    for brk in playlist['breaks']:
        items = brk['items']
        if not items: continue
        break_start = next((i['start'] for i in items if i.get('start')), None)
        if current_start and break_start and break_start < current_start:
            continue
        promo_refs = [i['ref'] for i in items if i['type'] == 'Promotion']
        dups = {r: c for r, c in Counter(promo_refs).items() if c > 1}
        if dups:
            after = brk.get('after_program', '?')
            for ref, cnt in dups.items():
                issues.append(
                    f'  ⚠  PROMO REPEAT after [{after}]'
                    f' @ {fmt_time(break_start)} UTC: {ref} {cnt}x'
                )
    return issues


def check_not_ingested(playlist, current_start=None):
    """All 07: tcIn assets. Programs grouped by episode ID."""
    lines = []
    seen_eps   = set()
    seen_other = set()
    for item in playlist['not_ingested']:
        if current_start and item['start'] and item['start'] < current_start:
            continue
        atype = item['type']
        aref  = item['asset_ref']
        if atype in ('Program', 'live'):
            ep_id = normalize_id(aref)
            if ep_id in seen_eps: continue
            seen_eps.add(ep_id)
            show = re.sub(r'\[\].*$', '', item['name']).strip()
            lines.append(
                f'  ⚠  NOT INGESTED [Program]: {ep_id}'
                f' @ {fmt_time(item["start"])} UTC | {show}'
            )
        else:
            if aref in seen_other: continue
            seen_other.add(aref)
            lines.append(
                f'  ⚠  NOT INGESTED [{atype}]: {aref}'
                f' @ {fmt_time(item["start"])} UTC | {item["name"]}'
            )
    return lines


def check_bugs(playlist, current_start=None):
    """
    Report LOGOHD_ANI / LOGO_LIVE bugs.
    One line per show (first segment only).
    """
    lines = []
    seen = {}   # ep_id -> info dict

    for ev in playlist['events']:
        ev_start  = parse_timecode(ev.get('startTime', ''))
        if current_start and ev_start and ev_start < current_start:
            continue
        behaviors = ev.get('behaviors', [])
        assets    = ev.get('assets', [])
        if not assets: continue

        aref  = assets[0].get('reference', '')
        atype = assets[0].get('type', '')
        if atype not in ('Program', 'live'):
            continue

        ep_id = normalize_id(aref)
        if ep_id in seen:
            continue

        for b in behaviors:
            if b.get('name') in ('LOGOHD_ANI', 'LOGO_LIVE') and not b.get('disabled', True):
                show = re.sub(r'\[\].*$', '', ev.get('name', '')).strip()
                seen[ep_id] = {
                    'ep_id': ep_id, 'show': show,
                    'behavior': b.get('name'), 'start': ev_start
                }
                break

    if not seen:
        lines.append('  ✓ No bugs scheduled')
    else:
        for info in sorted(seen.values(), key=lambda x: x['start'] or datetime.min):
            lines.append(
                f'  🔲 {info["behavior"]}: {info["ep_id"]}'
                f' @ {fmt_time(info["start"])} UTC | {info["show"]}'
            )
    return lines


def check_cue_tones(playlist):
    lines = []
    cts = playlist['cue_tones']
    ct_counter = Counter(ct['ct_id'] for ct in cts)
    lines.append(f'  Total cue tones: {len(cts)}')
    for ct_id, count in sorted(ct_counter.items()):
        times = [fmt_time(ct['start']) for ct in cts if ct['ct_id'] == ct_id]
        lines.append(f'  {ct_id}: {count}x | First: {times[0]} | Last: {times[-1]}')
    return lines


# ── AUTO FILE DETECTION ───────────────────────────────────────────────────────

def detect_files(uploaded_files):
    """
    Auto-detect channel and type from filename.
    Returns dict: {catv: {json,xml,grilla}, tvd: {json,xml,grilla}}
    and a list of unrecognized files.
    """
    result = {
        'catv': {'json': None, 'xml': None, 'grilla': None},
        'tvd':  {'json': None, 'xml': None, 'grilla': None},
    }
    unknown = []

    for f in uploaded_files:
        name = f.name.upper()
        ext  = f.name.lower().rsplit('.', 1)[-1] if '.' in f.name else ''

        # File type
        if ext == 'json':
            ftype = 'json'
        elif ext == 'xml':
            ftype = 'xml'
        elif ext in ('xlsx', 'xlsm'):
            ftype = 'grilla'
        else:
            unknown.append(f)
            continue

        # Channel — XML: CA… = CATV, TVD… = TVD; others: CATV/TVD in name
        if ext == 'xml':
            if name.startswith('CA'):
                channel = 'catv'
            elif name.startswith('TVD'):
                channel = 'tvd'
            else:
                unknown.append(f)
                continue
        else:
            if 'CATV' in name:
                channel = 'catv'
            elif 'TVD' in name:
                channel = 'tvd'
            else:
                unknown.append(f)
                continue

        result[channel][ftype] = f

    return result, unknown


# ── REPORT GENERATOR ──────────────────────────────────────────────────────────

def generate_report(channel, playlist, xml_rows, grilla_ids):
    lines = []
    sep = '═' * 60

    pt          = playlist['type']
    date_str    = str(playlist['date']) if playlist['date'] else 'Unknown'
    progs       = playlist['programs']
    first_start = progs[0]['start'] if progs else None
    current_start = first_start if pt == 'current' else None

    lines += [sep,
              f'CHANNEL: {channel.upper()}',
              f'DATE: {date_str}',
              f'PLAYLIST TYPE: {"FULL DAY" if pt == "full" else "CURRENT (partial)"}']
    if current_start:
        lines.append(f'CHECKING FROM: {fmt_time(current_start)} UTC to end of day')
    lines.append(sep)

    part_seq = build_show_sequence(progs, from_start=current_start)
    total_comms = len([c for c in playlist['commercials']
                       if not current_start or (c['start'] and c['start'] >= current_start)])
    lines.append(f'SUMMARY: {len(part_seq)} show blocks | {total_comms} commercials')
    lines.append('')

    # [1] Programs
    lines.append('── [1] PROGRAM CHECK (Playlist vs Grilla) ──')
    if not grilla_ids:
        lines.append('  ! Grilla not provided')
    else:
        lines += check_programs_vs_grilla(playlist, grilla_ids, current_start)
    lines.append('')

    # [2] Commercials
    lines.append('── [2] COMMERCIAL CHECK (Playlist vs XML) ──')
    if not xml_rows:
        lines.append('  ! XML log not provided')
    else:
        lines += check_commercials_vs_xml(playlist, xml_rows, current_start)
    lines.append('')

    # [3] Promo repeats
    lines.append('── [3] PROMO REPEAT CHECK ──')
    promo_issues = check_promo_repeats(playlist, current_start)
    lines += promo_issues if promo_issues else ['  ✓ No repeated promos within same break']
    lines.append('')

    # [4] Not ingested
    lines.append('── [4] NOT INGESTED ASSETS ──')
    ni = check_not_ingested(playlist, current_start)
    lines += ni if ni else ['  ✓ All assets ingested']
    lines.append('')

    # [5] Bugs
    lines.append('── [5] BUGS ──')
    lines += check_bugs(playlist, current_start)
    lines.append('')

    # [6] Cue tones (full only)
    if pt == 'full':
        lines.append('── [6] CUE TONE REPORT ──')
        lines += check_cue_tones(playlist)
        lines.append('')

    lines.append(sep)
    return '\n'.join(lines)
