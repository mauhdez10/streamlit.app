"""
Broadcast Playlist Checker — Core Logic v2
"""
import json
import re
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from html.parser import HTMLParser


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

def normalize_id(ep_id):
    """Strip segment suffix, normalize extra leading zeros."""
    if not ep_id: return ''
    ep_id = re.sub(r'_\d+$', '', str(ep_id).strip())
    ep_id = re.sub(r'([A-Za-z]+)0+(\d{4})$',
                   lambda m: m.group(1) + m.group(2), ep_id)
    return ep_id.upper()

def show_prefix(ep_id):
    """Return alpha prefix: LUNA0209 -> LUNA, COSA0327 -> COSA"""
    m = re.match(r'^([A-Za-z]+)', ep_id)
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

    programs = []
    commercials = []
    promos = []
    cue_tones = []
    breaks = []
    missing_assets = []
    current_break = []
    last_program = None

    for ev in events:
        ev_assets = ev.get('assets', [])
        ev_start  = parse_timecode(ev.get('startTime', ''))
        ev_dur    = parse_duration(ev.get('duration', ''))
        ev_name   = ev.get('name', '')
        ev_ref    = ev.get('reference', '')
        behaviors = ev.get('behaviors', [])

        # Cue tones
        for b in behaviors:
            if b.get('name') == 'CUEON' and not b.get('disabled', True):
                ct_asset = ev_assets[0].get('reference', ev_name) if ev_assets else ev_name
                cue_tones.append({'ref': ev_ref, 'name': ev_name,
                                  'ct_id': ct_asset, 'start': ev_start})

        for asset in ev_assets:
            atype = asset.get('type', '')
            aref  = asset.get('reference', '')
            tcin  = asset.get('tcIn', '')

            if atype in ('Program', 'live'):
                # Flush break
                if current_break:
                    breaks.append({'after_program': last_program,
                                   'items': current_break[:]})
                    current_break = []

                seg_m  = re.search(r'_(\d+)$', aref)
                seg    = int(seg_m.group(1)) if seg_m else 1
                ep_id  = normalize_id(aref)

                # Missing asset detection: 07: tcIn on a recorded program
                # (live shows legitimately have 00: tcIn)
                is_missing = (atype == 'Program' and tcin.startswith('07:'))

                prog = {
                    'episode_id': ep_id,
                    'episode_id_raw': aref,
                    'seg_num': seg,
                    'start': ev_start,
                    'duration': ev_dur,
                    'name': ev_name,
                    'ref': ev_ref,
                    'asset_type': atype,
                    'is_missing': is_missing
                }
                programs.append(prog)
                last_program = ep_id

                if is_missing and seg == 1:
                    missing_assets.append({'episode_id': ep_id,
                                           'start': ev_start,
                                           'name': ev_name})

            elif atype == 'Commercial':
                commercials.append({
                    'asset_ref': aref, 'name': ev_name,
                    'start': ev_start, 'duration': ev_dur, 'ref': ev_ref
                })
                current_break.append({'type': 'Commercial', 'ref': aref,
                                      'start': ev_start})

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
        'cue_tones': cue_tones, 'missing_assets': missing_assets
    }


# ── XML TRAFFIC LOG PARSER ────────────────────────────────────────────────────

def parse_xml_log(filepath_or_bytes):
    """Parse Vipe XML traffic log. Returns list of dicts."""
    try:
        if isinstance(filepath_or_bytes, (str, bytes)):
            if isinstance(filepath_or_bytes, bytes):
                root = ET.fromstring(filepath_or_bytes)
            else:
                tree = ET.parse(filepath_or_bytes)
                root = tree.getroot()
        else:
            content = filepath_or_bytes.read()
            root = ET.fromstring(content)

        traffic = root.find('traffic')
        if traffic is None:
            traffic = root

        items = []
        for item in traffic.findall('item'):
            items.append({
                'mediaid': item.get('mediaid', ''),
                'name': item.findtext('n', '').strip(),
                'description': item.findtext('description', '').strip(),
                'contenttype': item.findtext('contenttype', '').strip(),
                'startat': item.findtext('startat', '').strip(),
                'duration': item.findtext('duration', '').strip(),
                'externalid': item.findtext('externalid', '').strip(),
            })
        return items
    except Exception as e:
        return []

def xml_commercials(rows):
    return [r for r in rows if r.get('contenttype', '') == 'COMMERCIAL']

def xml_programs(rows):
    ct = r.get('contenttype', '') 
    return [r for r in rows if r.get('contenttype','') in
            ('PROGRAM_BEGIN', 'PROGRAM_SEGMENT')]


# ── GRILLA PARSER ─────────────────────────────────────────────────────────────

def parse_grilla(filepath_or_bytes, target_date):
    from openpyxl import load_workbook
    import io

    if isinstance(filepath_or_bytes, (str,)):
        wb = load_workbook(filepath_or_bytes, read_only=True)
    else:
        data = filepath_or_bytes.read() if hasattr(filepath_or_bytes, 'read') else filepath_or_bytes
        wb = load_workbook(io.BytesIO(data), read_only=True)

    ws = wb.active
    all_rows = list(ws.iter_rows(values_only=True))
    if len(all_rows) < 2:
        return []

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

    def extract_ids(val):
        if not val or not isinstance(val, str) or '=' in val:
            return []
        val = val.strip()
        if re.match(r'^[A-Z0-9]{2,}[0-9]{3,}$', val) and len(val) <= 15:
            return [normalize_id(val)]
        matches = re.findall(r'\b([A-Z][A-Z0-9]{1,}[0-9]{4})\b', val)
        return [normalize_id(m) for m in matches]

    seen = set()
    episode_ids = []
    for row in all_rows[2:]:
        val = row[target_col] if target_col < len(row) else None
        for ep in extract_ids(val):
            if ep and ep not in seen:
                seen.add(ep)
                episode_ids.append(ep)
    return episode_ids


# ── CHECKS ────────────────────────────────────────────────────────────────────

def check_programs_vs_grilla(playlist, grilla_ids, current_start=None):
    """
    Compare programs in playlist vs grilla.
    - Missing: in grilla, never appears in full playlist
    - Extra: in playlist (from current_start), not in grilla
    - ID Mismatch: same show prefix, different date code
    - Order: only checked for full playlists
    """
    issues = []
    all_programs  = playlist['programs']   # full day (for missing check)
    full_ids      = set(p['episode_id'] for p in all_programs)

    # Filtered playlist (from current_start)
    filtered = [p for p in all_programs
                if not current_start or (p['start'] and p['start'] >= current_start)]
    seen = set()
    filtered_eps = []
    for p in filtered:
        ep = p['episode_id']
        if ep not in seen:
            seen.add(ep)
            filtered_eps.append({'id': ep, 'start': p['start'], 'name': p['name']})
    filtered_set = set(f['id'] for f in filtered_eps)

    grilla_set    = set(grilla_ids)
    grilla_prefix = defaultdict(list)   # prefix -> [grilla IDs]
    for gid in grilla_ids:
        grilla_prefix[show_prefix(gid)].append(gid)

    # --- Missing from playlist (in grilla but NOT in any part of full playlist)
    for gid in grilla_ids:
        if gid not in full_ids:
            # Check if it's an ID mismatch (same prefix exists in filtered playlist)
            prefix = show_prefix(gid)
            pl_same_prefix = [f for f in filtered_eps
                              if show_prefix(f['id']) == prefix and f['id'] != gid]
            if pl_same_prefix:
                match = pl_same_prefix[0]
                issues.append(
                    f'  ⚠  ID MISMATCH: Grilla={gid} | Playlist={match["id"]}'
                    f' @ {fmt_time(match["start"])} UTC'
                )
            else:
                issues.append(f'  ✗  MISSING FROM PLAYLIST: {gid}')

    # --- Extra in playlist (from current_start, not in grilla)
    for f in filtered_eps:
        if f['id'] not in grilla_set:
            prefix = show_prefix(f['id'])
            grilla_same = [g for g in grilla_ids if show_prefix(g) == prefix and g != f['id']]
            if not grilla_same:  # already caught as mismatch above
                issues.append(
                    f'  ✗  EXTRA IN PLAYLIST: {f["id"]}'
                    f' @ {fmt_time(f["start"])} UTC (not in grilla)'
                )

    # --- Order check (full playlist only)
    if current_start is None:
        pl_ordered = [f['id'] for f in filtered_eps if f['id'] in grilla_set]
        gr_ordered = [g for g in grilla_ids if g in filtered_set]
        if pl_ordered != gr_ordered:
            for i, (pl, gr) in enumerate(zip(pl_ordered, gr_ordered)):
                if pl != gr:
                    issues.append(
                        f'  ⚠  ORDER MISMATCH pos {i+1}: Grilla expects {gr}, Playlist has {pl}'
                    )
    else:
        issues.append(
            '  ℹ  Order check skipped (partial playlist — run full-day JSON for order validation)'
        )

    return issues


def check_commercials_vs_xml(playlist, xml_rows, current_start=None):
    """
    Compare commercial sequence: JSON playlist vs XML log (order-based, not time-based).
    Time differences are expected due to live show segment length variations.
    """
    issues = []

    pl_comms = playlist['commercials']
    if current_start:
        pl_comms = [c for c in pl_comms if c['start'] and c['start'] >= current_start]

    pl_ids  = [c['asset_ref'] for c in pl_comms]
    xml_ids = [r['mediaid'] for r in xml_commercials(xml_rows)]

    # If partial, trim XML to start from first matching commercial
    if current_start and pl_ids and xml_ids:
        try:
            start_idx = xml_ids.index(pl_ids[0])
            xml_ids = xml_ids[start_idx:]
        except ValueError:
            pass

    pl_set  = Counter(pl_ids)
    xml_set = Counter(xml_ids)
    all_refs = set(pl_set.keys()) | set(xml_set.keys())

    diffs = []
    for ref in sorted(all_refs):
        pc = pl_set.get(ref, 0)
        xc = xml_set.get(ref, 0)
        if pc != xc:
            if pc == 0:
                diffs.append(f'  ✗  IN XML NOT IN PLAYLIST: {ref} ({xc}x in XML log)')
            elif xc == 0:
                diffs.append(f'  ✗  IN PLAYLIST NOT IN XML: {ref} ({pc}x in playlist)')
            else:
                diffs.append(f'  ⚠  COUNT DIFF: {ref} | XML={xc}x | Playlist={pc}x')

    if not diffs:
        issues.append(f'  ✓ All {len(pl_ids)} commercials match XML log')
    else:
        issues.extend(diffs)
    return issues


def check_promo_repeats(playlist, current_start=None):
    issues = []
    for brk in playlist['breaks']:
        items = brk['items']
        if not items:
            continue
        break_start = next((i['start'] for i in items if i.get('start')), None)
        if current_start and break_start and break_start < current_start:
            continue
        promo_refs = [i['ref'] for i in items if i['type'] == 'Promotion']
        counts = Counter(promo_refs)
        dups = {r: c for r, c in counts.items() if c > 1}
        if dups:
            after = brk.get('after_program', '?')
            for ref, cnt in dups.items():
                issues.append(
                    f'  ⚠  PROMO REPEAT after [{after}]'
                    f' @ {fmt_time(break_start)} UTC: {ref} {cnt}x'
                )
    return issues


def check_missing_assets(playlist, current_start=None):
    """Programs with 07: tcIn = not ingested into playout system."""
    lines = []
    seen = set()
    for item in playlist['missing_assets']:
        ep = item['episode_id']
        if ep in seen:
            continue
        seen.add(ep)
        if current_start and item['start'] and item['start'] < current_start:
            continue
        lines.append(
            f'  ⚠  NOT INGESTED: {ep}'
            f' @ {fmt_time(item["start"])} UTC | {item["name"]}'
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


# ── REPORT ────────────────────────────────────────────────────────────────────

def generate_report(channel, playlist, xml_rows, grilla_ids):
    lines = []
    sep = '═' * 60

    pt       = playlist['type']
    date_str = str(playlist['date']) if playlist['date'] else 'Unknown'
    progs    = playlist['programs']
    first_start = progs[0]['start'] if progs else None

    current_start = first_start if pt == 'current' else None

    lines.append(sep)
    lines.append(f'CHANNEL: {channel.upper()}')
    lines.append(f'DATE: {date_str}')
    lines.append(f'PLAYLIST TYPE: {"FULL DAY" if pt == "full" else "CURRENT (partial)"}')
    if pt == 'current':
        lines.append(f'CHECKING FROM: {fmt_time(current_start)} UTC to end of day')
    lines.append(sep)

    unique_progs = len(set(p['episode_id'] for p in progs
                           if not current_start or (p['start'] and p['start'] >= current_start)))
    total_comms  = len([c for c in playlist['commercials']
                        if not current_start or (c['start'] and c['start'] >= current_start)])
    lines.append(f'SUMMARY: {unique_progs} programs | {total_comms} commercials')
    lines.append('')

    # [1] Programs vs Grilla
    lines.append('── [1] PROGRAM CHECK (Playlist vs Grilla) ──')
    if not grilla_ids:
        lines.append('  ! Grilla not provided')
    else:
        prog_issues = check_programs_vs_grilla(playlist, grilla_ids, current_start)
        if not [i for i in prog_issues if i.startswith('  ✗') or i.startswith('  ⚠  ID') or i.startswith('  ⚠  ORDER')]:
            lines.append('  ✓ All programs match grilla')
        for issue in prog_issues:
            lines.append(issue)
    lines.append('')

    # [2] Commercials vs XML
    lines.append('── [2] COMMERCIAL CHECK (Playlist vs XML log) ──')
    if not xml_rows:
        lines.append('  ! XML log not provided')
    else:
        comm_issues = check_commercials_vs_xml(playlist, xml_rows, current_start)
        for issue in comm_issues:
            lines.append(issue)
    lines.append('')

    # [3] Promo repeats
    lines.append('── [3] PROMO REPEAT CHECK ──')
    promo_issues = check_promo_repeats(playlist, current_start)
    if not promo_issues:
        lines.append('  ✓ No repeated promos within same break')
    else:
        for issue in promo_issues:
            lines.append(issue)
    lines.append('')

    # [4] Missing assets (not ingested)
    lines.append('── [4] MISSING / NOT INGESTED ASSETS ──')
    missing_lines = check_missing_assets(playlist, current_start)
    if not missing_lines:
        lines.append('  ✓ All program assets ingested')
    else:
        for ml in missing_lines:
            lines.append(ml)
    lines.append('')

    # [5] Cue tones (full only)
    if pt == 'full':
        lines.append('── [5] CUE TONE REPORT ──')
        for cl in check_cue_tones(playlist):
            lines.append(cl)
        lines.append('')

    # [6] Missing summary (programs only — what's in grilla but never in playlist)
    lines.append('── [6] MISSING PROGRAMS SUMMARY ──')
    if not grilla_ids:
        lines.append('  ! Grilla not provided')
    else:
        full_ids = set(p['episode_id'] for p in playlist['programs'])
        truly_missing = []
        for gid in grilla_ids:
            if gid not in full_ids:
                prefix = show_prefix(gid)
                has_mismatch = any(
                    show_prefix(p['episode_id']) == prefix and p['episode_id'] != gid
                    for p in playlist['programs']
                )
                if has_mismatch:
                    truly_missing.append(f'  ⚠  WRONG EPISODE: {gid} not found (different episode of same show exists)')
                else:
                    truly_missing.append(f'  ✗  NOT IN PLAYLIST: {gid}')
        if not truly_missing:
            lines.append('  ✓ No programs missing from playlist')
        else:
            for tm in truly_missing:
                lines.append(tm)
    lines.append('')
    lines.append(sep)
    return '\n'.join(lines)
