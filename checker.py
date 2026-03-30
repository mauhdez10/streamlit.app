"""
Broadcast Playlist Checker — Core Logic v3
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

def is_episode_id(val):
    """
    Detect episode IDs like COSA0327, NS0326, INM197R, T2SV0327, RYM816, VD0329.
    Rule: starts with a letter, contains at least 3 digits, length 3-16, no spaces.
    """
    if not val or not isinstance(val, str): return False
    val = val.strip()
    if ' ' in val or len(val) < 3 or len(val) > 16: return False
    if not re.match(r'^[A-Z]', val): return False
    return len(re.findall(r'\d', val)) >= 3

def normalize_id(ep_id):
    """Strip segment suffix _N, normalize extra leading zeros before 4-digit date."""
    if not ep_id: return ''
    ep_id = re.sub(r'_\d+$', '', str(ep_id).strip())
    # Remove extra zeros: COSA00327 -> COSA0327
    ep_id = re.sub(r'([A-Za-z][A-Za-z0-9]*)0{2,}(\d{3,})',
                   lambda m: m.group(1) + m.group(2)[-4:] if len(m.group(2)) > 4 else m.group(1) + m.group(2),
                   ep_id)
    return ep_id.upper()

def show_prefix(ep_id):
    """Alpha prefix only: LUNA0209 -> LUNA, T2SV0327 -> T (too short, skip)
    Use first 2+ consecutive letters at start."""
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

    programs    = []
    commercials = []
    promos      = []
    cue_tones   = []
    breaks      = []
    not_ingested = []      # all items (any type) with 07: tcIn
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
                ct_asset = ev_assets[0].get('reference', ev_name) if ev_assets else ev_name
                cue_tones.append({'ref': ev_ref, 'name': ev_name,
                                  'ct_id': ct_asset, 'start': ev_start})

        for asset in ev_assets:
            atype = asset.get('type', '')
            aref  = asset.get('reference', '')
            tcin  = asset.get('tcIn', '')

            # Not ingested = 07: tcIn on a recorded asset (live legitimately uses 00:)
            # Applies to ALL asset types
            if tcin.startswith('07:') and atype != 'live':
                not_ingested.append({
                    'asset_ref': aref,
                    'name': ev_name,
                    'type': atype,
                    'start': ev_start,
                    'ref': ev_ref
                })

            if atype in ('Program', 'live'):
                if current_break:
                    breaks.append({'after_program': last_program,
                                   'items': current_break[:]})
                    current_break = []

                seg_m  = re.search(r'_(\d+)$', aref)
                seg    = int(seg_m.group(1)) if seg_m else 1
                ep_id  = normalize_id(aref)
                is_missing = (atype == 'Program' and tcin.startswith('07:'))

                programs.append({
                    'episode_id': ep_id, 'episode_id_raw': aref,
                    'seg_num': seg, 'start': ev_start, 'duration': ev_dur,
                    'name': ev_name, 'ref': ev_ref,
                    'asset_type': atype, 'is_missing': is_missing
                })
                last_program = ep_id

            elif atype == 'Commercial':
                commercials.append({
                    'asset_ref': aref, 'name': ev_name,
                    'start': ev_start, 'duration': ev_dur, 'ref': ev_ref
                })
                current_break.append({'type': 'Commercial', 'ref': aref, 'start': ev_start})

            elif atype == 'Promotion':
                promos.append({'asset_ref': aref, 'name': ev_name,
                               'start': ev_start, 'ref': ev_ref})
                current_break.append({'type': 'Promotion', 'ref': aref, 'start': ev_start})

    if current_break:
        breaks.append({'after_program': last_program, 'items': current_break})

    return {
        'type': playlist_type, 'date': date, 'events': events,
        'programs': programs, 'commercials': commercials,
        'promos': promos, 'breaks': breaks,
        'cue_tones': cue_tones, 'not_ingested': not_ingested
    }


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
    except Exception as e:
        return []

def xml_commercials(rows):
    return [r for r in rows if r.get('contenttype') == 'COMMERCIAL']

def xml_programs(rows):
    return [r for r in rows if r.get('contenttype') in
            ('PROGRAM_BEGIN', 'PROGRAM_SEGMENT')]


# ── GRILLA PARSER ─────────────────────────────────────────────────────────────

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

    # Find target column (Mon=2 … Sun=8)
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
        """Extract episode IDs from a cell — standalone or embedded in show name."""
        if not val or not isinstance(val, str) or '=' in val:
            return []
        val = val.strip()
        if is_episode_id(val):
            return [normalize_id(val)]
        # Scan for embedded IDs
        tokens = re.findall(r'[A-Z0-9]+', val.upper())
        return [normalize_id(t) for t in tokens if is_episode_id(t)]

    seen = set()
    episode_ids = []
    for row in all_rows[2:]:
        val = row[target_col] if target_col < len(row) else None
        for ep in extract_ids(val):
            if ep and ep not in seen:
                seen.add(ep)
                episode_ids.append(ep)
    return episode_ids


# ── PARTIAL POSITION FINDER ───────────────────────────────────────────────────

def find_partial_anchor(playlist_eps, grilla_ids):
    """
    For a partial playlist, find where in grilla_ids the playlist starts.
    Returns (anchor_index, first_matched_id) or (0, None) if not found.
    """
    if not playlist_eps or not grilla_ids:
        return 0, None

    # Try first 3 playlist programs to find anchor (first might be mid-show)
    for pep in playlist_eps[:3]:
        for i, gid in enumerate(grilla_ids):
            if gid == pep['id']:
                return i, gid
            # Same show prefix different episode
            if show_prefix(gid) == show_prefix(pep['id']) and show_prefix(gid):
                return i, gid
    return 0, None


# ── CHECKS ────────────────────────────────────────────────────────────────────

def check_programs_vs_grilla(playlist, grilla_ids, current_start=None):
    issues = []
    is_partial = (current_start is not None)

    # All unique programs in full day playlist
    all_eps = {}
    for p in playlist['programs']:
        ep = p['episode_id']
        if ep not in all_eps:
            all_eps[ep] = p

    # Programs from current_start onwards (partial view)
    seen = set()
    filtered_eps = []
    for p in playlist['programs']:
        if current_start and p['start'] and p['start'] < current_start:
            continue
        ep = p['episode_id']
        if ep not in seen:
            seen.add(ep)
            filtered_eps.append({'id': ep, 'start': p['start'], 'name': p['name']})
    filtered_set = {f['id'] for f in filtered_eps}

    # For partial: anchor grilla to where playlist starts
    if is_partial:
        anchor_idx, anchor_id = find_partial_anchor(filtered_eps, grilla_ids)
        if anchor_id:
            grilla_slice = grilla_ids[anchor_idx:]
            issues.append(f'  ℹ  Playlist anchored at grilla position {anchor_idx+1}: {anchor_id}')
        else:
            grilla_slice = grilla_ids
            issues.append('  ℹ  Could not anchor to grilla — showing full grilla comparison')
    else:
        grilla_slice = grilla_ids

    grilla_slice_set = set(grilla_slice)

    # Build prefix maps
    grilla_prefix = defaultdict(list)
    for gid in grilla_slice:
        pfx = show_prefix(gid)
        if pfx:
            grilla_prefix[pfx].append(gid)

    pl_prefix = defaultdict(list)
    for f in filtered_eps:
        pfx = show_prefix(f['id'])
        if pfx:
            pl_prefix[pfx].append(f)

    # --- Check each grilla entry
    reported = set()
    for gid in grilla_slice:
        if gid in reported:
            continue
        pfx = show_prefix(gid)

        if gid in filtered_set:
            reported.add(gid)
            continue  # Match

        # Same prefix in playlist?
        same_pfx_pl = pl_prefix.get(pfx, [])
        if same_pfx_pl:
            match = same_pfx_pl[0]
            issues.append(
                f'  ⚠  WRONG EPISODE: Grilla expects {gid} | '
                f'Playlist has {match["id"]} @ {fmt_time(match["start"])} UTC'
            )
            reported.add(gid)
        else:
            # Check if it aired before current_start (partial context)
            if is_partial and gid in all_eps:
                issues.append(
                    f'  ℹ  ALREADY AIRED: {gid} (aired before check window)'
                )
            elif is_partial and gid not in {p["episode_id"] for p in playlist["programs"]}:
                issues.append(f'  ✗  NOT IN PLAYLIST: {gid}')
            elif not is_partial:
                issues.append(f'  ✗  NOT IN PLAYLIST: {gid}')
            reported.add(gid)

    # --- Extra in filtered playlist (not in grilla slice at all)
    for f in filtered_eps:
        if f['id'] not in grilla_slice_set:
            pfx = show_prefix(f['id'])
            grilla_same = [g for g in grilla_slice if show_prefix(g) == pfx and g != f['id']]
            if not grilla_same:  # not already caught as wrong episode
                issues.append(
                    f'  ✗  EXTRA IN PLAYLIST: {f["id"]}'
                    f' @ {fmt_time(f["start"])} UTC (not in grilla)'
                )

    # --- Order check
    pl_ord = [f['id'] for f in filtered_eps if f['id'] in grilla_slice_set]
    gr_ord = [g for g in grilla_slice if g in filtered_set]

    if pl_ord != gr_ord:
        mismatches = 0
        for i, (pl, gr) in enumerate(zip(pl_ord, gr_ord)):
            if pl != gr and mismatches < 5:
                issues.append(
                    f'  ⚠  ORDER pos {i+1}: Grilla={gr} | Playlist={pl}'
                )
                mismatches += 1
        if mismatches == 0 and len(pl_ord) != len(gr_ord):
            issues.append(f'  ⚠  ORDER: count mismatch (grilla={len(gr_ord)}, playlist={len(pl_ord)})')

    if not issues:
        issues.append('  ✓ All programs match grilla')

    return issues


def check_commercials_vs_xml(playlist, xml_rows, current_start=None):
    issues = []

    pl_comms = playlist['commercials']
    if current_start:
        pl_comms = [c for c in pl_comms if c['start'] and c['start'] >= current_start]

    pl_ids  = [c['asset_ref'] for c in pl_comms]
    xml_ids = [r['mediaid'] for r in xml_commercials(xml_rows)]

    # Partial: anchor XML to first matching commercial
    if current_start and pl_ids and xml_ids:
        first_pl = pl_ids[0]
        try:
            start_idx = xml_ids.index(first_pl)
            xml_ids = xml_ids[start_idx:]
        except ValueError:
            pass  # can't anchor, use full list

    pl_set  = Counter(pl_ids)
    xml_set = Counter(xml_ids)
    all_refs = set(pl_set.keys()) | set(xml_set.keys())

    diffs = []
    for ref in sorted(all_refs):
        pc = pl_set.get(ref, 0)
        xc = xml_set.get(ref, 0)
        if pc != xc:
            if pc == 0:
                diffs.append(f'  ✗  IN XML, NOT IN PLAYLIST: {ref} ({xc}x in XML)')
            elif xc == 0:
                diffs.append(f'  ✗  IN PLAYLIST, NOT IN XML: {ref} ({pc}x in playlist)')
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
    """All assets with 07: tcIn — not yet loaded into playout system.
    Programs are grouped by episode ID (show once per show, not per segment).
    Promos/Commercials shown individually.
    """
    lines = []
    seen_eps = set()   # for programs: deduplicate by episode ID
    seen_other = set() # for promos/commercials: deduplicate by asset_ref

    for item in playlist['not_ingested']:
        if current_start and item['start'] and item['start'] < current_start:
            continue

        atype = item['type']
        aref  = item['asset_ref']

        if atype in ('Program', 'live'):
            ep_id = normalize_id(aref)
            if ep_id in seen_eps:
                continue
            seen_eps.add(ep_id)
            # Derive show name from the segment name (strip "[]: SEG. N")
            show_name = re.sub(r'\[\].*$', '', item['name']).strip()
            lines.append(
                f'  ⚠  NOT INGESTED [Program]: {ep_id}'
                f' @ {fmt_time(item["start"])} UTC | {show_name}'
            )
        else:
            if aref in seen_other:
                continue
            seen_other.add(aref)
            lines.append(
                f'  ⚠  NOT INGESTED [{atype}]: {aref}'
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

    pt          = playlist['type']
    date_str    = str(playlist['date']) if playlist['date'] else 'Unknown'
    progs       = playlist['programs']
    first_start = progs[0]['start'] if progs else None
    current_start = first_start if pt == 'current' else None

    lines += [sep,
              f'CHANNEL: {channel.upper()}',
              f'DATE: {date_str}',
              f'PLAYLIST TYPE: {"FULL DAY" if pt == "full" else "CURRENT (partial)"}']
    if pt == 'current':
        lines.append(f'CHECKING FROM: {fmt_time(current_start)} UTC to end of day')
    lines.append(sep)

    def count_from(items, field='start'):
        if not current_start: return len(items)
        return sum(1 for i in items if i.get(field) and i[field] >= current_start)

    unique_progs = len(set(
        p['episode_id'] for p in progs
        if not current_start or (p['start'] and p['start'] >= current_start)
    ))
    total_comms = count_from(playlist['commercials'])
    lines.append(f'SUMMARY: {unique_progs} programs | {total_comms} commercials')
    lines.append('')

    # [1] Programs
    lines.append('── [1] PROGRAM CHECK (Playlist vs Grilla) ──')
    if not grilla_ids:
        lines.append('  ! Grilla not provided')
    else:
        for issue in check_programs_vs_grilla(playlist, grilla_ids, current_start):
            lines.append(issue)
    lines.append('')

    # [2] Commercials
    lines.append('── [2] COMMERCIAL CHECK (Playlist vs XML log) ──')
    if not xml_rows:
        lines.append('  ! XML log not provided')
    else:
        for issue in check_commercials_vs_xml(playlist, xml_rows, current_start):
            lines.append(issue)
    lines.append('')

    # [3] Promo repeats
    lines.append('── [3] PROMO REPEAT CHECK ──')
    promo_issues = check_promo_repeats(playlist, current_start)
    lines += promo_issues if promo_issues else ['  ✓ No repeated promos within same break']
    lines.append('')

    # [4] Not ingested (all types)
    lines.append('── [4] NOT INGESTED ASSETS ──')
    ni_lines = check_not_ingested(playlist, current_start)
    lines += ni_lines if ni_lines else ['  ✓ All assets ingested']
    lines.append('')

    # [5] Cue tones (full only)
    if pt == 'full':
        lines.append('── [5] CUE TONE REPORT ──')
        lines += check_cue_tones(playlist)
        lines.append('')

    lines.append(sep)
    return '\n'.join(lines)
