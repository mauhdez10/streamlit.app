"""
Broadcast Playlist Checker — Core Logic v5
"""
import json, re, xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from datetime import datetime, timedelta


# ── TRANSLATION ───────────────────────────────────────────────────────────────

_S = {
    'section_programs':   {'en': 'PROGRAM CHECK (Playlist vs Grilla)',       'es': 'VERIFICACIÓN DE PROGRAMAS (Playlist vs Grilla)'},
    'section_commercials':{'en': 'COMMERCIAL CHECK (Playlist vs XML)',        'es': 'VERIFICACIÓN DE COMERCIALES (Playlist vs XML)'},
    'section_promos':     {'en': 'PROMO REPEAT CHECK',                        'es': 'PROMOS REPETIDAS'},
    'section_ingested':   {'en': 'NOT INGESTED ASSETS',                       'es': 'ACTIVOS NO INGESTADOS'},
    'section_bugs':       {'en': 'BUGS',                                       'es': 'BUGS'},
    'section_cues':       {'en': 'CUE TONE REPORT',                           'es': 'REPORTE DE CUE TONES'},
    'full_day':           {'en': 'FULL DAY',                                   'es': 'DÍA COMPLETO'},
    'partial':            {'en': 'CURRENT (partial)',                          'es': 'ACTUAL (parcial)'},
    'checking_from':      {'en': 'CHECKING FROM',                              'es': 'VERIFICANDO DESDE'},
    'channel':            {'en': 'CHANNEL',                                    'es': 'CANAL'},
    'date_lbl':           {'en': 'DATE',                                       'es': 'FECHA'},
    'type_lbl':           {'en': 'PLAYLIST TYPE',                              'es': 'TIPO DE PLAYLIST'},
    'summary':            {'en': 'SUMMARY',                                    'es': 'RESUMEN'},
    'show_blocks':        {'en': 'show blocks',                                'es': 'bloques de programa'},
    'commercials_lbl':    {'en': 'commercials',                                'es': 'comerciales'},
    'no_grilla':          {'en': '! Grilla not provided',                      'es': '! Grilla no proporcionada'},
    'no_xml':             {'en': '! XML log not provided',                     'es': '! Log XML no proporcionado'},
    'ok_programs':        {'en': '✓ All programs match grilla',                'es': '✓ Todos los programas coinciden con la grilla'},
    'ok_commercials':     {'en': '✓ All {n} commercials match XML log',        'es': '✓ Los {n} comerciales coinciden con el log XML'},
    'ok_promos':          {'en': '✓ No repeated promos within same break',     'es': '✓ Sin promos repetidas en el mismo break'},
    'ok_ingested':        {'en': '✓ All assets ingested',                      'es': '✓ Todos los activos están ingestados'},
    'ok_bugs':            {'en': '✓ No bugs scheduled',                        'es': '✓ Sin bugs programados'},
    'total_cues':         {'en': 'Total cue tones',                            'es': 'Total cue tones'},
    'anchored':           {'en': '  ℹ  Anchored at grilla position {i}: {id}', 'es': '  ℹ  Anclado en grilla pos {i}: {id}'},
    'already_aired':      {'en': '  ℹ  ALREADY AIRED: {id}',                  'es': '  ℹ  YA SE TRANSMITIÓ: {id}'},
    'wrong_ep':           {'en': '  ⚠  WRONG EPISODE: Grilla={g} | Playlist={p} @ {t}', 'es': '  ⚠  EPISODIO INCORRECTO: Grilla={g} | Playlist={p} @ {t}'},
    'not_in_pl':          {'en': '  ✗  NOT IN PLAYLIST: {id}',                'es': '  ✗  NO ESTÁ EN PLAYLIST: {id}'},
    'extra_pl':           {'en': '  ✗  EXTRA: {id} @ {t} (not in grilla)',     'es': '  ✗  EXTRA: {id} @ {t} (no está en grilla)'},
    'order_mis':          {'en': '  ⚠  ORDER pos {i}: Grilla={g} | Playlist={p}', 'es': '  ⚠  ORDEN pos {i}: Grilla={g} | Playlist={p}'},
    'xml_not_pl':         {'en': '  ✗  IN XML, NOT IN PLAYLIST: {ref} ({n}x in XML)', 'es': '  ✗  EN XML, NO EN PLAYLIST: {ref} ({n}x en XML)'},
    'pl_not_xml':         {'en': '  ✗  IN PLAYLIST, NOT IN XML: {ref} ({n}x in playlist)', 'es': '  ✗  EN PLAYLIST, NO EN XML: {ref} ({n}x en playlist)'},
    'count_diff':         {'en': '  ⚠  COUNT DIFF: {ref} | XML={xn}x | Playlist={pn}x', 'es': '  ⚠  DIFERENCIA: {ref} | XML={xn}x | Playlist={pn}x'},
    'promo_rep':          {'en': '  ⚠  PROMO REPEAT after [{after}] @ {t}: {ref} {n}x', 'es': '  ⚠  PROMO REPETIDA después de [{after}] @ {t}: {ref} {n}x'},
    'ni_program':         {'en': '  ⚠  NOT INGESTED [Program]: {id} @ {t} | {show}', 'es': '  ⚠  NO INGESTADO [Programa]: {id} @ {t} | {show}'},
    'ni_other':           {'en': '  ⚠  NOT INGESTED [{typ}]: {ref} @ {t} | {name}', 'es': '  ⚠  NO INGESTADO [{typ}]: {ref} @ {t} | {name}'},
    'bug_line':           {'en': '  🔲 {beh} Cmd:{cmd} — {id} @ {t} | {show}', 'es': '  🔲 {beh} Cmd:{cmd} — {id} @ {t} | {show}'},
}

def T(key, lang='en', **kwargs):
    s = _S.get(key, {}).get(lang, _S.get(key, {}).get('en', key))
    return s.format(**kwargs) if kwargs else s


# ── TIME HELPERS ──────────────────────────────────────────────────────────────

def _edt_start(year):
    """Second Sunday of March 2:00 AM."""
    m1 = datetime(year, 3, 1)
    d = (6 - m1.weekday()) % 7
    return datetime(year, 3, (m1 + timedelta(days=d+7)).day, 2)

def _est_start(year):
    """First Sunday of November 2:00 AM."""
    n1 = datetime(year, 11, 1)
    d = (6 - n1.weekday()) % 7
    return datetime(year, 11, (n1 + timedelta(days=d)).day, 2)

def utc_to_et(dt):
    if dt is None: return None
    edt = _edt_start(dt.year)
    est = _est_start(dt.year)
    offset = -4 if edt <= dt < est else -5
    tz = 'EDT' if offset == -4 else 'EST'
    return dt + timedelta(hours=offset), tz

def fmt_time(dt):
    """UTC only."""
    if dt is None: return '??:??:??'
    return dt.strftime('%H:%M:%S')

def fmt_t(dt):
    """UTC / ET string."""
    if dt is None: return '??:??:?? UTC'
    et, tz = utc_to_et(dt)
    return f'{dt.strftime("%H:%M:%S")} UTC / {et.strftime("%H:%M:%S")} {tz}'

def parse_timecode(tc):
    try:
        tc = tc.split(';')[0].split('@')[0].strip()
        return datetime.strptime(tc, '%Y-%m-%d %H:%M:%S')
    except: return None

def parse_duration(dur):
    try:
        dur = dur.split(';')[0].split('@')[0]
        h,m,s = dur.split(':')
        return int(h)*3600 + int(m)*60 + int(s)
    except: return 0

def parse_xml_time(ts):
    """XML startat: '15:40:00:00' → datetime (dummy date)."""
    try:
        p = ts.split(':')
        return datetime(2000, 1, 1, int(p[0]), int(p[1]), int(p[2]))
    except: return None


# ── ID HELPERS ────────────────────────────────────────────────────────────────

def is_episode_id(val):
    if not val or not isinstance(val, str): return False
    val = val.strip()
    if ' ' in val or len(val) < 3 or len(val) > 16: return False
    if not re.match(r'^[A-Z]', val): return False
    return len(re.findall(r'\d', val)) >= 3

def normalize_id(ep_id):
    if not ep_id: return ''
    ep_id = re.sub(r'_\d+$', '', str(ep_id).strip())
    ep_id = re.sub(r'([A-Za-z][A-Za-z0-9]*)0{2,}(\d{3,})',
                   lambda m: m.group(1) + (m.group(2)[-4:] if len(m.group(2)) > 4 else m.group(2)),
                   ep_id)
    return ep_id.upper()

def show_prefix(ep_id):
    m = re.match(r'^([A-Za-z]{2,})', ep_id)
    return m.group(1).upper() if m else ''


# ── JSON PARSER ───────────────────────────────────────────────────────────────

def parse_json_playlist(data):
    events = data.get('events', [])
    has_marker = any(a.get('type') == 'marker'
                     for ev in events[:3] for a in ev.get('assets', []))
    playlist_type = 'full' if has_marker else 'current'

    date = None
    for ev in events:
        dt = parse_timecode(ev.get('startTime', ''))
        if dt: date = dt.date(); break

    programs, commercials, promos, cue_tones, not_ingested, breaks = [], [], [], [], [], []
    current_break, last_program = [], None

    for ev in events:
        ev_assets = ev.get('assets', [])
        ev_start  = parse_timecode(ev.get('startTime', ''))
        ev_dur    = parse_duration(ev.get('duration', ''))
        ev_name   = ev.get('name', '')
        ev_ref    = ev.get('reference', '')
        behaviors = ev.get('behaviors', [])

        for b in behaviors:
            if b.get('name') == 'CUEON' and not b.get('disabled', True):
                ct = ev_assets[0].get('reference', ev_name) if ev_assets else ev_name
                cue_tones.append({'ref': ev_ref, 'name': ev_name, 'ct_id': ct, 'start': ev_start})

        for asset in ev_assets:
            atype = asset.get('type', '')
            aref  = asset.get('reference', '')
            tcin  = asset.get('tcIn', '')

            if tcin.startswith('07:') and atype != 'live':
                not_ingested.append({'asset_ref': aref, 'name': ev_name,
                                     'type': atype, 'start': ev_start, 'ref': ev_ref})

            if atype in ('Program', 'live'):
                if current_break:
                    breaks.append({'after_program': last_program, 'items': current_break[:]})
                    current_break = []
                seg_m = re.search(r'_(\d+)$', aref)
                seg   = int(seg_m.group(1)) if seg_m else 1
                ep_id = normalize_id(aref)
                programs.append({'episode_id': ep_id, 'episode_id_raw': aref,
                                  'seg_num': seg, 'start': ev_start, 'duration': ev_dur,
                                  'name': ev_name, 'ref': ev_ref, 'asset_type': atype,
                                  'is_missing': (atype == 'Program' and tcin.startswith('07:'))})
                last_program = ep_id

            elif atype == 'Commercial':
                commercials.append({'asset_ref': aref, 'name': ev_name,
                                    'start': ev_start, 'duration': ev_dur,
                                    'ref': ev_ref})
                current_break.append({'type': 'Commercial', 'ref': aref,
                                      'start': ev_start, 'event_ref': ev_ref})
            elif atype == 'Promotion':
                promos.append({'asset_ref': aref, 'name': ev_name, 'start': ev_start, 'ref': ev_ref})
                current_break.append({'type': 'Promotion', 'ref': aref, 'start': ev_start})

    if current_break:
        breaks.append({'after_program': last_program, 'items': current_break})

    return {'type': playlist_type, 'date': date, 'events': events,
            'programs': programs, 'commercials': commercials, 'promos': promos,
            'breaks': breaks, 'cue_tones': cue_tones, 'not_ingested': not_ingested}


def build_show_sequence(programs, from_start=None):
    seq, prev = [], None
    for p in programs:
        if from_start and p['start'] and p['start'] < from_start: continue
        ep = p['episode_id']
        if ep != prev:
            seq.append({'id': ep, 'start': p['start'], 'raw': p['episode_id_raw']})
            prev = ep
    return seq


# ── XML PARSER ────────────────────────────────────────────────────────────────

def parse_xml_log(filepath_or_bytes):
    try:
        if hasattr(filepath_or_bytes, 'read'): content = filepath_or_bytes.read()
        elif isinstance(filepath_or_bytes, bytes): content = filepath_or_bytes
        else:
            with open(filepath_or_bytes, 'rb') as f: content = f.read()
        root = ET.fromstring(content)
        traffic = root.find('traffic')
        if traffic is None: traffic = root
        return [{'mediaid': i.get('mediaid',''), 'name': i.findtext('n','').strip(),
                 'contenttype': i.findtext('contenttype','').strip().upper(),
                 'startat': i.findtext('startat','').strip(),
                 'externalid': i.findtext('externalid','').strip()}
                for i in traffic.findall('item')]
    except: return []

def xml_commercials(rows):
    return [r for r in rows if r.get('contenttype') == 'COMMERCIAL']

def find_xml_anchor(programs, xml_rows, current_start):
    """
    Find anchor in XML using first program segment from partial JSON.
    Matches by segment raw ID + closest time-of-day in XML.
    Returns start index in xml_rows for commercial comparison.
    """
    if not current_start: return 0
    first_seg = next((p for p in programs if p['start'] and p['start'] >= current_start), None)
    if not first_seg: return 0

    seg_raw = first_seg['episode_id_raw']   # e.g. MANGU0330_6
    seg_tod = first_seg['start'].hour*3600 + first_seg['start'].minute*60 + first_seg['start'].second

    candidates = []
    for i, row in enumerate(xml_rows):
        if row['mediaid'] == seg_raw:
            xt = parse_xml_time(row['startat'])
            if xt:
                xt_tod = xt.hour*3600 + xt.minute*60 + xt.second
                candidates.append((i, abs(xt_tod - seg_tod)))

    if not candidates:
        # Fallback: externalid match on first JSON event
        ext_idx = {r['externalid']: i for i, r in enumerate(xml_rows)}
        for ev in programs[:3]:
            # Find any event ref from the playlist events (stored in seg raw)
            pass
        return 0

    best_idx = min(candidates, key=lambda x: x[1])[0]
    return best_idx


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
    if len(all_rows) < 2: return []

    header_row = all_rows[1]
    target_col = None
    try:
        monday_val = header_row[2]
        monday = monday_val.date() if hasattr(monday_val, 'date') else None
        if monday:
            for offset in range(7):
                if monday + timedelta(days=offset) == target_date:
                    target_col = 2 + offset; break
    except: pass
    if target_col is None: target_col = 2 + target_date.weekday()

    def resolve_cell(val):
        if not val or not isinstance(val, str): return val
        m = re.match(r'^=([A-Z]+)(\d+)$', val.strip())
        if not m: return val
        col_str, row_num = m.group(1), int(m.group(2))
        col_idx = sum((ord(c)-ord('A')+1) * (26**i) for i,c in enumerate(reversed(col_str))) - 1
        row_idx = row_num - 1
        if row_idx < len(all_rows) and col_idx < len(all_rows[row_idx]):
            return all_rows[row_idx][col_idx]
        return val

    def extract_ids(val):
        val = resolve_cell(val)
        if not val or not isinstance(val, str) or val.startswith('='): return []
        val = val.strip()
        if is_episode_id(val): return [normalize_id(val)]
        tokens = re.findall(r'[A-Z0-9]+', val.upper())
        return [normalize_id(t) for t in tokens if is_episode_id(t)]

    episode_ids = []
    for row in all_rows[2:]:
        val = row[target_col] if target_col < len(row) else None
        for ep in extract_ids(val):
            if ep: episode_ids.append(ep)
    return episode_ids


# ── FILE DETECTION ────────────────────────────────────────────────────────────

def extract_date_from_filename(name):
    # YYYYMMDD (JSON files: ..._20260330_...)
    m = re.search(r'(\d{4})(\d{2})(\d{2})', name)
    if m:
        try: return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))).date()
        except: pass
    # MMDDYYYY (XML files: TVD03302026.xml, CA03302026.xml)
    m = re.search(r'(\d{2})(\d{2})(\d{4})', name)
    if m:
        try: return datetime(int(m.group(3)), int(m.group(1)), int(m.group(2))).date()
        except: pass
    return None

def detect_files(uploaded_files):
    """
    Group uploaded files by (date, channel).
    Grillas are separate (one per channel, cover whole week).
    Returns: days dict, grillas dict, unknown list.
    """
    days    = {}   # {(date_str, channel): {'json': [files], 'xml': file, 'date': date}}
    grillas = {}   # {channel: file}
    unknown = []

    for f in uploaded_files:
        name_up = f.name.upper()
        ext = f.name.lower().rsplit('.', 1)[-1] if '.' in f.name else ''

        if ext == 'json':       ftype = 'json'
        elif ext == 'xml':      ftype = 'xml'
        elif ext in ('xlsx','xlsm'): ftype = 'grilla'
        else: unknown.append(f); continue

        # Channel detection
        if ext == 'xml':
            if name_up.startswith('CA'):    channel = 'catv'
            elif name_up.startswith('TVD'): channel = 'tvd'
            else: unknown.append(f); continue
        else:
            if 'CATV' in name_up:   channel = 'catv'
            elif 'TVD' in name_up:  channel = 'tvd'
            else: unknown.append(f); continue

        if ftype == 'grilla':
            grillas[channel] = f
            continue

        date = extract_date_from_filename(f.name)
        if date is None: unknown.append(f); continue

        key = (str(date), channel)
        if key not in days:
            days[key] = {'json': [], 'xml': None, 'date': date}
        if ftype == 'json':  days[key]['json'].append(f)
        elif ftype == 'xml': days[key]['xml'] = f

    return days, grillas, unknown


# ── CHECKS ────────────────────────────────────────────────────────────────────

def check_programs_vs_grilla(playlist, grilla_ids, current_start, lang):
    issues = []
    is_partial = (current_start is not None)
    full_seq = build_show_sequence(playlist['programs'])
    part_seq = build_show_sequence(playlist['programs'], from_start=current_start)

    if is_partial and part_seq:
        anchor, first_id = 0, part_seq[0]['id']
        first_pfx = show_prefix(first_id)
        for i, gid in enumerate(grilla_ids):
            if gid == first_id or (first_pfx and show_prefix(gid) == first_pfx):
                anchor = i; break
        issues.append(T('anchored', lang, i=anchor+1, id=grilla_ids[anchor] if grilla_ids else '?'))
        grilla_slice = grilla_ids[anchor:]
    else:
        grilla_slice = grilla_ids

    pl_set     = {p['id'] for p in part_seq}
    grilla_set = set(grilla_slice)
    reported   = set()

    for gid in grilla_slice:
        if gid in pl_set or gid in reported: continue
        pfx = show_prefix(gid)
        pl_same = [p for p in part_seq if show_prefix(p['id']) == pfx and p['id'] != gid]
        if pl_same:
            issues.append(T('wrong_ep', lang, g=gid, p=pl_same[0]['id'], t=fmt_t(pl_same[0]['start'])))
        elif is_partial and any(p['episode_id'] == gid for p in playlist['programs']):
            issues.append(T('already_aired', lang, id=gid))
        else:
            issues.append(T('not_in_pl', lang, id=gid))
        reported.add(gid)

    for p in part_seq:
        if p['id'] not in grilla_set:
            pfx = show_prefix(p['id'])
            if not [g for g in grilla_slice if show_prefix(g) == pfx and g != p['id']]:
                issues.append(T('extra_pl', lang, id=p['id'], t=fmt_t(p['start'])))

    pl_ord = [p['id'] for p in part_seq if p['id'] in grilla_set]
    gr_ord = [g for g in grilla_slice if g in pl_set]
    n_mis  = 0
    for i, (pl, gr) in enumerate(zip(pl_ord, gr_ord)):
        if pl != gr and n_mis < 6:
            issues.append(T('order_mis', lang, i=i+1, g=gr, p=pl))
            n_mis += 1

    has_errors = any(x.strip().startswith(('✗','⚠  W','⚠  O','⚠  E')) for x in issues)
    if not has_errors:
        ok_line = T('ok_programs', lang)
        issues.append(f'  {ok_line}')
    return issues


def check_commercials_vs_xml(playlist, xml_rows, current_start, lang):
    all_comms = playlist['commercials']
    anchor = find_xml_anchor(playlist['programs'], xml_rows, current_start) if current_start else 0
    xml_rows_use = xml_rows[anchor:]
    pl_comms = [c for c in all_comms if not current_start or (c['start'] and c['start'] >= current_start)]

    pl_set  = Counter(c['asset_ref'] for c in pl_comms)
    xml_set = Counter(r['mediaid'] for r in xml_commercials(xml_rows_use))

    diffs = []
    for ref in sorted(set(pl_set) | set(xml_set)):
        pc, xc = pl_set.get(ref,0), xml_set.get(ref,0)
        if pc != xc:
            if pc == 0: diffs.append(T('xml_not_pl', lang, ref=ref, n=xc))
            elif xc == 0: diffs.append(T('pl_not_xml', lang, ref=ref, n=pc))
            else: diffs.append(T('count_diff', lang, ref=ref, xn=xc, pn=pc))

    if not diffs:
        return [f'  {T("ok_commercials", lang, n=len(pl_comms))}']
    return diffs


def check_promo_repeats(playlist, current_start=None, lang='en'):
    issues = []
    for brk in playlist['breaks']:
        items = brk['items']
        if not items: continue
        bs = next((i['start'] for i in items if i.get('start')), None)
        if current_start and bs and bs < current_start: continue
        promo_refs = [i['ref'] for i in items if i['type'] == 'Promotion']
        for ref, cnt in Counter(promo_refs).items():
            if cnt > 1:
                issues.append(T('promo_rep', lang, after=brk.get('after_program','?'),
                                t=fmt_t(bs), ref=ref, n=cnt))
    return issues


def check_not_ingested(playlist, current_start=None, lang='en'):
    lines, seen_eps, seen_other = [], set(), set()
    for item in playlist['not_ingested']:
        if current_start and item['start'] and item['start'] < current_start: continue
        atype, aref = item['type'], item['asset_ref']
        if atype in ('Program','live'):
            ep = normalize_id(aref)
            if ep in seen_eps: continue
            seen_eps.add(ep)
            show = re.sub(r'\[\].*$', '', item['name']).strip()
            lines.append(T('ni_program', lang, id=ep, t=fmt_t(item['start']), show=show))
        else:
            if aref in seen_other: continue
            seen_other.add(aref)
            lines.append(T('ni_other', lang, typ=atype, ref=aref,
                           t=fmt_t(item['start']), name=item['name']))
    return lines


def check_bugs(playlist, current_start=None, lang='en'):
    """Report LOGOHD_ANI/LOGO_LIVE bugs with Command value. One entry per show."""
    first_seg  = {}  # ep_id -> info of first segment with bug
    seg_count  = {}  # ep_id -> count of segments with bug

    for ev in playlist['events']:
        ev_start  = parse_timecode(ev.get('startTime',''))
        if current_start and ev_start and ev_start < current_start: continue
        assets = ev.get('assets',[])
        if not assets: continue
        aref  = assets[0].get('reference','')
        atype = assets[0].get('type','')
        if atype not in ('Program','live'): continue
        ep_id = normalize_id(aref)

        for b in ev.get('behaviors',[]):
            if b.get('name') in ('LOGOHD_ANI','LOGO_LIVE') and not b.get('disabled',True):
                seg_count[ep_id] = seg_count.get(ep_id, 0) + 1
                if ep_id not in first_seg:
                    show = re.sub(r'\[\].*$', '', ev.get('name','')).strip()
                    cmd  = b.get('params',{}).get('Command','?')
                    first_seg[ep_id] = {'show': show, 'behavior': b['name'],
                                        'cmd': cmd, 'start': ev_start}
                break

    if not first_seg:
        return [f'  {T("ok_bugs", lang)}']

    lines = []
    for ep_id, info in sorted(first_seg.items(), key=lambda x: x[1]['start'] or datetime.min):
        segs = seg_count.get(ep_id, 1)
        lines.append(T('bug_line', lang, beh=info['behavior'], cmd=info['cmd'],
                       id=ep_id, t=fmt_t(info['start']), show=f'{info["show"]} ({segs} segs)'))
    return lines


def check_cue_tones(playlist, lang='en'):
    cts = playlist['cue_tones']
    ct_counter = Counter(ct['ct_id'] for ct in cts)
    lines = [f'  {T("total_cues",lang)}: {len(cts)}']
    for ct_id, count in sorted(ct_counter.items()):
        times = [fmt_time(ct['start']) for ct in cts if ct['ct_id'] == ct_id]
        lines.append(f'  {ct_id}: {count}x | First: {times[0]} | Last: {times[-1]}')
    return lines


# ── REPORT ────────────────────────────────────────────────────────────────────

def generate_report(channel, playlist, xml_rows, grilla_ids, lang='en'):
    sep = '═' * 60
    pt  = playlist['type']
    current_start = playlist['programs'][0]['start'] if pt == 'current' and playlist['programs'] else None
    part_seq = build_show_sequence(playlist['programs'], from_start=current_start)
    total_comms = len([c for c in playlist['commercials']
                       if not current_start or (c['start'] and c['start'] >= current_start)])

    lines = [sep,
             f'{T("channel",lang)}: {channel.upper()}',
             f'{T("date_lbl",lang)}: {playlist["date"]}',
             f'{T("type_lbl",lang)}: {T("full_day",lang) if pt=="full" else T("partial",lang)}']
    if current_start:
        lines.append(f'{T("checking_from",lang)}: {fmt_t(current_start)}')
    lines += [sep, f'{T("summary",lang)}: {len(part_seq)} {T("show_blocks",lang)} | {total_comms} {T("commercials_lbl",lang)}', '']

    lines.append(f'── [1] {T("section_programs",lang)} ──')
    lines += ([T('no_grilla', lang)] if not grilla_ids else
              check_programs_vs_grilla(playlist, grilla_ids, current_start, lang))
    lines.append('')

    lines.append(f'── [2] {T("section_commercials",lang)} ──')
    lines += ([T('no_xml', lang)] if not xml_rows else
              check_commercials_vs_xml(playlist, xml_rows, current_start, lang))
    lines.append('')

    lines.append(f'── [3] {T("section_promos",lang)} ──')
    pi = check_promo_repeats(playlist, current_start, lang)
    lines += pi if pi else [f'  {T("ok_promos",lang)}']
    lines.append('')

    lines.append(f'── [4] {T("section_ingested",lang)} ──')
    ni = check_not_ingested(playlist, current_start, lang)
    lines += ni if ni else [f'  {T("ok_ingested",lang)}']
    lines.append('')

    lines.append(f'── [5] {T("section_bugs",lang)} ──')
    lines += check_bugs(playlist, current_start, lang)
    lines.append('')

    if pt == 'full':
        lines.append(f'── [6] {T("section_cues",lang)} ──')
        lines += check_cue_tones(playlist, lang)
        lines.append('')

    lines.append(sep)
    return '\n'.join(lines)
