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
    'bug_line':           {'en': '  🔲 {beh_label} : {cmd} — {id} @ {t} | {show}', 'es': '  🔲 {beh_label} : {cmd} — {id} @ {t} | {show}'},
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
    return f'{dt.strftime("%H:%M:%S")} UTC / {et.strftime("%H:%M:%S")} ET'

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
    # Only normalize extra leading zeros before 4+ digit date suffixes (e.g. COSA00327→COSA0327)
    # Do NOT touch 3-digit episode numbers (e.g. LATPAN001 stays LATPAN001)
    ep_id = re.sub(r'([A-Za-z][A-Za-z0-9]*)0{2,}(\d{4,})',
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
    current_break, last_program, last_program_raw = [], None, None

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
                    breaks.append({'after_program': last_program,
                                   'after_program_raw': last_program_raw,
                                   'items': current_break[:]})
                    current_break = []
                seg_m = re.search(r'_(\d+)$', aref)
                seg   = int(seg_m.group(1)) if seg_m else 1
                ep_id = normalize_id(aref)
                programs.append({'episode_id': ep_id, 'episode_id_raw': aref,
                                  'seg_num': seg, 'start': ev_start, 'duration': ev_dur,
                                  'name': ev_name, 'ref': ev_ref, 'asset_type': atype,
                                  'is_missing': (atype == 'Program' and tcin.startswith('07:'))})
                last_program     = ep_id
                last_program_raw = aref

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
        breaks.append({'after_program': last_program,
                       'after_program_raw': last_program_raw,
                       'items': current_break})

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
                 'duration': i.findtext('duration','').strip(),
                 'externalid': i.findtext('externalid','').strip()}
                for i in traffic.findall('item')]
    except: return []

def parse_xml_log_tn(filepath_or_bytes):
    """
    Parser for Todonovelas XML — <tabledata><data><row><column-N> format.
    column-1=LocalTime, column-4=MediaId, column-5=Type, column-6=Title
    Returns same dict format as parse_xml_log for compatibility.
    """
    try:
        if hasattr(filepath_or_bytes, 'read'): content = filepath_or_bytes.read()
        elif isinstance(filepath_or_bytes, bytes): content = filepath_or_bytes
        else:
            with open(filepath_or_bytes, 'rb') as f: content = f.read()
        root = ET.fromstring(content)
        items = []
        for row in root.findall('.//row'):
            local_time = row.findtext('column-1', '').strip()
            mediaid    = row.findtext('column-4', '').strip()
            typ        = row.findtext('column-5', '').strip().upper()
            title      = row.findtext('column-6', '').strip()
            duration   = row.findtext('column-3', '').strip()
            # Normalise type to match standard contenttype values
            if typ == 'PROGRAM': ct = 'PROGRAM_BEGIN'
            elif typ == 'PROMOTION': ct = 'PROMO'
            else: ct = typ
            items.append({'mediaid': mediaid, 'name': title,
                          'contenttype': ct, 'startat': local_time,
                          'duration': duration, 'externalid': ''})
        return items
    except: return []

def _xml_dur_secs(dur_str):
    """Parse XML duration HH:MM:SS:FF → seconds (ignore frames)."""
    try:
        p = dur_str.split(':')
        return int(p[0])*3600 + int(p[1])*60 + int(p[2])
    except: return 0

def _is_xml_program_anchor(item):
    """Only PROGRAM_BEGIN/PROGRAM_SEGMENT used for break-by-break alignment."""
    return item.get('contenttype','') in ('PROGRAM_BEGIN','PROGRAM_SEGMENT')

def _is_xml_start_anchor(item):
    """For partial start detection: program segments + infomercials (CM ≥ 20min)."""
    ct = item.get('contenttype','')
    if ct in ('PROGRAM_BEGIN','PROGRAM_SEGMENT'): return True
    if ct == 'COMMERCIAL' and _xml_dur_secs(item.get('duration','')) >= 1200: return True
    return False

def build_xml_breaks(xml_rows):
    """
    Walk XML, group commercials between PROGRAM segments only (not infomercials).
    Infomercials appear as commercials inside a break, matching JSON behavior.
    Returns list of {'anchor_id', 'commercials': [mediaid, ...]}
    Includes breaks with zero commercials for alignment.
    """
    result = []
    anchor = None
    comms  = []
    for item in xml_rows:
        if _is_xml_program_anchor(item):
            if anchor is not None:
                result.append({'anchor_id': anchor, 'commercials': comms[:]})
            anchor = item['mediaid']
            comms  = []
        elif item.get('contenttype') == 'COMMERCIAL':
            comms.append(item['mediaid'])
    if anchor is not None:
        result.append({'anchor_id': anchor, 'commercials': comms[:]})
    return result

def xml_commercials(rows):
    return [r for r in rows if r.get('contenttype') == 'COMMERCIAL']

def find_xml_anchor_by_extid(events, xml_rows):
    """
    Find where partial JSON starts in XML using externalid/reference match.
    Returns start index in xml_rows for commercial comparison.
    """
    ext_idx = {row['externalid']: i for i, row in enumerate(xml_rows)}
    for ev in events:
        ref = ev.get('reference', '')
        if ref in ext_idx:
            return ext_idx[ref]
    return 0


# ── GRILLA PARSER ─────────────────────────────────────────────────────────────

def parse_grilla(filepath_or_bytes, target_date, channel='catv'):
    """Route to the correct grilla parser based on channel type."""
    if channel in ('latam', 'us'):
        return _parse_grilla_pasiones(filepath_or_bytes, target_date)
    if channel == 'tn':
        return _parse_grilla_tn(filepath_or_bytes, target_date)
    return _parse_grilla_catv_tvd(filepath_or_bytes, target_date)

def _parse_grilla_catv_tvd(filepath_or_bytes, target_date):
    """Original CATV/TVD grilla parser — single active sheet, datetime header."""
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

def _parse_date_str(val, force_year=None):
    """Parse date from string like 'Lun. / Mon. 03/30/26'.
    force_year overrides the year in the string (handles typos like 03/30/25 when it should be 2026)."""
    if not val: return None
    m = re.search(r'(\d{1,2})/(\d{1,2})/(\d{2,4})', str(val))
    if m:
        mo, d, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if force_year:
            y = force_year
        else:
            y = 2000 + y if y < 100 else y
        try: return datetime(y, mo, d).date()
        except: pass
    return None

def _parse_grilla_pasiones(filepath_or_bytes, target_date):
    """
    Pasiones grilla parser — multi-tab yearly workbook.
    Scans tabs last→first, finds the one containing target_date.
    Header row contains date strings like 'Mar. / Tue. 03/31/26'.
    Episode IDs are in alternating rows (show name row, then ID row).
    """
    from openpyxl import load_workbook
    import io
    if isinstance(filepath_or_bytes, str):
        wb = load_workbook(filepath_or_bytes, read_only=True)
    else:
        raw = filepath_or_bytes.read() if hasattr(filepath_or_bytes, 'read') else filepath_or_bytes
        wb = load_workbook(io.BytesIO(raw), read_only=True)

    target_ws = None
    target_col = None

    for name in reversed(wb.sheetnames):
        ws = wb[name]
        rows = list(ws.iter_rows(max_row=3, values_only=True))
        if len(rows) < 2: continue
        header = rows[1]
        # Use target_date's year to avoid typos in spreadsheet year field
        col_dates = [(i, _parse_date_str(cell, force_year=target_date.year))
                     for i, cell in enumerate(header)
                     if _parse_date_str(cell, force_year=target_date.year)]
        if not col_dates: continue
        first_d = col_dates[0][1]
        last_d  = col_dates[-1][1]
        if first_d <= target_date <= last_d:
            for col_i, d in col_dates:
                if d == target_date:
                    target_col = col_i
                    target_ws  = ws
                    # Re-read full sheet
                    all_rows = list(ws.iter_rows(values_only=True))
                    break
            break

    if target_ws is None or target_col is None:
        return []

    # Episode ID rows have ET column (col 1) = None, show name rows have a time value.
    # This reliably catches IDs with any digit count (LATUV49, UMM14, etc.)
    ET_COL = 1
    episode_ids = []
    for row in all_rows[2:]:
        et_val = row[ET_COL] if ET_COL < len(row) else 'x'
        if et_val is not None:
            continue  # show name row — skip
        val = row[target_col] if target_col < len(row) else None
        if val and isinstance(val, str):
            val = val.strip()
            if val:
                episode_ids.append(normalize_id(val))
    return episode_ids

def _parse_grilla_tn(filepath_or_bytes, target_date):
    """
    Todonovelas grilla — multi-tab, same header format as Pasiones.
    Returns list of (show_name, episode_num) tuples for program matching.
    Episode numbers are integers in the grid (55, 121...).
    Show name rows: ET column has value. Episode rows: ET column is None.
    """
    from openpyxl import load_workbook
    import io
    if isinstance(filepath_or_bytes, str):
        wb = load_workbook(filepath_or_bytes, read_only=True)
    else:
        raw = filepath_or_bytes.read() if hasattr(filepath_or_bytes, 'read') else filepath_or_bytes
        wb = load_workbook(io.BytesIO(raw), read_only=True)

    target_ws = None
    target_col = None
    all_rows = []

    for name in reversed(wb.sheetnames):
        ws = wb[name]
        rows = list(ws.iter_rows(max_row=3, values_only=True))
        if len(rows) < 2: continue
        header = rows[1]
        col_dates = [(i, _parse_date_str(cell, force_year=target_date.year))
                     for i, cell in enumerate(header)
                     if _parse_date_str(cell, force_year=target_date.year)]
        if not col_dates: continue
        if col_dates[0][1] <= target_date <= col_dates[-1][1]:
            for col_i, d in col_dates:
                if d == target_date:
                    target_col = col_i
                    target_ws  = ws
                    all_rows   = list(ws.iter_rows(values_only=True))
                    break
            break

    if not all_rows or target_col is None:
        return []

    ET_COL = 1
    result = []
    current_show = None
    for row in all_rows[2:]:
        et_val = row[ET_COL] if ET_COL < len(row) else 'x'
        val    = row[target_col] if target_col < len(row) else None
        if val is None: continue
        if et_val is not None:
            # Show name row
            current_show = str(val).strip() if val else None
        else:
            # Episode number row — val is an integer
            if current_show and val is not None:
                try:
                    ep_num = int(val)
                    result.append((current_show, ep_num))
                except (ValueError, TypeError):
                    pass
    return result


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

def _date_from_json_content(f):
    """Extract date from JSON file content (first event's startTime). Most reliable."""
    try:
        f.seek(0)
        data = json.load(f)
        f.seek(0)
        for ev in data.get('events', []):
            tc = ev.get('startTime', '')
            if tc:
                m = re.search(r'(\d{4})-(\d{2})-(\d{2})', tc)
                if m:
                    return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3))).date()
    except: pass
    return None

def _date_from_xml_filename(name):
    """XML filenames date extraction.
    Standard: MMDDYYYY (TVD03302026.xml, CA03302026.xml, PL03312026.xml)
    TN:       MMDDYY   (TN_033126_TUESDAY.xml)
    """
    # Try MMDDYYYY first (8 digits)
    m = re.search(r'(\d{2})(\d{2})(\d{4})', name)
    if m:
        try: return datetime(int(m.group(3)), int(m.group(1)), int(m.group(2))).date()
        except: pass
    # Try MMDDYY (6 digits, 2-digit year)
    m = re.search(r'(\d{2})(\d{2})(\d{2})(?!\d)', name)
    if m:
        try: return datetime(2000 + int(m.group(3)), int(m.group(1)), int(m.group(2))).date()
        except: pass
    return None

def detect_files(uploaded_files):
    """
    Group uploaded files by (date, channel).
    JSON: date always from content (startTime). Works for renamed files.
    XML:  date from filename (MMDDYYYY pattern, always reliable).
    Grilla: week-based, stored by channel only.
    Returns: days dict, grillas dict, unknown list.
    """
    days    = {}
    grillas = {}
    unknown = []

    for f in uploaded_files:
        name_up = f.name.upper()
        ext = f.name.lower().rsplit('.', 1)[-1] if '.' in f.name else ''

        if ext == 'json':            ftype = 'json'
        elif ext == 'xml':           ftype = 'xml'
        elif ext in ('xlsx','xlsm'): ftype = 'grilla'
        else: unknown.append(f); continue

        # Channel detection
        if ext == 'xml':
            if name_up.startswith('CA'):    channel = 'catv'
            elif name_up.startswith('TVD'): channel = 'tvd'
            elif name_up.startswith('PL'):  channel = 'latam'
            elif name_up.startswith('PUS'): channel = 'us'
            elif name_up.startswith('TN'):  channel = 'tn'
            else: unknown.append(f); continue
        else:
            if 'CATV' in name_up:                       channel = 'catv'
            elif 'TVD' in name_up:                      channel = 'tvd'
            elif 'PASIONES_LATAM' in name_up or 'PASIONES LATAM' in name_up: channel = 'latam'
            elif 'PASIONES_US' in name_up or 'PASIONES US' in name_up:       channel = 'us'
            elif 'FAST_TODONOVELAS' in name_up or 'FAST TODONOVELAS' in name_up or ('TODO' in name_up and 'NOVELA' in name_up): channel = 'tn'
            else: unknown.append(f); continue

        if ftype == 'grilla':
            grillas[channel] = f
            continue

        # Date extraction
        if ftype == 'json':
            date = _date_from_json_content(f)    # always from content
        else:  # xml
            date = _date_from_xml_filename(f.name)  # always from filename

        if date is None:
            unknown.append(f)
            continue

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
    """
    Break-by-break commercial comparison.
    Aligns XML and JSON by segment ID (after_program_raw).
    Handles show replacements with a look-ahead window.
    Returns (issues_list, manual_warnings_list).
    """
    WINDOW = 15  # max segments to look ahead for replacement recovery

    # --- Build XML break list ---
    xml_breaks = build_xml_breaks(xml_rows)

    # --- Build JSON break list (only those with a raw anchor) ---
    json_breaks = [b for b in playlist['breaks'] if b.get('after_program_raw')]

    # --- For partial: align by matching first JSON break's segment in XML ---
    if current_start:
        # Filter json_breaks to those at/after current_start
        def _break_time(b):
            return next((i['start'] for i in b.get('items',[]) if i.get('start')), None)
        json_breaks = [b for b in json_breaks
                       if not _break_time(b) or _break_time(b) >= current_start]

        if json_breaks:
            first_seg = json_breaks[0].get('after_program_raw', '')
            if first_seg:
                # Find this segment in XML breaks and start from there
                xi = next((i for i, xb in enumerate(xml_breaks)
                           if xb['anchor_id'] == first_seg), None)
                if xi is not None:
                    xml_breaks = xml_breaks[xi:]  # start FROM this segment (not after)

    # --- Labels ---
    added_lbl    = {'en': 'added to playlist',    'es': 'agregado a playlist'}
    removed_lbl  = {'en': 'removed from playlist', 'es': 'eliminado de playlist'}
    replaced_lbl = {'en': 'SHOW REPLACED',         'es': 'PROGRAMA REEMPLAZADO'}
    lost_lbl     = {'en': 'ALIGNMENT LOST',        'es': 'ALINEACIÓN PERDIDA'}
    summary_lbl  = {'en': 'COMMERCIAL CHANGES SUMMARY', 'es': 'RESUMEN DE CAMBIOS'}
    added_tot    = {'en': 'added',    'es': 'agregados'}
    removed_tot  = {'en': 'removed',  'es': 'eliminados'}
    manual_cap   = {'en': '!!! DOUBLE CHECK MANUALLY !!!', 'es': '!!! VERIFICAR MANUALMENTE !!!'}

    issues       = []
    all_added    = Counter()
    all_removed  = Counter()
    manual_warns = []

    def _break_start(jb):
        return next((i['start'] for i in jb['items'] if i.get('start')), None)

    def _compare_pair(xb, jb):
        """Compare one aligned XML/JSON break pair. Returns (lines, added, removed)."""
        xml_c  = Counter(xb['commercials'])
        json_c = Counter(i['ref'] for i in jb['items'] if i['type'] == 'Commercial')
        anchor = jb.get('after_program_raw', '?')
        bs     = _break_start(jb)

        lines, add, rem = [], Counter(), Counter()
        for ref in sorted(set(xml_c) | set(json_c)):
            xc, jc = xml_c.get(ref, 0), json_c.get(ref, 0)
            if xc == jc: continue
            diff = jc - xc
            if diff > 0:
                lines.append(f'     + {ref} x{diff}  ({added_lbl[lang]})')
                add[ref] += diff
            else:
                lines.append(f'     - {ref} x{abs(diff)}  ({removed_lbl[lang]})')
                rem[ref] += abs(diff)

        if lines:
            header = [f'  ⚠  Break after [{anchor}] @ {fmt_t(bs)}']
            return header + lines, add, rem
        return [], add, rem

    def _compare_pool(xml_blist, json_blist, xml_label, json_label, bs):
        """Compare pooled commercials from a replaced block."""
        xml_c  = Counter(ref for xb in xml_blist for ref in xb['commercials'])
        json_c = Counter(ref for jb in json_blist for i in jb['items']
                         if i['type'] == 'Commercial' for ref in [i['ref']])
        lines, add, rem = [], Counter(), Counter()
        for ref in sorted(set(xml_c) | set(json_c)):
            xc, jc = xml_c.get(ref, 0), json_c.get(ref, 0)
            if xc == jc: continue
            diff = jc - xc
            if diff > 0: lines.append(f'     + {ref} x{diff}  ({added_lbl[lang]})'); add[ref] += diff
            else:        lines.append(f'     - {ref} x{abs(diff)}  ({removed_lbl[lang]})'); rem[ref] += abs(diff)
        return lines, add, rem

    # --- Walk both break lists in parallel ---
    xi, ji = 0, 0
    while xi < len(xml_breaks) and ji < len(json_breaks):
        xb = xml_breaks[xi]
        jb = json_breaks[ji]
        x_anc = xb['anchor_id']
        j_anc = jb.get('after_program_raw', '')

        if x_anc == j_anc:
            # Perfect match
            lines, add, rem = _compare_pair(xb, jb)
            issues.extend(lines)
            all_added.update(add)
            all_removed.update(rem)
            xi += 1; ji += 1

        else:
            # Mismatch — look ahead in both directions to recover
            found_xi = next((i for i in range(xi+1, min(xi+WINDOW, len(xml_breaks)))
                             if xml_breaks[i]['anchor_id'] == j_anc), None)
            found_ji = next((i for i in range(ji+1, min(ji+WINDOW, len(json_breaks)))
                             if json_breaks[i].get('after_program_raw') == x_anc), None)

            bs = _break_start(jb)

            if found_xi is not None and (found_ji is None or (found_xi-xi) <= (found_ji-ji)):
                # XML has more segments here — pooled comparison
                xml_block  = xml_breaks[xi:found_xi]
                json_block = [jb]
                x_show = normalize_id(x_anc)
                j_show = normalize_id(j_anc)
                issues.append(f'  ⚠  {replaced_lbl[lang]}: XML=[{x_show}...] → Playlist=[{j_show}] @ {fmt_t(bs)}')
                pool_lines, add, rem = _compare_pool(xml_block, json_block, x_show, j_show, bs)
                if pool_lines:
                    issues.extend(pool_lines)
                    warn = f'{manual_cap[lang]}: {replaced_lbl[lang]} [{x_show}→{j_show}] @ {fmt_t(bs)}'
                    manual_warns.append(warn)
                    issues.append(f'     ⚠  {warn}')
                else:
                    issues.append(f'     ✓ Commercials match within replaced block')
                all_added.update(add); all_removed.update(rem)
                xi = found_xi; ji += 1

            elif found_ji is not None:
                # JSON has more segments here — pooled comparison
                xml_block  = [xb]
                json_block = json_breaks[ji:found_ji]
                x_show = normalize_id(x_anc)
                j_show = normalize_id(j_anc)
                issues.append(f'  ⚠  {replaced_lbl[lang]}: XML=[{x_show}] → Playlist=[{j_show}...] @ {fmt_t(bs)}')
                pool_lines, add, rem = _compare_pool(xml_block, json_block, x_show, j_show, bs)
                if pool_lines:
                    issues.extend(pool_lines)
                    warn = f'{manual_cap[lang]}: {replaced_lbl[lang]} [{x_show}→{j_show}] @ {fmt_t(bs)}'
                    manual_warns.append(warn)
                    issues.append(f'     ⚠  {warn}')
                else:
                    issues.append(f'     ✓ Commercials match within replaced block')
                all_added.update(add); all_removed.update(rem)
                xi += 1; ji = found_ji

            else:
                # Can't recover — skip both and warn loudly
                warn = f'{manual_cap[lang]}: {lost_lbl[lang]} [{normalize_id(x_anc)} vs {normalize_id(j_anc)}] @ {fmt_t(bs)}'
                manual_warns.append(warn)
                issues.append(f'  ⚠  {warn}')
                xi += 1; ji += 1

    # --- Summary ---
    if all_added or all_removed:
        issues += ['', f'  ── {summary_lbl[lang]} ──']
        if all_added:
            total = sum(all_added.values())
            issues.append(f'  +{total} {added_tot[lang]}:')
            for ref, cnt in sorted(all_added.items()):
                issues.append(f'    {ref} x{cnt}')
        if all_removed:
            total = sum(all_removed.values())
            issues.append(f'  -{total} {removed_tot[lang]}:')
            for ref, cnt in sorted(all_removed.items()):
                issues.append(f'    {ref} x{cnt}')
    elif not issues:
        total_pl = len(playlist['commercials'])
        issues.append(f'  {T("ok_commercials", lang, n=total_pl)}')

    return issues, manual_warns


def check_promo_repeats(playlist, current_start=None, lang='en'):
    INFOMERCIAL_SECS = 1200  # 20 min
    issues = []

    for brk in playlist['breaks']:
        items = brk['items']
        if not items: continue
        bs = next((i['start'] for i in items if i.get('start')), None)
        if current_start and bs and bs < current_start: continue

        # Split break at infomercials (Commercial ≥ 20min) — they act as sub-break separators
        sub_breaks = []
        current_sub = []
        for item in items:
            if item['type'] == 'Commercial' and parse_duration(item.get('duration','00:00:00')) >= INFOMERCIAL_SECS:
                if current_sub:
                    sub_breaks.append(current_sub)
                current_sub = []  # reset after infomercial
            else:
                current_sub.append(item)
        if current_sub:
            sub_breaks.append(current_sub)
        if not sub_breaks:
            sub_breaks = [items]

        for sub in sub_breaks:
            promo_refs = [i['ref'] for i in sub if i['type'] == 'Promotion']
            for ref, cnt in Counter(promo_refs).items():
                if cnt > 1:
                    after = brk.get('after_program', '?')
                    sub_start = next((i['start'] for i in sub if i.get('start')), bs)
                    issues.append(T('promo_rep', lang, after=after,
                                    t=fmt_t(sub_start), ref=ref, n=cnt))


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
        beh_label = 'Bug Server' if info['behavior'] == 'LOGOHD_ANI' else 'Bug Live'
        lines.append(T('bug_line', lang, beh_label=beh_label, cmd=info['cmd'],
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

def check_programs_vs_grilla_tn(playlist, grilla_pairs, current_start, lang):
    """
    TN program check: grilla has (show_name, ep_num) pairs (including re-airs).
    JSON names are like LA_HOGUERA_AMBICION_E055 — parse episode number.
    Deduplicate both sides by episode number and compare as sets.
    """
    import re as _re

    def parse_ep_num(name):
        m = _re.search(r'_E(\d+)$', str(name))
        return int(m.group(1)) if m else None

    # Unique episode numbers from grilla (deduplicated)
    grilla_eps = {}  # ep_num -> show_name (first occurrence)
    for show_name, ep_num in grilla_pairs:
        if ep_num not in grilla_eps:
            grilla_eps[ep_num] = show_name

    # Unique episodes from JSON
    seen, json_eps = set(), {}
    for p in playlist['programs']:
        ref = p['episode_id_raw']
        if ref in seen: continue
        seen.add(ref)
        if current_start and p['start'] and p['start'] < current_start: continue
        ep_num = parse_ep_num(p['name'])
        if ep_num is not None:
            json_eps[ep_num] = {'name': p['name'], 'start': p['start']}

    issues = []
    # In grilla but not in JSON
    for ep, show in sorted(grilla_eps.items()):
        if ep not in json_eps:
            issues.append(f'  ✗  NOT IN PLAYLIST: {show} ep{ep}')

    # In JSON but not in grilla
    for ep, info in sorted(json_eps.items()):
        if ep not in grilla_eps:
            issues.append(
                f'  ✗  EXTRA IN PLAYLIST: {info["name"]} @ {fmt_t(info["start"])} (not in grilla)'
            )

    if not issues:
        issues.append(f'  ✓ All {len(json_eps)} episodes match grilla')
    return issues


def generate_report(channel, playlist, xml_rows, grilla_ids, lang='en', is_tn=False):
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
    if is_tn and grilla_ids:
        lines += check_programs_vs_grilla_tn(playlist, grilla_ids, current_start, lang)
    elif not grilla_ids:
        lines.append(T('no_grilla', lang))
    else:
        lines += check_programs_vs_grilla(playlist, grilla_ids, current_start, lang)
    lines.append('')

    lines.append(f'── [2] {T("section_commercials",lang)} ──')
    manual_warns = []
    if not xml_rows:
        lines.append(T('no_xml', lang))
    elif is_tn:
        # TN has no commercials — simple count only, no break-by-break needed
        lines.append(f'  {T("ok_commercials", lang, n=total_comms)}')
    else:
        comm_lines, manual_warns = check_commercials_vs_xml(playlist, xml_rows, current_start, lang)
        lines += comm_lines
    lines.append('')

    lines.append(f'── [3] {T("section_promos",lang)} ──')
    pi = check_promo_repeats(playlist, current_start, lang)
    lines += pi if pi else [f'  {T("ok_promos",lang)}']
    lines.append('')

    lines.append(f'── [4] {T("section_ingested",lang)} ──')
    ni = check_not_ingested(playlist, current_start, lang)
    lines += ni if ni else [f'  {T("ok_ingested",lang)}']
    lines.append('')

    if not is_tn:
        lines.append(f'── [5] {T("section_bugs",lang)} ──')
        lines += check_bugs(playlist, current_start, lang)
        lines.append('')

    if pt == 'full':
        lines.append(f'── [6] {T("section_cues",lang)} ──')
        lines += check_cue_tones(playlist, lang)
        lines.append('')

    lines.append(sep)
    return '\n'.join(lines), manual_warns
