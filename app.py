"""
Broadcast Playlist Checker — Streamlit App v28
"""
import streamlit as st
import json, io
from datetime import datetime
import os, sys
sys.path.insert(0, os.path.dirname(__file__))
from checker import (
    parse_json_playlist, parse_xml_log, parse_xml_log_tn, parse_grilla,
    generate_report, check_promo_repeats, detect_files,
    parse_sony_xml_log, check_sony, pair_sony_files, SONY_CHANNEL_MAP,
    parse_sony_json_markers, pick_grilla_for_date,
    parse_holatv_xlsx_log, parse_holatv_txt_log,
    parse_grilla_holatv, generate_report_holatv,
    _date_from_xml_filename as _dxml,
)

st.set_page_config(page_title='Broadcast Playlist Checker', layout='wide')

SONY_EMOJI = {
    'A1':'🅰️','A2':'🅰️','A3':'🅰️','A4':'🅰️','A5':'🅰️','A6':'🅰️',
    'F1':'🎬','F4':'🎬',
    'S1':'📡','S2':'📡','S3':'📡','S4':'📡','S5':'📡','S6':'📡',
}

CH_DISPLAY_GLOBAL = {
    'catv':  'CATV 🌎',
    'tvd':   'TVD 📺',
    'latam': 'Pasiones Latam 🌹',
    'us':    'Pasiones US ⭐',
    'tn':    'Fast Todonovelas 📺',
    'hu':    'HolaTV US 👋',
    'hl':    'HolaTV Latam 🌺',
}

SONY_L = {
    'markers_hdr':  {'en': '── [1] MARKERS ──',                    'es': '── [1] MARCADORES ──'},
    'no_marker':    {'en': '  ℹ  No markers found (partial/current playlist)', 'es': '  ℹ  Sin marcadores (playlist parcial/actual)'},
    'log_hdr':      {'en': '── [2] LOG FILE MATCH ──',             'es': '── [2] VERIFICACIÓN DE ARCHIVO LOG ──'},
    'ep_hdr':       {'en': '── [3] ENDPOINT CHECK ──',             'es': '── [3] VERIFICACIÓN DE PUNTO FINAL ──'},
    'seg_hdr':      {'en': '── [4] SEGMENT TIMING CHECK (<=5s tolerance) ──', 'es': '── [4] VERIFICACIÓN DE TIEMPOS (tolerancia <=5s) ──'},
    'pl_full':      {'en': 'FULL (marker present)',                 'es': 'COMPLETO (marcador presente)'},
    'pl_partial':   {'en': 'CURRENT (partial)',                     'es': 'ACTUAL (parcial)'},
    'pl_none':      {'en': '— no JSON —',                          'es': '— sin JSON —'},
    'no_json':      {'en': '  ℹ  Log provided but no matching JSON found', 'es': '  ℹ  Log provisto pero sin JSON correspondiente'},
    'no_log':       {'en': '  ℹ  JSON found but no matching log provided', 'es': '  ℹ  JSON encontrado pero sin log correspondiente'},
}
def SL(key, lang): return SONY_L.get(key, {}).get(lang, SONY_L.get(key, {}).get('en', key))

# ── LANGUAGE ──────────────────────────────────────────────────────────────────
lang = st.radio('🌐', ['English', 'Español'], horizontal=True, label_visibility='collapsed')
lang = 'es' if lang == 'Español' else 'en'

L = {
    'title':    {'en': '📋 Broadcast Playlist Checker',             'es': '📋 Verificador de Playlist'},
    'upload':   {'en': 'Drop all files here — JSON, XML, XLSX, PDF, TXT', 'es': 'Arrastra archivos aquí — JSON, XML, XLSX, PDF, TXT'},
    'run':      {'en': '▶  Run Check',                              'es': '▶  Verificar'},
    'dl':       {'en': '⬇ Download Report (.txt)',                  'es': '⬇ Descargar Reporte (.txt)'},
    'detected': {'en': 'Detected files',                            'es': 'Archivos detectados'},
    'unknown':  {'en': '⚠ Unrecognized:',                          'es': '⚠ No reconocidos:'},
    'hint':     {'en': 'JSON → promo check  |  +XML/TXT/XLSX → commercials  |  +Grilla XLSX/PDF → programs',
                 'es': 'JSON → promos  |  +XML/TXT/XLSX → comerciales  |  +Grilla XLSX/PDF → programas'},
    'channels': {'en': 'Channels to check:',                        'es': 'Canales a verificar:'},
    'tab_all':  {'en': '📋 All',                                    'es': '📋 Todo'},
    'no_json':  {'en': 'Upload at least one Vipe JSON.',            'es': 'Sube al menos un JSON de Vipe.'},
    'report':   {'en': '📄 Report',                                 'es': '📄 Reporte'},
    'running':  {'en': 'Running checks...',                         'es': 'Verificando...'},
}
def t(k): return L[k][lang]

st.title(t('title'))

# ── UPLOAD ────────────────────────────────────────────────────────────────────
uploaded = st.file_uploader(t('upload'), accept_multiple_files=True, type=None, key='all_files')
result = detect_files(uploaded) if uploaded else ({}, {}, [], [])
days, grillas, unknown_files, sony_files_raw = result

# ── DETECTION TABLE ───────────────────────────────────────────────────────────
if uploaded:
    total_detected = (
        sum(len(info['json'])
            + (1 if info.get('xml') else 0)
            + (1 if info.get('log') else 0)
            for info in days.values())
        + sum(len(gl) if isinstance(gl, list) else 1 for gl in grillas.values())
        + len(unknown_files)
        + len(sony_files_raw)
    )
    st.markdown(f"**{t('detected')}: {total_detected}** / {len(uploaded)} uploaded")

    rows = []
    for (date_str, channel), info in sorted(days.items()):
        ch = CH_DISPLAY_GLOBAL.get(channel, channel.upper())
        try:
            d = datetime.strptime(date_str, '%Y-%m-%d')
            short_date = d.strftime('%m/%d')
        except:
            short_date = date_str
        for jf in info['json']:
            rows.append({'Date': date_str, 'Channel': ch,
                         'Type': f'Playlist {short_date}', 'File': jf.name})
        if info.get('xml'):
            rows.append({'Date': date_str, 'Channel': ch,
                         'Type': f'Log {short_date}', 'File': info['xml'].name})
        if info.get('log'):
            rows.append({'Date': date_str, 'Channel': ch,
                         'Type': f'Log {short_date}', 'File': info['log'].name})

    for ch_key, gf_list in grillas.items():
        ch = CH_DISPLAY_GLOBAL.get(ch_key, ch_key.upper())
        if not isinstance(gf_list, list):
            gf_list = [gf_list]
        for gf in gf_list:
            grilla_date_str = '(PDF)' if gf.name.lower().endswith('.pdf') else '(Week)'
            if not gf.name.lower().endswith('.pdf'):
                try:
                    from openpyxl import load_workbook
                    from checker import _parse_date_str
                    gf.seek(0)
                    wb = load_workbook(io.BytesIO(gf.read()), read_only=True)
                    gf.seek(0)
                    if ch_key in ('latam', 'us', 'tn', 'hl'):
                        # Extract date range from first sheet with dates (scan all sheets in reverse order as checker.py does)
                        for name in reversed(wb.sheetnames):
                            ws = wb[name]
                            rs = list(ws.iter_rows(max_row=2, values_only=True))
                            if len(rs) > 1:
                                dates_found = []
                                for cell in rs[1]:
                                    d = _parse_date_str(cell)
                                    if d:
                                        dates_found.append(d)
                                if dates_found:
                                    # Show first and last date of the week range
                                    dates_found.sort()
                                    first_d = dates_found[0]
                                    last_d = dates_found[-1]
                                    if first_d == last_d:
                                        grilla_date_str = first_d.strftime('%m/%d')
                                    else:
                                        grilla_date_str = f'{first_d.strftime("%m/%d")}-{last_d.strftime("%m/%d")}'
                                    break
                        if grilla_date_str == '(Week)':
                            # Fallback: look for dates in string format
                            for name in wb.sheetnames:
                                ws = wb[name]
                                rs = list(ws.iter_rows(max_row=2, values_only=True))
                                if len(rs) > 1:
                                    for cell in rs[1]:
                                        if cell and isinstance(cell, str) and '/' in str(cell):
                                            import re as _re
                                            m = _re.search(r'(\d{1,2})/(\d{1,2})', str(cell))
                                            if m:
                                                grilla_date_str = f'{m.group(1).zfill(2)}/{m.group(2).zfill(2)}'
                                                break
                                    if grilla_date_str != '(Week)':
                                        break
                    else:
                        ws = wb.active
                        rs = list(ws.iter_rows(max_row=2, values_only=True))
                        if len(rs) > 1:
                            mv = rs[1][2] if len(rs[1]) > 2 else None
                            if mv and hasattr(mv, 'strftime'):
                                grilla_date_str = mv.strftime('%m/%d')
                except:
                    pass
            grid_lbl = f'{"Grilla" if lang=="es" else "Grid"} {grilla_date_str}'
            rows.append({'Date': grilla_date_str, 'Channel': ch,
                         'Type': grid_lbl, 'File': gf.name})

    for uf in unknown_files:
        rows.append({'Date': '?', 'Channel': '?', 'Type': '?', 'File': uf.name})
    for sf in sony_files_raw:
        ch_name = SONY_CHANNEL_MAP.get(sf['code'], sf['code'])
        ch_display = f'{sf["code"]} {SONY_EMOJI.get(sf["code"],"📺")} {ch_name}'
        d = _dxml(sf['file'].name)
        type_label = f'{"Log" if sf["ftype"]=="xml" else "Playlist"} {d.strftime("%m/%d") if d else "?"}'
        rows.append({'Date': str(d) if d else '?', 'Channel': ch_display,
                     'Type': type_label, 'File': sf['file'].name})

    if rows:
        import pandas as pd
        st.dataframe(pd.DataFrame(rows), use_container_width=True,
                     hide_index=True, height=min(500, 35 + len(rows)*35))
    st.caption(t('hint'))
    st.divider()

# ── CHANNEL SELECTOR ──────────────────────────────────────────────────────────
available = sorted(set(ch for (_, ch) in days.keys()) | set(grillas.keys()))
sony_codes_present = sorted(set(sf['code'] for sf in sony_files_raw))
all_options = available + sony_codes_present

CH_FORMAT = {**CH_DISPLAY_GLOBAL}
CH_FORMAT.update({
    code: f'{code} {SONY_EMOJI.get(code,"📺")} {SONY_CHANNEL_MAP.get(code,code)}'
    for code in sony_codes_present
})

if all_options:
    selected_all = st.multiselect(
        t('channels'), options=all_options, default=all_options,
        format_func=lambda x: CH_FORMAT.get(x, x)
    )
else:
    selected_all = []
selected      = [x for x in selected_all if x in available]
selected_sony = [x for x in selected_all if x in sony_codes_present]

# ── RUN ───────────────────────────────────────────────────────────────────────
if st.button(t('run'), type='primary', use_container_width=True):
    if not days and not sony_files_raw:
        st.error(t('no_json')); st.stop()

    CH_LABELS = {
        'catv': 'CATV', 'tvd': 'TVD',
        'latam': 'Pasiones Latam', 'us': 'Pasiones US',
        'tn': 'Fast Todonovelas',
        'hu': 'HolaTV US', 'hl': 'HolaTV Latam',
    }

    def _load_holatv_log(info):
        """Load HolaTV log rows from XML, XLSX, or TXT — whichever is available."""
        log_date = info.get('date')
        if info.get('xml'):
            try:
                info['xml'].seek(0)
                return parse_xml_log(info['xml'])
            except:
                return []
        if info.get('log'):
            lf  = info['log']
            ext = lf.name.lower().rsplit('.', 1)[-1]
            try:
                lf.seek(0)
                if ext in ('xlsx', 'xlsm'):
                    return parse_holatv_xlsx_log(lf, log_date)
                else:
                    return parse_holatv_txt_log(lf, log_date)
            except:
                return []
        return []

    def _pick_grilla(channel, date_str):
        """Select the correct grilla file for this channel+date."""
        gf_list = grillas.get(channel, [])
        if not isinstance(gf_list, list):
            gf_list = [gf_list] if gf_list else []
        if not gf_list:
            return None
        try:
            td = datetime.strptime(date_str, '%Y-%m-%d').date()
        except:
            td = None
        return pick_grilla_for_date(gf_list, td, channel) if td else gf_list[0]

    def process_one(channel, json_file, xml_file, grilla_f, date_str=None):
        is_tn  = (channel == 'tn')
        lines  = []
        file_info = {
            'json':   json_file.name if json_file else None,
            'xml':    xml_file.name  if xml_file  else None,
            'grilla': grilla_f.name  if grilla_f  else None,
        }
        try:
            json_file.seek(0)
            data     = json.load(json_file)
            playlist = parse_json_playlist(data)
        except Exception as e:
            return [f'ERROR parsing JSON: {e}'], []

        xml_rows = []
        if xml_file:
            try:
                xml_file.seek(0)
                xml_rows = parse_xml_log_tn(xml_file) if is_tn else parse_xml_log(xml_file)
            except Exception as e:
                lines.append(f'  WARNING: XML error: {e}')

        grilla_ids = []
        if grilla_f and playlist['date']:
            try:
                grilla_f.seek(0)
                grilla_ids = parse_grilla(grilla_f, playlist['date'], channel)
            except Exception as e:
                lines.append(f'  WARNING: Grilla error: {e}')

        if not xml_rows and not grilla_ids:
            pi = check_promo_repeats(playlist, lang=lang)
            ch_label = CH_LABELS.get(channel, channel.upper())
            lines += ['='*60,
                      f'CHANNEL: {ch_label} | DATE: {playlist["date"]} | '
                      f'{"PROMO CHECK" if lang=="en" else "VERIFICACIÓN DE PROMOS"}',
                      '='*60]
            lines += pi if pi else ['  Check: No repeated promos']
            lines.append('')
            return lines, []

        ch_label = CH_LABELS.get(channel, channel.upper())
        report_text, manual_warns = generate_report(
            ch_label, playlist, xml_rows, grilla_ids, lang,
            is_tn=is_tn, file_info=file_info
        )
        lines.append(report_text)
        return lines, manual_warns

    def process_holatv(channel, json_file, info, grilla_f):
        lines = []
        try:
            json_file.seek(0)
            data     = json.load(json_file)
            playlist = parse_json_playlist(data)
        except Exception as e:
            return [f'ERROR parsing JSON: {e}']

        xml_rows = _load_holatv_log(info)

        grilla_entries = []
        if grilla_f and playlist['date']:
            try:
                grilla_f.seek(0)
                if grilla_f.name.lower().endswith('.pdf'):
                    grilla_entries = parse_grilla_holatv(grilla_f, playlist['date'])
                else:
                    grilla_entries = parse_grilla(grilla_f, playlist['date'], channel)
            except Exception as e:
                lines.append(f'  WARNING: Grilla error: {e}')

        log_file = info.get('xml') or info.get('log')
        file_info = {
            'json':   json_file.name,
            'xml':    log_file.name if log_file else None,
            'grilla': grilla_f.name if grilla_f else None,
        }
        ch_label = CH_DISPLAY_GLOBAL.get(channel, channel.upper())
        report_text = generate_report_holatv(
            ch_label, playlist, xml_rows, grilla_entries, lang, file_info=file_info
        )
        lines.append(report_text)
        return lines

    # ── Build per-day reports ─────────────────────────────────────────────────
    sorted_dates   = sorted(set(d for (d, _) in days.keys()))
    day_reports    = {}
    header_lines   = [
        'BROADCAST PLAYLIST CHECK REPORT' if lang=='en' else 'REPORTE DE VERIFICACIÓN DE PLAYLIST',
        f'Generated / Generado: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
        '='*60, ''
    ]
    all_manual_warns = []

    with st.spinner(t('running')):
        for date_str in sorted_dates:
            d_lines    = [f'{"DATE" if lang=="en" else "FECHA"}: {date_str}', '-'*60]
            ch_reports = {}

            for channel in ['catv', 'tvd', 'latam', 'us', 'tn', 'hu', 'hl']:
                if channel not in selected: continue
                key = (date_str, channel)
                if key not in days: continue
                info     = days[key]
                grilla_f = _pick_grilla(channel, date_str)
                jsons    = sorted(info['json'], key=lambda f: f.name)
                ch_lines = []

                for jf in jsons:
                    if channel in ('hu', 'hl'):
                        plines = process_holatv(channel, jf, info, grilla_f)
                        ch_lines += plines
                    else:
                        xml_file = info.get('xml')
                        if xml_file:  xml_file.seek(0)
                        if grilla_f:  grilla_f.seek(0)
                        result = process_one(channel, jf, xml_file, grilla_f, date_str)
                        plines, warns = result if isinstance(result, tuple) else (result, [])
                        ch_lines += plines
                        if warns:
                            all_manual_warns.append(
                                (CH_DISPLAY_GLOBAL.get(channel, channel.upper()), date_str, len(warns))
                            )

                ch_reports[channel] = ch_lines
                d_lines += ch_lines

            d_lines.append('')
            day_reports[date_str] = {'all': d_lines, 'channels': ch_reports}

    # ── SONY / AXN PROCESSING ────────────────────────────────────────────────
    sony_date_ch = {}
    sony_by_code = {}
    if sony_files_raw and selected_sony:
        with st.spinner(t('running')):
            sony_pairings = pair_sony_files(sony_files_raw, lang)
        sep60 = '='*60
        for pair in sony_pairings:
            if pair['code'] not in selected_sony: continue
            code     = pair['code']
            date_str = str(pair['date']) if pair['date'] else '?'
            markers_in_json = parse_sony_json_markers(pair['json_data']) if pair['json_data'] else []
            if pair['json_data'] is None:   pl_type = SL('pl_none', lang)
            elif markers_in_json:           pl_type = SL('pl_full', lang)
            else:                           pl_type = SL('pl_partial', lang)

            pairing_lines = [sep60,
                             f'CHANNEL: {code} — {pair["channel_name"]}',
                             f'DATE: {date_str}',
                             f'PLAYLIST TYPE: {pl_type}',
                             f'JSON: {pair["json_file"].name if pair["json_file"] else "— not provided —"}',
                             f'LOG:  {pair["xml_filename"] or "— not provided —"}',
                             sep60]

            if pair['json_data'] is None and pair['xml_file']:
                pairing_lines.append(SL('no_json', lang))
            elif pair['json_data'] is not None and pair['xml_file'] is None:
                pairing_lines.append(SL('no_log', lang))
                try:
                    r_lines, _ = check_sony(pair['json_data'], [], None, lang)
                    pairing_lines += r_lines
                except: pass
            elif pair['json_data'] is not None and pair['xml_file'] is not None:
                try:
                    pair['xml_file'].seek(0)
                    xml_rows_sony = parse_sony_xml_log(pair['xml_file'])
                    r_lines, _ = check_sony(pair['json_data'], xml_rows_sony,
                                            pair['xml_filename'], lang)
                    pairing_lines += r_lines
                except Exception as e:
                    pairing_lines.append(f'  ERROR: {e}')
            pairing_lines.append('')

            sony_date_ch.setdefault(date_str, {})
            sony_date_ch[date_str].setdefault(code, [])
            sony_date_ch[date_str][code] += pairing_lines
            sony_by_code.setdefault(code, [])
            sony_by_code[code] += pairing_lines

    # ── BUILD FULL REPORT TEXT ────────────────────────────────────────────────
    st.subheader(t('report'))
    all_lines = header_lines[:]
    for d in sorted_dates:
        all_lines += day_reports.get(d, {}).get('all', [])
    for date_str, codes in sorted(sony_date_ch.items()):
        all_lines += [f'{"DATE" if lang=="en" else "FECHA"}: {date_str} (Sony/AXN)', '-'*60]
        for code, lines in sorted(codes.items()):
            all_lines += lines
    full_text = '\n'.join(all_lines)

    if all_manual_warns:
        warn_lines = ['⚠ MANUAL REVIEW NEEDED:' if lang=='en' else '⚠ REVISION MANUAL:']
        for ch_lbl, d_str, cnt in all_manual_warns:
            warn_lines.append(f'  {ch_lbl} — {d_str}: {cnt} block{"s" if cnt>1 else ""} need manual review')
        st.error('\n'.join(warn_lines))

    CH_DISPLAY2 = {**CH_DISPLAY_GLOBAL,
                   **{code: f'{code} {SONY_EMOJI.get(code,"📺")} {SONY_CHANNEL_MAP.get(code,code)}'
                      for code in sony_by_code}}

    all_tab_dates = sorted(set(list(sorted_dates) + list(sony_date_ch.keys())))

    def _render_day_tab(date_str, key_prefix):
        day_data      = day_reports.get(date_str, {})
        ch_reports    = dict(day_data.get('channels', {}))
        sony_for_date = sony_date_ch.get(date_str, {})
        for code, lines in sorted(sony_for_date.items()):
            ch_reports[code] = lines
        day_text = '\n'.join(header_lines + day_data.get('all', []) +
                             [l for lines in sony_for_date.values() for l in lines])
        if len(ch_reports) > 1:
            ch_tab_labels = [CH_DISPLAY2.get(ch, ch) for ch in ch_reports]
            ch_tabs = st.tabs(ch_tab_labels)
            for j, (ch, ch_lines) in enumerate(ch_reports.items()):
                with ch_tabs[j]:
                    ch_text = '\n'.join(header_lines + ch_lines)
                    st.text(ch_text)
                    st.download_button(t('dl'), ch_text,
                                       file_name=f'report_{date_str}_{ch}_{datetime.now().strftime("%H%M%S")}.txt',
                                       mime='text/plain', use_container_width=True,
                                       key=f'dl_{key_prefix}_{date_str}_{ch}')
        else:
            st.text(day_text)
        st.download_button(f'⬇ {date_str} (.txt)',
                           day_text,
                           file_name=f'report_{date_str}_{datetime.now().strftime("%H%M%S")}.txt',
                           mime='text/plain', use_container_width=True,
                           key=f'dl_{key_prefix}_{date_str}_all')

    if all_tab_dates:
        tab_labels = [t('tab_all')] + [f'📅 {d}' for d in all_tab_dates]
        tabs = st.tabs(tab_labels)
        with tabs[0]:
            st.text(full_text)
            st.download_button(
                '⬇ Full Report (.txt)' if lang=='en' else '⬇ Reporte Completo (.txt)',
                full_text,
                file_name=f'report_all_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt',
                mime='text/plain', use_container_width=True, key='dl_all')
        for i, date_str in enumerate(all_tab_dates):
            with tabs[i+1]:
                _render_day_tab(date_str, 'day')
    else:
        st.text(full_text)
        st.download_button(
            '⬇ Full Report (.txt)' if lang=='en' else '⬇ Reporte Completo (.txt)',
            full_text,
            file_name=f'report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt',
            mime='text/plain', use_container_width=True, key='dl_single_all')
