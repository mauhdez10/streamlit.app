"""
Broadcast Playlist Checker — Streamlit App v6
"""
import streamlit as st
import json
from datetime import datetime
import os, sys
sys.path.insert(0, os.path.dirname(__file__))
from checker import (
    parse_json_playlist, parse_xml_log, parse_xml_log_tn, parse_grilla,
    generate_report, check_promo_repeats, detect_files,
    parse_sony_xml_log, check_sony, pair_sony_files, SONY_CHANNEL_MAP
)

st.set_page_config(page_title='Broadcast Playlist Checker', layout='wide')

# ── LANGUAGE ──────────────────────────────────────────────────────────────────
lang = st.radio('🌐', ['English', 'Español'], horizontal=True, label_visibility='collapsed')
lang = 'es' if lang == 'Español' else 'en'

L = {
    'title':    {'en': '📋 Broadcast Playlist Checker',             'es': '📋 Verificador de Playlist'},
    'upload':   {'en': 'Drop all files here — auto-detected (JSON, XML, XLSX)', 'es': 'Arrastra archivos aquí — detección automática (JSON, XML, XLSX)'},
    'run':      {'en': '▶  Run Check',                              'es': '▶  Verificar'},
    'dl':       {'en': '⬇ Download Report (.txt)',                  'es': '⬇ Descargar Reporte (.txt)'},
    'detected': {'en': '**Detected files:**',                       'es': '**Archivos detectados:**'},
    'unknown':  {'en': '⚠ Unrecognized:',                          'es': '⚠ No reconocidos:'},
    'hint':     {'en': 'JSON → promo check  |  +XML → commercial check  |  +Grilla → program check',
                 'es': 'JSON → promos  |  +XML → comerciales  |  +Grilla → programas'},
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
    st.markdown(t('detected'))
    CH_DISPLAY = {'catv':'CATV 🌎','tvd':'TVD 📺','latam':'Pasiones Latam 🌹',
                  'us':'Pasiones US ⭐','tn':'Fast Todonovelas 📺'}
    rows = []
    for (date_str, channel), info in sorted(days.items()):
        ch = CH_DISPLAY.get(channel, channel.upper())
        try:
            from datetime import datetime as _dt
            d = _dt.strptime(date_str, '%Y-%m-%d')
            short_date = d.strftime('%m/%d')
        except: short_date = date_str
        for jf in info['json']:
            rows.append({'Date': date_str, 'Channel': ch,
                         'Type': f'Playlist {short_date}', 'File': jf.name})
        if info['xml']:
            rows.append({'Date': date_str, 'Channel': ch,
                         'Type': f'Log {short_date}', 'File': info['xml'].name})

    # For grillas, read the week-start Monday date from the file content
    for ch_key, gf in grillas.items():
        ch = CH_DISPLAY.get(ch_key, ch_key.upper())
        grilla_date_str = '(Week)'
        try:
            from openpyxl import load_workbook
            import io
            gf.seek(0)
            wb = load_workbook(io.BytesIO(gf.read()), read_only=True)
            gf.seek(0)
            ws = wb.active
            rows_g = list(ws.iter_rows(max_row=3, values_only=True))
            if len(rows_g) > 1:
                monday_val = rows_g[1][2] if len(rows_g[1]) > 2 else None
                if monday_val and hasattr(monday_val, 'strftime'):
                    grilla_date_str = monday_val.strftime('%m/%d')
        except: pass
        grilla_label = f'{"Grilla" if lang=="es" else "Grid"} {grilla_date_str}'
        rows.append({'Date': grilla_date_str, 'Channel': ch,
                     'Type': grilla_label, 'File': gf.name})
    for uf in unknown_files:
        rows.append({'Date': '?', 'Channel': '?', 'Type': '?', 'File': uf.name})
    # Sony files
    for sf in sony_files_raw:
        ch = SONY_CHANNEL_MAP.get(sf['code'], sf['code'])
        rows.append({'Date': '(Sony)', 'Channel': ch,
                     'Type': sf['ftype'].upper(), 'File': sf['file'].name})
    if rows:
        import pandas as pd
        st.dataframe(pd.DataFrame(rows), use_container_width=True,
                     hide_index=True, height=min(500, 35 + len(rows)*35))
    st.caption(t('hint'))
    st.divider()

# ── CHANNEL SELECTOR ──────────────────────────────────────────────────────────
available = sorted(set(ch for (_, ch) in days.keys()) | set(grillas.keys()))
has_sony = bool(sony_files_raw)
if available:
    selected = st.multiselect(
        t('channels'), options=available, default=available,
        format_func=lambda x: {'catv':'CATV 🌎','tvd':'TVD 📺','latam':'Pasiones Latam 🌹','us':'Pasiones US ⭐','tn':'Fast Todonovelas 📺'}.get(x,x)
    )
else:
    selected = []

# ── RUN ───────────────────────────────────────────────────────────────────────
if st.button(t('run'), type='primary', use_container_width=True):
    if not days and not sony_files_raw:
        st.error(t('no_json')); st.stop()

    CH_LABELS = {
        'catv': 'CATV', 'tvd': 'TVD',
        'latam': 'Pasiones Latam', 'us': 'Pasiones US',
        'tn': 'Fast Todonovelas'
    }

    def process_one(channel, json_file, xml_file, grilla_file):
        is_tn = (channel == 'tn')
        lines = []
        try:
            json_file.seek(0)
            data = json.load(json_file)
            playlist = parse_json_playlist(data)
        except Exception as e:
            return [f'ERROR parsing JSON: {e}'], []
        xml_rows = []
        if xml_file:
            try:
                xml_file.seek(0)
                xml_rows = parse_xml_log_tn(xml_file) if is_tn else parse_xml_log(xml_file)
            except Exception as e: lines.append(f'  WARNING: XML error: {e}')
        grilla_ids = []
        if grilla_file and playlist['date']:
            try:
                grilla_file.seek(0)
                grilla_ids = parse_grilla(grilla_file, playlist['date'], channel)
            except Exception as e: lines.append(f'  WARNING: Grilla error: {e}')
        if not xml_rows and not grilla_ids:
            pi = check_promo_repeats(playlist, lang=lang)
            ch_label = CH_LABELS.get(channel, channel.upper())
            lines += ['═'*60,
                      f'CHANNEL: {ch_label} | DATE: {playlist["date"]} | {"PROMO CHECK" if lang=="en" else "VERIFICACIÓN DE PROMOS"}',
                      '═'*60]
            lines += pi if pi else ['  ✓ No repeated promos']
            lines.append('')
            return lines, []
        ch_label = CH_LABELS.get(channel, channel.upper())
        report_text, manual_warns = generate_report(ch_label, playlist, xml_rows, grilla_ids, lang, is_tn=is_tn)
        lines.append(report_text)
        return lines, manual_warns

    # Build per-day reports
    sorted_dates = sorted(set(d for (d, _) in days.keys()))
    day_reports  = {}  # date_str -> list of lines
    header_lines = [
        'BROADCAST PLAYLIST CHECK REPORT' if lang=='en' else 'REPORTE DE VERIFICACIÓN DE PLAYLIST',
        f'Generated / Generado: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
        '═'*60, ''
    ]

    all_manual_warns = []  # list of (channel_label, date_str, count)
    CH_DISPLAY = {'catv':'CATV 🌎','tvd':'TVD 📺','latam':'Pasiones Latam 🌹','us':'Pasiones US ⭐','tn':'Fast Todonovelas 📺'}
    with st.spinner(t('running')):
        for date_str in sorted_dates:
            d_lines = [f'{"DATE" if lang=="en" else "FECHA"}: {date_str}', '─'*60]
            ch_reports = {}  # channel -> lines
            for channel in ['catv', 'tvd', 'latam', 'us', 'tn']:
                if channel not in selected: continue
                key = (date_str, channel)
                if key not in days: continue
                info     = days[key]
                xml_file = info.get('xml')
                grilla_f = grillas.get(channel)
                jsons    = sorted(info['json'], key=lambda f: f.name)
                ch_lines = []
                for jf in jsons:
                    ch_lines.append(f'JSON: {jf.name}')
                    if xml_file:  xml_file.seek(0)
                    if grilla_f:  grilla_f.seek(0)
                    result = process_one(channel, jf, xml_file, grilla_f)
                    plines, warns = result if isinstance(result, tuple) else (result, [])
                    ch_lines += plines
                    if warns:
                        all_manual_warns.append(
                            (CH_DISPLAY.get(channel, channel.upper()), date_str, len(warns))
                        )
                ch_reports[channel] = ch_lines
                d_lines += ch_lines
            d_lines.append('')
            day_reports[date_str] = {'all': d_lines, 'channels': ch_reports}

    # ── SONY / AXN PROCESSING ─────────────────────────────────────────────────
    sony_report_lines = []
    if sony_files_raw:
        sony_pairings = pair_sony_files(sony_files_raw, lang)
        sep60 = '═' * 60
        for pair in sony_pairings:
            sony_report_lines.append(sep60)
            sony_report_lines.append(f'CHANNEL: {pair["channel_name"]}')
            if pair['date']:
                sony_report_lines.append(f'DATE: {pair["date"]}')
            sony_report_lines.append(f'JSON: {pair["json_file"].name if pair["json_file"] else "— not provided —"}')
            sony_report_lines.append(f'LOG:  {pair["xml_filename"] or "— not provided —"}')
            sony_report_lines.append(sep60)

            if pair['json_data'] is None and pair['xml_file']:
                sony_report_lines.append('  ℹ  Log provided but no matching JSON found')
            elif pair['json_data'] is not None and pair['xml_file'] is None:
                sony_report_lines.append('  ℹ  JSON found but no matching log provided')
                # Still run marker list
                try:
                    r_lines, _ = check_sony(pair['json_data'], [], None, lang)
                    sony_report_lines += r_lines
                except: pass
            elif pair['json_data'] is not None and pair['xml_file'] is not None:
                try:
                    pair['xml_file'].seek(0)
                    xml_rows = parse_sony_xml_log(pair['xml_file'])
                    r_lines, has_err = check_sony(pair['json_data'], xml_rows,
                                                   pair['xml_filename'], lang)
                    sony_report_lines += r_lines
                except Exception as e:
                    sony_report_lines.append(f'  ERROR: {e}')
            sony_report_lines.append('')

    # ── DISPLAY WITH TABS ─────────────────────────────────────────────────────
    st.subheader(t('report'))
    all_lines = header_lines[:]
    for d in sorted_dates:
        all_lines += day_reports.get(d, {}).get('all', [])
    if sony_report_lines:
        all_lines += ['', '── SONY / AXN ──────────────────────────────────'] + sony_report_lines
    full_text = '\n'.join(all_lines)

    if all_manual_warns:
        warn_lines = ['⚠ MANUAL REVIEW NEEDED:' if lang == 'en' else '⚠ REVISIÓN MANUAL REQUERIDA:']
        for ch_lbl, d_str, cnt in all_manual_warns:
            warn_lines.append(f'  {ch_lbl} — {d_str}: {cnt} block{"s" if cnt>1 else ""} need manual review')
        st.error('\n'.join(warn_lines))

    if len(sorted_dates) > 1:
        tab_labels = [t('tab_all')] + [f'📅 {d}' for d in sorted_dates]
        tabs = st.tabs(tab_labels)

        with tabs[0]:
            st.text(full_text)
            st.download_button(t('dl'), full_text,
                               file_name=f'report_all_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt',
                               mime='text/plain', use_container_width=True, key='dl_all')

        for i, date_str in enumerate(sorted_dates):
            with tabs[i+1]:
                day_data   = day_reports.get(date_str, {})
                ch_reports = day_data.get('channels', {})
                day_text   = '\n'.join(header_lines + day_data.get('all', []))

                if len(ch_reports) > 1:
                    ch_tab_labels = [CH_DISPLAY.get(ch, ch) for ch in ch_reports]
                    ch_tabs = st.tabs(ch_tab_labels)
                    for j, (ch, ch_lines) in enumerate(ch_reports.items()):
                        with ch_tabs[j]:
                            ch_text = '\n'.join(header_lines + [f'DATE: {date_str}', '─'*60] + ch_lines)
                            st.text(ch_text)
                            st.download_button(t('dl'), ch_text,
                                               file_name=f'report_{date_str}_{ch}_{datetime.now().strftime("%H%M%S")}.txt',
                                               mime='text/plain', use_container_width=True,
                                               key=f'dl_{date_str}_{ch}')
                else:
                    st.text(day_text)

                st.download_button(t('dl') + f' ({date_str})', day_text,
                                   file_name=f'report_{date_str}_{datetime.now().strftime("%H%M%S")}.txt',
                                   mime='text/plain', use_container_width=True,
                                   key=f'dl_{date_str}_all')
    else:
        # Single day — show channel tabs if multiple channels
        day_data   = day_reports.get(sorted_dates[0], {}) if sorted_dates else {}
        ch_reports = day_data.get('channels', {})

        if len(ch_reports) > 1:
            ch_tab_labels = [CH_DISPLAY.get(ch, ch) for ch in ch_reports]
            ch_tabs = st.tabs(ch_tab_labels)
            for j, (ch, ch_lines) in enumerate(ch_reports.items()):
                with ch_tabs[j]:
                    ch_text = '\n'.join(header_lines + ch_lines)
                    st.text(ch_text)
                    st.download_button(t('dl'), ch_text,
                                       file_name=f'report_{sorted_dates[0]}_{ch}_{datetime.now().strftime("%H%M%S")}.txt',
                                       mime='text/plain', use_container_width=True,
                                       key=f'dl_single_{ch}')
        else:
            st.text(full_text)

        st.download_button(t('dl'), full_text,
                           file_name=f'report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt',
                           mime='text/plain', use_container_width=True, key='dl_single_all')
