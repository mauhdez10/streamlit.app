"""
Broadcast Playlist Checker — Streamlit App v6
"""
import streamlit as st
import json
from datetime import datetime
import os, sys
sys.path.insert(0, os.path.dirname(__file__))
from checker import (
    parse_json_playlist, parse_xml_log, parse_grilla,
    generate_report, check_promo_repeats, detect_files
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
days, grillas, unknown_files = detect_files(uploaded) if uploaded else ({}, {}, [])

# ── DETECTION TABLE ───────────────────────────────────────────────────────────
if uploaded:
    st.markdown(t('detected'))
    CH_DISPLAY = {'catv':'CATV 🌎','tvd':'TVD 📺','latam':'Pasiones Latam 🌹','us':'Pasiones US ⭐'}
    rows = []
    for (date_str, channel), info in sorted(days.items()):
        ch = CH_DISPLAY.get(channel, channel.upper())
        # Format date as MM/DD for display
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
    if rows:
        import pandas as pd
        st.dataframe(pd.DataFrame(rows), use_container_width=True,
                     hide_index=True, height=min(400, 35 + len(rows)*35))
    st.caption(t('hint'))
    st.divider()

# ── CHANNEL SELECTOR ──────────────────────────────────────────────────────────
available = sorted(set(ch for (_, ch) in days.keys()) | set(grillas.keys()))
if available:
    selected = st.multiselect(
        t('channels'), options=available, default=available,
        format_func=lambda x: {'catv':'CATV 🌎','tvd':'TVD 📺','latam':'Pasiones Latam 🌹','us':'Pasiones US ⭐'}.get(x,x)
    )
else:
    selected = []

# ── RUN ───────────────────────────────────────────────────────────────────────
if st.button(t('run'), type='primary', use_container_width=True):
    if not days:
        st.error(t('no_json')); st.stop()

    CH_LABELS = {
        'catv': 'CATV', 'tvd': 'TVD',
        'latam': 'Pasiones Latam', 'us': 'Pasiones US'
    }

    def process_one(channel, json_file, xml_file, grilla_file):
        lines = []
        try:
            json_file.seek(0)
            data = json.load(json_file)
            playlist = parse_json_playlist(data)
        except Exception as e:
            return [f'ERROR parsing JSON: {e}'], []
        xml_rows = []
        if xml_file:
            try: xml_file.seek(0); xml_rows = parse_xml_log(xml_file)
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
        report_text, manual_warns = generate_report(ch_label, playlist, xml_rows, grilla_ids, lang)
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

    all_manual_warns = []
    with st.spinner(t('running')):
        for date_str in sorted_dates:
            d_lines = [f'{"DATE" if lang=="en" else "FECHA"}: {date_str}', '─'*60]
            for channel in ['catv', 'tvd']:
                if channel not in selected: continue
                key = (date_str, channel)
                if key not in days: continue
                info      = days[key]
                xml_file  = info.get('xml')
                grilla_f  = grillas.get(channel)
                jsons     = sorted(info['json'], key=lambda f: f.name)
                for jf in jsons:
                    d_lines.append(f'JSON: {jf.name}')
                    if xml_file:  xml_file.seek(0)
                    if grilla_f:  grilla_f.seek(0)
                    result = process_one(channel, jf, xml_file, grilla_f)
                    plines, warns = result if isinstance(result, tuple) else (result, [])
                    d_lines += plines
                    all_manual_warns.extend(warns)
            d_lines.append('')
            day_reports[date_str] = d_lines

    # ── DISPLAY WITH TABS ─────────────────────────────────────────────────────
    st.subheader(t('report'))
    all_lines = header_lines[:]
    for d in sorted_dates:
        all_lines += day_reports.get(d, [])
    full_text = '\n'.join(all_lines)

    # Display manual check warnings in red
    if all_manual_warns:
        st.error('\n'.join(all_manual_warns))

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
                day_text = '\n'.join(header_lines + day_reports.get(date_str, []))
                st.text(day_text)
                st.download_button(t('dl'), day_text,
                                   file_name=f'report_{date_str}_{datetime.now().strftime("%H%M%S")}.txt',
                                   mime='text/plain', use_container_width=True,
                                   key=f'dl_{date_str}')
    else:
        st.text(full_text)
        st.download_button(t('dl'), full_text,
                           file_name=f'report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt',
                           mime='text/plain', use_container_width=True)
