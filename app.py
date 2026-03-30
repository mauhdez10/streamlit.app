"""
Broadcast Playlist Checker — Streamlit App v5
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

# ── LANGUAGE TOGGLE ───────────────────────────────────────────────────────────
lang = st.radio('🌐 Language / Idioma', ['English', 'Español'],
                horizontal=True, label_visibility='collapsed')
lang = 'es' if lang == 'Español' else 'en'
TITLES = {'en': '📋 Broadcast Playlist Checker', 'es': '📋 Verificador de Playlist'}
UPLOAD_LABEL = {'en': 'Drop all files here — auto-detected by filename (JSON, XML, XLSX)',
                'es': 'Arrastra todos los archivos aquí — detección automática (JSON, XML, XLSX)'}
RUN_BTN = {'en': '▶  Run Check', 'es': '▶  Ejecutar Verificación'}
DL_BTN  = {'en': '⬇ Download Report (.txt)', 'es': '⬇ Descargar Reporte (.txt)'}
DETECTED_LBL = {'en': 'Detected files:', 'es': 'Archivos detectados:'}
UNKNOWN_LBL  = {'en': 'Unrecognized:', 'es': 'No reconocidos:'}

st.title(TITLES[lang])

# ── SINGLE UPLOAD ─────────────────────────────────────────────────────────────
uploaded = st.file_uploader(
    UPLOAD_LABEL[lang],
    accept_multiple_files=True,
    type=None,
    key='all_files'
)

days, grillas, unknown_files = detect_files(uploaded) if uploaded else ({}, {}, [])

# ── DETECTION SUMMARY ─────────────────────────────────────────────────────────
if uploaded:
    st.markdown(f'**{DETECTED_LBL[lang]}**')

    # Show all detected files in one scrollable table
    all_detected = []
    for (date_str, channel), info in sorted(days.items()):
        ch_label = 'CATV 🌎' if channel == 'catv' else 'TVD 📺'
        for jf in info['json']:
            all_detected.append({'Date': date_str, 'Channel': ch_label,
                                  'Type': 'JSON', 'File': jf.name})
        if info['xml']:
            all_detected.append({'Date': date_str, 'Channel': ch_label,
                                  'Type': 'XML', 'File': info['xml'].name})
    for ch, gf in grillas.items():
        ch_label = 'CATV 🌎' if ch == 'catv' else 'TVD 📺'
        all_detected.append({'Date': '(week)', 'Channel': ch_label,
                              'Type': 'Grilla', 'File': gf.name})
    if unknown_files:
        for uf in unknown_files:
            all_detected.append({'Date': '?', 'Channel': '?', 'Type': '?', 'File': uf.name})

    if all_detected:
        import pandas as pd
        df = pd.DataFrame(all_detected)
        st.dataframe(df, use_container_width=True, hide_index=True, height=min(400, 35 + len(all_detected)*35))

    st.caption('💡 JSON → promo check  |  + XML → commercial check  |  + Grilla → program check')
    st.divider()

# ── CHANNEL SELECTOR ──────────────────────────────────────────────────────────
available_channels = sorted(set(ch for (_, ch) in days.keys()) | set(grillas.keys()))
if available_channels:
    ch_options = {'catv': 'CATV 🌎', 'tvd': 'TVD 📺'}
    selected_channels = st.multiselect(
        'Channels to check:' if lang == 'en' else 'Canales a verificar:',
        options=available_channels,
        default=available_channels,
        format_func=lambda x: ch_options.get(x, x)
    )
else:
    selected_channels = []

# ── RUN ───────────────────────────────────────────────────────────────────────
if st.button(RUN_BTN[lang], type='primary', use_container_width=True):
    if not days:
        st.error('Upload at least one Vipe JSON.' if lang == 'en' else 'Sube al menos un JSON de Vipe.')
        st.stop()

    report_lines = [
        'BROADCAST PLAYLIST CHECK REPORT' if lang == 'en' else 'REPORTE DE VERIFICACIÓN DE PLAYLIST',
        f'Generated / Generado: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
        '═' * 60, ''
    ]

    def process_one(channel, json_file, xml_file, grilla_file, date):
        lines = []
        try:
            json_file.seek(0)
            data = json.load(json_file)
            playlist = parse_json_playlist(data)
        except Exception as e:
            return [f'ERROR parsing JSON: {e}']

        xml_rows = []
        if xml_file:
            try:
                xml_file.seek(0)
                xml_rows = parse_xml_log(xml_file)
            except Exception as e:
                lines.append(f'  WARNING: XML error: {e}')

        grilla_ids = []
        if grilla_file and playlist['date']:
            try:
                grilla_file.seek(0)
                grilla_ids = parse_grilla(grilla_file, playlist['date'])
            except Exception as e:
                lines.append(f'  WARNING: Grilla error: {e}')

        if not xml_rows and not grilla_ids:
            pi = check_promo_repeats(playlist, lang=lang)
            sep = '═' * 60
            ch_label = 'CATV' if channel == 'catv' else 'TVD'
            lines += [sep,
                      f'CHANNEL: {ch_label} | DATE: {playlist["date"]} | PROMO CHECK ONLY',
                      sep]
            lines += pi if pi else ['  ✓ No repeated promos']
            lines.append('')
            return lines

        lines.append(generate_report('CATV' if channel=='catv' else 'TVD',
                                     playlist, xml_rows, grilla_ids, lang))
        return lines

    with st.spinner('Running checks...' if lang == 'en' else 'Verificando...'):
        # Process by date then channel
        dates_sorted = sorted(set(d for (d, _) in days.keys()))
        for date_str in dates_sorted:
            report_lines.append(f'{"DATE" if lang=="en" else "FECHA"}: {date_str}')
            report_lines.append('─' * 60)

            for channel in ['catv', 'tvd']:
                if channel not in selected_channels:
                    continue
                key = (date_str, channel)
                if key not in days:
                    continue

                info     = days[key]
                xml_file = info.get('xml')
                grilla_f = grillas.get(channel)
                jsons    = sorted(info['json'], key=lambda f: f.name)

                ch_label = 'CATV 🌎' if channel == 'catv' else 'TVD 📺'

                for jf in jsons:
                    report_lines.append(f'JSON: {jf.name}')
                    if xml_file: xml_file.seek(0)
                    if grilla_f: grilla_f.seek(0)
                    report_lines += process_one(channel, jf, xml_file, grilla_f, date_str)

            report_lines.append('')

    report_text = '\n'.join(report_lines)
    st.subheader('📄 Report' if lang == 'en' else '📄 Reporte')
    st.text(report_text)
    st.download_button(DL_BTN[lang], report_text,
                       file_name=f'broadcast_check_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt',
                       mime='text/plain', use_container_width=True)
