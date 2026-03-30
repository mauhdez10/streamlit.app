"""
Broadcast Playlist Checker — Streamlit App v4
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
st.title('📋 Broadcast Playlist Checker')

# ── SINGLE UPLOAD ─────────────────────────────────────────────────────────────
uploaded = st.file_uploader(
    'Drop all files here — JSON, XML, and XLSX will be auto-detected by filename',
    accept_multiple_files=True,
    type=None,
    key='all_files'
)

detected, unknown_files = detect_files(uploaded) if uploaded else ({'catv': {}, 'tvd': {}}, [])

# Detection summary
if uploaded:
    col1, col2 = st.columns(2)
    icons = {'json': '🗂', 'xml': '📄', 'grilla': '📊'}
    labels = {'json': 'Vipe JSON', 'xml': 'Playlist XML', 'grilla': 'Grilla XLSX'}

    with col1:
        st.caption('**CATV 🌎** — detected files')
        for ft in ['json', 'xml', 'grilla']:
            f = detected['catv'].get(ft)
            if f:
                st.success(f'{icons[ft]} {labels[ft]}: {f.name}', icon='✅')
            else:
                st.warning(f'{labels[ft]}: not uploaded', icon='⬜')

    with col2:
        st.caption('**TVD 📺** — detected files')
        for ft in ['json', 'xml', 'grilla']:
            f = detected['tvd'].get(ft)
            if f:
                st.success(f'{icons[ft]} {labels[ft]}: {f.name}', icon='✅')
            else:
                st.warning(f'{labels[ft]}: not uploaded', icon='⬜')

    if unknown_files:
        st.error(f'⚠ Unrecognized: {", ".join(f.name for f in unknown_files)}')

    st.caption('💡 JSON only → promo repeat check. + XML → commercial check. + Grilla → program check.')
    st.divider()

# ── RUN ───────────────────────────────────────────────────────────────────────
if st.button('▶  Run Check', type='primary', use_container_width=True):
    if not detected.get('catv', {}).get('json') and not detected.get('tvd', {}).get('json'):
        st.error('Upload at least one Vipe JSON to proceed.')
        st.stop()

    report_lines = [
        'BROADCAST PLAYLIST CHECK REPORT',
        f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
        '═' * 60, ''
    ]

    def process(channel_key, label):
        ch    = detected.get(channel_key, {})
        jfile = ch.get('json')
        xfile = ch.get('xml')
        gfile = ch.get('grilla')

        if not jfile:
            return []

        lines = []
        try:
            data = json.load(jfile)
            playlist = parse_json_playlist(data)
        except Exception as e:
            return [f'ERROR parsing JSON for {label}: {e}']

        xml_rows = []
        if xfile:
            try:
                xfile.seek(0)
                xml_rows = parse_xml_log(xfile)
                if not xml_rows:
                    lines.append(f'  WARNING: XML log empty or unreadable')
            except Exception as e:
                lines.append(f'  WARNING: XML parse error: {e}')

        grilla_ids = []
        if gfile and playlist['date']:
            try:
                gfile.seek(0)
                grilla_ids = parse_grilla(gfile, playlist['date'])
            except Exception as e:
                lines.append(f'  WARNING: Grilla parse error: {e}')

        # Promo-only mode
        if not xml_rows and not grilla_ids:
            promo_issues = check_promo_repeats(playlist)
            lines += [
                '═' * 60,
                f'CHANNEL: {label} — PROMO REPEAT CHECK ONLY',
                f'DATE: {playlist["date"]} | TYPE: {"FULL DAY" if playlist["type"]=="full" else "CURRENT"}',
                '═' * 60,
            ]
            lines += promo_issues if promo_issues else ['  ✓ No repeated promos']
            lines.append('')
            return lines

        lines.append(generate_report(label, playlist, xml_rows, grilla_ids))
        return lines

    with st.spinner('Running checks...'):
        report_lines += process('catv', 'CATV')
        report_lines += process('tvd',  'TVD')

    report_text = '\n'.join(report_lines)
    st.subheader('📄 Report')
    st.text(report_text)
    st.download_button(
        '⬇ Download Report (.txt)', report_text,
        file_name=f'broadcast_check_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt',
        mime='text/plain', use_container_width=True
    )
