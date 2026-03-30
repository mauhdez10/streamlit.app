"""
Broadcast Playlist Checker — Streamlit App v3
"""
import streamlit as st
import json
from datetime import datetime
import os, sys
sys.path.insert(0, os.path.dirname(__file__))
from checker import parse_json_playlist, parse_xml_log, parse_grilla, generate_report, check_promo_repeats

st.set_page_config(page_title='Broadcast Playlist Checker', layout='wide')
st.title('📋 Broadcast Playlist Checker')
st.caption('Upload JSON + XML + Grilla for TVD and/or CATV. XML and Grilla are optional but needed for full checks.')

col1, col2 = st.columns(2)

with col1:
    st.subheader('TVD 🇩🇴')
    tvd_json = st.file_uploader('Vipe JSON (TVD)', type=['json'], key='tvd_json')
    tvd_xml  = st.file_uploader('Playlist XML (TVD)', type=None, key='tvd_xml')
    tvd_grid = st.file_uploader('Grilla XLSX (TVD)', type=None, key='tvd_grid')

with col2:
    st.subheader('CATV 🌎')
    catv_json = st.file_uploader('Vipe JSON (CATV)', type=['json'], key='catv_json')
    catv_xml  = st.file_uploader('Playlist XML (CATV)', type=None, key='catv_xml')
    catv_grid = st.file_uploader('Grilla XLSX (CATV)', type=None, key='catv_grid')

st.caption('💡 JSON only → promo repeat check.  JSON + XML → commercial check.  JSON + Grilla → program check.')
st.divider()

if st.button('▶  Run Check', type='primary', use_container_width=True):
    if not tvd_json and not catv_json:
        st.error('Upload at least one Vipe JSON to proceed.')
        st.stop()

    report_lines = ['BROADCAST PLAYLIST CHECK REPORT',
                    f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
                    '═' * 60, '']

    def process(label, json_file, xml_file, grid_file):
        if not json_file:
            return []
        lines = []
        try:
            data = json.load(json_file)
            playlist = parse_json_playlist(data)
        except Exception as e:
            return [f'ERROR parsing JSON for {label}: {e}']

        xml_rows = []
        if xml_file:
            try:
                xml_rows = parse_xml_log(xml_file)
                if not xml_rows:
                    lines.append(f'  WARNING: XML log parsed but returned 0 rows')
            except Exception as e:
                lines.append(f'  WARNING: Could not parse XML log: {e}')

        grilla_ids = []
        if grid_file and playlist['date']:
            try:
                grilla_ids = parse_grilla(grid_file, playlist['date'])
            except Exception as e:
                lines.append(f'  WARNING: Could not parse Grilla: {e}')

        # Promo-only mode (no XML and no grilla)
        if not xml_rows and not grilla_ids:
            promo_issues = check_promo_repeats(playlist)
            lines += ['═' * 60,
                      f'CHANNEL: {label.upper()} — PROMO REPEAT CHECK ONLY',
                      f'DATE: {playlist["date"]} | TYPE: {"FULL DAY" if playlist["type"]=="full" else "CURRENT"}',
                      '═' * 60]
            lines += promo_issues if promo_issues else ['  ✓ No repeated promos within any break']
            lines.append('')
            return lines

        lines.append(generate_report(label, playlist, xml_rows, grilla_ids))
        return lines

    with st.spinner('Running checks...'):
        report_lines += process('TVD',  tvd_json, tvd_xml,  tvd_grid)
        report_lines += process('CATV', catv_json, catv_xml, catv_grid)

    report_text = '\n'.join(report_lines)
    st.subheader('📄 Report')
    st.text(report_text)
    st.download_button('⬇ Download Report (.txt)', report_text,
                       file_name=f'broadcast_check_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt',
                       mime='text/plain', use_container_width=True)
