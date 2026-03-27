"""
Broadcast Playlist Checker - Streamlit App
Upload JSON + XLS + Grilla for TVD and/or CATV and get a plain-text report.
"""
import streamlit as st
import json
from datetime import datetime
import os, sys
sys.path.insert(0, os.path.dirname(__file__))
from checker import (
    parse_json_playlist, parse_xls_log, parse_grilla,
    generate_report, check_promo_repeats, fmt_time
)

st.set_page_config(page_title='Broadcast Playlist Checker', layout='wide')
st.title('📋 Broadcast Playlist Checker')
st.caption('Upload files for TVD and/or CATV. XLS log + Grilla are optional but required for full checks.')

# ── FILE UPLOADS ──────────────────────────────────────────────────────────────

col1, col2 = st.columns(2)

with col1:
    st.subheader('🇩🇴 TVD')
    tvd_json = st.file_uploader('Vipe JSON (TVD)', type=['json'], key='tvd_json')
    tvd_xls  = st.file_uploader('XLS Traffic Log (TVD)', type=['xls'], key='tvd_xls')
    tvd_grid = st.file_uploader('Grilla XLSX (TVD)', type=['xlsx'], key='tvd_grid')

with col2:
    st.subheader('🌎 CATV')
    catv_json = st.file_uploader('Vipe JSON (CATV)', type=['json'], key='catv_json')
    catv_xls  = st.file_uploader('XLS Traffic Log (CATV)', type=['xls'], key='catv_xls')
    catv_grid = st.file_uploader('Grilla XLSX (CATV)', type=['xlsx'], key='catv_grid')

st.divider()

# ── PROMO-ONLY MODE ───────────────────────────────────────────────────────────

st.caption('💡 If you only upload JSON files (no XLS/Grilla), the tool will run a promo repeat check only.')

# ── RUN BUTTON ────────────────────────────────────────────────────────────────

if st.button('▶ Run Check', type='primary', use_container_width=True):

    any_file = any([tvd_json, catv_json])
    if not any_file:
        st.error('Upload at least one JSON file to proceed.')
        st.stop()

    full_report_lines = [
        f'BROADCAST PLAYLIST CHECK REPORT',
        f'Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
        '═' * 60,
        ''
    ]

    def process_channel(label, json_file, xls_file, grid_file):
        lines = []
        if not json_file:
            return lines

        # Parse JSON
        try:
            data = json.load(json_file)
            playlist = parse_json_playlist(data)
        except Exception as e:
            lines.append(f'ERROR parsing JSON for {label}: {e}')
            return lines

        # Parse XLS
        xls_rows = []
        if xls_file:
            try:
                import tempfile
                with tempfile.NamedTemporaryFile(delete=False, suffix='.xls') as tmp:
                    tmp.write(xls_file.read())
                    tmp_path = tmp.name
                xls_rows = parse_xls_log(tmp_path)
                os.unlink(tmp_path)
            except Exception as e:
                lines.append(f'  WARNING: Could not parse XLS log: {e}')

        # Parse Grilla
        grilla_ids = []
        if grid_file and playlist['date']:
            try:
                import tempfile
                with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
                    tmp.write(grid_file.read())
                    tmp_path = tmp.name
                grilla_ids = parse_grilla(tmp_path, playlist['date'])
                os.unlink(tmp_path)
            except Exception as e:
                lines.append(f'  WARNING: Could not parse Grilla: {e}')

        # Promo-only mode
        if not xls_rows and not grilla_ids:
            promo_issues = check_promo_repeats(playlist)
            lines.append('═' * 60)
            lines.append(f'CHANNEL: {label.upper()} — PROMO REPEAT CHECK ONLY')
            lines.append(f'DATE: {playlist["date"]} | TYPE: {"FULL DAY" if playlist["type"] == "full" else "CURRENT"}')
            lines.append('═' * 60)
            if not promo_issues:
                lines.append('  ✓ No repeated promos within any break')
            else:
                for issue in promo_issues:
                    lines.append(issue)
            lines.append('')
            return lines

        # Full check
        report = generate_report(label, playlist, xls_rows, grilla_ids)
        lines.append(report)
        return lines

    with st.spinner('Running checks...'):
        tvd_lines  = process_channel('TVD',  tvd_json,  tvd_xls,  tvd_grid)
        catv_lines = process_channel('CATV', catv_json, catv_xls, catv_grid)

    full_report_lines.extend(tvd_lines)
    full_report_lines.extend(catv_lines)

    report_text = '\n'.join(full_report_lines)

    st.subheader('📄 Report')
    st.text(report_text)

    st.download_button(
        label='⬇ Download Report (.txt)',
        data=report_text,
        file_name=f'broadcast_check_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt',
        mime='text/plain',
        use_container_width=True
    )

