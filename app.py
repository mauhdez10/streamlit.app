# Broadcast Playlist Checker

## What it does
Checks Vipe JSON playlists against XLS traffic logs and Grilla schedule grids.
Works for TVD and CATV simultaneously.

## Files you upload each run
| File | Required | Notes |
|------|----------|-------|
| Vipe JSON | ✅ Yes | Either "currently airing" or "full day" — auto-detected |
| XLS Traffic Log | Optional | Needed for commercial check |
| Grilla XLSX | Optional | Needed for program order/ID check |

## What it checks
1. **Programs vs Grilla** — correct IDs, correct order (fuzzy match handles extra zeros)
2. **Commercials vs XLS Log** — every spot accounted for, counts match
3. **Break structure** — every break has at least one commercial
4. **Promo repeats** — same promo in same break flagged
5. **Cue tones** — count and time range per cue tone ID (full playlist only)
6. **Missing items** — list of what's in the log/grilla but not in playlist

## JSON type detection (automatic)
- **Full day**: starts at 10:00 UTC, has marker event → runs all checks + cue tone count
- **Currently airing**: starts mid-day → checks only from current position to end of day

## Promo-only mode
Upload just a JSON file (no XLS, no Grilla) → runs promo repeat check only.

## Deploy to Streamlit Cloud (free)
1. Create a GitHub account at github.com
2. Create a new repository, upload these 3 files: app.py, checker.py, requirements.txt
3. Go to share.streamlit.io → sign in with GitHub → deploy your repo
4. You get a permanent URL — open it in any browser, upload files, click Run

## Run locally (if Python installed)
```
pip install streamlit openpyxl
streamlit run app.py
```
