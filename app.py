import streamlit as st

# ─────────── Runtime Dependency Installer ───────────

import subprocess
import sys

def ensure_dependency(module_name: str, package_name: str = None):
    """
    If `module_name` isn't importable, pip‑install `package_name` (or module_name if not provided).
    """
    pkg = package_name or module_name
    try:
        __import__(module_name)
    except ImportError:
        st.info(f"🔄 Installing missing package: {pkg}…")
        subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])

# Ensure the core dependencies are present
ensure_dependency("googleapiclient", "google-api-python-client")
ensure_dependency("oauth2client")
ensure_dependency("gspread")
ensure_dependency("isodate")
ensure_dependency("pandas")

# ──────────────────────────────────────────────────────────

from googleapiclient.discovery import build
import threading
import time
from datetime import datetime, timedelta, timezone
import pandas as pd
from googleapiclient.errors import HttpError
from isodate import parse_duration
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# --------------------------- Scheduler Thread Logic ---------------------------

def scheduler_loop():
    time.sleep(1)
    while True:
        now = datetime.now()
        next_hour = (now.replace(minute=0, second=0, microsecond=0)
                     + timedelta(hours=1))
        secs_to_next_hour = (next_hour - now).total_seconds()
        time.sleep(secs_to_next_hour)
        run_once_and_append()

_scheduler_thread = None

def start_scheduler_thread():
    global _scheduler_thread
    if _scheduler_thread is None:
        _scheduler_thread = threading.Thread(
            target=scheduler_loop, daemon=True
        )
        _scheduler_thread.start()

# --------------------------- Configuration ---------------------------

st.set_page_config(layout="wide")
API_KEY = st.secrets["youtube"]["api_key"]

CHANNEL_IDS = [
    "UCrgxgGQJWp_a2iWGaSJLzRA"
]

GOOGLE_SHEET_URL = (
    "https://docs.google.com/spreadsheets/"
    "d/1nUfwXMJyEQsvDNMY_A1UAGlcMd5928HY2u_2vlnaTIA/edit"
)

EXPECTED_HEADER = [
    "Short ID", "Channel", "Upload Date", "Cronjob time", "Views",
    "Likes", "Comment", "VPH", "Engagement rate", "Engagement rate %"
]

# ----------------------- Google Sheets Helpers ---------------------------

@st.cache_resource(ttl=3600)
def get_google_sheet_client():
    try:
        creds_dict = st.secrets["gcp_service_account"]
        scopes = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scopes)
        return gspread.authorize(creds)
    except Exception as e:
        st.error(f"❌ Error setting up Google Sheets client: {e}")
        return None

@st.cache_resource(ttl=3600)
def get_worksheet():
    client = get_google_sheet_client()
    if not client:
        return None
    try:
        spreadsheet = client.open_by_url(GOOGLE_SHEET_URL)
        return spreadsheet.worksheet("Sheet1")
    except Exception as e:
        st.error(f"❌ Error opening worksheet 'Sheet1': {e}")
        return None

# ----------------------- YouTube Helper Functions ----------------------------

def create_youtube_client():
    return build("youtube", "v3", developerKey=API_KEY)

def iso8601_to_seconds(duration_str: str) -> int:
    try:
        return int(parse_duration(duration_str).total_seconds())
    except:
        return 0

def get_midnight_ist_utc() -> datetime:
    now_utc = datetime.now(timezone.utc)
    ist_tz = timezone(timedelta(hours=5, minutes=30))
    now_ist = now_utc.astimezone(ist_tz)
    today_ist = now_ist.date()
    midnight_ist = datetime(
        year=today_ist.year, month=today_ist.month, day=today_ist.day,
        hour=0, minute=0, second=0, tzinfo=ist_tz
    )
    return midnight_ist.astimezone(timezone.utc)

def is_within_today(published_at_str: str) -> bool:
    try:
        pub_dt = datetime.fromisoformat(
            published_at_str.replace("Z", "+00:00")
        ).astimezone(timezone.utc)
    except:
        return False
    midnight_utc = get_midnight_ist_utc()
    next_midnight_utc = midnight_utc + timedelta(days=1)
    return midnight_utc <= pub_dt < next_midnight_utc

def retry_youtube_call(func_or_request, *args, **kwargs):
    if hasattr(func_or_request, "execute") and not callable(func_or_request):
        request = func_or_request
        try:
            return request.execute()
        except HttpError as e:
            st.warning(f"⚠️ YouTube API error (first attempt): {e}")
            time.sleep(2)
            try:
                return request.execute()
            except HttpError as e2:
                st.error(f"❌ YouTube API error (second attempt): {e2}")
                return None
    else:
        try:
            return func_or_request(*args, **kwargs).execute()
        except HttpError as e:
            st.warning(f"⚠️ YouTube API error (first attempt): {e}")
            time.sleep(2)
            try:
                return func_or_request(*args, **kwargs).execute()
            except HttpError as e2:
                st.error(f"❌ YouTube API error (second attempt): {e2}")
                return None

def discover_shorts():
    youtube = create_youtube_client()
    video_to_channel = {}
    video_to_published = {}
    logs = []
    all_short_ids = []

    for idx, channel_id in enumerate(CHANNEL_IDS, start=1):
        ch_resp = retry_youtube_call(
            youtube.channels().list,
            part="snippet,contentDetails",
            id=channel_id
        )
        if not ch_resp or not ch_resp.get("items"):
            logs.append(f"❌ Error fetching channel info for {channel_id}. Skipping channel.")
            return {}, {}, logs, True

        channel_title = ch_resp["items"][0]["snippet"]["title"]
        uploads_playlist = ch_resp["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
        logs.append(f"🔍 Checking channel {idx}/{len(CHANNEL_IDS)}: '{channel_title}'")

        pl_req = youtube.playlistItems().list(
            part="snippet", playlistId=uploads_playlist, maxResults=50
        )
        while pl_req:
            pl_resp = retry_youtube_call(pl_req)
            if not pl_resp:
                logs.append(f"❌ Error fetching playlistItems for '{channel_title}'. Aborting discovery.")
                return {}, {}, logs, True

            for item in pl_resp.get("items", []):
                vid_id = item["snippet"]["resourceId"]["videoId"]
                if not is_within_today(item["snippet"]["publishedAt"]):
                    continue

                cd_resp = retry_youtube_call(
                    youtube.videos().list,
                    part="contentDetails,snippet",
                    id=vid_id
                )
                if not cd_resp or not cd_resp.get("items"):
                    logs.append(f"⚠️ Could not fetch contentDetails for {vid_id}. Skipping.")
                    continue

                duration_secs = iso8601_to_seconds(
                    cd_resp["items"][0]["contentDetails"]["duration"]
                )
                if duration_secs <= 180:
                    pub_iso = cd_resp["items"][0]["snippet"]["publishedAt"]
                    pub_dt = datetime.fromisoformat(
                        pub_iso.replace("Z", "+00:00")
                    ).astimezone(timezone.utc)
                    video_to_channel[vid_id] = channel_title
                    video_to_published[vid_id] = pub_dt
                    all_short_ids.append(vid_id)

            pl_req = youtube.playlistItems().list_next(pl_req, pl_resp)

        if all_short_ids:
            logs.append(f"✅ Found {len(all_short_ids)} Shorts so far (including this channel).")

    if not all_short_ids:
        logs.append("ℹ️ No Shorts published today in IST across all channels.")
        return {}, {}, logs, True

    logs.append(f"ℹ️ Total discovered Shorts: {len(all_short_ids)}")
    return video_to_channel, video_to_published, logs, False

def fetch_statistics(video_ids):
    youtube = create_youtube_client()
    stats_dict = {}

    for i in range(0, len(video_ids), 50):
        batch = video_ids[i : i + 50]
        resp = retry_youtube_call(
            youtube.videos().list,
            part="statistics",
            id=",".join(batch)
        )
        if not resp:
            continue
        for item in resp.get("items", []):
            vid = item["id"]
            stat = item.get("statistics", {})
            stats_dict[vid] = {
                "viewCount": int(stat.get("viewCount", 0)),
                "likeCount": int(stat.get("likeCount", 0)),
                "commentCount": int(stat.get("commentCount", 0)),
            }

    return stats_dict

# ----------------------- Core “Run Now” Function ----------------------------
# Paste your existing run_once_and_append() logic here, unchanged:
def run_once_and_append():
    # … your implementation …
    pass

# ----------------------- Streamlit Layout ----------------------------

if "scheduler_started" not in st.session_state:
    start_scheduler_thread()
    st.session_state.scheduler_started = True

st.title("📊 YouTube Shorts VPH & Engagement Tracker")

st.write(
    """
    **How this works**:
    1. A background scheduler runs **at the top of every hour** and calls our “Run Once” logic:
       - It reads all tracked Shorts from the Google Sheet (column “Short ID”).
       - It discovers any *new* Shorts published *today in IST* and starts tracking them.
       - It fetches the latest stats for *all* tracked Shorts (old + new), computes VPH & engagement rate, 
         and appends a row per video with the new timestamp.
       - **Both “Upload Date” and “Cronjob time” are in IST `dd/mm/yyyy HH:MM:SS` format.**
         Engagement rate is written as a decimal and as a percentage.
    2. You can also click the button below to force a “Run Now” immediately.
    3. The sheet accumulates one row per (Short ID, Cronjob time), letting you watch metrics evolve hour by hour.
    """
)

if st.button("▶️ Run Now: Discover & Append to Sheet"):
    run_once_and_append()

st.markdown("---")
st.subheader("View Entire Sheet Contents")

ws = get_worksheet()
if ws:
    try:
        data = ws.get_all_values()
        if data:
            df_sheet = pd.DataFrame(data[1:], columns=data[0])
            st.dataframe(df_sheet, height=600)
        else:
            st.info("ℹ️ The sheet is currently empty (no header/data).")
    except Exception as e:
        st.error(f"❌ Could not read sheet contents: {e}")
else:
    st.error("❌ Cannot connect to Google Sheet (check credentials).")
