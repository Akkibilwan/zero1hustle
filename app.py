import streamlit as st
import threading
import time
from datetime import datetime, timedelta, timezone
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from isodate import parse_duration
import gspread
from google.oauth2.service_account import Credentials

# --------------------------- Scheduler Thread Logic ---------------------------

def scheduler_loop():
    """
    Runs in a daemon thread. Sleeps until the next top-of-hour boundary, then calls
    run_once_and_append(). After each run, waits for the next hour.
    """
    # Give Streamlit a moment to finish loading
    time.sleep(1)

    while True:
        now = datetime.now()
        # Compute next hour boundary (minute=0, second=0, microsecond=0)
        next_hour = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
        secs_to_next_hour = (next_hour - now).total_seconds()
        time.sleep(secs_to_next_hour)
        run_once_and_append()

_scheduler_thread = None

def start_scheduler_thread():
    """
    Launch scheduler_loop() in a background daemon thread exactly once.
    """
    global _scheduler_thread
    if _scheduler_thread is None:
        _scheduler_thread = threading.Thread(target=scheduler_loop, daemon=True)
        _scheduler_thread.start()


# --------------------------- Configuration ---------------------------

st.set_page_config(layout="wide")
API_KEY = st.secrets["youtube"]["api_key"]

CHANNEL_IDS = [
    "UCrgxgGQJWp_a2iWGaSJLzRA",
]

GOOGLE_SHEET_URL = (
    "https://docs.google.com/spreadsheets/"
    "d/1nUfwXMJyEQsvDNMY_A1UAGlcMd5928HY2u_2vlnaTIA/edit"
)

# This must exactly match the sheet's 10 columns
EXPECTED_HEADER = [
    "Short ID",
    "Channel",
    "Upload Date",
    "Cronjob time",
    "Views",
    "Likes",
    "Comment",
    "VPH",
    "Engagement rate",
    "Engagement rate %"
]


# ----------------------- Google Sheets Helpers ---------------------------

@st.cache_resource(ttl=3600)
def get_google_sheet_client():
    """
    Authorize via service-account JSON stored in st.secrets using modern google-auth.
    Returns None + logs an error if authentication fails.
    """
    try:
        creds_dict = st.secrets["gcp_service_account"]
        scopes = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        st.error(f"❌ Error setting up Google Sheets client: {e}")
        return None

@st.cache_resource(ttl=3600)
def get_worksheet():
    """
    Open the Google Sheet by URL, then fetch the "Sheet1" worksheet.
    Returns None + logs if anything goes wrong.
    """
    client = get_google_sheet_client()
    if not client:
        return None
    try:
        spreadsheet = client.open_by_url(GOOGLE_SHEET_URL)
        worksheet = spreadsheet.worksheet("Sheet1")
        return worksheet
    except Exception as e:
        st.error(f"❌ Error opening worksheet 'Sheet1': {e}")
        return None


# ----------------------- YouTube Helper Functions ----------------------------

def create_youtube_client():
    """
    Build a YouTube Data API v3 client using the provided API key.
    """
    return build("youtube", "v3", developerKey=API_KEY)

def iso8601_to_seconds(duration_str: str) -> int:
    """
    Convert an ISO 8601 duration (e.g. "PT2M30S") into total seconds.
    Returns 0 on parse errors.
    """
    try:
        return int(parse_duration(duration_str).total_seconds())
    except:
        return 0

def get_midnight_ist_utc() -> datetime:
    """
    Return a timezone-aware UTC datetime corresponding to 00:00:00 IST today.
    IST = UTC + 5:30
    """
    now_utc = datetime.now(timezone.utc)
    ist_tz = timezone(timedelta(hours=5, minutes=30))
    now_ist = now_utc.astimezone(ist_tz)
    today_ist = now_ist.date()
    midnight_ist = datetime(
        year=today_ist.year,
        month=today_ist.month,
        day=today_ist.day,
        hour=0,
        minute=0,
        second=0,
        tzinfo=ist_tz
    )
    return midnight_ist.astimezone(timezone.utc)

def is_within_today(published_at_str: str) -> bool:
    """
    Given a publishedAt timestamp (RFC3339: "YYYY-MM-DDThh:mm:ssZ"),
    return True iff that UTC moment falls between [00:00 IST today, 24h later).
    """
    try:
        pub_dt = datetime.fromisoformat(published_at_str.replace("Z", "+00:00")).astimezone(timezone.utc)
    except:
        return False
    midnight_utc = get_midnight_ist_utc()
    next_midnight_utc = midnight_utc + timedelta(days=1)
    return midnight_utc <= pub_dt < next_midnight_utc

def retry_youtube_call(func_or_request, *args, **kwargs):
    """
    Retry pattern for YouTube API calls. If `func_or_request` is an HttpRequest
    object, call request.execute(). If it's a callable (like youtube.videos().list),
    call it with (*args, **kwargs).execute(). On HttpError, wait 2s and retry once.
    Returns parsed JSON on success or None on two failures.
    """
    # If it's a built HttpRequest (has .execute(), not callable), run execute()
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
        # If it's a callable (like youtube.videos().list), call then execute
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
    """
    Discover all Shorts (<= 180s) published "today in IST" across CHANNEL_IDS.
    Returns:
      • video_to_channel: {video_id: channel_title}
      • video_to_published: {video_id: published_datetime_UTC}
      • logs: [string, …] for display
      • no_shorts_flag: True if no Shorts found (or a fatal YouTube error)
    """
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
            part="snippet",
            playlistId=uploads_playlist,
            maxResults=50
        )
        while pl_req:
            pl_resp = retry_youtube_call(pl_req)
            if not pl_resp:
                logs.append(f"❌ Error fetching playlistItems for '{channel_title}'. Aborting discovery.")
                return {}, {}, logs, True

            for item in pl_resp.get("items", []):
                vid_id = item["snippet"]["resourceId"]["videoId"]
                published_at = item["snippet"]["publishedAt"]
                if not is_within_today(published_at):
                    continue

                cd_resp = retry_youtube_call(
                    youtube.videos().list,
                    part="contentDetails,snippet",
                    id=vid_id
                )
                if not cd_resp or not cd_resp.get("items"):
                    logs.append(f"⚠️ Could not fetch contentDetails for {vid_id}. Skipping.")
                    continue

                duration_secs = iso8601_to_seconds(cd_resp["items"][0]["contentDetails"]["duration"])
                if duration_secs <= 180:
                    pub_iso = cd_resp["items"][0]["snippet"]["publishedAt"]
                    pub_dt = datetime.fromisoformat(pub_iso.replace("Z", "+00:00")).astimezone(timezone.utc)
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
    """
    Given a list of video IDs, fetch their current statistics
    (viewCount, likeCount, commentCount) in batches of up to 50.
    Returns {video_id: {"viewCount": int, "likeCount": int, "commentCount": int}}.
    """
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


# ----------------------- Core "Run Now" Function ----------------------------

def run_once_and_append():
    """
    1) Read the entire sheet → discover which video_ids we have already been tracking.
    2) Call discover_shorts() to find any *new* Shorts published today in IST. Add them to our tracking list.
    3) Fetch the latest stats for *all* tracked Shorts (old + new).
    4) Compute VPH & engagement_rate for each, build a new row:
       [Short ID, Channel, Upload Date, Cronjob time, Views, Likes, Comment, VPH, Engagement rate, Engagement rate %]
    5) Filter out (video_id, cronjob_time) duplicates if that exact combination already exists in the sheet.
    6) Append the remaining new rows in one batch.
    7) Display debug info in Streamlit (how many new rows, how many skipped).
    """
    st.info("🔍 Reading the entire sheet to find tracked video IDs…")
    ws = get_worksheet()
    if ws is None:
        st.error("Cannot connect to Google Sheet. Aborting.")
        return

    # --- Step 1: Read everything currently in the sheet ---
    try:
        all_data = ws.get_all_values()
    except Exception as e:
        st.error(f"❌ Error reading sheet: {e}")
        return

    header = all_data[0] if all_data else []
    rows = all_data[1:] if len(all_data) > 1 else []

    # If the sheet is empty OR header doesn't exactly match EXPECTED_HEADER, re-initialize:
    if header != EXPECTED_HEADER:
        try:
            ws.clear()
            ws.append_row(EXPECTED_HEADER, value_input_option="RAW")
            all_data = ws.get_all_values()
            header = all_data[0]
            rows = []
            st.success("✔️ Initialized header row in the sheet.")
        except Exception as e:
            st.error(f"❌ Error initializing header row: {e}")
            return

    # Build indices from header
    idxShortID     = header.index("Short ID")
    idxChannel     = header.index("Channel")
    idxUploadDate  = header.index("Upload Date")
    idxCronjobTime = header.index("Cronjob time")
    idxViews       = header.index("Views")
    idxLikes       = header.index("Likes")
    idxComment     = header.index("Comment")
    idxVPH         = header.index("VPH")
    idxEngRate     = header.index("Engagement rate")
    idxEngRatePct  = header.index("Engagement rate %")

    # Build a set of all tracked video_ids and map their upload‐times (UTC)
    tracked_ids = set()
    video_to_channel_past = {}
    video_to_published_past = {}

    # IST timezone
    ist_tz = timezone(timedelta(hours=5, minutes=30))

    for r in rows:
        if len(r) < 4:
            continue
        vid = r[idxShortID]
        if vid not in tracked_ids:
            tracked_ids.add(vid)
            video_to_channel_past[vid] = r[idxChannel]

            # Parse "Upload Date" as either ISO‐8601 UTC or IST dd/mm/yyyy hh:mm:ss
            pub_str = r[idxUploadDate]
            try:
                if "T" in pub_str and pub_str.endswith("Z"):
                    # ISO-8601 in UTC
                    dt_utc = datetime.fromisoformat(pub_str.replace("Z", "+00:00"))
                    video_to_published_past[vid] = dt_utc.astimezone(timezone.utc)
                else:
                    # IST dd/mm/yyyy hh:mm:ss
                    dt_ist = datetime.strptime(pub_str, "%d/%m/%Y %H:%M:%S")
                    dt_ist = dt_ist.replace(tzinfo=ist_tz)
                    video_to_published_past[vid] = dt_ist.astimezone(timezone.utc)
            except Exception:
                pass

    st.write(f"➡️  Currently tracking {len(tracked_ids)} unique Short(s) from previous runs.")

    # --- Step 2: Discover any new Shorts published today in IST ---
    st.info("🔍 Checking for new Shorts published today in IST…")
    video_to_channel_new, video_to_published_new, discover_logs, no_shorts_flag = discover_shorts()
    for msg in discover_logs:
        st.write(msg)

    if not no_shorts_flag:
        added_count = 0
        for vid, ch in video_to_channel_new.items():
            if vid not in tracked_ids:
                tracked_ids.add(vid)
                video_to_channel_past[vid] = ch
                video_to_published_past[vid] = video_to_published_new[vid]
                added_count += 1
        st.success(f"ℹ️ Now tracking {len(tracked_ids)} Shorts in total (added {added_count} today).")
    else:
        st.warning("ℹ️ No new Shorts found today (IST). Will poll stats for existing IDs only.")

    if not tracked_ids:
        st.warning("⚠️ No Shorts to track at all. Aborting.")
        return

    # --- Step 3: Fetch current stats for ALL tracked Shorts ---
    st.info(f"🕒 Fetching stats for {len(tracked_ids)} tracked Short(s)…")
    all_ids = list(tracked_ids)
    stats = fetch_statistics(all_ids)
    if not stats:
        st.error("❌ Failed to fetch statistics for any tracked video.")
        return

    # Gather all existing (video_id, cronjob_time) pairs so we avoid duplicates
    existing_pairs = set()
    for r in rows:
        if len(r) >= 4:
            vid = r[idxShortID]
            cron_str = r[idxCronjobTime]
            existing_pairs.add((vid, cron_str))

    # --- Step 4: Build new rows for each tracked Short ---
    new_rows = []
    now_ist = datetime.now(timezone.utc).astimezone(ist_tz)
    cron_str = now_ist.strftime("%d/%m/%Y %H:%M:%S")  # Cronjob time in IST

    for vid in all_ids:
        if vid not in stats:
            st.warning(f"⚠️ Skipping {vid} (no stats returned).")
            continue
        if vid not in video_to_published_past:
            st.warning(f"⚠️ Skipping {vid} (missing published_at info).")
            continue

        # Convert stored UTC → IST for "Upload Date"
        published_dt_utc = video_to_published_past[vid]
        published_dt_ist = published_dt_utc.astimezone(ist_tz)
        upload_str = published_dt_ist.strftime("%d/%m/%Y %H:%M:%S")

        channel_title = video_to_channel_past.get(vid, "N/A")
        viewCount = stats[vid]["viewCount"]
        likeCount = stats[vid]["likeCount"]
        commentCount = stats[vid]["commentCount"]

        delta_hours = max((datetime.now(timezone.utc) - published_dt_utc).total_seconds() / 3600.0, 1/3600.0)
        vph = viewCount / delta_hours
        eng_rate = ((likeCount + commentCount) / viewCount) if viewCount > 0 else 0.0
        eng_rate_pct = eng_rate * 100.0

        # Only append if (vid, cron_str) is not already in the sheet
        if (vid, cron_str) not in existing_pairs:
            new_rows.append([
                vid,
                channel_title,
                upload_str,    # "dd/mm/yyyy hh:mm:ss" IST for Upload Date
                cron_str,      # "dd/mm/yyyy hh:mm:ss" IST for Cronjob time
                str(viewCount),
                str(likeCount),
                str(commentCount),
                f"{vph:.2f}",
                f"{eng_rate:.4f}",
                f"{eng_rate_pct:.2f}"
            ])

    st.write(f"➡️ {len(new_rows)} new row(s) to append (out of {len(all_ids)} total Shorts).")

    if not new_rows:
        st.info("ℹ️ No new rows to append (all duplicates).")
        return

    # --- Step 5: Append the new rows in one batch ---
    try:
        ws.append_rows(new_rows, value_input_option="RAW")
        st.success(f"✅ Appended {len(new_rows)} row(s) to the sheet successfully.")
    except Exception as e:
        st.error(f"❌ Error appending rows to sheet: {e}")
        return


# ----------------------- Streamlit Layout ----------------------------

# Start the background scheduler thread exactly once
if "scheduler_started" not in st.session_state:
    start_scheduler_thread()
    st.session_state.scheduler_started = True

st.title("📊 YouTube Shorts VPH & Engagement Tracker")

st.write(
    """
    **How this works**:
    1. A background scheduler runs **at the top of every hour** and calls our "Run Once" logic:
       - It reads all tracked Shorts from the Google Sheet (column "Short ID").
       - It discovers any *new* Shorts published *today in IST* and starts tracking them.
       - It fetches the latest stats for *all* tracked Shorts (old + new), computes VPH & engagement rate, 
         and appends a row per video with the new timestamp.
       - **Both "Upload Date" and "Cronjob time" are in IST `dd/mm/yyyy HH:MM:SS` format.**
         Engagement rate is written as a decimal and as a percentage.
    2. You can also click the button below to force a "Run Now" immediately.
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
