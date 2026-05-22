"""
youtube_outreach.py — YouTube Subscriber Outreach Automation

Runs on a configurable cron schedule (default: 5x/day) and:
  1. Authenticates via OAuth 2.0
  2. Fetches the channel's latest uploaded video
  3. Fetches all public subscribers
  4. Identifies which subscribers are themselves creators (by sub count)
  5. Exports a ranked CSV of creator-subscribers for manual collab outreach
  6. Posts a Community Post with the latest video link

Quota budget per run (worst-case, ~1k public subscribers):
  channels.list (own)        1 unit
  channels.list (uploads)    1 unit
  playlistItems.list         1 unit
  subscriptions.list x N    ~1 unit / 1k subs
  channels.list (stats) x M  1 unit / 50 channels
  communityPosts.insert      50 units
  ─────────────────────────  ~55 + subscriber overhead
  5 runs/day ≈ 275+ units (well within 10,000 daily quota for most channels)

NOTE: If you have >5,000 public subscribers, request a quota increase at
https://console.cloud.google.com before running at full scale.
"""

import csv
import io
import json
import logging
import os
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler

import googleapiclient.errors
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

# ─── Configuration ────────────────────────────────────────────────────────────

CLIENT_SECRETS_FILE = "client_secrets.json"
TOKEN_FILE = "token.json"

# Minimum subscriber count for a subscriber to be flagged as a creator
CREATOR_MIN_SUBSCRIBERS = 5

# How many times per day to run (spread evenly between 7 AM – 11 PM)
RUNS_PER_DAY = 5

# Output CSV for the ranked creator-subscriber list
REPORT_FILE = "creator_subscribers.csv"

# Persistent state file — tracks last posted video and per-creator outreach history
STATE_FILE = "outreach_state.json"

# How many days between polls (1 = one poll per day, 2 = every other day, etc.)
POLL_FREQUENCY_DAYS = 1

# Poll templates — rotated in order, one per POLL_FREQUENCY_DAYS interval
POLL_TEMPLATES = [
    {
        "question": "What's your biggest money struggle right now?",
        "options": [
            "Too much debt",
            "Can't save consistently",
            "Don't know how to invest",
            "Saving for a big purchase",
        ],
    },
    {
        "question": "What should I cover next?",
        "options": [
            "How to invest your first $1,000",
            "Side hustles that actually pay",
            "Paying off student loans faster",
            "Getting out of credit card debt",
        ],
    },
    {
        "question": "Which one are you?",
        "options": [
            "Spender trying to save",
            "Saver afraid to invest",
            "No budget, no plan",
            "Spreadsheet for everything",
        ],
    },
]

# Rotating log file (max 5 MB × 3 backups)
LOG_FILE = "outreach.log"

# Community Post body — supports {video_title} and {video_url} placeholders
COMMUNITY_POST_TEMPLATE = """\
🎬 New video just dropped!

{video_title}

Watch it here → {video_url}

Drop a comment and let me know what you think! 👇\
"""

# OAuth scopes required for reading subscriber data and posting community posts
SCOPES = [
    "https://www.googleapis.com/auth/youtube.force-ssl",
    "https://www.googleapis.com/auth/youtube.readonly",
]

# ─── Logging Setup ────────────────────────────────────────────────────────────

log = logging.getLogger("youtube_outreach")
log.setLevel(logging.INFO)

_fmt = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Force UTF-8 on the console so Unicode in video titles / emoji never crashes on Windows
_console = logging.StreamHandler(
    stream=io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
)
_console.setFormatter(_fmt)
log.addHandler(_console)

_file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3)
_file_handler.setFormatter(_fmt)
log.addHandler(_file_handler)

# ──────────────────────────────────────────────────────────────────────────────


def load_state():
    """
    Load persistent outreach state from STATE_FILE.

    State schema:
        last_posted_video_id  : str | None   — video ID of the last successful community post
        last_posted_at        : str | None   — ISO timestamp of that post
        creator_subscribers   : dict         — keyed by channel_id:
            first_seen              : str   — date string (YYYY-MM-DD) when first identified
            community_post_sent     : bool  — True once a community post went out while subscribed
            community_post_sent_date: str | None — date of that post

    Quota cost: 0 (local I/O only)
    Returns: state dict (never raises — returns a fresh default on any error)
    """
    default = {
        "last_posted_video_id": None,
        "last_posted_at": None,
        "last_poll_date": None,
        "last_poll_index": -1,
        "creator_subscribers": {},
    }
    if not os.path.exists(STATE_FILE):
        return default
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as fh:
            state = json.load(fh)
        # Ensure all top-level keys exist in case the file is from an older version
        for key, val in default.items():
            state.setdefault(key, val)
        log.info("State loaded from %s (%d known creator-subscribers).",
                 STATE_FILE, len(state["creator_subscribers"]))
        return state
    except (json.JSONDecodeError, OSError) as exc:
        log.warning("Could not read state file (%s) — starting with fresh state.", exc)
        return default


def save_state(state):
    """
    Persist outreach state to STATE_FILE atomically (write to .tmp then rename).

    Quota cost: 0 (local I/O only)
    """
    tmp = STATE_FILE + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(state, fh, indent=2)
        os.replace(tmp, STATE_FILE)
        log.info("State saved to %s.", STATE_FILE)
    except OSError as exc:
        log.error("Failed to save state file: %s", exc)


def get_authenticated_service():
    """
    Authenticate with the YouTube Data API v3 via OAuth 2.0.

    Loads cached credentials from TOKEN_FILE when available.
    Refreshes expired tokens automatically.
    Falls back to a browser-based consent flow on first run.

    Quota cost: 0 units
    Returns: authenticated googleapiclient Resource object
    """
    creds = None

    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            log.info("Refreshing expired OAuth token...")
            creds.refresh(Request())
        else:
            log.info("No valid token found — starting browser-based OAuth flow...")
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(TOKEN_FILE, "w") as fh:
            fh.write(creds.to_json())
        log.info("Token cached to %s", TOKEN_FILE)

    service = build("youtube", "v3", credentials=creds)
    log.info("Authenticated successfully.")
    return service


def get_my_channel_id(youtube):
    """
    Return the authenticated user's own YouTube channel ID.

    Quota cost: 1 unit
    Returns: channel ID string, or None on failure
    """
    log.info("Fetching own channel ID...")
    try:
        response = youtube.channels().list(part="id", mine=True).execute()
        items = response.get("items", [])
        if not items:
            log.error("No channel found for the authenticated account.")
            return None
        channel_id = items[0]["id"]
        log.info("Own channel ID: %s", channel_id)
        return channel_id
    except googleapiclient.errors.HttpError as exc:
        log.error("Failed to fetch channel ID (HTTP %s): %s", exc.resp.status, exc)
        return None


def get_latest_video(youtube, channel_id):
    """
    Fetch the most recently uploaded video for the given channel.

    Uses the uploads playlist rather than search.list to avoid the 100-unit
    search cost — fetching the uploads playlist ID costs 1 unit and reading
    the first playlist item costs another 1 unit.

    Quota cost: 2 units
    Returns: (video_id, video_title) tuple, or (None, None) on failure
    """
    log.info("Fetching latest video for channel %s...", channel_id)
    try:
        # Quota: 1 unit — get the uploads playlist ID
        ch_response = youtube.channels().list(
            part="contentDetails",
            id=channel_id,
        ).execute()

        items = ch_response.get("items", [])
        if not items:
            log.error("Channel %s not found.", channel_id)
            return None, None

        uploads_playlist_id = items[0]["contentDetails"]["relatedPlaylists"]["uploads"]

        # Quota: 1 unit — grab the first (most recent) playlist item
        pl_response = youtube.playlistItems().list(
            part="snippet",
            playlistId=uploads_playlist_id,
            maxResults=1,
        ).execute()

        pl_items = pl_response.get("items", [])
        if not pl_items:
            log.warning("Uploads playlist is empty for channel %s.", channel_id)
            return None, None

        snippet = pl_items[0]["snippet"]
        video_id = snippet["resourceId"]["videoId"]
        video_title = snippet["title"]
        log.info("Latest video: '%s' (id=%s)", video_title, video_id)
        return video_id, video_title

    except googleapiclient.errors.HttpError as exc:
        log.error("Failed to fetch latest video (HTTP %s): %s", exc.resp.status, exc)
        return None, None


def get_all_subscribers(youtube):
    """
    Paginate through all public subscribers of the authenticated channel.

    IMPORTANT: subscriptions.list with mySubscribers=True only returns
    subscribers who have set their subscription list to PUBLIC. This is
    controlled by each user's YouTube privacy settings — there is no
    workaround.

    Quota cost: ~1 unit per 1,000 subscribers (maxResults=1000 per page)
    Returns: list of channel ID strings
    """
    log.info("Fetching all public subscribers (paginated)...")
    subscriber_ids = []
    next_page_token = None
    page = 0

    while True:
        try:
            params = {
                "part": "subscriberSnippet",
                "mySubscribers": True,
                "maxResults": 1000,
            }
            if next_page_token:
                params["pageToken"] = next_page_token

            response = youtube.subscriptions().list(**params).execute()
            items = response.get("items", [])

            for item in items:
                cid = item.get("subscriberSnippet", {}).get("channelId")
                if cid:
                    subscriber_ids.append(cid)

            page += 1
            log.info(
                "Page %d: %d subscribers on this page (running total: %d)",
                page, len(items), len(subscriber_ids),
            )

            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break

        except googleapiclient.errors.HttpError as exc:
            log.error(
                "Error fetching subscribers on page %d (HTTP %s): %s",
                page, exc.resp.status, exc,
            )
            break

    log.info("Total public subscribers fetched: %d", len(subscriber_ids))
    return subscriber_ids


def get_channel_stats_batch(youtube, channel_ids):
    """
    Batch-fetch snippet + statistics for a list of channel IDs.

    Processes IDs in chunks of 50 (the API maximum per call).

    Quota cost: 1 unit per batch of 50 channels
    Returns: list of dicts with keys:
        channel_id, title, subscriber_count, video_count, description, channel_url
    """
    results = []

    for i in range(0, len(channel_ids), 50):
        batch = channel_ids[i : i + 50]
        try:
            response = youtube.channels().list(
                part="snippet,statistics",
                id=",".join(batch),
                maxResults=50,
            ).execute()

            for item in response.get("items", []):
                stats = item.get("statistics", {})
                snippet = item.get("snippet", {})

                # hiddenSubscriberCount=True means the creator hid their count
                if stats.get("hiddenSubscriberCount", False):
                    sub_count = 0
                else:
                    sub_count = int(stats.get("subscriberCount", 0))

                results.append({
                    "channel_id": item["id"],
                    "title": snippet.get("title", ""),
                    "subscriber_count": sub_count,
                    "video_count": int(stats.get("videoCount", 0)),
                    "description": snippet.get("description", "")[:200],
                    "channel_url": f"https://www.youtube.com/channel/{item['id']}",
                })

        except googleapiclient.errors.HttpError as exc:
            log.error(
                "Error fetching stats for batch [%d:%d] (HTTP %s): %s",
                i, i + 50, exc.resp.status, exc,
            )
            continue

    log.info("Fetched stats for %d channels.", len(results))
    return results


def post_community_post(youtube, message):
    """
    Publish a text Community Post to all subscribers.

    Requirements:
    - Channel must have 500+ subscribers
    - communityPosts().insert() may not be available for all accounts via API;
      if this call fails, post manually via YouTube Studio as a fallback.

    Quota cost: 50 units
    Returns: True on success, False on failure
    """
    log.info("Posting Community Post (%d chars)...", len(message))
    try:
        response = youtube.communityPosts().insert(
            part="snippet",
            body={
                "snippet": {
                    "type": "textPost",
                    "textOriginal": message,
                }
            },
        ).execute()

        post_id = response.get("id", "unknown")
        log.info("Community Post published. Post ID: %s", post_id)
        return True

    except googleapiclient.errors.HttpError as exc:
        status = exc.resp.status
        if status == 403:
            log.warning(
                "Community Post blocked (403 Forbidden). Your channel may not meet "
                "the eligibility requirements (500+ subscribers, API access enabled). "
                "Post manually via YouTube Studio. Full error: %s", exc,
            )
        elif status == 400:
            log.error("Community Post rejected (400 Bad Request). Check message content. Error: %s", exc)
        elif status == 401:
            log.error("Community Post failed (401 Unauthorized). Token may need re-authorization.")
        else:
            log.error("Community Post failed (HTTP %s): %s", status, exc)
        return False

    except AttributeError:
        # communityPosts() not present in older API client builds
        log.warning(
            "communityPosts() endpoint not available in this API client version. "
            "Post manually via YouTube Studio as a fallback."
        )
        return False


def post_community_poll(youtube, poll_template):
    """
    Publish a poll as a Community Post to all subscribers.

    Rotates through POLL_TEMPLATES in order, one every POLL_FREQUENCY_DAYS days.
    If the API endpoint is unavailable, logs the poll text for manual posting
    in YouTube Studio (Create -> Create post -> Add poll).

    Quota cost: 50 units
    Returns: True on success, False on failure
    """
    question = poll_template["question"]
    options = poll_template["options"]

    log.info("Posting poll: '%s' (%d options)...", question, len(options))

    try:
        response = youtube.communityPosts().insert(
            part="snippet",
            body={
                "snippet": {
                    "type": "pollPost",
                    "textOriginal": question,
                    "pollOptions": [{"optionText": opt} for opt in options],
                }
            },
        ).execute()

        post_id = response.get("id", "unknown")
        log.info("Poll published successfully. Post ID: %s", post_id)
        return True

    except googleapiclient.errors.HttpError as exc:
        status = exc.resp.status
        if status == 403:
            log.warning(
                "Poll blocked (403 Forbidden). Channel may not meet eligibility requirements. "
                "Post manually via YouTube Studio. Full error: %s", exc,
            )
        else:
            log.error("Poll failed (HTTP %s): %s", status, exc)
        _log_poll_for_manual_posting(question, options)
        return False

    except AttributeError:
        log.warning(
            "communityPosts() endpoint not available in this API client version. "
            "Post this poll manually via YouTube Studio -> Create -> Create post -> Add poll."
        )
        _log_poll_for_manual_posting(question, options)
        return False


def _log_poll_for_manual_posting(question, options):
    """Print the poll content clearly so it can be copy-pasted into YouTube Studio."""
    log.info("--- POLL TO POST MANUALLY ---")
    log.info("Question: %s", question)
    for i, opt in enumerate(options, 1):
        log.info("  Option %d: %s", i, opt)
    log.info("-----------------------------")


def maybe_post_poll(youtube, state, today):
    """
    Post the next poll in rotation if POLL_FREQUENCY_DAYS have passed since the last one.

    Checks state['last_poll_date'] and only fires once per POLL_FREQUENCY_DAYS interval
    regardless of how many scheduler runs happen in a day.

    Quota cost: 50 units when a poll is posted, 0 units when skipped
    Returns: updated state dict
    """
    last_poll_date = state.get("last_poll_date")

    if last_poll_date:
        from datetime import date, timedelta
        last = date.fromisoformat(last_poll_date)
        today_date = date.fromisoformat(today)
        days_since = (today_date - last).days
        if days_since < POLL_FREQUENCY_DAYS:
            log.info(
                "Poll skipped — last poll was %d day(s) ago (frequency: every %d day(s)).",
                days_since, POLL_FREQUENCY_DAYS,
            )
            return state

    # Advance to next poll in rotation
    next_index = (state.get("last_poll_index", -1) + 1) % len(POLL_TEMPLATES)
    poll = POLL_TEMPLATES[next_index]

    success = post_community_poll(youtube, poll)

    # Update state regardless of API success so the manual-post reminder
    # doesn't repeat on every run within the same day
    state["last_poll_date"] = today
    state["last_poll_index"] = next_index
    log.info(
        "Poll rotation: index %d / %d — next poll in %d day(s).",
        next_index + 1, len(POLL_TEMPLATES), POLL_FREQUENCY_DAYS,
    )
    return state


def save_creator_report(creator_subscribers):
    """
    Write the filtered creator-subscriber list to REPORT_FILE as CSV.

    Rows are sorted by subscriber_count descending.

    Quota cost: 0 (local I/O only)
    Columns: title, subscriber_count, video_count, channel_url, description,
             channel_id, first_seen, community_post_sent, community_post_sent_date
    """
    if not creator_subscribers:
        log.warning("No creator-subscribers to save — skipping CSV write.")
        return

    sorted_creators = sorted(
        creator_subscribers, key=lambda x: x["subscriber_count"], reverse=True
    )

    fieldnames = [
        "title",
        "subscriber_count",
        "video_count",
        "channel_url",
        "description",
        "channel_id",
        "first_seen",
        "community_post_sent",
        "community_post_sent_date",
    ]

    with open(REPORT_FILE, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(sorted_creators)

    log.info(
        "Creator report saved -> %s (%d entries, top: '%s' with %s subs)",
        REPORT_FILE,
        len(sorted_creators),
        sorted_creators[0]["title"],
        f"{sorted_creators[0]['subscriber_count']:,}",
    )


def run_outreach_job():
    """
    Orchestrate one complete outreach run in 9 sequential steps:

    1. Load persistent state
    2. Authenticate with YouTube API
    3. Resolve own channel ID
    4. Fetch the latest uploaded video
    5. Fetch all public subscribers
    6. Batch-fetch channel stats for each subscriber
    7. Filter by CREATOR_MIN_SUBSCRIBERS, merge with state (first_seen / post history)
    8. Save ranked CSV report
    9. Post Community Post only if this video hasn't been posted before; update state
    10. Post rotating poll once per POLL_FREQUENCY_DAYS
    """
    log.info("=" * 60)
    log.info("OUTREACH JOB START  %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("=" * 60)

    # Step 1 — Load state
    state = load_state()
    today = datetime.now().strftime("%Y-%m-%d")

    # Step 2 — Authenticate
    youtube = get_authenticated_service()

    # Step 3 — Own channel ID
    channel_id = get_my_channel_id(youtube)
    if not channel_id:
        log.error("Cannot proceed without channel ID. Aborting run.")
        return

    # Step 4 — Latest video
    video_id, video_title = get_latest_video(youtube, channel_id)
    video_url = f"https://www.youtube.com/watch?v={video_id}" if video_id else None
    if not video_id:
        log.warning("No video found — Community Post will be skipped this run.")

    # Step 5 — All public subscribers
    subscriber_ids = get_all_subscribers(youtube)

    creator_subscribers = []
    if subscriber_ids:
        # Step 6 — Batch stats
        all_stats = get_channel_stats_batch(youtube, subscriber_ids)

        # Step 7 — Filter creators and merge with persistent state
        known = state["creator_subscribers"]
        for ch in all_stats:
            if ch["subscriber_count"] < CREATOR_MIN_SUBSCRIBERS:
                continue
            cid = ch["channel_id"]
            record = known.get(cid, {})
            ch["first_seen"] = record.get("first_seen", today)
            ch["community_post_sent"] = record.get("community_post_sent", False)
            ch["community_post_sent_date"] = record.get("community_post_sent_date")
            creator_subscribers.append(ch)

        new_count = sum(
            1 for ch in creator_subscribers
            if ch["first_seen"] == today and ch["channel_id"] not in known
        )
        log.info(
            "Creator-subscribers: %d total / %d new this run (threshold >= %s subs)",
            len(creator_subscribers), new_count, f"{CREATOR_MIN_SUBSCRIBERS:,}",
        )

        # Step 8 — Save report (includes first_seen, community_post_sent columns)
        save_creator_report(creator_subscribers)
    else:
        log.warning("No public subscribers returned — skipping stats fetch and CSV export.")

    # Step 9 — Community Post (skip if this video was already posted)
    post_sent = False
    if video_id and video_title:
        if state["last_posted_video_id"] == video_id:
            log.info(
                "Community Post skipped — video '%s' was already posted on %s.",
                video_title, state.get("last_posted_at", "unknown date"),
            )
        else:
            message = COMMUNITY_POST_TEMPLATE.format(
                video_title=video_title,
                video_url=video_url,
            )
            post_sent = post_community_post(youtube, message)

            if post_sent:
                # Record which video was posted
                state["last_posted_video_id"] = video_id
                state["last_posted_at"] = datetime.now().isoformat()

                # Mark every creator-subscriber currently in the list as reached
                for ch in creator_subscribers:
                    cid = ch["channel_id"]
                    entry = state["creator_subscribers"].setdefault(cid, {})
                    entry["first_seen"] = ch["first_seen"]
                    if not entry.get("community_post_sent"):
                        entry["community_post_sent"] = True
                        entry["community_post_sent_date"] = today
                        ch["community_post_sent"] = True
                        ch["community_post_sent_date"] = today

                # Re-save CSV now that community_post_sent flags are updated
                save_creator_report(creator_subscribers)
    else:
        log.warning("Skipping Community Post — no video available.")

    # Step 10 — Rotating poll (once per POLL_FREQUENCY_DAYS)
    state = maybe_post_poll(youtube, state, today)

    # Persist any new first_seen records even if no post was sent
    for ch in creator_subscribers:
        cid = ch["channel_id"]
        entry = state["creator_subscribers"].setdefault(cid, {})
        entry.setdefault("first_seen", ch["first_seen"])
        entry.setdefault("community_post_sent", ch["community_post_sent"])
        entry.setdefault("community_post_sent_date", ch["community_post_sent_date"])

    save_state(state)

    log.info("OUTREACH JOB END    %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("=" * 60)


def compute_run_times(runs_per_day):
    """
    Spread N runs evenly across the 7:00 AM – 11:00 PM window (16 hours).

    Examples:
        runs_per_day=1  → [(7, 0)]
        runs_per_day=5  → [(7,0), (11,0), (15,0), (19,0), (23,0)]

    Returns: list of (hour, minute) tuples
    """
    window_start_min = 7 * 60   # 7:00 AM in minutes
    window_end_min = 23 * 60    # 11:00 PM in minutes
    total_minutes = window_end_min - window_start_min

    if runs_per_day == 1:
        return [(7, 0)]

    interval = total_minutes // (runs_per_day - 1)
    times = []
    for i in range(runs_per_day):
        offset = window_start_min + i * interval
        times.append((offset // 60, offset % 60))
    return times


def main():
    """
    Entry point.

    Fires one immediate run on startup, then hands control to the cron scheduler.
    Update the timezone string below to match your local timezone.
    Full list: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
    """
    log.info("YouTube Outreach Automation starting up.")
    log.info(
        "Config: RUNS_PER_DAY=%d | CREATOR_MIN_SUBSCRIBERS=%d",
        RUNS_PER_DAY, CREATOR_MIN_SUBSCRIBERS,
    )

    # Immediate first run so you get results without waiting for the schedule
    log.info("Firing immediate startup run...")
    run_outreach_job()

    # Build the daily cron schedule
    scheduler = BlockingScheduler(timezone="America/Los_Angeles")
    run_times = compute_run_times(RUNS_PER_DAY)

    log.info("Scheduling %d daily runs:", len(run_times))
    for hour, minute in run_times:
        log.info("  -> %02d:%02d", hour, minute)
        scheduler.add_job(
            run_outreach_job,
            trigger=CronTrigger(hour=hour, minute=minute),
            misfire_grace_time=300,   # Still run if up to 5 minutes late
            name=f"outreach_{hour:02d}{minute:02d}",
        )

    log.info("Scheduler running. Press Ctrl+C to stop.")
    try:
        scheduler.start()
    except KeyboardInterrupt:
        log.info("Scheduler stopped by user (KeyboardInterrupt).")


if __name__ == "__main__":
    main()
