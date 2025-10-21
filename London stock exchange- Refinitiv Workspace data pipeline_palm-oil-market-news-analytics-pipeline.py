import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import lseg.data as ld
from lseg.data.content import news
from tqdm import tqdm
from bs4 import BeautifulSoup
import pytz

import lseg.data as ld # Install with `pip install lseg.data`
session = ld.session.desktop.Definition(name = "workspace",app_key = "").get_session()
session.open()
ld.session.set_default(session)
SAVE_DIR = Path("C:/Users/reuters/Desktop/LSEGNEWS")
SAVE_DIR.mkdir(parents=True, exist_ok=True)

# define all palm-oil related topics
QUERIES = ["(TOPIC:POIL OR biofuel) AND NOT (RIC:* OR COMPANY:*)",]

START_YEAR = 2010
END_YEAR = 2025

## Daily limit: 30% of 10,000
DAILY_REQUEST_LIMIT = 3000
REQUEST_COUNTER = 0
LAST_RESET_DATE = None
MY_TZ = pytz.timezone("Asia/Kuala_Lumpur")

# Exclusion list for keywords
EXCLUDE_KEYWORDS = ["Golden Crop's sukuk debt", "eps", "dividend", "technicals","www.buysellsignals.com", "Información de","Stock Exchange", "Exchange Group", "LTD","BERNAMA","Nigerian Company News Bites - Stock Report"]

# ---------------- Helpers ----------------
def can_make_request(n=1):
    """Check and manage daily request quota."""
    global REQUEST_COUNTER, LAST_RESET_DATE
    now_my = datetime.now(MY_TZ)
    today_reset = now_my.replace(hour=8, minute=0, second=0, microsecond=0)

    if LAST_RESET_DATE is None or now_my >= today_reset > LAST_RESET_DATE:
        REQUEST_COUNTER = 0
        LAST_RESET_DATE = now_my
        print(f"🔄 Daily limit reset at {today_reset} (MYT)")

    if REQUEST_COUNTER + n > DAILY_REQUEST_LIMIT:
        next_reset = (now_my + timedelta(days=1)).replace(hour=8, minute=0, second=0, microsecond=0)
        sleep_sec = (next_reset - now_my).total_seconds()
        print(f"⏸️ Hit {DAILY_REQUEST_LIMIT} requests. Sleeping until next reset ({next_reset}) ...")
        time.sleep(sleep_sec)
        REQUEST_COUNTER = 0
        LAST_RESET_DATE = datetime.now(MY_TZ)
        return True

    REQUEST_COUNTER += n
    return True


def safe_get_data(func, retries=3, wait=2):
    """Fetch data with retries and daily limit check."""
    for attempt in range(retries):
        try:
            if not can_make_request():
                return None
            return func()
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(wait)
            else:
                raise e


def daterange(start_date, end_date, months=6):
    """Yield (from,to) date ranges every <months> months."""
    current = start_date
    while current < end_date:
        nxt = current + timedelta(days=30 * months)
        yield current, min(nxt - timedelta(days=1), end_date)
        current = nxt


def fetch_story_text(story_id):
    """Fetch full story text using LSEG API."""
    try:
        html = safe_get_data(lambda: ld.news.get_story(story_id, format=ld.news.Format.HTML))
        if not html:
            html = safe_get_data(lambda: ld.news.get_story(story_id, format=ld.news.Format.TEXT))
        if not html:
            return None
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(separator="\n").strip()
        return text if text else None
    except Exception as e:
        print(f"⚠️ Story fetch failed for {story_id}: {e}")
        return None


# ---------------- Fetch Logic ----------------
def fetch_halfyearly_palm_news():
    ld.open_session()  # authenticate session

    for q in QUERIES:
        print(f"\n🔍 Fetching query: {q}")
        for year in range(START_YEAR, END_YEAR + 1):
            for start, end in daterange(datetime(year, 1, 1), datetime(year, 12, 31), months=1):
                label = f"{start.date()}_{end.date()}"
                file_path = SAVE_DIR / f"news_{q.replace(':', '_')}_{label}.parquet"

                if file_path.exists():
                    print(f"⏩ Skipping existing {file_path.name}")
                    continue

                print(f"📅 Fetching {label} ...")
                try:
                    # Correct API usage: start/end instead of date_from/date_to
                    headlines_df = safe_get_data(lambda: ld.news.get_headlines(
                        query=q,
                        start=start.isoformat(),
                        end=end.isoformat(),
                        count=100
                    ))

                    if headlines_df is None or headlines_df.empty:
                        print(f"⚠️ No data for {label}")
                        continue

                    headlines_df.drop_duplicates(subset=["storyId"], inplace=True)

                    # Fetch full story text
                    story_texts = []
                    for sid, headline in tqdm(zip(headlines_df["storyId"], headlines_df["headline"]), total=len(headlines_df), desc="Stories"):
                        text = fetch_story_text(sid)
                        # 🔹 Exclude if keyword found in headline or body
                        if text and not any(kw.lower() in (headline.lower() + " " + text.lower()) for kw in EXCLUDE_KEYWORDS):
                            story_texts.append(text)
                        else:
                            story_texts.append(None)

                    headlines_df["storyText"] = story_texts
                    headlines_df = headlines_df.dropna(subset=["storyText"])  # 🔹 remove excluded rows

                    if headlines_df.empty:
                        print(f"⚠️ All stories excluded for {label}")
                        continue

                    headlines_df.to_parquet(file_path, index=False)
                    print(f"✅ Saved {file_path.name} ({len(headlines_df)} rows)")

                except Exception as e:
                    print(f"❌ {start.date()} ({q}) failed: {e}")
                    continue

    ld.close_session()


# ---------------- Run ----------------
if __name__ == "__main__":
    # 🔹 Wait until 6:30 PM MYT before running
    now_my = datetime.now(MY_TZ)
    target_time = now_my.replace(hour=18, minute=30, second=0, microsecond=0)
    if now_my < target_time:
        wait_sec = (target_time - now_my).total_seconds()
        print(f"🕡 Waiting until 6:30 PM MYT to start ({wait_sec/60:.1f} minutes)...")
        time.sleep(wait_sec)

    fetch_halfyearly_palm_news()