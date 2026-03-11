import os
import json
from datetime import datetime, timezone
from googleapiclient.discovery import build
from supabase import create_client, Client

YT_API_KEY = os.environ.get("YT_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# 更新後的頻道 ID (請確保沒有多餘的字母)
CHANNEL_IDS = [
    "UCgTzsBI0DIRopMylJEDqnog", # 小雀とと
]

def get_yt_client():
    return build("youtube", "v3", developerKey=YT_API_KEY)

def get_supabase_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_and_save():
    youtube = get_yt_client()
    supabase = get_supabase_client()
    
    print(f"📡 正在向 YouTube API 請求 {len(CHANNEL_IDS)} 個頻道的數據...")
    
    request = youtube.channels().list(
        part="snippet,statistics",
        id=",".join(CHANNEL_IDS)
    )
    response = request.execute()

    items = response.get("items", [])
    print(f"🔎 API 回傳了 {len(items)} 個頻道的結果。")

    if not items:
        print("⚠️ 警告：找不到任何頻道資料，請檢查 CHANNEL_IDS 是否正確。")
        return

    for item in items:
        channel_id = item["id"]
        snippet = item.get("snippet", {})
        stats = item.get("statistics", {})
        title = snippet.get('title')
        
        print(f"💾 正在寫入: {title} ({channel_id})")

        # 1. 更新母表
        try:
            supabase.table("yt_channels").upsert({
                "channel_id": channel_id,
                "title": title,
                "custom_url": snippet.get("customUrl"),
            }).execute()
        except Exception as e:
            print(f"❌ 寫入 yt_channels 失敗: {e}")

        # 2. 插入快照
        try:
            live_status = snippet.get("liveBroadcastContent", "none")
            snapshot_data = {
                "channel_id": channel_id,
                "subscriber_count": int(stats.get("subscriberCount", 0)),
                "total_views": int(stats.get("viewCount", 0)),
                "is_live": live_status == "live",
                "live_status": live_status,
                "check_time": datetime.now(timezone.utc).isoformat(),
                "raw_json": {"snippet": snippet, "statistics": stats}
            }
            supabase.table("yt_stats_daily").insert(snapshot_data).execute()
            print(f"✅ {title} 快照已存檔。")
        except Exception as e:
            print(f"❌ 寫入 yt_stats_daily 失敗: {e}")

if __name__ == "__main__":
    fetch_and_save()
