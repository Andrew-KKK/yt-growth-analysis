import os
import sys
from datetime import datetime, timezone
from googleapiclient.discovery import build
from supabase import create_client, Client

# 強制讓 print 訊息立即顯示在 GitHub 日誌中
sys.stdout.reconfigure(line_buffering=True)

YT_API_KEY = os.environ.get("YT_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# --- 這裡我加了一個版本號，幫你確認有沒有跑對版 ---
VERSION = "2024.03.11.V3" 


CHANNEL_IDS = [
    "UCgTzsBI0DIRopMylJEDqnog", # 小雀とと (Toto Kogara) 的 ID
]

def get_yt_client():
    return build("youtube", "v3", developerKey=YT_API_KEY)

def get_supabase_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_and_save():
    print(f"🚀 [版本 {VERSION}] 啟動採集任務...")
    
    youtube = get_yt_client()
    supabase = get_supabase_client()
    
    print(f"📡 正在請求頻道: {CHANNEL_IDS}")
    
    try:
        request = youtube.channels().list(
            part="snippet,statistics",
            id=",".join(CHANNEL_IDS)
        )
        response = request.execute()
    except Exception as e:
        print(f"❌ YouTube API 呼叫失敗: {e}")
        return

    items = response.get("items", [])
    print(f"🔎 YouTube API 回傳了 {len(items)} 筆資料")

    if not items:
        print("⚠️ 警告：回傳結果為空！請檢查 CHANNEL_IDS 是否正確。")
        return

    for item in items:
        channel_id = item["id"]
        snippet = item.get("snippet", {})
        stats = item.get("statistics", {})
        title = snippet.get('title')
        
        print(f"💾 正在寫入: {title} ({channel_id})")

        # 寫入母表
        try:
            supabase.table("yt_channels").upsert({
                "channel_id": channel_id,
                "title": title,
                "custom_url": snippet.get("customUrl"),
            }).execute()
        except Exception as e:
            print(f"❌ 寫入母表失敗 (請檢查 RLS): {e}")

        # 寫入快照
        try:
            snapshot_data = {
                "channel_id": channel_id,
                "subscriber_count": int(stats.get("subscriberCount", 0)),
                "total_views": int(stats.get("viewCount", 0)),
                "is_live": snippet.get("liveBroadcastContent") == "live",
                "live_status": snippet.get("liveBroadcastContent"),
                "check_time": datetime.now(timezone.utc).isoformat(),
                "raw_json": {"snippet": snippet, "statistics": stats}
            }
            supabase.table("yt_stats_daily").insert(snapshot_data).execute()
            print(f"✅ {title} 資料已成功寫入 Supabase")
        except Exception as e:
            print(f"❌ 寫入快照失敗 (請檢查 RLS): {e}")

if __name__ == "__main__":
    fetch_and_save()
