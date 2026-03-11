import os
import json
from datetime import datetime, timezone
from googleapiclient.discovery import build
from supabase import create_client, Client

# 1. 從 GitHub Secrets 讀取環境變數
YT_API_KEY = os.environ.get("YT_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# 2. 定義要監控的頻道 ID (老闆請在這裡換成你想追蹤的頻道)
# 你可以在 YouTube 頻道網址找到這些 ID (例如 UC... 開頭的字串)
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
    
    # 呼叫 YouTube API 獲取數據
    request = youtube.channels().list(
        part="snippet,statistics",
        id=",".join(CHANNEL_IDS)
    )
    response = request.execute()

    if not response.get("items"):
        print("No data found for the provided channel IDs.")
        return

    for item in response.get("items", []):
        channel_id = item["id"]
        snippet = item.get("snippet", {})
        stats = item.get("statistics", {})
        
        # A. 更新或插入頻道基本資訊
        channel_data = {
            "channel_id": channel_id,
            "title": snippet.get("title"),
            "custom_url": snippet.get("customUrl"),
            # category 欄位留給之後的分群分析使用
        }
        # 使用 upsert，如果 ID 已存在則更新，不存在則插入
        supabase.table("yt_channels").upsert(channel_data).execute()

        # B. 寫入每日快照數據 (Snapshot)
        live_status = snippet.get("liveBroadcastContent", "none")
        snapshot_data = {
            "channel_id": channel_id,
            "subscriber_count": int(stats.get("subscriberCount", 0)),
            "total_views": int(stats.get("viewCount", 0)),
            "is_live": live_status == "live",
            "live_status": live_status,
            "check_time": datetime.now(timezone.utc).isoformat(),
            # 節省空間：只存 snippet 和 stats 的 JSON
            "raw_json": {"snippet": snippet, "statistics": stats}
        }
        supabase.table("yt_stats_daily").insert(snapshot_data).execute()
        
        print(f"✅ 成功更新: {snippet.get('title')}")

if __name__ == "__main__":
    if not all([YT_API_KEY, SUPABASE_URL, SUPABASE_KEY]):
        print("❌ 錯誤：找不到環境變數，請檢查 GitHub Secrets 設定。")
    else:
        try:
            fetch_and_save()
        except Exception as e:
            print(f"❌ 執行發生錯誤: {e}")
