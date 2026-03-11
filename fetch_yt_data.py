import os
import sys
import json
from datetime import datetime, timezone
from googleapiclient.discovery import build
from supabase import create_client, Client

# 強制讓 print 訊息立即顯示在 GitHub 日誌中
sys.stdout.reconfigure(line_buffering=True)

YT_API_KEY = os.environ.get("YT_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# 版本號 V6：加入 API 原始欄位偵錯
VERSION = "2026.03.11.V6" 

# 2. 定義要監控的頻道 ID (老闆請在這裡換成你想追蹤的頻道)
# 你可以在 YouTube 頻道網址找到這些 ID (例如 UC... 開頭的字串)
CHANNEL_IDS = [
    "UCgTzsBI0DIRopMylJEDqnog", # 範例直播待機頻道
    "UCp_3ej2br9l9L1DSoHVDZGw", # 範例正規直播頻道
    "UCexpzYDEnfmAvPSfG4xbcjA", # 範例不間斷直播頻道
]

def get_yt_client():
    return build("youtube", "v3", developerKey=YT_API_KEY)

def get_supabase_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_and_save():
    print(f"🚀 [版本 {VERSION}] 啟動採集任務...")
    
    youtube = get_yt_client()
    supabase = get_supabase_client()
    
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

    for item in items:
        channel_id = item["id"]
        snippet = item.get("snippet", {})
        stats = item.get("statistics", {})
        title = snippet.get('title')
        
        # --- [偵錯重點] 打印所有 snippet 的鍵值，看看 liveBroadcastContent 是否存在 ---
        print(f"--- 頻道偵錯資訊: {title} ---")
        print(f"Snippet 包含的欄位: {list(snippet.keys())}")
        
        # 獲取原始直播狀態
        live_status = snippet.get("liveBroadcastContent") 
        print(f"YouTube 官方回傳的 liveBroadcastContent 值: '{live_status}'")
        
        # 判定 is_live
        if live_status is not None:
            is_live = (live_status == "live")
        else:
            is_live = None
        
        # 寫入母表
        try:
            supabase.table("yt_channels").upsert({
                "channel_id": channel_id,
                "title": title,
                "custom_url": snippet.get("customUrl"),
            }).execute()
        except Exception as e:
            print(f"❌ 寫入母表失敗: {e}")

        # 寫入快照
        try:
            snapshot_data = {
                "channel_id": channel_id,
                "subscriber_count": int(stats.get("subscriberCount", 0)),
                "total_views": int(stats.get("viewCount", 0)),
                "is_live": is_live,
                "live_status": live_status,
                "check_time": datetime.now(timezone.utc).isoformat(),
                "raw_json": {"snippet": snippet, "statistics": stats}
            }
            supabase.table("yt_stats_daily").insert(snapshot_data).execute()
            print(f"✅ {title} 數據寫入完畢 | is_live: {is_live} | status: {live_status}")
        except Exception as e:
            print(f"❌ 寫入快照失敗: {e}")

if __name__ == "__main__":
    if not all([YT_API_KEY, SUPABASE_URL, SUPABASE_KEY]):
        print("❌ 錯誤：環境變數缺失。")
    else:
        fetch_and_save()
