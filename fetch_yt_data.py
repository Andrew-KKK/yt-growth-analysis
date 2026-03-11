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

# 版本號，方便確認跑的是最新版
VERSION = "2026.03.11.V5" 

# 頻道 ID 清單
CHANNEL_IDS = [
    "UCgTzsBI0DIRopMylJEDqnog", # 小雀とと
    "UCPGjKniCXat6xZ5T7DrWRBQ", # 測試直播中tag用頻道
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
        # 請求頻道數據
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
        print("⚠️ 警告：回傳結果為空！")
        return

    for item in items:
        channel_id = item["id"]
        snippet = item.get("snippet", {})
        stats = item.get("statistics", {})
        title = snippet.get('title')
        
        # --- 處理直播狀態邏輯 (允許 NULL) ---
        # 依照老闆要求，不設定預設值。如果 API 沒回傳該欄位，則變數為 None (資料庫存為 NULL)
        live_status = snippet.get("liveBroadcastContent") 
        
        # 如果 live_status 是 None，is_live 也應該是 None，代表「未知」
        if live_status is not None:
            is_live = (live_status == "live")
        else:
            is_live = None
        
        print(f"💾 正在寫入: {title} | 狀態: {live_status if live_status else 'NULL'}")

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
            print(f"✅ {title} 數據寫入成功 (live_status: {live_status if live_status else 'NULL'})")
        except Exception as e:
            print(f"❌ 寫入快照失敗: {e}")

if __name__ == "__main__":
    if not all([YT_API_KEY, SUPABASE_URL, SUPABASE_KEY]):
        print("❌ 錯誤：環境變數缺失。")
    else:
        fetch_and_save()
