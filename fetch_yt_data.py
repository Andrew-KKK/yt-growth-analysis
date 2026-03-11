import os
import sys
from datetime import datetime, timezone
from googleapiclient.discovery import build
from supabase import create_client, Client

# 強制輸出立即顯示
sys.stdout.reconfigure(line_buffering=True)

YT_API_KEY = os.environ.get("YT_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# 版本號 V7：升級雙階段偵測邏輯，解決 channels API 缺少直播欄位的問題
VERSION = "2026.03.11.V7" 

# 2. 定義要監控的頻道 ID (老闆請在這裡換成你想追蹤的頻道)
# 你可以在 YouTube 頻道網址找到這些 ID (例如 UC... 開頭的字串)
CHANNEL_IDS = [
    "UCgTzsBI0DIRopMylJEDqnog", # 範例直播待機頻道: 小雀とと
    "UCp_3ej2br9l9L1DSoHVDZGw", # 範例正規直播頻道: Eris Suzukami
    "UCexpzYDEnfmAvPSfG4xbcjA", # 範例不間斷直播頻道: 公視新聞網
    "UC2QXdY1z2UxgIh8h1RhsB_Q"  # 範例無直播無待機頻道: 黯冰泓-K
]

def get_yt_client():
    return build("youtube", "v3", developerKey=YT_API_KEY)

def get_supabase_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_and_save():
    print(f"🚀 [版本 {VERSION}] 啟動高精準採集任務...")
    youtube = get_yt_client()
    supabase = get_supabase_client()

    # --- 1. 批量獲取頻道基本統計資料 (Subscribers, Total Views) ---
    print(f"📡 步驟 1: 抓取 {len(CHANNEL_IDS)} 個頻道的統計數據...")
    ch_request = youtube.channels().list(
        part="snippet,statistics",
        id=",".join(CHANNEL_IDS)
    )
    ch_response = ch_request.execute()
    
    # 建立一個暫存物件來合併資料
    channel_map = {}
    for item in ch_response.get("items", []):
        cid = item["id"]
        channel_map[cid] = {
            "title": item["snippet"].get("title"),
            "custom_url": item["snippet"].get("customUrl"),
            "subscriber_count": int(item["statistics"].get("subscriberCount", 0)),
            "total_views": int(item["statistics"].get("viewCount", 0)),
            "raw_snippet": item["snippet"],
            "raw_stats": item["statistics"],
            "latest_video_id": None # 待填入
        }

    # --- 2. 逐一獲取最近活動以找出「潛在直播影片 ID」 ---
    # 此動作每頻道耗費 1 點，對 50 頻道來說非常划算
    print(f"📡 步驟 2: 偵測各頻道的最新活動...")
    video_ids_to_check = []
    cid_to_vid_map = {}

    for cid in CHANNEL_IDS:
        try:
            act_request = youtube.activities().list(
                part="snippet,contentDetails",
                channelId=cid,
                maxResults=3 # 檢查最近 3 個活動，確保不漏掉直播
            )
            act_response = act_request.execute()
            
            for act in act_response.get("items", []):
                act_type = act["snippet"]["type"]
                # 尋找上傳或直播活動
                vid = None
                if act_type == "upload":
                    vid = act["contentDetails"]["upload"].get("videoId")
                elif act_type == "broadcast":
                    vid = act["contentDetails"]["broadcast"].get("id")
                
                if vid:
                    video_ids_to_check.append(vid)
                    cid_to_vid_map[cid] = vid
                    break # 找到最新的就跳出
        except Exception as e:
            print(f"   ⚠️ 無法獲取頻道 {cid} 的活動: {e}")

    # --- 3. 批量檢查影片狀態 (取得真正的 liveBroadcastContent) ---
    # 1 點配額可檢查 50 個影片，極度省錢
    live_info_map = {}
    if video_ids_to_check:
        print(f"📡 步驟 3: 批量檢查 {len(video_ids_to_check)} 個影片的直播狀態...")
        vid_request = youtube.videos().list(
            part="snippet",
            id=",".join(video_ids_to_check)
        )
        vid_response = vid_request.execute()
        for v_item in vid_response.get("items", []):
            vid = v_item["id"]
            live_info_map[vid] = v_item["snippet"].get("liveBroadcastContent")

    # --- 4. 整合資料並寫入 Supabase ---
    print(f"💾 步驟 4: 整合並寫入資料庫...")
    for cid, data in channel_map.items():
        # 從影片狀態圖中找出對應的直播狀態
        target_vid = cid_to_vid_map.get(cid)
        live_status = live_info_map.get(target_vid) if target_vid else None
        
        # 判定 is_live
        is_live = (live_status == "live") if live_status else None
        
        print(f"   📝 {data['title']} | 狀態: {live_status if live_status else 'NULL'}")

        # 寫入母表
        try:
            supabase.table("yt_channels").upsert({
                "channel_id": cid,
                "title": data["title"],
                "custom_url": data["custom_url"],
            }).execute()
        except Exception as e:
            print(f"   ❌ 母表失敗: {e}")

        # 寫入快照
        try:
            supabase.table("yt_stats_daily").insert({
                "channel_id": cid,
                "subscriber_count": data["subscriber_count"],
                "total_views": data["total_views"],
                "is_live": is_live,
                "live_status": live_status,
                "check_time": datetime.now(timezone.utc).isoformat(),
                "raw_json": {"snippet": data["raw_snippet"], "statistics": data["raw_stats"]}
            }).execute()
        except Exception as e:
            print(f"   ❌ 快照失敗: {e}")

    print(f"✅ 全體任務完成！")

if __name__ == "__main__":
    if not all([YT_API_KEY, SUPABASE_URL, SUPABASE_KEY]):
        print("❌ 錯誤：環境變數缺失。")
    else:
        fetch_and_save()
        print("❌ 錯誤：環境變數缺失。")
    else:
        fetch_and_save()
