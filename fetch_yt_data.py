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

# 版本號 V9：彈性深挖邏輯，區分 none 與 NULL (未知)
VERSION = "2026.03.11.V9" 

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
    print(f"🚀 [版本 {VERSION}] 啟動彈性深挖採集任務...")
    youtube = get_yt_client()
    supabase = get_supabase_client()

    # --- 1. 批量獲取頻道基本統計資料 ---
    print(f"📡 步驟 1: 抓取 {len(CHANNEL_IDS)} 個頻道的統計數據...")
    ch_request = youtube.channels().list(
        part="snippet,statistics",
        id=",".join(CHANNEL_IDS)
    )
    ch_response = ch_request.execute()
    
    channel_map = {}
    for item in ch_response.get("items", []):
        cid = item["id"]
        channel_map[cid] = {
            "title": item["snippet"].get("title"),
            "custom_url": item["snippet"].get("customUrl"),
            "subscriber_count": int(item["statistics"].get("subscriberCount", 0)),
            "total_views": int(item["statistics"].get("viewCount", 0)),
            "raw_snippet": item["snippet"],
            "raw_stats": item["statistics"]
        }

    # --- 2. 收集各頻道活動 (設定查找上限為 10) ---
    print(f"📡 步驟 2: 進行彈性深挖 (上限 10 個活動)...")
    all_video_ids = []
    cid_to_video_ids = {}

    for cid in CHANNEL_IDS:
        try:
            act_request = youtube.activities().list(
                part="snippet,contentDetails",
                channelId=cid,
                maxResults=10 # 提高上限以確保穿透多個待機室
            )
            act_response = act_request.execute()
            
            items = act_response.get("items", [])
            vids = []
            for act in items:
                act_type = act["snippet"]["type"]
                vid = None
                if act_type == "upload":
                    vid = act["contentDetails"]["upload"].get("videoId")
                elif act_type == "broadcast":
                    vid = act["contentDetails"]["broadcast"].get("id")
                
                if vid and vid not in vids:
                    vids.append(vid)
                    if vid not in all_video_ids:
                        all_video_ids.append(vid)
            
            cid_to_video_ids[cid] = vids
            if not vids:
                print(f"   ℹ️ 頻道 {cid} 目前沒有任何近期活動紀錄。")
        except Exception as e:
            print(f"   ⚠️ 無法獲取頻道 {cid} 的活動: {e}")

    # --- 3. 批量檢查影片狀態 ---
    live_info_map = {}
    if all_video_ids:
        print(f"📡 步驟 3: 批量解析 {len(all_video_ids)} 個潛在影片狀態...")
        # 由於 videos API 一次上限 50 個，目前頻道數少可直接執行
        vid_request = youtube.videos().list(
            part="snippet",
            id=",".join(all_video_ids[:50])
        )
        vid_response = vid_request.execute()
        for v_item in vid_response.get("items", []):
            vid = v_item["id"]
            live_info_map[vid] = v_item["snippet"].get("liveBroadcastContent")

    # --- 4. 判定與寫入 ---
    print(f"💾 步驟 4: 狀態判定與資料庫存檔...")
    # 優先級：live > upcoming > none > (未知/NULL)
    status_priority = {"live": 3, "upcoming": 2, "none": 1}

    for cid, data in channel_map.items():
        vids = cid_to_video_ids.get(cid, [])
        
        best_status = None
        current_max_prio = -1

        for vid in vids:
            s = live_info_map.get(vid)
            prio = status_priority.get(s, 0)
            if prio > current_max_prio:
                current_max_prio = prio
                best_status = s
            # 如果已經抓到最高級別的 live，就不用再看後面的活動了
            if current_max_prio == 3:
                break
        
        # 判定 is_live
        is_live = (best_status == "live") if best_status else None
        
        print(f"   📝 {data['title']} | 判定結果: {best_status if best_status else 'NULL (未知)'}")

        # 寫入母表
        try:
            supabase.table("yt_channels").upsert({
                "channel_id": cid,
                "title": data["title"],
                "custom_url": data["custom_url"],
            }).execute()
        except Exception as e:
            print(f"   ❌ 母表更新失敗: {e}")

        # 寫入快照
        try:
            supabase.table("yt_stats_daily").insert({
                "channel_id": cid,
                "subscriber_count": data["subscriber_count"],
                "total_views": data["total_views"],
                "is_live": is_live,
                "live_status": best_status,
                "check_time": datetime.now(timezone.utc).isoformat(),
                "raw_json": {"snippet": data["raw_snippet"], "statistics": data["raw_stats"]}
            }).execute()
        except Exception as e:
            print(f"   ❌ 快照寫入失敗: {e}")

    print(f"✅ 全體採集任務圓滿完成！")

if __name__ == "__main__":
    if not all([YT_API_KEY, SUPABASE_URL, SUPABASE_KEY]):
        print("❌ 錯誤：環境變數缺失，請檢查 GitHub Secrets。")
    else:
        fetch_and_save()
