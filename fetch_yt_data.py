import os
import sys
import re
from datetime import datetime, timezone, timedelta
from googleapiclient.discovery import build
from supabase import create_client, Client

# 強制輸出立即顯示
sys.stdout.reconfigure(line_buffering=True)

# 環境變數獲取
YT_API_KEY = os.environ.get("YT_API_KEY")
YT_API_KEY_2 = os.environ.get("YT_API_KEY_2") # 備用金鑰
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# 版本號 V24：修復Cron Throttling 導致的一系列問題
VERSION = "2026.03.15.V24" 

def load_channel_ids(filename="channels.txt"):
    """從外部純文字檔讀取頻道 ID 清單"""
    ids = []
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'): continue
                actual_id = line.split('#')[0].strip()
                actual_id = actual_id.replace('"', '').replace("'", "").replace(',', '')
                if actual_id and actual_id not in ids:
                    ids.append(actual_id)
        return ids
    except FileNotFoundError:
        print(f"❌ 嚴重錯誤：找不到 {filename}！請確保該檔案存在於儲存庫中。")
        sys.exit(1)

WAITING_ROOM_THRESHOLD_DAYS = 30

def get_api_key_info():
    """決定當下要使用的金鑰並回傳遮蔽資訊"""
    if YT_API_KEY_2 and datetime.now(timezone.utc).hour % 2 == 0:
        masked = f"***{YT_API_KEY_2[-3:]}" if YT_API_KEY_2 else "None"
        return YT_API_KEY_2, f"備用金鑰 (Key 2) [{masked}]"
    
    masked = f"***{YT_API_KEY_2[-3:]}" if YT_API_KEY else "None"
    return YT_API_KEY, f"主金鑰 (Key 1) [{masked}]"

def get_yt_client(api_key):
    return build("youtube", "v3", developerKey=api_key)

def get_supabase_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def parse_duration_to_seconds(duration_str):
    if not duration_str: return 0
    match = re.match(r'^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$', duration_str)
    if not match: return 0
    h, m, s = [int(x) if x else 0 for x in match.groups()]
    return h * 3600 + m * 60 + s

def fetch_and_save():
    # --- A. 模式判定與金鑰設定 ---
    now_utc = datetime.now(timezone.utc)
    supabase = get_supabase_client()
    
    # [修改] 捨棄脆弱的時間字串比對，改用資料庫真實紀錄判斷 (免疫 GitHub 延遲)
    is_snapshot_mode = False
    try:
        res = supabase.table("yt_stats_daily").select("check_time").order("check_time", desc=True).limit(1).execute()
        if res.data and "check_time" in res.data[0]:
            last_check = datetime.fromisoformat(res.data[0]["check_time"].replace("Z", "+00:00"))
            time_diff = now_utc - last_check
            # 如果距離上次快照超過 2 小時 45 分鐘，就啟動全量快照 (留 15 分鐘緩衝)
            if time_diff >= timedelta(hours=2, minutes=45):
                is_snapshot_mode = True
        else:
            is_snapshot_mode = True # 如果資料庫是空的，強制跑第一次快照
    except Exception as e:
        print(f"⚠️ 無法查詢上次快照時間 ({e})，安全起見執行全量快照。")
        is_snapshot_mode = True
    
    api_key, key_name = get_api_key_info()
    youtube = get_yt_client(api_key)
    
    mode_text = "【全量快照 + 同接監控】" if is_snapshot_mode else "【僅同接監控】"
    print(f"🚀 [版本 2026.03.16.V24] 啟動{mode_text}任務...")
    print(f"🔑 目前使用金鑰: {key_name}")
    
    channel_ids = load_channel_ids("channels.txt")
    if not channel_ids:
        print("❌ 警告：頻道清單為空，請檢查 channels.txt 內容。")
        return

    quota_used = 0

    # --- B. 頻道基本資料與統計 ---
    print(f"📡 步驟 1: 獲取頻道清單狀態 (頻道數: {len(channel_ids)})...")
    channel_map = {}
    parts = "snippet,statistics" if is_snapshot_mode else "snippet"
    
    for i in range(0, len(channel_ids), 50):
        batch = channel_ids[i:i+50]
        try:
            ch_res = youtube.channels().list(part=parts, id=",".join(batch)).execute()
            quota_used += 1
            for item in ch_res.get("items", []):
                stats = item.get("statistics", {})
                channel_map[item["id"]] = {
                    "title": item["snippet"].get("title"),
                    "custom_url": item["snippet"].get("customUrl"),
                    "subs": int(stats.get("subscriberCount", 0)) if is_snapshot_mode else None,
                    "views": int(stats.get("viewCount", 0)) if is_snapshot_mode else None,
                    "raw_snippet": item["snippet"],
                    "raw_stats": stats
                }
        except Exception as e:
            print(f"   ❌ 獲取頻道資料失敗: {e}")

    # --- C. 偵測活動 ---
    print(f"📡 步驟 2: 掃描最近活動...")
    all_video_ids = []
    cid_to_video_ids = {}
    for cid in channel_ids:
        try:
            max_r = 15 if is_snapshot_mode else 5
            act_res = youtube.activities().list(part="snippet,contentDetails", channelId=cid, maxResults=max_r).execute()
            quota_used += 1
            vids = []
            for act in act_res.get("items", []):
                t = act["snippet"]["type"]
                vid = None
                if t == "upload": vid = act["contentDetails"]["upload"].get("videoId")
                elif t == "liveBroadcast": vid = act["contentDetails"]["liveBroadcast"].get("id")
                if vid and vid not in vids:
                    vids.append(vid)
                    if vid not in all_video_ids: all_video_ids.append(vid)
            cid_to_video_ids[cid] = vids
        except Exception as e:
            pass # 單一頻道活動抓取失敗可忽略，避免洗版

    # --- D. 批量解析影片狀態與同接 ---
    live_info_map = {}
    video_details_list = []
    live_logs_to_insert = []
    
    if all_video_ids:
        print(f"📡 步驟 3: 解析 {len(all_video_ids)} 支影片的數據...")
        # [關鍵修正] 不論模式為何，都抓取完整數據，確保外鍵存在且最新
        vid_parts = "snippet,liveStreamingDetails,contentDetails,statistics"
        
        for i in range(0, len(all_video_ids), 50):
            batch_vids = all_video_ids[i:i+50]
            try:
                vid_res = youtube.videos().list(part=vid_parts, id=",".join(batch_vids)).execute()
                quota_used += 1
                
                for v_item in vid_res.get("items", []):
                    vid = v_item["id"]
                    snippet = v_item.get("snippet", {})
                    lsd = v_item.get("liveStreamingDetails", {})
                    stats = v_item.get("statistics", {})
                    
                    status = snippet.get("liveBroadcastContent")
                    ccv = int(lsd.get("concurrentViewers")) if "concurrentViewers" in lsd else None
                    actual_start = lsd.get("actualStartTime")
                    
                    if status == "upcoming":
                        sch = lsd.get("scheduledStartTime")
                        if sch:
                            sch_time = datetime.fromisoformat(sch.replace("Z", "+00:00"))
                            if (sch_time - now_utc) > timedelta(days=WAITING_ROOM_THRESHOLD_DAYS):
                                status = "none"
                    
                    live_info_map[vid] = {"status": status, "ccv": ccv, "start": actual_start}

                    if status == "live" and ccv is not None:
                        live_logs_to_insert.append({
                            "channel_id": snippet.get("channelId"),
                            "video_id": vid,
                            "ccv": ccv,
                            "captured_at": now_utc.isoformat()
                        })

                    # [關鍵修正] 取消 is_snapshot_mode 的限制，永遠更新最新 5 支影片的數據與基礎資料
                    v_type = "Live" if "liveStreamingDetails" in v_item else "Shorts" if parse_duration_to_seconds(v_item.get("contentDetails", {}).get("duration", "")) <= 61 else "Video"
                    video_details_list.append({
                        "video_id": vid, "channel_id": snippet.get("channelId"), "title": snippet.get("title"),
                        "video_type": v_type, "published_at": snippet.get("publishedAt"),
                        "view_count": int(stats["viewCount"]) if "viewCount" in stats else None,
                        "like_count": int(stats["likeCount"]) if "likeCount" in stats else None,
                        "comment_count": int(stats["commentCount"]) if "commentCount" in stats else None
                    })
            except Exception as e:
                print(f"   ❌ 影片數據解析失敗: {e}")

    # --- E. 寫入資料庫與終端機日誌輸出 ---
    print(f"💾 步驟 4: 執行資料庫存檔與狀態報告...")
    status_priority = {"live": 3, "upcoming": 2, "none": 1}
    
    for cid, data in channel_map.items():
        best_vid = None
        current_max_prio = -1
        for vid in cid_to_video_ids.get(cid, []):
            info = live_info_map.get(vid, {})
            prio = status_priority.get(info.get("status"), 0)
            if prio > current_max_prio:
                current_max_prio = prio
                best_vid = vid
            if current_max_prio == 3: break
        
        final_info = live_info_map.get(best_vid, {})
        best_status = final_info.get("status", "none")
        ccv_val = final_info.get("ccv")
        
        log_msg = f"   📝 {data['title']} | 判定結果: {best_status}"
        if best_status == "live" and ccv_val is not None:
            log_msg += f" (同接: {ccv_val} 人)"
        print(log_msg)
        
        try:
            supabase.table("yt_channels").upsert({"channel_id": cid, "title": data["title"], "custom_url": data["custom_url"]}).execute()
        except Exception as e:
            print(f"      ❌ 頻道母表更新失敗: {e}")

        if is_snapshot_mode:
            try:
                supabase.table("yt_stats_daily").insert({
                    "channel_id": cid, "subscriber_count": data["subs"], "total_views": data["views"],
                    "is_live": (best_status == "live"), "live_status": best_status,
                    "concurrent_viewers": ccv_val if best_status == "live" else None,
                    "actual_start_time": final_info.get("start"),
                    "check_time": now_utc.isoformat(),
                    "raw_json": {"snippet": data["raw_snippet"], "statistics": data["raw_stats"]}
                }).execute()
            except Exception as e:
                print(f"      ❌ 快照寫入失敗: {e}")

    # [關鍵修正] 取消 is_snapshot_mode 限制，確保先寫入母表
    if video_details_list:
        print(f"🎬 批次更新影片清單 ({len(video_details_list)} 筆)...")
        try: 
            supabase.table("yt_videos").upsert(video_details_list).execute()
        except Exception as e: 
            print(f"      ❌ 影片清單寫入失敗: {e}")

    if live_logs_to_insert:
        print(f"📈 記錄即時同接數據 ({len(live_logs_to_insert)} 筆)...")
        try: 
            supabase.table("yt_live_logs").insert(live_logs_to_insert).execute()
        except Exception as e: 
            print(f"      ❌ 同接數據寫入失敗: {e}")

    # --- F. 總結報告 ---
    tw_now = now_utc.astimezone(timezone(timedelta(hours=8)))
    print(f"\n📊 --- 任務總結報告 ({VERSION}) ---")
    print(f"📡 模式: {'全量快照' if is_snapshot_mode else '僅同接監控'}")
    print(f"💰 本次消耗 Quota: {quota_used} | 每日配額佔比: {(quota_used / 10000) * 100:.2f}%")
    print(f"🇹🇼 台灣時間: {tw_now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"------------------------\n")

if __name__ == "__main__":
    fetch_and_save()
