import os
import sys
import re
from datetime import datetime, timezone, timedelta
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from supabase import create_client, Client

# 強制輸出立即顯示
sys.stdout.reconfigure(line_buffering=True)

# 環境變數獲取
YT_API_KEY = os.environ.get("YT_API_KEY")
YT_API_KEY_2 = os.environ.get("YT_API_KEY_2") # 備用金鑰
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# 版本號 V26：導入 SmartYouTubeAPI 代理層 (時間分流 + 403 配額耗盡自動熱切換)
VERSION = "2026.03.23.V26" 

# 待機室過濾門檻：超過 30 天後的待機室忽略不計
WAITING_ROOM_THRESHOLD_DAYS = 30

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

def get_keys_by_preference():
    """根據時間決定金鑰的優先使用順序 (時間分流機制)"""
    keys = []
    # 偶數小時優先用 Key 2，奇數小時優先用 Key 1
    if YT_API_KEY_2 and datetime.now(timezone.utc).hour % 2 == 0:
        keys = [YT_API_KEY_2, YT_API_KEY]
    else:
        keys = [YT_API_KEY, YT_API_KEY_2]
    
    # 過濾掉未設定的 None 金鑰
    valid_keys = [k for k in keys if k]
    return valid_keys

class SmartYouTubeAPI:
    """智慧 API 代理層：處理自動連線與配額耗盡時的熱切換 (Failover)"""
    def __init__(self, keys):
        if not keys:
            print("❌ 錯誤：未提供任何 YouTube API 金鑰！")
            sys.exit(1)
        self.keys = keys
        self.current_idx = 0
        self.client = build("youtube", "v3", developerKey=self.keys[self.current_idx])
        
        self._print_current_key("初始化連線")

    def _print_current_key(self, action_msg):
        current_key = self.keys[self.current_idx]
        masked = f"***{current_key[-3:]}"
        key_label = "主金鑰 (Key 1)" if current_key == YT_API_KEY else "備用金鑰 (Key 2)"
        print(f"🔑 {action_msg}: 使用 {key_label} [{masked}]")

    def _handle_error_and_retry(self, error):
        """檢查是否為配額耗盡，並嘗試切換金鑰"""
        if isinstance(error, HttpError) and error.resp.status in [403]:
            error_content = str(error).lower()
            if "quotaexceeded" in error_content or "daily limit" in error_content:
                if self.current_idx + 1 < len(self.keys):
                    self.current_idx += 1
                    print(f"\n⚠️ 警告：偵測到 API 配額耗盡 (Quota Exceeded)！")
                    self._print_current_key("自動熱切換至下一把金鑰")
                    # 重新建立 YouTube 客戶端連線
                    self.client = build("youtube", "v3", developerKey=self.keys[self.current_idx])
                    return True # 允許重試
        return False # 無法重試或非配額錯誤，交由外層處理

    # --- 以下為 API 封裝方法，內建無窮重試迴圈直到成功或金鑰全數耗盡 ---
    
    def get_channels(self, **kwargs):
        while True:
            try:
                return self.client.channels().list(**kwargs).execute()
            except Exception as e:
                if not self._handle_error_and_retry(e): raise

    def get_activities(self, **kwargs):
        while True:
            try:
                return self.client.activities().list(**kwargs).execute()
            except Exception as e:
                if not self._handle_error_and_retry(e): raise

    def get_videos(self, **kwargs):
        while True:
            try:
                return self.client.videos().list(**kwargs).execute()
            except Exception as e:
                if not self._handle_error_and_retry(e): raise

def safe_parse_iso(date_str):
    """強健的 ISO 時間解析，處理 Supabase 回傳的不規則微秒位數"""
    if not date_str: return None
    date_str = date_str.replace('Z', '+00:00')
    try:
        return datetime.fromisoformat(date_str)
    except ValueError:
        match = re.match(r"(.+?)\.(\d+)([+-].+)", date_str)
        if match:
            base, micros, tz = match.groups()
            standardized = f"{base}.{micros.ljust(6, '0')[:6]}{tz}"
            return datetime.fromisoformat(standardized)
        raise

def get_supabase_client() -> Client: 
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def parse_duration_to_seconds(duration_str):
    if not duration_str: return 0
    match = re.match(r'^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$', duration_str)
    if not match: return 0
    h, m, s = [int(x) if x else 0 for x in match.groups()]
    return h * 3600 + m * 60 + s

def fetch_and_save():
    now_utc = datetime.now(timezone.utc)
    supabase = get_supabase_client()
    
    # --- 模式判定 (狀態驅動) ---
    is_snapshot_mode = False
    try:
        res = supabase.table("yt_stats_daily").select("check_time").order("check_time", desc=True).limit(1).execute()
        if res.data and "check_time" in res.data[0]:
            last_check = safe_parse_iso(res.data[0]["check_time"])
            time_diff = now_utc - last_check
            if time_diff >= timedelta(hours=2, minutes=45):
                is_snapshot_mode = True
        else:
            is_snapshot_mode = True 
    except Exception as e:
        print(f"⚠️ 無法查詢上次快照時間 ({e})，安全起見執行全量快照。")
        is_snapshot_mode = True
    
    mode_text = "【全量快照 + 同接監控】" if is_snapshot_mode else "【僅同接監控】"
    print(f"🚀 [版本 {VERSION}] 啟動{mode_text}任務...")
    
    # 初始化智慧 API 代理層
    available_keys = get_keys_by_preference()
    yt_api = SmartYouTubeAPI(available_keys)
    
    channel_ids = load_channel_ids("channels.txt")
    if not channel_ids: return

    quota_used = 0

    # --- 步驟 1: 頻道基本資料與統計 ---
    print(f"📡 步驟 1: 獲取頻道清單狀態 (頻道數: {len(channel_ids)})...")
    channel_map = {}
    parts = "snippet,statistics" if is_snapshot_mode else "snippet"
    
    for i in range(0, len(channel_ids), 50):
        batch = channel_ids[i:i+50]
        try:
            # 替換為使用智慧代理層
            ch_res = yt_api.get_channels(part=parts, id=",".join(batch))
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

    # --- 步驟 2: 偵測活動 ---
    print(f"📡 步驟 2: 掃描最近活動...")
    all_video_ids = []
    cid_to_video_ids = {}
    for cid in channel_ids:
        try:
            max_r = 15 if is_snapshot_mode else 5
            # 替換為使用智慧代理層
            act_res = yt_api.get_activities(part="snippet,contentDetails", channelId=cid, maxResults=max_r)
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
            pass 

    # --- 步驟 3: 批量解析影片狀態與同接 ---
    live_info_map = {}
    video_details_list = []
    live_logs_to_insert = []
    
    if all_video_ids:
        print(f"📡 步驟 3: 解析 {len(all_video_ids)} 支影片的數據...")
        vid_parts = "snippet,liveStreamingDetails,contentDetails,statistics"
        
        for i in range(0, len(all_video_ids), 50):
            batch_vids = all_video_ids[i:i+50]
            try:
                # 替換為使用智慧代理層
                vid_res = yt_api.get_videos(part=vid_parts, id=",".join(batch_vids))
                quota_used += 1
                
                for v_item in vid_res.get("items", []):
                    vid = v_item["id"]
                    snippet = v_item.get("snippet", {})
                    lsd = v_item.get("liveStreamingDetails", {})
                    stats = v_item.get("statistics", {})
                    
                    status = snippet.get("liveBroadcastContent")
                    ccv = int(lsd.get("concurrentViewers")) if "concurrentViewers" in lsd else None
                    actual_start = lsd.get("actualStartTime")
                    
                    # 待機室過濾
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

    # --- 步驟 4: 執行資料庫存檔與狀態報告 ---
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
    
    # --- 總結報告 ---
    
    print(f"📊 --- 任務總結報告 ({VERSION}) ---")
    print(f"📡 模式: {'全量快照' if is_snapshot_mode else '僅同接監控'}")
    print(f"💰 本次消耗估計 Quota: {quota_used} | 每日配額佔比估計: {(quota_used / 10000) * 100:.2f}%")
    utc_now = datetime.now(timezone.utc)
    print(f"🕒 任務結束時間 (UTC): {utc_now.strftime('%Y-%m-%d %H:%M:%S')}")
    tw_now = utc_now.astimezone(timezone(timedelta(hours=8)))
    print(f"🇹🇼 任務結束時間 (台灣): {tw_now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"------------------------\n")

if __name__ == "__main__":
    fetch_and_save()
