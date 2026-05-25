import os
import sys
import re
import requests
from datetime import datetime, timezone, timedelta
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from supabase import create_client, Client

# 強制輸出立即顯示
sys.stdout.reconfigure(line_buffering=True)

# 環境變數獲取
YT_API_KEY = os.environ.get("YT_API_KEY")
YT_API_KEY_2 = os.environ.get("YT_API_KEY_2")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
N8N_WATCHDOG_WEBHOOK = os.environ.get("N8N_WATCHDOG_WEBHOOK") 

# 版本號 V32：導入資料庫端 RPC 狀態機鎖 (Database FSM Lock)
VERSION = "2026.05.11.V32.2-FSMLock" 

COOLDOWN_MINUTES = 25 # 冷卻時間設定 (分鐘)
WAITING_ROOM_THRESHOLD_DAYS = 30 # 待機室過濾門檻：超過 30 天後的待機室忽略不計
DEADLOCK_MINUTES = 10 # 超過此時間的 RUNNING 視為執行錯誤導致沒有被解開的鎖

def load_channel_ids(filename="channels.txt"):
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
        print(f"❌ 嚴重錯誤：找不到 {filename}！")
        sys.exit(1)

def get_keys_by_preference():
    keys = []
    if YT_API_KEY_2 and datetime.now(timezone.utc).hour % 2 == 0:
        keys = [YT_API_KEY_2, YT_API_KEY]
    else:
        keys = [YT_API_KEY, YT_API_KEY_2]
    return [k for k in keys if k]

class SmartYouTubeAPI:
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
        if isinstance(error, HttpError) and error.resp.status in [403]:
            error_content = str(error).lower()
            if "quotaexceeded" in error_content or "daily limit" in error_content:
                if self.current_idx + 1 < len(self.keys):
                    self.current_idx += 1
                    print(f"\n⚠️ 警告：偵測到 API 配額耗盡！")
                    self._print_current_key("自動熱切換至下一把金鑰")
                    self.client = build("youtube", "v3", developerKey=self.keys[self.current_idx])
                    return True
        return False

    def get_channels(self, **kwargs):
        while True:
            try: return self.client.channels().list(**kwargs).execute()
            except Exception as e:
                if not self._handle_error_and_retry(e): raise

    def get_activities(self, **kwargs):
        while True:
            try: return self.client.activities().list(**kwargs).execute()
            except Exception as e:
                if not self._handle_error_and_retry(e): raise

    def get_videos(self, **kwargs):
        while True:
            try: return self.client.videos().list(**kwargs).execute()
            except Exception as e:
                if not self._handle_error_and_retry(e): raise

def safe_parse_iso(date_str):
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
    supabase = get_supabase_client()
    now_utc = datetime.now(timezone.utc)
    tw_now = now_utc.astimezone(timezone(timedelta(hours=8)))
    
    skip_cooldown = (os.environ.get("SKIP_COOLDOWN") == "true")
    source = "n8n" if os.environ.get("N8N_TRIGGER") else "github_cron"    
    print(f"🚀 [版本 {VERSION}] 啟動環境與狀態機守衛...")
    print(f"🕒 目前時間 (UTC): {now_utc.strftime('%Y-%m-%d %H:%M:%S')} | 🇹🇼 台灣: {tw_now.strftime('%H:%M:%S')}")

    # ==============================================================================
    # 🌟 核心升級：呼叫資料庫狀態機 (RPC FSM Lock)
    # 不再由 Python 判斷時間，完全交由資料庫的 Guard Conditions 把關
    # ==============================================================================
    current_log_id = None
    try:
        rpc_params = {
            "p_cooldown_min": 0 if skip_cooldown else COOLDOWN_MINUTES,
            "p_deadlock_min": DEADLOCK_MINUTES,
            "p_source": f"{source}{'_forced' if skip_cooldown else ''}",
            "p_version": VERSION
        }
        lock_res = supabase.rpc("rpc_acquire_lock", rpc_params).execute()
        current_log_id = lock_res.data
        
        if not current_log_id:
            print(f"🛑 守衛攔截：狀態機拒絕轉換 (仍在執行中或處於 {COOLDOWN_MINUTES} 分鐘冷卻期內)。")
            print("💤 本次任務安全退出，等待下次喚醒。")
            return  # 被狀態機擋下，直接退出

        print(f"✅ 成功通過守衛，獲取狀態機鎖 (Log ID: {current_log_id})")
        
        # 呼叫 n8n 看門狗掛號
        if N8N_WATCHDOG_WEBHOOK:
            try:
                requests.post(N8N_WATCHDOG_WEBHOOK, json={"log_id": current_log_id}, timeout=3)
                print(f"🐕 已呼叫 n8n 看門狗查驗狀態。")
            except Exception as e:
                print(f"⚠️ 呼叫看門狗失敗 ({e})，但不影響執行。")
                
    except Exception as e:
        print(f"⚠️ 狀態機鎖獲取失敗 ({e})，可能 RPC 未設定。安全起見，繼續執行。")

    # --- 高低頻模式判定 (保留原本以時間差作為排程分流的邏輯) ---
    is_snapshot_mode = False
    try:
        res = supabase.table("yt_stats_daily").select("check_time").order("check_time", desc=True).limit(1).execute()
        if res.data and "check_time" in res.data[0]:
            last_check = safe_parse_iso(res.data[0]["check_time"])
            if (now_utc - last_check) >= timedelta(hours=2, minutes=45):
                is_snapshot_mode = True
        else:
            is_snapshot_mode = True 
    except Exception as e:
        is_snapshot_mode = True
    
    mode_text = "【全量快照 (per 3hr) + 同接監控】" if is_snapshot_mode else "【僅同接監控 (per 30min)】"
    print(f"🚀 啟動 {mode_text} 任務...")
    
    available_keys = get_keys_by_preference()
    yt_api = SmartYouTubeAPI(available_keys)
    channel_ids = load_channel_ids("channels.txt")
    if not channel_ids: return
    quota_used = 0

    # --- 步驟 1: 頻道基本資料 ---
    print(f"📡 步驟 1: 獲取頻道清單狀態 (頻道數: {len(channel_ids)})...")
    channel_map = {}
    parts = "snippet,statistics" if is_snapshot_mode else "snippet"
    for i in range(0, len(channel_ids), 50):
        batch = channel_ids[i:i+50]
        try:
            ch_res = yt_api.get_channels(part=parts, id=",".join(batch))
            quota_used += 1
            for item in ch_res.get("items", []):
                stats = item.get("statistics", {})
                channel_map[item["id"]] = {
                    "title": item["snippet"].get("title"),
                    "custom_url": item["snippet"].get("customUrl"),
                    "subs": int(stats.get("subscriberCount", 0)) if is_snapshot_mode else None,
                    "views": int(stats.get("viewCount", 0)) if is_snapshot_mode else None,
                    "raw_snippet": item["snippet"], "raw_stats": stats
                }
        except Exception as e:
            print(f"   ❌ 獲取頻道資料失敗: {e}")

    # --- 步驟 2: 偵測活動 ---
    print(f"📡 步驟 2: 掃描最近活動...")
    all_video_ids, cid_to_video_ids = [], {}
    for cid in channel_ids:
        try:
            max_r = 15 if is_snapshot_mode else 5
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
        except Exception: pass 

    # --- 步驟 3: 解析影片與同接 ---
    live_info_map, video_details_list, live_logs_to_insert = {}, [], []
    if all_video_ids:
        print(f"📡 步驟 3: 解析 {len(all_video_ids)} 支影片數據...")
        for i in range(0, len(all_video_ids), 50):
            batch_vids = all_video_ids[i:i+50]
            try:
                vid_res = yt_api.get_videos(part="snippet,liveStreamingDetails,contentDetails,statistics", id=",".join(batch_vids))
                quota_used += 1
                for v_item in vid_res.get("items", []):
                    vid = v_item["id"]
                    snippet = v_item.get("snippet", {})
                    lsd = v_item.get("liveStreamingDetails", {})
                    stats = v_item.get("statistics", {})
                    
                    status = snippet.get("liveBroadcastContent")
                    ccv = int(lsd.get("concurrentViewers")) if "concurrentViewers" in lsd else None
                    
                    if status == "upcoming":
                        sch = lsd.get("scheduledStartTime")
                        if sch and (datetime.fromisoformat(sch.replace("Z", "+00:00")) - now_utc) > timedelta(days=WAITING_ROOM_THRESHOLD_DAYS):
                            status = "none"
                    
                    live_info_map[vid] = {"status": status, "ccv": ccv, "start": lsd.get("actualStartTime")}

                    if status == "live" and ccv is not None:
                        live_logs_to_insert.append({
                            "channel_id": snippet.get("channelId"), "video_id": vid,
                            "ccv": ccv, "captured_at": now_utc.isoformat()
                        })

                    v_type = "Live" if "liveStreamingDetails" in v_item else "Shorts" if parse_duration_to_seconds(v_item.get("contentDetails", {}).get("duration", "")) <= 61 else "Video"
                    video_details_list.append({
                        "video_id": vid, "channel_id": snippet.get("channelId"), "title": snippet.get("title"),
                        "video_type": v_type, "published_at": snippet.get("publishedAt"),
                        "view_count": int(stats.get("viewCount", 0)) if "viewCount" in stats else None,
                        "like_count": int(stats.get("likeCount", 0)) if "likeCount" in stats else None,
                        "comment_count": int(stats.get("commentCount", 0)) if "commentCount" in stats else None,
                        "last_updated_at": now_utc.isoformat()
                    })
            except Exception as e: print(f"   ❌ 解析失敗: {e}")

    # --- 步驟 4: 存檔 ---
    print(f"💾 步驟 4: 執行資料庫存檔...")
    status_priority = {"live": 3, "upcoming": 2, "none": 1}
    for cid, data in channel_map.items():
        best_vid, current_max_prio = None, -1
        for vid in cid_to_video_ids.get(cid, []):
            prio = status_priority.get(live_info_map.get(vid, {}).get("status"), 0)
            if prio > current_max_prio:
                current_max_prio = prio
                best_vid = vid
            if current_max_prio == 3: break
        
        final_info = live_info_map.get(best_vid, {})
        best_status = final_info.get("status", "none")
        ccv_val = final_info.get("ccv")
        
        print(f"   📝 {data['title']} | 判定: {best_status}" + (f" (同接: {ccv_val})" if best_status == "live" and ccv_val is not None else ""))
        
        try: supabase.table("yt_channels").upsert({"channel_id": cid, "title": data["title"], "custom_url": data["custom_url"]}).execute()
        except Exception: pass

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
            except Exception: pass

    if video_details_list:
        try: supabase.table("yt_videos").upsert(video_details_list).execute()
        except Exception: pass

    if live_logs_to_insert:
        try: supabase.table("yt_live_logs").insert(live_logs_to_insert).execute()
        except Exception: pass

    # ==============================================================================
    # 🌟 狀態機釋放：將 RUNNING 轉換為 COMPLETED
    # ==============================================================================
    if current_log_id:
        try:
            supabase.table("github_actions_logs").update({"status": "COMPLETED"}).eq("log_id", current_log_id).execute()
            print("🔓 任務完成，狀態機解鎖 (標記為 COMPLETED)。")
        except Exception as e:
            print(f"⚠️ 系統解鎖更新失敗 ({e})，n8n Watchdog 可能會觸發 Deadlock 警報。")
    
    print(f"\n📊 --- 任務總結報告 ({VERSION}) ---")
    print(f"💰 本次消耗估計 Quota: {quota_used} | 每日佔比估計: {(quota_used / 10000) * 100:.2f}%")
    print(f"------------------------\n")

if __name__ == "__main__":
    fetch_and_save()
