import os
import sys
import re
from datetime import datetime, timezone, timedelta
from googleapiclient.discovery import build
from supabase import create_client, Client

# 強制輸出立即顯示
sys.stdout.reconfigure(line_buffering=True)

YT_API_KEY = os.environ.get("YT_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# 版本號 V17：新增影片的觀看、按讚、留言數抓取
VERSION = "2026.03.13.V17" 

# 2. 定義要監控的頻道 ID (老闆請在這裡換成你想追蹤的頻道)
# 你可以在 YouTube 頻道網址找到這些 ID (例如 UC... 開頭的字串)
CHANNEL_IDS = [
# VSPO!
    "UCyLGcqYs7RsBb3L0SJfzGYA", #花芽すみれ
    "UCiMG6VdScBabPhJ1ZtaVmbw", #花芽なずな
    "UCgTzsBI0DIRopMylJEDqnog", # 小雀とと / Toto Kogara
    "UC5LyYg6cCA4yHEYvtUsir3g", # 一ノ瀬うるは
    "UCIcAj6WkJ8vZ7DeJVgmeqKw", # 胡桃のあ
    "UCnvVG9RbOW3J6Ifqo-zKLiw", # 兎咲ミミ / Tosaki Mimi
    "UCF_U2GCKHvDz52jWdizppIA", # 空澄セナ -Asumi Sena-
    "UCvUc0m317LWTTPZoBQV479A", # 橘ひなの / Hinano Tachibana
    "UCurEA8YoqFwimJcAuSHU0MQ", # 英リサ.Hanabusa Lisa
    "UCGWa1dMU_sDCaRQjdabsVgg", # 如月れん -Ren kisaragi-
    "UCMp55EbT_ZlqiMS3lCj01BQ", # 神成きゅぴ / Kaminari Qpi
    "UCjXBuHmWkieBApgBhDuJMMQ", # 八雲べに
    "UCPkKpOHxEDcwmUAnRpIu-Ng", # 藍沢エマ / Aizawa Ema
    "UCD5W21JqNMv_tV9nfjvF9sw", # 紫宮るな /shinomiya runa
    "UCIjdfjcSaEgdjwbgjxC3ZWg", # 猫汰つな / Nekota Tsuna
    "UC61OwuYOVuKkpKnid-43Twg", # 白波らむね / Shiranami Ramune
    "UCzUNASdzI4PV5SlqtYwAkKQ", # Met Channel / 小森めと 
    "UCS5l_Y0oMVTjEos2LuyeSZQ", # Akari ch.夢野あかり
    "UCX4WL24YEOUYd7qDsFSLDOw", # 夜乃くろむ / Yano Kuromu
    "UC-WX1CXssCtCtc2TNIRnJzg", # 紡木こかげ
    "UCuDY3ibSP2MFRgf7eo3cojg", # 千燈ゆうひ
    "UCL9hJsdk9eQa0IlWbFB2oRg", # 蝶屋はなび / Choya Hanabi
    "UC8vKBjGY2HVfbW9GAmgikWw", # 甘結もか / Amayui Moka
    "UC2xXx1m1jeL0W84_0jTg-Yw", # 銀城サイネ / Ginjo Saine
    "UCoW8qQy80mKH0RJTKAK-nNA", # 龍巻ちせ / Tatsumaki Chise
    "UCCra1t-eIlO3ULyXQQMD9Xw", # 【VACATION】 Remia Aotsuki 【VSPO! EN】
    "UCLlJpxXt6L5d-XQ0cDdIyDQ", # Arya Kuroha 【VSPO! EN】
    "UCeCWj-SiJG9SWN6wGORiLmw", # Jira Jisaki 【VSPO! EN】
    "UCKSpM183c85d5V2cW5qaUjA", # Narin Mikure【VSPO! EN】
    "UC7Xglp1fske9zmRe7Oj8YyA", # Riko Solari【VSPO! EN】
    "UCp_3ej2br9l9L1DSoHVDZGw", # Eris Suzukami 【VSPO! EN】
]

WAITING_ROOM_THRESHOLD_DAYS = 30

def get_yt_client():
    return build("youtube", "v3", developerKey=YT_API_KEY)

def get_supabase_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def parse_duration_to_seconds(duration_str):
    if not duration_str:
        return 0
    match = re.match(r'^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$', duration_str)
    if not match:
        return 0
    h, m, s = match.groups()
    h = int(h) if h else 0
    m = int(m) if m else 0
    s = int(s) if s else 0
    return h * 3600 + m * 60 + s

def fetch_and_save():
    print(f"🚀 [版本 {VERSION}] 啟動採集任務 (含效能優化)...")
    youtube = get_yt_client()
    supabase = get_supabase_client()
    quota_used = 0

    # 1. 頻道統計
    print(f"📡 步驟 1: 抓取 {len(CHANNEL_IDS)} 個頻道的統計數據...")
    channel_map = {}
    for i in range(0, len(CHANNEL_IDS), 50):
        batch = CHANNEL_IDS[i:i+50]
        ch_response = youtube.channels().list(part="snippet,statistics", id=",".join(batch)).execute()
        quota_used += 1
        for item in ch_response.get("items", []):
            channel_map[item["id"]] = {
                "title": item["snippet"].get("title"),
                "custom_url": item["snippet"].get("customUrl"),
                "subscriber_count": int(item["statistics"].get("subscriberCount", 0)),
                "total_views": int(item["statistics"].get("viewCount", 0)),
                "raw_snippet": item["snippet"],
                "raw_stats": item["statistics"]
            }

    # 2. 頻道活動深挖
    print(f"📡 步驟 2: 解析頻道活動...")
    all_video_ids = []
    cid_to_video_ids = {}

    for cid in CHANNEL_IDS:
        try:
            act_response = youtube.activities().list(part="snippet,contentDetails", channelId=cid, maxResults=15).execute()
            quota_used += 1
            vids = []
            for act in act_response.get("items", []):
                t = act["snippet"]["type"]
                vid = None
                if t == "upload":
                    vid = act["contentDetails"]["upload"].get("videoId")
                elif t == "liveBroadcast":
                    vid = act["contentDetails"]["liveBroadcast"].get("id")
                
                if vid and vid not in vids:
                    vids.append(vid)
                    if vid not in all_video_ids: all_video_ids.append(vid)
            cid_to_video_ids[cid] = vids
        except Exception as e:
            print(f"   ⚠️ 頻道 {cid} 活動抓取失敗: {e}")

    # 3. 批量檢查影片狀態 & 收集影片資訊
    live_info_map = {}
    video_details_list = [] 
    
    if all_video_ids:
        print(f"📡 步驟 3: 解析 {len(all_video_ids)} 個影片的狀態、類型與迴響數據...")
        for i in range(0, len(all_video_ids), 50):
            batch_vids = all_video_ids[i:i+50]
            # [修改處] 在 part 中加入 statistics
            vid_response = youtube.videos().list(
                part="snippet,liveStreamingDetails,contentDetails,statistics", 
                id=",".join(batch_vids)
            ).execute()
            quota_used += 1
            
            for v_item in vid_response.get("items", []):
                vid = v_item["id"]
                snippet = v_item.get("snippet", {})
                stats = v_item.get("statistics", {}) # 取得數據區塊
                
                # --- A. 處理直播狀態 ---
                base_status = snippet.get("liveBroadcastContent")
                if base_status == "upcoming":
                    scheduled_str = v_item.get("liveStreamingDetails", {}).get("scheduledStartTime")
                    if scheduled_str:
                        scheduled_time = datetime.fromisoformat(scheduled_str.replace("Z", "+00:00"))
                        if (scheduled_time - datetime.now(timezone.utc)) > timedelta(days=WAITING_ROOM_THRESHOLD_DAYS):
                            base_status = "none"
                live_info_map[vid] = base_status

                # --- B. 處理 yt_videos 欄位判定 ---
                v_title = snippet.get("title")
                v_published_at = snippet.get("publishedAt")
                v_channel_id = snippet.get("channelId")
                
                v_type = "Video"
                if "liveStreamingDetails" in v_item:
                    v_type = "Live"
                else:
                    duration_str = v_item.get("contentDetails", {}).get("duration", "")
                    sec = parse_duration_to_seconds(duration_str)
                    if sec <= 61 and sec > 0: 
                        v_type = "Shorts"
                
                # [修改處] 安全地取得迴響數據 (如果被隱藏或尚未產生，預設為 0)
                v_views = int(stats.get("viewCount", 0))
                v_likes = int(stats.get("likeCount", 0))
                v_comments = int(stats.get("commentCount", 0))
                
                video_details_list.append({
                    "video_id": vid,
                    "channel_id": v_channel_id,
                    "title": v_title,
                    "video_type": v_type,
                    "published_at": v_published_at,
                    "view_count": v_views,
                    "like_count": v_likes,
                    "comment_count": v_comments
                })

    # 4. 寫入 Supabase (使用 Batch 批次處理)
    print(f"💾 步驟 4: 狀態判定與資料庫存檔...")
    status_priority = {"live": 3, "upcoming": 2, "none": 1}

    # 4-1. 寫入快照與母表 (此處維持迴圈，因為涉及邏輯運算)
    for cid, data in channel_map.items():
        best_status = None
        current_max_prio = -1
        for vid in cid_to_video_ids.get(cid, []):
            s = live_info_map.get(vid)
            prio = status_priority.get(s, 0)
            if prio > current_max_prio:
                current_max_prio = prio
                best_status = s
            if current_max_prio == 3: break
        
        is_live = (best_status == "live") if best_status else None
        
        try:
            supabase.table("yt_channels").upsert({"channel_id": cid, "title": data["title"], "custom_url": data["custom_url"]}).execute()
            supabase.table("yt_stats_daily").insert({
                "channel_id": cid, "subscriber_count": data["subscriber_count"], "total_views": data["total_views"],
                "is_live": is_live, "live_status": best_status, "check_time": datetime.now(timezone.utc).isoformat(),
                "raw_json": {"snippet": data["raw_snippet"], "statistics": data["raw_stats"]}
            }).execute()
        except Exception as e:
            print(f"      ❌ {data['title']} 頻道資料寫入失敗: {e}")

    # 4-2. 批次寫入影片清單 (將 100 多次請求縮減為 1 次)
    print(f"🎬 正在準備批次同步 {len(video_details_list)} 筆影片資料...")
    if video_details_list:
        try:
            # 直接將整個 List 丟給 Supabase，它會自動發送一個 Bulk Request
            supabase.table("yt_videos").upsert(video_details_list).execute()
            print(f"      ✅ 成功完成單次批次寫入！")
        except Exception as e:
            print(f"      ❌ 批次影片寫入失敗: {e}")

    print(f"\n📊 --- 任務總結報告 ---")
    print(f"💰 本次抓取預估花費 Quota: {quota_used} 點")
    print(f"📈 每日額度消耗佔比: {(quota_used / 10000) * 100:.2f}%")
    
    utc_now = datetime.now(timezone.utc)
    tw_now = utc_now.astimezone(timezone(timedelta(hours=8)))
    print(f"🕒 任務結束時間 (UTC): {utc_now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🇹🇼 任務結束時間 (台灣): {tw_now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"------------------------\n")

if __name__ == "__main__":
    fetch_and_save()
