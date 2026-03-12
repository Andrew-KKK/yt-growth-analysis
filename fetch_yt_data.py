import os
import sys
from datetime import datetime, timezone, timedelta
from googleapiclient.discovery import build
from supabase import create_client, Client

# 強制輸出立即顯示
sys.stdout.reconfigure(line_buffering=True)

YT_API_KEY = os.environ.get("YT_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# 版本號 V12：加入直播時間過濾，過濾掉「萬年聊天待機室」
VERSION = "2026.03.11.V12" 

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

# --- 設定：多久以後的直播才算「萬年待機室」 (預設 7 天) ---
WAITING_ROOM_THRESHOLD_DAYS = 30

def get_yt_client():
    return build("youtube", "v3", developerKey=YT_API_KEY)

def get_supabase_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_and_save():
    print(f"🚀 [版本 {VERSION}] 啟動採集任務...")
    youtube = get_yt_client()
    supabase = get_supabase_client()
    
    quota_used = 0

    # 1. 頻道統計
    print(f"📡 步驟 1: 抓取統計數據...")
    ch_response = youtube.channels().list(part="snippet,statistics", id=",".join(CHANNEL_IDS)).execute()
    quota_used += 1
    
    channel_map = {item["id"]: {
        "title": item["snippet"].get("title"),
        "custom_url": item["snippet"].get("customUrl"),
        "subscriber_count": int(item["statistics"].get("subscriberCount", 0)),
        "total_views": int(item["statistics"].get("viewCount", 0)),
        "raw_snippet": item["snippet"],
        "raw_stats": item["statistics"]
    } for item in ch_response.get("items", [])}

    # 2. 頻道活動深挖
    print(f"📡 步驟 2: 彈性深挖活動...")
    all_video_ids = []
    cid_to_video_ids = {}

    for cid in CHANNEL_IDS:
        try:
            act_response = youtube.activities().list(part="snippet,contentDetails", channelId=cid, maxResults=10).execute()
            quota_used += 1
            vids = [act["contentDetails"]["upload"].get("videoId") if act["snippet"]["type"] == "upload" 
                    else act["contentDetails"]["broadcast"].get("id") 
                    for act in act_response.get("items", []) if act["snippet"]["type"] in ["upload", "broadcast"]]
            vids = [v for v in vids if v]
            cid_to_video_ids[cid] = vids
            for v in vids:
                if v not in all_video_ids: all_video_ids.append(v)
        except: pass

    # 3. 批量檢查影片狀態 (關鍵：加入 liveStreamingDetails)
    live_info_map = {}
    if all_video_ids:
        print(f"📡 步驟 3: 批量解析影片狀態與預計開播時間...")
        vid_response = youtube.videos().list(
            part="snippet,liveStreamingDetails", # 多抓取 streaming 詳情
            id=",".join(all_video_ids[:50])
        )
        vid_response = vid_response.execute()
        quota_used += 1
        
        for v_item in vid_response.get("items", []):
            vid = v_item["id"]
            base_status = v_item["snippet"].get("liveBroadcastContent")
            
            # --- 萬年待機室檢查邏輯 ---
            if base_status == "upcoming":
                scheduled_str = v_item.get("liveStreamingDetails", {}).get("scheduledStartTime")
                if scheduled_str:
                    scheduled_time = datetime.fromisoformat(scheduled_str.replace("Z", "+00:00"))
                    now = datetime.now(timezone.utc)
                    diff = scheduled_time - now
                    
                    # 如果預計開播時間大於設定的門檻 (例如 7 天)
                    if diff > timedelta(days=WAITING_ROOM_THRESHOLD_DAYS):
                        print(f"   💡 發現長效待機室 ({vid})，開播時間在 {diff.days} 天後，標記為 none")
                        base_status = "none" # 降級處理
            
            live_info_map[vid] = base_status

    # 4. 判定與寫入
    print(f"💾 步驟 4: 狀態判定與資料庫存檔...")
    status_priority = {"live": 3, "upcoming": 2, "none": 1}

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
        print(f"   📝 {data['title']} | 判定結果: {best_status if best_status else 'NULL (未知)'}")

        try:
            supabase.table("yt_channels").upsert({"channel_id": cid, "title": data["title"], "custom_url": data["custom_url"]}).execute()
            supabase.table("yt_stats_daily").insert({
                "channel_id": cid, "subscriber_count": data["subscriber_count"], "total_views": data["total_views"],
                "is_live": is_live, "live_status": best_status, "check_time": datetime.now(timezone.utc).isoformat(),
                "raw_json": {"snippet": data["raw_snippet"], "statistics": data["raw_stats"]}
            }).execute()
        except Exception as e:
            print(f"      ❌ 寫入失敗: {e}")

    print(f"\n📊 --- 任務總結報告 ---")
    print(f"💰 本次抓取預估花費 Quota: {quota_used} 點")
    print(f"📈 每日額度消耗佔比: {(quota_used / 10000) * 100:.2f}%")
    print(f"------------------------\n")

if __name__ == "__main__":
    fetch_and_save()
