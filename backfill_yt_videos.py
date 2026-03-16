# 歷史資料回填腳本
import os
import sys
from googleapiclient.discovery import build
from supabase import create_client, Client

# 強制輸出立即顯示
sys.stdout.reconfigure(line_buffering=True)

YT_API_KEY = os.environ.get("YT_API_KEY")
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

def get_yt_client():
    return build("youtube", "v3", developerKey=YT_API_KEY)

def get_supabase_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def run_backfill():
    print("🚀 啟動歷史影片數據回填任務 (Data Backfill)...")
    youtube = get_yt_client()
    supabase = get_supabase_client()

    # 1. 從資料庫撈出所有影片 ID
    print("📡 步驟 1: 從資料庫尋找所有已記錄的影片...")
    try:
        res = supabase.table("yt_videos").select("video_id").execute()
        all_videos = res.data
    except Exception as e:
        print(f"❌ 無法連線至 Supabase: {e}")
        return

    if not all_videos:
        print("   ℹ️ 資料庫目前沒有影片。")
        return

    video_ids = [v["video_id"] for v in all_videos]
    print(f"   🔍 共找到 {len(video_ids)} 支影片，準備進行狀態同步。")

    # 2. 批次向 YouTube API 請求最新數據
    print("📡 步驟 2: 向 YouTube API 請求最新數據並更新...")
    update_count = 0
    quota_used = 0
    
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        try:
            vid_response = youtube.videos().list(
                part="statistics",
                id=",".join(batch)
            ).execute()
            quota_used += 1
            
            # 3. 逐筆更新回 Supabase
            for v_item in vid_response.get("items", []):
                vid = v_item["id"]
                stats = v_item.get("statistics", {})
                
                # 套用 V18 的精確缺失值邏輯
                v_views = int(stats["viewCount"]) if "viewCount" in stats else None
                v_likes = int(stats["likeCount"]) if "likeCount" in stats else None
                v_comments = int(stats["commentCount"]) if "commentCount" in stats else None
                
                # 使用 update 只更新特定欄位，絕不影響原本的標題或發布時間
                supabase.table("yt_videos").update({
                    "view_count": v_views,
                    "like_count": v_likes,
                    "comment_count": v_comments
                }).eq("video_id", vid).execute()
                
                update_count += 1
                
        except Exception as e:
            print(f"   ❌ 批次更新失敗: {e}")

    print(f"\n✅ 回填任務完成！成功修復/更新了 {update_count} 支影片的數據。")
    print(f"💰 本次修復花費 Quota: {quota_used} 點")

if __name__ == "__main__":
    if not all([YT_API_KEY, SUPABASE_URL, SUPABASE_KEY]):
        print("❌ 錯誤：環境變數缺失。")
    else:
        run_backfill()
