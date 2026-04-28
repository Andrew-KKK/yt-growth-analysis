# yt-growth-analysis
**YouTube 頻道經營與成長影響力分析系統**

這是一套具備「高可用性 (High Availability)」、「智慧分流」等能力的 YouTube 數據自動化採集管線 (Data Pipeline)。專為追蹤 VTuber 與創作者的成長趨勢及即時直播熱度所設計。

🤖 **AI 協作聲明：此專題主要是由本人發想核心概念與研究目標，並由 AI 輔助建構系統架構與實作程式碼。**

## **💡 專案背景與目標**

在 YouTube API 的每日免費配額限制下（10,000 Units），傳統的定時抓取腳本難以同時滿足「大樣本數」與「高解析度（即時同接人數）」的需求。

本專案透過重新設計**資料庫架構**與**雙系統排程邏輯**，打造出一個在全免費資源下，**足以支持每 30 分鐘記錄約 200 個頻道狀態，並能持續運行3個月以上**的數據採集管線。

本專題的核心目的，是**嘗試從這些高頻率的追蹤記錄裡，分析出影響頻道成長的關鍵因素**，並為後續的「頻道經營分群 (Clustering)」與「特徵工程」打下穩固的資料基石。

## **🚀 核心工程亮點 (Key Features)**

### **1. 高低頻雙軌分流 (Dual-Track Polling)**

- **高頻同接層 (Heartbeat, 每 30 分鐘)**：專注捕捉直播瞬間的同接人數 (CCV)，繪製直播聚人曲線。
- **低頻快照層 (Snapshot, 每 3 小時)**：記錄訂閱數與總觀看數成長，並更新頻道最新的影片數據庫（觀看數、按讚數、留言數）。

### **2. 自建 Linux 伺服器與零信任全域鎖 (Self-Hosted Server & Zero-Trust Lock)**

- **穩定核心：自建 Linux 伺服器**：為徹底解決公有雲 (如 GitHub Actions) 常態性 1~30 分鐘以上不等的排程延遲 (Cron Jitter)，本專案特別部署了一台自建 Linux 伺服器來穩定運行 n8n，作為系統的核心監控塔台，確保極致的排程精準度。
- **零信任內網穿透 (Zero-Trust Tunneling)**：為了保護自建伺服器不暴露於危險的公網，導入了 Tailscale VPN。雲端的 GitHub Actions 虛擬機在執行期間，會透過 Ephemeral Auth Key 短暫加入私有內網，安全呼叫本地端 n8n 後無痕註銷，達成極致的資安防護。
- **分散式鎖定與全域冷卻 (Distributed Lock & Cooldown)**：建立在地端與雲端雙系統協作的基礎上，為防範排程交錯引發的「競爭危害」，於 Supabase 實作了帶有 `NOT NULL` 約束的嚴謹狀態機。距離上次執行小於 25 分鐘的任務將被自動煞車，完美避免 API 配額浪費；並保留透過 UI 手動傳遞 `SKIP_COOLDOWN=true` 以繞過限制的維運彈性。

### **3. 雙活看門狗異常監控 (Active-Passive Watchdog)**

透過 n8n 構建兩道防護網，避免腳本崩潰造成鎖的遺失：

- **路徑 A (精準事件驅動)**：Python 啟動上鎖時，透過 Webhook 通知 n8n 啟動 15 分鐘精準倒數，超時未解鎖即發送 Email 警報。
- **路徑 B (全局定時掃描)**：n8n 每 15 分鐘主動巡邏 Supabase，精準捕捉「猝死 (未成功發送 Webhook 即崩潰)」的殭屍鎖定紀錄。
- **自動修復**：下次排程啟動時，Python 會自動識別超過 15 分鐘的死鎖，並強制接管系統進行自我修復。

### **4. 數據不可變性設計 (Immutable Logs for OLAP)**

- 避免傳統的 Foreign Key 連鎖刪除，yt_live_logs 與 yt_stats_daily 為不可變的歷史事實，即使未來母表中的頻道或影片被清理，歷史同接與成長曲線依然保留。

### **5. 數據不可變性設計 (Immutable Logs for OLAP)**

- 避免傳統的 Foreign Key 連鎖刪除，`yt_live_logs` 與 `yt_stats_daily` 為不可變的歷史事實，即使未來母表中的頻道或影片被清理，歷史同接與成長曲線依然保留。
## **🗄️ 系統架構與資料庫設計 (Schema)**

系統採用 PostgreSQL (Supabase) 進行分層儲存：

| **資料表 (Table)** | **說明與職責** |
| --- | --- |
| yt_channels | 頻道母表（ID、標題、自定義網址）。 |
| yt_videos | 影片目錄，透過 Batch Upsert 維護近期最新影片的動態迴響數據。 |
| yt_stats_daily | 成長快照日誌（純字串結構，免疫外鍵衝突），記錄訂閱與觀看成長。 |
| yt_live_logs | 同接日誌（純字串結構），記錄 video_id 與 ccv，支撐熱度分析。 |
| tags / channel_tags | 標籤系統。預留給未來執行 K-Means 或特徵分群 (Clustering) 使用。 |
| github_actions_logs | 系統心跳表。記錄每次執行的觸發來源 (n8n/GitHub) 與時間，用於全域冷卻控制。 |

## **🛠️ 技術棧 (Tech Stack)**

- **語言**: Python 3.11
- **核心套件**: google-api-python-client (YouTube Data API v3), supabase
- **自動化與 CI/CD**: GitHub Actions, n8n (Webhook)
- **資料庫**: Supabase (PostgreSQL)

## **📈 未來展望 (Roadmap)**

- [ ]  **SQL Views 建置**：編寫虛擬檢視表，自動計算「直播頭 1 小時聚人斜率」與「互動轉化率」。
- [ ]  **自動化分群 (Clustering)**：導入機器學習演算法，自動化為頻道打上 tags。
- [ ]  **Dashboard 視覺化**：使用網頁設計或其他工具、方式進行數據展現。

*Developed with ☕ and Data Engineering mindset by* [Andrew-KKK](https://github.com/Andrew-KKK)*.*
