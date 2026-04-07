# 00992A ETF Tracker

目前自動化拆成兩段：

1. `Daily Fetch Holdings`
   - 每個交易日傍晚抓取群益投信最新公布的 00992A 持股投資組合
   - 保存原始 XLSX 與乾淨版 `data/YYYY-MM-DD.csv`

2. `Finalize Prices And Cost Basis`
   - 交易日收盤後透過 Shioaji API 補齊 `prices/YYYY-MM-DD.csv`
   - 重建 `reports/change_table_*.csv`
   - 更新 `data/cost_basis.csv` 與 `data/realized_gains_log.csv`

另提供兩個手動修復 workflow：

- `Backfill Holdings And Prices`
  - 回補群益投信歷史投組、Shioaji 價格、報表與成本
- `Repair Prices And Reports`
  - 只重補價格、報表與成本

## 必要 Secrets

- `GH_PAT`
- `SHIOAJI_API_KEY`
- `SHIOAJI_SECRET_KEY`

## 可選通知 Secrets

- `EMAIL_USERNAME`
- `EMAIL_PASSWORD`
- `EMAIL_TO`
- `SENDGRID_API_KEY`

若未設定 email 類 secrets，workflow 失敗時會退回建立或更新 GitHub issue 作為通知。
