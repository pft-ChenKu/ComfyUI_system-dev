# ComfyUI System Dev

ComfyUI 的系統監控與效能分析工具,用於測量 workflow 執行時的 VRAM、RAM 使用量與執行時間。

## 功能特色

- 即時監控每個 node 的執行時間
- 追蹤 VRAM 和 RAM 的峰值使用量
- 自動產生 CSV 報表
- 支援背景輪詢監控（PeakPoller）
- 可選擇性強制清理 GPU cache

## 安裝

### 依賴套件

```bash
pip install -r requirements.txt
```

需要的套件：
- pandas >= 2.0
- psutil >= 5.9
- nvidia-ml-py3 >= 7.352.0

### 安裝到 ComfyUI

將此資料夾複製到 ComfyUI 的 `custom_nodes` 目錄下：

```bash
cd /path/to/ComfyUI/custom_nodes
git clone <repository_url> ComfyUI_system-dev
```

## 使用方式

### Node: Execution Time

在 ComfyUI 的 workflow 中加入 **Execution Time** node (分類: `system/debug`)

#### 參數說明

| 參數 | 類型 | 預設值 | 說明 |
|------|------|--------|------|
| `filedir` | STRING | `/workspace/tmp/csv` | CSV 檔案儲存目錄 |
| `filename` | STRING | 空字串 | CSV 檔案名稱（留空則使用時間戳記） |
| `forced_empty_cache` | BOOLEAN | `False` | 強制在每個 node 後清理 VRAM<br>⚠️ **注意**: 開啟會影響執行速度，若要測量時間請關閉此選項 |
| `console_log` | BOOLEAN | `False` | 將各個 node 的數據輸出至 log |
| `enable_node` | BOOLEAN | `True` | 啟用/停用此 node 的功能 |
| `input` (可選) | ANY | - | 連接至此 node 後開始計算 |

#### 輸出格式

執行完成後會產生 CSV 檔案，包含以下欄位：

| 欄位 | 說明 |
|------|------|
| `node_id` | Node ID |
| `class_type` | Node 的類別名稱 |
| `status` | 執行狀態 (`success`/`failure`/`interrupted`) |
| `node_time` | 單一 node 執行時間 (秒) |
| `relative_time` | 相對於 workflow 開始的時間 (秒) |
| `node_start_vram` | Node 開始時的 VRAM (GB) |
| `vram_peak` | Node 執行期間的 VRAM 峰值 (GB) |
| `node_end_vram` | Node 結束時的 VRAM (GB) |
| `clean_cache` | 清理的 cache 大小 (GB) |
| `ram_peak` | Node 執行期間的 RAM 峰值 (GB) |

最後一行會有 `SUMMARY` 摘要資訊。

## 技術細節

### 核心元件

#### `utils.py`

- **`PeakPoller`**: 背景執行緒輪詢器,定期採樣 VRAM/RAM 使用量
- **`get_pid_tree_vram_mb()`**: 取得程序樹的 VRAM 使用量 (MB)
- **`get_pid_tree_rss_mb()`**: 取得程序樹的 RAM 使用量 (MB)

#### `nodes/execution_time.py`

- Hook ComfyUI 的 `execution.execute` 函式
- 監控每個 node 的執行狀態
- 收集效能指標並產生報表

### 監控機制

系統透過以下方式監控資源：

1. **NVML (NVIDIA Management Library)**: 監控 GPU VRAM
2. **psutil**: 監控 RAM 和程序資訊
3. **背景輪詢**: 每 0.3 秒採樣一次,捕捉峰值

### Workflow 生命週期

```
START_WORKFLOW → Node 1 → Node 2 → ... → Node N → SUMMARY
```

每個階段都會記錄：
- 執行時間
- VRAM/RAM 使用情況
- 成功/失敗狀態

## 範例輸出

```csv
node_id,class_type,status,node_time,relative_time,node_start_vram,vram_peak,node_end_vram,clean_cache,ram_peak
0,START_WORKFLOW,,0,0,2.34,2.34,2.34,0,4.56
1,LoadImage,success,0.15,0.15,2.34,2.45,2.45,0,4.78
2,NodesFaceDetector,success,1.23,1.38,2.45,3.12,3.12,0,5.23
0,SUMMARY,FINISHED,0,total:1.38,3.12,3.12,2.89,0.23,5.23
```

## 注意事項

**重要提醒**:

1. `forced_empty_cache=True` 會在每個 node 後清理 GPU cache,這會：
   - 提供準確的 VRAM 測量
   - 顯著降低執行速度
   - 時間測量會不準確

2. 建議使用方式：
   - **測量時間**: `forced_empty_cache=False`
   - **測量 VRAM**: `forced_empty_cache=True`

3. CSV 檔案會自動加上時間戳記避免覆蓋

## 授權

請參考專案的 LICENSE 檔案。

