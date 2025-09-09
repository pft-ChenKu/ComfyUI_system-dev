# import asyncio
# import threading
# import time
# import logging
# import execution
# import server
# import os
# import psutil
# import pynvml
# from typing import Dict, Optional
# from datetime import timedelta ,datetime
# import pandas as pd
# '''
# CSV_PATH
# '''
# CSV_DIR= "/workspace/tmp/csv"
# os.makedirs(CSV_DIR, exist_ok=True)
# SAVE_TO_CSV=True
# DEBUG_MODE=True # true 會印詳細錯誤訊息
# QUEUE_REMAINING=1 # 0 表示全部結束才算 workflow 結束
# class ExecutionTime:
#     CATEGORY = "PF/Debug"

#     @classmethod
#     def INPUT_TYPES(s):
#         return {"required": {}}

#     RETURN_TYPES = ()
#     RETURN_NAMES = ()
#     FUNCTION = "process"

#     def process(self):
#         return ()


# # 每個 prompt_id 的開始時間（與 comfy 的 prompt_worker 相同起點）
# _PROMPT_T0 = {}



# # ========== Hook 1: 記錄開始時間（execute 入口） ==========
# _orig_execute = execution.PromptExecutor.execute

# def _execute_patch(self, prompt, prompt_id, extra_data, execute_outputs):
#     global _PROMPT_T0
#     _PROMPT_T0[prompt_id] = time.perf_counter()
#     return _orig_execute(self, prompt, prompt_id, extra_data, execute_outputs)

# # 安裝 patch（確保只裝一次）
# if getattr(execution.PromptExecutor, "_chen_wc_patched", False) is False:
#     execution.PromptExecutor.execute = _execute_patch
#     execution.PromptExecutor._chen_wc_patched = True

# # ========== Hook 2: 偵測結束（send_sync: executing + node=None） ==========
# _orig_send_sync = server.PromptServer.send_sync

# def _send_sync_patch(self, event, data, sid=None):
#     global PROMPT_METRICS ,_PROMPT_T0 ,_WORKFLOW_T0 ,QUEUE_REMAINING
#     logging.info(rf"EVENT:{event,data}")
#     try:
#         exec_info = (data.get("status") or {}).get("exec_info")
#         # 判斷 workflow 全部跑完
#         if (
#             event == "status"
#             and isinstance(data, dict)
#             and isinstance(exec_info, dict)
#             and "queue_remaining" in exec_info  # 只確認 key 存在
#             # and exec_info["queue_remaining"] == 0  # 若也要確認已清空，打開這行
#         ):
#             remain_queue = exec_info["queue_remaining"]
#             if QUEUE_REMAINING - remain_queue != 1:
#                 QUEUE_REMAINING = remain_queue
#                 return _orig_send_sync(self, event, data, sid)
#             QUEUE_REMAINING = remain_queue
            

#             if _WORKFLOW_T0 is None:
#                 return _orig_send_sync(self, event, data, sid)
            
#             ##自行計算整體時間##
#             total=0
#             total2=0
#             if _WORKFLOW_T0 is not None:
#                 total = time.perf_counter() - _WORKFLOW_T0
#                 _WORKFLOW_T0 = None
#             ## 與 comfy 對齊的時間（從 prompt_worker() 開始算起）
#             pid  = data.get("prompt_id") or _CURRENT_PROMPT_ID
#             t0 = _PROMPT_T0.pop(pid, None)
#             if t0 is not None:
#                 total2 = time.perf_counter() - t0
            
#             ## Save CSV
#             df = pd.DataFrame(PROMPT_METRICS)
#             max_vram = df["vram_gb_peak"].max()
#             max_ram = df["ram_gb_peak"].max()
#             ## Add summary 
#             summary = {
#                 "node_id": 0,
#                 "class_type": "SUMMARY",
#                 "status": "FINISHED",
#                 "time_sec": 0,
#                 "relative_time": rf"total:{total:.2f}, comfy_align:{total2:.2f}",
#                 "vram_gb_peak": round(max_vram,2),
#                 "ram_gb_peak": round(max_ram,2),
#                 "vram_diff": 0,
#                 "ram_diff": 0,
#             }
#             df.loc[len(df)] = summary
#             timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
#             df.to_csv(os.path.join(CSV_DIR, f"{timestamp}_{_CURRENT_PROMPT_ID}.csv"), index=False, encoding="utf-8")
#             PROMPT_METRICS=[]
#     except Exception as e:
#         logging.warning(f"send_sync_patch error: {e}")

#     return _orig_send_sync(self, event, data, sid)


# # 安裝 patch（確保只裝一次）
# if getattr(server.PromptServer, "_chen_wc_patched", False) is False:
#     server.PromptServer.send_sync = _send_sync_patch
#     server.PromptServer._chen_wc_patched = True



# # ---- NVML 初始化 ----
# _NVML_READY = False


# def _nvml_init_once():
#     global _NVML_READY
#     if not _NVML_READY:
#         pynvml.nvmlInit()
#         _NVML_READY = True


# # ---- 幫助函式：取子進程 ----
# def _pid_tree(p: psutil.Process, recursive=True):
#     arr = [p]
#     try:
#         arr += p.children(recursive=recursive)
#     except Exception:
#         pass
#     return arr


# # ---- Getter fallback ----
# def _get_nvml_procs(handle):
#     getters = []
#     for base in ["Compute", "Graphics"]:
#         for suffix in ["_v3", ""]:  # 先試 v3，不行退回 v2
#             name = f"nvmlDeviceGet{base}RunningProcesses{suffix}"
#             if hasattr(pynvml, name):
#                 getters.append(getattr(pynvml, name))
#     return getters


# # ---- VRAM / RAM 量測 ----
# def get_pid_tree_vram_mb(pid: int) -> int:
#     """回傳 pid + 子進程 在所有 GPU 的 VRAM 使用量總和（MiB）。"""
#     _nvml_init_once()
#     try:
#         plist = _pid_tree(psutil.Process(pid))
#         pids = {pp.pid for pp in plist}
#     except Exception:
#         pids = {pid}

#     total = 0
#     count = pynvml.nvmlDeviceGetCount()
#     for i in range(count):
#         h = pynvml.nvmlDeviceGetHandleByIndex(i)
#         for getter in _get_nvml_procs(h):
#             try:
#                 procs = getter(h)
#             except pynvml.NVMLError:
#                 continue
#             for pr in procs:
#                 used = getattr(pr, "usedGpuMemory", None) or getattr(
#                     pr, "gpuInstanceMemoryUsed", None
#                 )
#                 if pr.pid in pids and used and used > 0:
#                     total += int(used // (1024**2))
#     return total


# def get_pid_tree_rss_mb(pid: int) -> float:
#     """回傳 pid + 子進程 RSS（MiB）。"""
#     try:
#         procs = _pid_tree(psutil.Process(pid))
#     except Exception:
#         return 0.0
#     acc = 0
#     for pp in procs:
#         try:
#             acc += pp.memory_info().rss
#         except Exception:
#             pass
#     return acc / (1024**2)


# # ---- 背景輪詢 PeakPoller ----
# class PeakPoller:
#     def __init__(self, pid: int, interval: float = 0.3, include_children: bool = True):
#         self.pid = pid
#         self.interval = interval
#         self.include_children = include_children
#         self._thr: Optional[threading.Thread] = None
#         self._stop = threading.Event()
#         self._peaks: Dict[str, float] = {
#             "vram_mb_peak": 0.0,
#             "ram_mb_peak": 0.0,
#             "samples": 0,
#         }

#     def start(self):
#         if self._thr and self._thr.is_alive():
#             return
#         self._stop.clear()
#         self._thr = threading.Thread(
#             target=self._loop, name=f"PeakPoller-{self.pid}", daemon=True
#         )
#         self._thr.start()
#         self._sample_once()  # 啟動當下先採樣一次

#     def stop(self):
#         if not self._thr:
#             return dict(self._peaks)
#         self._sample_once()  # 停止前補一次
#         self._stop.set()
#         self._thr.join(timeout=2.0)
#         return dict(self._peaks)

#     def _sample_once(self):
#         try:
#             vram_mb = float(get_pid_tree_vram_mb(self.pid))
#             ram_mb = float(get_pid_tree_rss_mb(self.pid))
#             if vram_mb > self._peaks["vram_mb_peak"]:
#                 self._peaks["vram_mb_peak"] = vram_mb
#             if ram_mb > self._peaks["ram_mb_peak"]:
#                 self._peaks["ram_mb_peak"] = ram_mb
#             self._peaks["samples"] += 1
#         except Exception as e:
#             logging.debug(f"sample_once error: {e}")

#     def _loop(self):
#         try:
#             _nvml_init_once()
#         except Exception:
#             pass
#         while not self._stop.is_set():
#             self._sample_once()
#             self._stop.wait(self.interval)


# # ---- swizzle ----
# _CURRENT_PROMPT_ID: Optional[str] = None
# _ACTIVE: Dict[tuple, PeakPoller] = {}
# _LAST_METRICS: Dict[str, float] = {
#     "vram_delta_from_baseline": 0.0,
#     "ram_delta_from_baseline": 0.0,
# }
# _WORKFLOW_T0: Optional[float] = None  # workflow 開始時間
# _BASELINE = {"vram": 0, "ram": 0}
# PROMPT_METRICS = []

# def _key(prompt_id, node_id):
#     return (prompt_id, node_id)


# origin_execute = execution.execute


# async def swizzle_execute(
#     server,
#     dynprompt,
#     caches,
#     current_item,
#     extra_data,
#     executed,
#     prompt_id,
#     execution_list,
#     pending_subgraph_results,
#     pending_async_nodes,
# ):
#     global _WORKFLOW_T0, _BASELINE, _CURRENT_PROMPT_ID, _ACTIVE, _LAST_METRICS,PROMPT_METRICS
#     node_id = current_item
#     class_type = dynprompt.get_node(current_item)["class_type"]
#     if _CURRENT_PROMPT_ID is None or _CURRENT_PROMPT_ID != prompt_id:
#         if DEBUG_MODE:
#             logging.info(f"Start new workflow: [{prompt_id}] ")
#         PROMPT_METRICS=[]
#         _CURRENT_PROMPT_ID = prompt_id
#         _WORKFLOW_T0 = time.perf_counter()
#         _BASELINE = {
#             "vram": get_pid_tree_vram_mb(os.getpid()),
#             "ram": get_pid_tree_rss_mb(os.getpid()),
#         }
#         _LAST_METRICS = {
#             "vram_delta_from_baseline": 0.0,
#             "ram_delta_from_baseline": 0.0,
#         }
#         # 停掉所有舊的 poller
#         for k, (p, _) in _ACTIVE.items():
#             p.stop()
#         _ACTIVE = {}
        
#         ## initial row
#         row = {
#             "node_id": 0,
#             "class_type": "START_WORKFLOW",
#             "status": "",
#             "time_sec": 0,
#             "relative_time":  0,
#             "vram_gb_peak": (_BASELINE["vram"] / 1024),
#             "ram_gb_peak":  (_BASELINE["ram"] / 1024),
#             "vram_diff": 0,
#             "ram_diff": 0
#         }
#         PROMPT_METRICS.append(row)
        
#     k = _key(prompt_id, node_id)

#     if k not in _ACTIVE:
#         poller = PeakPoller(pid=os.getpid(), interval=0.3, include_children=True)
#         poller.start()
#         _ACTIVE[k] = (poller, time.perf_counter())

#     try:
#         result = await origin_execute(
#             server,
#             dynprompt,
#             caches,
#             current_item,
#             extra_data,
#             executed,
#             prompt_id,
#             execution_list,
#             pending_subgraph_results,
#             pending_async_nodes,
#         )
#     except asyncio.CancelledError:
#         log_data(k, node_id, class_type, prompt_id, "interrupted")
#         raise
#     except Exception:
#         log_data(k, node_id, class_type, prompt_id, "failure")
#         raise

#     exec_result = result[0]
#     name = getattr(exec_result, "name", None)
#     if name == "PENDING":
#         return result
#     status = "success" if name == "SUCCESS" else "failure"
#     log_data(k, node_id, class_type, prompt_id, status)

#     return result


# def log_data(k, node_id, class_type, prompt_id, status):
#     global _WORKFLOW_T0, _LAST_METRICS
#     p, t0 = _ACTIVE.pop(k, (None, None))
#     if p:
#         t1 = time.perf_counter()
#         data = p.stop()
#         data["time_sec"] = round(t1 - t0, 2)
#         data["relative_time"] = round(t1 - _WORKFLOW_T0, 2)

#         # MB → GB 換算
#         vram_gb = data["vram_mb_peak"] / 1024
#         ram_gb = data["ram_mb_peak"] / 1024
#         data["vram_gb_peak"] = round(vram_gb, 2)
#         data["ram_gb_peak"] = round(ram_gb, 2)

#         # 計算相對 baseline
#         vram_delta_gb = vram_gb - (_BASELINE["vram"] / 1024)
#         ram_delta_gb = ram_gb - (_BASELINE["ram"] / 1024)
#         data["vram_delta_from_baseline"] = round(vram_delta_gb, 2)
#         data["ram_delta_from_baseline"] = round(ram_delta_gb, 2)

#         # 計算相對上一個 node
#         data["vram_diff"] = round(
#             vram_delta_gb - _LAST_METRICS["vram_delta_from_baseline"], 2
#         )
#         data["ram_diff"] = round(
#             ram_delta_gb - _LAST_METRICS["ram_delta_from_baseline"], 2
#         )

#         _LAST_METRICS["vram_delta_from_baseline"] = vram_delta_gb
#         _LAST_METRICS["ram_delta_from_baseline"] = ram_delta_gb 
        
#         # 移除 MB 欄位
#         data.pop("vram_mb_peak", None)
#         data.pop("ram_mb_peak", None)

#         ## set row
#         row = {
#             "node_id": node_id,
#             "class_type": class_type,
#             "status": status,
#             "time_sec": round(data["time_sec"],2),
#             "relative_time":  round(data["relative_time"],2),
#             "vram_gb_peak": round(vram_gb,2),
#             "ram_gb_peak":  round(ram_gb,2),
#             "vram_diff": round(data["vram_diff"],2),
#             "ram_diff":  round(data["ram_diff"],2)
#         }
#         data['prompt_id']=prompt_id
#         PROMPT_METRICS.append(row)
#         if DEBUG_MODE:
#             logging.info(f"[node {node_id} {class_type}] {status}: {data}")
#         # if getattr(server, "client_id", None):
#         #     server.send_sync(
#         #         "node_metrics",
#         #         {"node": node_id, "prompt_id": prompt_id, "status": status, **data},
#         #         server.client_id,
#         #     )


# execution.execute = swizzle_execute
