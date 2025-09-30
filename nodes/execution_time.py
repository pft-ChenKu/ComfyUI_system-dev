import asyncio
import gc
import threading
import time
import logging
import execution
import server
import os
import torch
from typing import Dict, Optional
from datetime import timedelta, datetime
import pandas as pd
from ..utils import PeakPoller, get_pid_tree_rss_mb, get_pid_tree_vram_mb
from collections import defaultdict
from dataclasses import dataclass, field

@dataclass
class PromptState:
    t0: Optional[float] = None 
    workflow_t0: Optional[float] = None
    baseline_vram_mb: float = 0.0
    baseline_ram_mb: float = 0.0
    last_end_vram_mb: float = 0.0
    last_end_ram_mb: float = 0.0
    active: dict = field(default_factory=dict)          # (node_id) -> (poller, t0)
    metrics: list = field(default_factory=list)         # rows

# 取代原本的一堆 global
_PROMPTS: Dict[str, PromptState] = {}

SAVE_TO_CSV = True
CONSOLE_LOG = True  # true 會印詳細錯誤訊息
FILEPATH = "/workspace/tmp/csv/0.csv"
FORCED_EMPTY_CACHE = False
# 每個 prompt_id 的開始時間（與 comfy 的 prompt_worker 相同起點）
_PROMPT_T0 = {}
# ---- swizzle ----
_CURRENT_PROMPT_ID: Optional[str] = None


ORIGIN_EXEC = execution.execute
# ========== Hook 1: 記錄開始時間（execute 入口） ==========
ORIGIN_PROMPT_EXEC = execution.PromptExecutor.execute
ORIGN_SEND_SYNC = server.PromptServer.send_sync


def _send_sync_patch(self, event, data, sid=None):
    try:
        # 你想看的事件就記 log；不要做收尾判定
        if event in ("progress_state", "executing", "status"):
            logging.debug(f"EVENT: {event} {data}")
    except Exception as e:
        logging.warning(f"send_sync_patch error: {e}")
    return ORIGN_SEND_SYNC(self, event, data, sid)


def _finish_workflow(pid: str):
    """一次性收尾：寫 CSV、加總 summary、清理狀態"""
    global CONSOLE_LOG ,FILEPATH ,SAVE_TO_CSV, _PROMPTS
    st = _PROMPTS.pop(pid, None)
    if not st:
        return
    if CONSOLE_LOG:
        logging.info(
            f"_finish_workflow: pid={pid}, current={_CURRENT_PROMPT_ID}, prompt_t0_keys={list(_PROMPT_T0.keys())}"
        )
    try:
        # workflow 牆鐘時間（你自己的起點）
        total = (time.perf_counter() - st.workflow_t0) if st.workflow_t0 else 0.0
        total2 = 0.0

        # comfy 對齊的起點（execute 進來時記）
        t0 = _PROMPT_T0.pop(pid, None)
        if t0 is not None:
            total2 = time.perf_counter() - t0

        # 存 CSV（含 summary）
        df = pd.DataFrame(st.metrics)
        if not df.empty:
            max_vram = round(float(df["vram_peak"].max()), 2)   # GB
            max_ram  = round(float(df["ram_peak"].max()), 2)    # GB

            end_vram_gb = round(get_pid_tree_vram_mb(os.getpid())/1024, 2)
            if torch.cuda.is_available():
                gc.collect(); torch.cuda.empty_cache()
            end_vram_after_gb = round(get_pid_tree_vram_mb(os.getpid())/1024, 2)

            summary = {
                "node_id": 0, "class_type": "SUMMARY", "status": "FINISHED",
                "node_time": 0,
                "relative_time": f"total:{total:.2f}, comfy_align:{total2:.2f}",
                "node_start_vram": end_vram_gb,
                "vram_peak": max_vram,
                "node_end_vram": end_vram_after_gb,
                "clean_cache": round(max(0.0, end_vram_gb - end_vram_after_gb), 2),
                "ram_peak": max_ram,
            }
            df.loc[len(df)] = summary

            if SAVE_TO_CSV and FILEPATH:
                # 避免阻塞主執行緒：背景寫檔（可改成同步 if 你想簡單）
                threading.Thread(
                    target=lambda: df.to_csv(FILEPATH, index=False, encoding="utf-8"),
                    daemon=True,
                ).start()

        # 清理
        _PROMPTS.clear()

    except Exception as e:
        logging.warning(f"finish_workflow error: {e}")




def _execute_patch(self, prompt, prompt_id, extra_data, execute_outputs):
    global _PROMPT_T0, _CURRENT_PROMPT_ID
    _CURRENT_PROMPT_ID = prompt_id
    _PROMPT_T0[prompt_id] = time.perf_counter()

    try:
        return ORIGIN_PROMPT_EXEC(self, prompt, prompt_id, extra_data, execute_outputs)
    finally:
        # 不論成功/失敗，這次 workflow 已經結束
        _finish_workflow(prompt_id)


async def swizzle_execute(
    server,
    dynprompt,
    caches,
    current_item,
    extra_data,
    executed,
    prompt_id,
    execution_list,
    pending_subgraph_results,
    pending_async_nodes,
):
    global _PROMPTS ,CONSOLE_LOG
    node_id = current_item
    class_type = dynprompt.get_node(current_item)["class_type"]
    if prompt_id not in _PROMPTS:
        st = PromptState()
        _PROMPTS[prompt_id] = st
    else:
        st = _PROMPTS[prompt_id]
    
    if st.workflow_t0 is None:
        if CONSOLE_LOG:
            logging.info(f"Start new workflow: [{prompt_id}] ")
        st.workflow_t0 = time.perf_counter()
        st.t0 = st.workflow_t0
        st.baseline_vram_mb = get_pid_tree_vram_mb(os.getpid())
        st.baseline_ram_mb = get_pid_tree_rss_mb(os.getpid())
        st.last_end_vram_mb = st.baseline_vram_mb
        st.last_end_ram_mb  = st.baseline_ram_mb
        # START 行
        st.metrics.append({
            "node_id": 0, "class_type": "START_WORKFLOW", "status": "",
            "node_time": 0, "relative_time": 0,
            "node_start_vram": round(st.baseline_vram_mb/1024, 2),
            "vram_peak": round(st.baseline_vram_mb/1024, 2),
            "node_end_vram": round(st.baseline_vram_mb/1024, 2),
            "clean_cache": 0,
            "ram_peak": round(st.baseline_ram_mb/1024, 2),
        })   
    k = node_id
    if k not in st.active:
        poller = PeakPoller(pid=os.getpid(), interval=0.3, include_children=True)
        poller.start()
        st.active[k] = (poller, time.perf_counter())

    try:
        result = await ORIGIN_EXEC(
            server,
            dynprompt,
            caches,
            current_item,
            extra_data,
            executed,
            prompt_id,
            execution_list,
            pending_subgraph_results,
            pending_async_nodes,
        )
    except asyncio.CancelledError:
        log_data(k, node_id, class_type, prompt_id, "interrupted")
        raise
    except Exception:
        log_data(k, node_id, class_type, prompt_id, "failure")
        raise

    exec_result = result[0]
    name = getattr(exec_result, "name", None)
    if name == "PENDING":
        return result
    status = "success" if name == "SUCCESS" else "failure"
    log_data(k, node_id, class_type, prompt_id, status)

    return result


def log_data(k, node_id, class_type, prompt_id, status):
    st = _PROMPTS.get(prompt_id)
    p, t0 = st.active.pop(node_id, (None, None))
    if p:
        t1 = time.perf_counter()
        data = p.stop()

        # calulate the time
        data["node_time"] = round(t1 - t0, 2)
        data["relative_time"] = round(t1 - st.workflow_t0, 2)

        node_start_vram_gb = round(st.last_end_vram_mb/1024, 2)

        
        vram_peak_gb = data["vram_mb_peak"] / 1024
        ram_peak_gb = data["ram_mb_peak"] / 1024

        data["vram_peak_gb"] = round(vram_peak_gb, 2)
        data["ram_peak"] = round(ram_peak_gb, 2)

        ## test inference vram
        data["vram"] = round(vram_peak_gb, 2)
        data["ram"] = round(ram_peak_gb, 2)

        if FORCED_EMPTY_CACHE and torch.cuda.is_available():
            gc.collect(); torch.cuda.empty_cache()
            vram_mb_now = get_pid_tree_vram_mb(os.getpid())
            clean_cache_mb = max(0, data["vram_mb_peak"] - vram_mb_now)
        else:
            clean_cache_mb = 0
            vram_mb_now = data["vram_mb_peak"]
        st.last_end_vram_mb = vram_mb_now
        st.last_end_ram_mb = data["ram_mb_peak"]


        
        ## set row
        row = {
            "node_id": node_id, "class_type": class_type, "status": status,
            "node_time": round(data["node_time"], 2),
            "relative_time": round(data["relative_time"], 2),
            "node_start_vram": node_start_vram_gb,
            "vram_peak": round((data["vram_mb_peak"]/1024), 2),
            "node_end_vram": round((vram_mb_now/1024), 2),
            "clean_cache": round((clean_cache_mb/1024), 2),
            "ram_peak": round((data["ram_mb_peak"]/1024), 2),
            }
        st.metrics.append(row)
        # 移除 MB 欄位
        data.pop("vram_mb_peak", None)
        data.pop("ram_mb_peak", None)
        data["prompt_id"] = prompt_id
        if CONSOLE_LOG:
            logging.info(f"[node {node_id} {class_type}] {status}: {data}")


class AnyType(str):
    """A special class that is always equal in not equal comparisons. Credit to pythongosssss"""

    def __ne__(self, __value: object) -> bool:
        return False


any_type = AnyType("*")


def FORCE_ENABLE_FIRST():
    execution.execute = swizzle_execute
    if not getattr(server.PromptServer, "custom_wc_patched_send", False):
        server.PromptServer.send_sync = _send_sync_patch
        server.PromptServer.custom_wc_patched_send = True
    if not getattr(execution.PromptExecutor, "custom_wc_patched_execute", False):
        execution.PromptExecutor.execute = _execute_patch
        execution.PromptExecutor.custom_wc_patched_execute = True


FORCE_ENABLE_FIRST()


class ExecutionTime:
    CATEGORY = "system/debug"
    INPUT_TYPES = lambda: {
        "required": {
            "filepath": ("STRING", {"default": FILEPATH}),
            "forced_empty_cache": (
                "BOOLEAN",
                {
                    "default": FORCED_EMPTY_CACHE,
                    "tooltip": "用來測量每個node準確的vram用量，注意,一旦開啟的話時間的測量就不準了",
                },
            ),
            "console_log": ("BOOLEAN", {"default": CONSOLE_LOG}),
            "enable_node": ("BOOLEAN", {"default": True, "tooltip": "啟用這個node"}),
        },
        "optional": {
            "input": ("*", {"tooltip": "會從這個node後開始計算"}),
        },
    }

    RETURN_TYPES = (any_type,)
    RETURN_NAMES = ("*",)
    OUTPUT_NODE = True
    FUNCTION = "execute"

    @classmethod
    def VALIDATE_INPUTS(s, input_types):
        return True

    def execute(
        self,
        filepath: str,
        forced_empty_cache: bool,
        console_log: bool,
        enable_node: bool,
        input=None,
    ):
        logging.info(rf"Filename:{filepath}")
        global SAVE_TO_CSV, CONSOLE_LOG, FORCED_EMPTY_CACHE, FILEPATH, _PROMPTS
        if enable_node:
            if filepath == "":
                SAVE_TO_CSV = False
            else:
                os.makedirs(os.path.dirname(filepath), exist_ok=True)
            FILEPATH = filepath
            CONSOLE_LOG = console_log
            FORCED_EMPTY_CACHE = forced_empty_cache
            execution.execute = swizzle_execute
            if not getattr(server.PromptServer, "custom_wc_patched_send", False):
                server.PromptServer.send_sync = _send_sync_patch
                server.PromptServer.custom_wc_patched_send = True
            if not getattr(
                execution.PromptExecutor, "custom_wc_patched_execute", False
            ):
                execution.PromptExecutor.execute = _execute_patch
                execution.PromptExecutor.custom_wc_patched_execute = True
        else:
            execution.execute = ORIGIN_EXEC
            server.PromptServer.send_sync = ORIGN_SEND_SYNC
            execution.PromptExecutor.execute = ORIGIN_PROMPT_EXEC
            server.PromptServer.custom_wc_patched_send = False
            execution.PromptExecutor.custom_wc_patched_execute = False
            ## clean up data
            for st in _PROMPTS.values():
                for p, _ in st.active.values():
                    try: p.stop()
                    except: pass
            _PROMPTS.clear()
        return (input,)
