import asyncio
import threading
import time
import logging
import execution
import server
import os
import psutil
import pynvml
from typing import Dict, Optional
# ---- NVML 初始化 ----
_NVML_READY = False
def _nvml_init_once():
    global _NVML_READY
    if not _NVML_READY:
        pynvml.nvmlInit()
        _NVML_READY = True


# ---- 幫助函式：取子進程 ----
def _pid_tree(p: psutil.Process, recursive=True):
    arr = [p]
    try:
        arr += p.children(recursive=recursive)
    except Exception:
        pass
    return arr


# ---- Getter fallback ----
def _get_nvml_proc_getter():
    # 優先選 v3（有些欄位較完整），找不到再退而求其次
    for base in ["Compute", "Graphics"]:
        for suffix in ["_v3", ""]:
            name = f"nvmlDeviceGet{base}RunningProcesses{suffix}"
            if hasattr(pynvml, name):
                return getattr(pynvml, name)
    return None



# ---- VRAM / RAM 量測 ----
def get_pid_tree_vram_mb(pid: int) -> int:
    _nvml_init_once()
    try:
        plist = _pid_tree(psutil.Process(pid))
        pids = {pp.pid for pp in plist}
    except Exception:
        pids = {pid}

    total = 0
    getter = _get_nvml_proc_getter()
    if getter is None:
        return 0

    count = pynvml.nvmlDeviceGetCount()
    for i in range(count):
        h = pynvml.nvmlDeviceGetHandleByIndex(i)
        try:
            procs = getter(h)
        except pynvml.NVMLError:
            continue
        for pr in procs:
            used = getattr(pr, "usedGpuMemory", None) or getattr(pr, "gpuInstanceMemoryUsed", None)
            if pr.pid in pids and used and used > 0:
                total += int(used // (1024**2))
    return total


def get_pid_tree_rss_mb(pid: int) -> float:
    """回傳 pid + 子進程 RSS（MiB）。"""
    try:
        procs = _pid_tree(psutil.Process(pid))
    except Exception:
        return 0.0
    acc = 0
    for pp in procs:
        try:
            acc += pp.memory_info().rss
        except Exception:
            pass
    return acc / (1024**2)



# ---- 背景輪詢 PeakPoller ----
class PeakPoller:
    def __init__(self, pid: int, interval: float = 0.3, include_children: bool = True):
        self.pid = pid
        self.interval = interval
        self.include_children = include_children
        self._thr: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._peaks: Dict[str, float] = {
            "vram_mb_peak": 0.0,
            "ram_mb_peak": 0.0,
            "samples": 0,
        }

    def start(self):
        if self._thr and self._thr.is_alive():
            return
        self._stop.clear()
        self._thr = threading.Thread(
            target=self._loop, name=f"PeakPoller-{self.pid}", daemon=True
        )
        self._thr.start()
        self._sample_once()  # 啟動當下先採樣一次

    def stop(self):
        if not self._thr:
            return dict(self._peaks)
        self._sample_once()  # 停止前補一次
        self._stop.set()
        self._thr.join(timeout=2.0)
        return dict(self._peaks)

    def _sample_once(self):
        try:
            vram_mb = float(get_pid_tree_vram_mb(self.pid))
            ram_mb = float(get_pid_tree_rss_mb(self.pid))
            if vram_mb > self._peaks["vram_mb_peak"]:
                self._peaks["vram_mb_peak"] = vram_mb
            if ram_mb > self._peaks["ram_mb_peak"]:
                self._peaks["ram_mb_peak"] = ram_mb
            self._peaks["samples"] += 1
        except Exception as e:
            logging.debug(f"sample_once error: {e}")

    def _loop(self):
        try:
            _nvml_init_once()
        except Exception:
            pass
        while not self._stop.is_set():
            self._sample_once()
            self._stop.wait(self.interval)
            