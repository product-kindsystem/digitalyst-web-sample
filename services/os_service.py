import os
import platform
from models.registration.systemsetting_model import SystemSettingModel


def set_process_priority(process_priority: str):
    import psutil
    proc = psutil.Process(os.getpid())
    system = platform.system()

    if system == "Windows":
        priority_map = {
            SystemSettingModel.ProcessPriorityEnum.NORMAL.value: psutil.NORMAL_PRIORITY_CLASS,
            SystemSettingModel.ProcessPriorityEnum.ABOVE_NORMAL.value: psutil.ABOVE_NORMAL_PRIORITY_CLASS,
            SystemSettingModel.ProcessPriorityEnum.HIGH.value: psutil.HIGH_PRIORITY_CLASS,
            # "low": psutil.IDLE_PRIORITY_CLASS,
        }
        proc.nice(priority_map.get(process_priority, psutil.NORMAL_PRIORITY_CLASS))
    elif system in ("Darwin", "Linux"):
        # Mac/Linux は nice 値を使う
        priority_map = {
            SystemSettingModel.ProcessPriorityEnum.NORMAL.value: 0,
            SystemSettingModel.ProcessPriorityEnum.ABOVE_NORMAL.value: -10,
            SystemSettingModel.ProcessPriorityEnum.HIGH.value: -20,
            # "low": 10,
        }
        try:
            proc.nice(priority_map.get(process_priority, 0))
        except PermissionError:
            print("⚠️ この優先度を設定するには管理者権限（sudo）が必要です。")


def get_process_priority() -> str:
    import psutil
    proc = psutil.Process(os.getpid())
    system = platform.system()

    if system == "Windows":
        priority_class = proc.nice()
        if priority_class == psutil.REALTIME_PRIORITY_CLASS:
            return SystemSettingModel.ProcessPriorityEnum.HIGH.value
        elif priority_class == psutil.HIGH_PRIORITY_CLASS:
            return SystemSettingModel.ProcessPriorityEnum.ABOVE_NORMAL.value
        elif priority_class == psutil.NORMAL_PRIORITY_CLASS:
            return SystemSettingModel.ProcessPriorityEnum.NORMAL.value
        else:
            return SystemSettingModel.ProcessPriorityEnum.NORMAL.value  # fallback

    elif system in ("Darwin", "Linux"):
        nice_value = proc.nice()
        if nice_value <= -15:
            return SystemSettingModel.ProcessPriorityEnum.HIGH.value
        elif nice_value <= -5:
            return SystemSettingModel.ProcessPriorityEnum.ABOVE_NORMAL.value
        else:
            return SystemSettingModel.ProcessPriorityEnum.NORMAL.value

    return SystemSettingModel.ProcessPriorityEnum.NORMAL.value  # fallback
