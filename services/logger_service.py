import logging
import threading
import os
import sys
import inspect
import traceback
from enum import Enum
from datetime import datetime
from services.path_serivce import Path

class CustomLogger:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, log_dir, log_base_name):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(CustomLogger, cls).__new__(cls)
                    cls._instance._initialize(log_dir, log_base_name)
        return cls._instance

    def _initialize(self, log_dir, log_base_name):
        os.makedirs(log_dir, exist_ok=True)  # ログフォルダを作成する（なければ）

        today_str = datetime.now().strftime("%Y%m%d")
        log_file = os.path.join(log_dir, f"{today_str}_{log_base_name}.log")

        self._logger = logging.getLogger("app_logger")
        self._logger.setLevel(logging.DEBUG)  # ハンドラレベルはDEBUG固定で、出力制御は内部でする
        self._debug_log = False

        if not self._logger.handlers:
            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            formatter = logging.Formatter(
                "%(asctime)s [%(levelname)s] [%(filename)s:%(funcName)s:%(lineno)d] %(message)s"
            )
            file_handler.setFormatter(formatter)
            self._logger.addHandler(file_handler)

            console_handler = logging.StreamHandler(sys.stdout)
            console_formatter = logging.Formatter(
                "%(asctime)s [%(levelname)s] [%(filename)s:%(funcName)s:%(lineno)d] %(message)s"
            )
            console_handler.setFormatter(console_formatter)
            self._logger.addHandler(console_handler)

    def set_is_debug_log(self, flag):
        self._debug_log = flag

    def debug(self, message):
        if self._debug_log:
            self._logger.debug(message, stacklevel=2)

    def info(self, message):
        self._logger.info(message, stacklevel=2)

    def warning(self, message):
        self._logger.warning(message, stacklevel=2)

    def error(self, message, ex: Exception = None):
        if ex is not None:
            message += f"\nException: {type(ex).__name__} - {str(ex)}"
            message += "\n" + ''.join(traceback.format_exception(type(ex), ex, ex.__traceback__))
        else:
            message += "\nCall Stack:\n" + '\n'.join(traceback.format_stack())
        self._logger.error(message)

    def snack_bar_info(self, message):
        self._logger.info(message, stacklevel=3)

    def snack_bar_warning(self, message):
        self._logger.warning(message, stacklevel=3)

    def snack_bar_error(self, message):
        self._logger.error(message, stacklevel=3)

    def manual_event_info(self, param=""):
        frame = inspect.currentframe()
        try:
            caller_frame = frame.f_back
            func_name = caller_frame.f_code.co_name
            self._logger.info(f"MANUAL > {func_name} {param}", stacklevel=2)
        finally:
            del frame  # フレームを明示的に破棄
        
Logger = CustomLogger(log_dir=Path.get_data_log_dir(), log_base_name="app")
