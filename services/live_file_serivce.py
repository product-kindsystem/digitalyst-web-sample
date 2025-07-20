import os
from datetime import datetime, date
from models.registration.device_model import DeviceModel
from services.path_serivce import Path
from services.logger_service import Logger


class LiveFile:

    @staticmethod
    def get_live_db_date_list():
        dir_path = Path.get_data_live_dir()
        date_list = []
        try:
            for file_name in os.listdir(dir_path):
                # "_LIVE.db"で終わるファイルを対象とする
                if file_name.endswith("_LIVE.db"):
                    date_str = file_name.split("_")[0]  # "20250307"を取り出す
                    try:
                        date_dt = datetime.strptime(date_str, "%Y%m%d")
                        date_list.append(date_dt)
                    except ValueError as ve:
                        Logger.error(f"Invalid date format in file name: {file_name} - {ve}")
        except Exception as ex:
            Logger.error(f"Failed to read directory: {dir_path}", ex)
        return date_list

    @staticmethod
    def get_live_db_path_by_date(target_date: date):
        dir_path = Path.get_data_live_dir()
        date_str = target_date.strftime("%Y%m%d")
        expected_filename = f"{date_str}_LIVE.db"
        full_path = os.path.join(dir_path, expected_filename)
        
        if os.path.exists(full_path):
            return full_path
        else:
            Logger.info(f"Live DB file not found for date: {target_date}")
            return None
