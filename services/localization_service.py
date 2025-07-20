import pandas as pd
from datetime import datetime, timezone, timedelta
import time


class LocalizationService:
    _localization_data = {}
    _language_setting = 'ja'
    _language_settings = {
        'ja': '日本語',
        'en': '英語'
    }
    _locale_settings = {
        'ja': 'ja_JP',
        'en': 'en_US'
    }

    @classmethod
    def load_localization(cls, file_path):
        # エクセルファイルの読み込み
        df = pd.read_excel(file_path)

        # 辞書の作成
        cls._localization_data = {
            row['key']: {
                'ja': row['ja'],
                'en': row['en']
            }
            for _, row in df.iterrows()
        }

    @classmethod
    def set_language_setting(cls, language):
        if language in cls._language_settings.keys():
            cls._language_setting = language
        else:
            raise ValueError("Unsupported language. Use 'ja' or 'en'.")

    @classmethod
    def get_language_setting(cls):
        return cls._language_setting

    @classmethod
    def get_locale_setting(cls):
        return cls._locale_settings[cls._language_setting]
    
    @staticmethod
    def get_timezone_offset():
        UTC = timezone(timedelta(hours=0), 'UTC')
        time_local_unix = time.mktime(datetime.now().timetuple())
        time_utc_unix = time.mktime(datetime.now(UTC).timetuple())
        timezone_delta = pd.Timestamp(time_local_unix - time_utc_unix, unit='s').hour
        return timezone_delta

def _(key):
    try:
        return LocalizationService._localization_data.get(key, key).get(LocalizationService._language_setting, key)
    except:
        return key

# builtins._ = _
