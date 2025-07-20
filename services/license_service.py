
from Crypto.Cipher import AES
from datetime import datetime
from services.localization_service import _


class LicenseService:

    @staticmethod
    def check(device_sn, license_key, team_device_id):

        if license_key is None or license_key == "":
            return False, None, f"{device_sn} : License Key is empty."

        secret_key = device_sn
        if len(secret_key) % 16 != 0:
            secret_key_16byte = secret_key
            for ii in range(16 - (len(secret_key) % 16)):
                secret_key_16byte += "_"
        else:
            secret_key_16byte = secret_key
        iv = "gps_glo_gal_qzss".encode("utf-8")
        secret_key_16byte = secret_key_16byte.encode("utf-8")
        crypto = AES.new(secret_key_16byte, AES.MODE_CBC, iv)
        original_message = crypto.decrypt(license_key)
        expire_date_str = str(original_message)[2:12]
        expire_date_str = expire_date_str.replace('_', '')
        try:
            expire_datetime = datetime.strptime(expire_date_str, '%Y-%m-%d')
        except Exception as e:
            return False, None, f"{device_sn} : {e}"

        link_num = str(original_message)[12:13]
        license_plan1 = str(original_message)[13:14]
        license_plan2 = str(original_message)[14:15]
        license_plan3 = str(original_message)[15:16]
        if link_num == '_':
            link_num = 3
        if license_plan1 == '_':
            license_plan1 = 0
        if license_plan2 == '_':
            license_plan2 = 0
        if license_plan3 == '_':
            license_plan3 = 0

        expire_date = expire_datetime.date()
        if expire_date < datetime.today().date():
            err_msg = _("Message_License_Expired_TeamDeviceID_1") + str(team_device_id) + _("Message_License_Expired_TeamDeviceID_2")
            return False, expire_date, err_msg
        else:
            return True, expire_date, None

    @staticmethod
    def check_is_license_expire_alert(today, expire_date) -> str | None:        
        days_left = (expire_date - today).days
        if days_left >= 120:
            return False, None, None
        return True, days_left, expire_date

    @staticmethod
    def get_alert_message(days_left, expire_date):
        if days_left < 0:
            return _("Message_License_Expired")
        elif days_left == 0:
            return _("Message_License_Expired_Today")
        elif 1 <= days_left <= 7:
            return _("Message_License_Expired_Day").replace("{0}", f"{days_left}").replace("{1}", f"{expire_date}")
        elif 8 <= days_left <= 13:
            return _("Message_License_Expired_Week").replace("{0}", "1").replace("{1}", f"{expire_date}")
        elif 14 <= days_left <= 20:
            return _("Message_License_Expired_Week").replace("{0}", "2").replace("{1}", f"{expire_date}")
        elif 21 <= days_left <= 27:
            return _("Message_License_Expired_Week").replace("{0}", "3").replace("{1}", f"{expire_date}")
        elif 28 <= days_left <= 34:
            return _("Message_License_Expired_Week").replace("{0}", "4").replace("{1}", f"{expire_date}")
        elif 35 <= days_left < 60:
            return _("Message_License_Expired_Month").replace("{0}", "1").replace("{1}", f"{expire_date}")
        elif 60 <= days_left < 90:
            return _("Message_License_Expired_Month").replace("{0}", "2").replace("{1}", f"{expire_date}")
        elif 90 <= days_left < 120:
            return _("Message_License_Expired_Month").replace("{0}", "3").replace("{1}", f"{expire_date}")
        else:
            return None  # 4ヶ月以上ある場合は警告不要
