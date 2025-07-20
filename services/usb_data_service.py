import re
import pandas as pd
import subprocess
import elevate
import shutil
import json
from time import sleep
import os
import psutil
from distutils.dir_util import copy_tree, remove_tree
from datetime import datetime
import asyncio
from models.registration.device_model import DeviceModel
from services.path_serivce import Path
from services.license_service import LicenseService
import asyncio

import os
import json
import subprocess
from shutil import copytree
from datetime import datetime
from Crypto.Cipher import AES
from services.localization_service import _
from services.logger_service import Logger

def register_usb_device(device_sn, drive_letter):
    Logger.info(f"デバイス登録開始 : {device_sn} ({drive_letter})")

    try:
        # DeviceSN
        try:
            with open(Path.get_drive_ID_file(drive_letter)) as f:
                usb_device_sn = f.read()
        except Exception as e:
            Logger.error(f"DeviceSN Error", e)
            return False
        
        # TeamDeviceID
        team_device_id = None
        try:
            with open(Path.get_drive_TeamDeviceID_file(drive_letter)) as f:
                team_device_id = f.read()
        except Exception as e:
            Logger.error(f"TeamDeviceID Error", e)
            return False
        
        # LicenseKey
        try:
            with open(Path.get_drive_KEY_file(drive_letter, device_sn), 'rb') as g:
                license_key = g.read()
        except Exception as e:
            Logger.error(f"LicenseKey Error", e)
            return False
        
        # ExpireDate
        expire_date = ""
        if device_sn == usb_device_sn:
            result, expire_date, err_msg = LicenseService.check(device_sn, license_key, team_device_id)                 
        else:
            Logger.error(f"usb_device_sn Error")
            return False
        
        try:
            wifi_setting = None
            if os.path.exists(Path.get_drive_wifi_setting_file(drive_letter)):
                # UTF-8 エンコーディングを指定してファイルを開く
                with open(Path.get_drive_wifi_setting_file(drive_letter), 'r', encoding='utf-8') as f:
                    file_content = f.read()
                    file_content = file_content.replace('+', '')        
                    tests = file_content.split('#')
                    wifi_setting = json.loads(tests[0])
        except Exception as e:
            Logger.warning(f"wifi_setting warning : {e}")
            pass
    
        try:
            os.makedirs(Path.get_data_device_dir(), exist_ok=True)
            copytree(Path.get_drive_ID_dir(drive_letter), Path.get_devicesn_ID_dir(device_sn), dirs_exist_ok=True)
            copytree(Path.get_drive_KEY_dir(drive_letter), Path.get_devicesn_KEY_dir(device_sn), dirs_exist_ok=True)            
            if os.path.exists(Path.get_drive_SN_dir(drive_letter)):
                copytree(Path.get_drive_SN_dir(drive_letter), Path.get_devicesn_SN_dir(device_sn), dirs_exist_ok=True)
        except Exception as e:
            Logger.error(f"copytree Error : log += ", e)
            return False

        try:
            if os.path.exists(Path.get_devicesn_SN_dir(device_sn)):
                subprocess.check_call(["attrib", "+H", Path.get_devicesn_SN_dir(device_sn)])
            subprocess.check_call(["attrib", "+H", Path.get_devicesn_ID_dir(device_sn)])
            subprocess.check_call(["attrib", "+H", Path.get_devicesn_KEY_dir(device_sn)])
        except Exception as e:
            Logger.warning(f"check_call warning : {e}")

        device = {
            'DeviceSN': device_sn, 
            'TeamDeviceID': team_device_id, 
            'LicenseKey': license_key, 
            'ExpireDate': expire_date,
        }
        if wifi_setting:
            device['SSID'] = wifi_setting['ssid']
            device['WifiPW'] = wifi_setting['password']
            device['IP'] = wifi_setting['udpserver']
            device['Port'] = wifi_setting['udpport']
            device['TimeZone'] = wifi_setting['timezone']
            device['Mode'] = wifi_setting['mode']

        DeviceModel.upsert(**device)
        Logger.info(f"デバイス登録完了 : {device_sn}")
        return True

    except Exception as e:
        Logger.error(f"デバイス登録エラー", e)

    return False

def write_usb_device_info(device_sn, drive_letter):
    Logger.info(f"USBデバイス情報更新 開始 : {device_sn} ({drive_letter})")
    try:
        device = DeviceModel.load_by_device_sn(device_sn)
        if not device:
            Logger.warning(f"デバイス {device_sn} の情報がデータベースに存在しません。")
            return False

        # 一時フォルダを準備
        temp_dir_path = Path.get_data_temp_update_device_info_dir()
        if not os.path.exists(temp_dir_path):
            os.makedirs(temp_dir_path)

        # --- TeamDeviceID 書き込み ---
        local_team_path = os.path.join(temp_dir_path, ".TeamDeviceID.TXT")
        with open(local_team_path, "w", encoding="utf-8") as f:
            f.write(str(device.TeamDeviceID))

        target_team_path = Path.get_drive_TeamDeviceID_file(drive_letter)
        subprocess.run(["attrib", "-H", "-R", str(target_team_path)], creationflags=subprocess.CREATE_NO_WINDOW)
        if os.path.exists(target_team_path):
            try:
                os.remove(target_team_path)
            except Exception as ex:
                Logger.warning(f"既存 TeamDeviceID 削除失敗: {target_team_path} / {ex}")
        shutil.copyfile(local_team_path, target_team_path)
        subprocess.run(["attrib", "+H", str(target_team_path)], creationflags=subprocess.CREATE_NO_WINDOW)
        Logger.info(f"TeamDeviceID : {target_team_path}")

        # --- WiFi設定書き戻し ---
        wifi_setting = {
            "ssid": device.SSID,
            "password": device.WifiPW,
            "udpserver": device.IP,
            "udpport": device.Port,
            "timezone": device.TimeZone,
            "mode": device.Mode,
        }
        local_wifi_path = os.path.join(temp_dir_path, "settings.txt")
        with open(local_wifi_path, "w", encoding="utf-8") as f:
            f.write(json.dumps(wifi_setting, ensure_ascii=False))

        target_wifi_path = Path.get_drive_wifi_setting_file(drive_letter)
        if os.path.exists(target_wifi_path):
            try:
                subprocess.run(["attrib", "-H", "-R", str(target_wifi_path)], creationflags=subprocess.CREATE_NO_WINDOW)
                os.remove(target_wifi_path)
                shutil.copyfile(local_wifi_path, target_wifi_path)
                subprocess.run(["attrib", "+H", str(target_wifi_path)], creationflags=subprocess.CREATE_NO_WINDOW)
                Logger.info(f"wifi_json : {target_wifi_path}")
            except Exception as ex:
                Logger.warning(f"既存 WiFi設定 削除失敗: {target_wifi_path} / {ex}")
        else:
            Logger.info(f"skipped wifi_json because no {target_wifi_path}")

        # --- 一時ファイル削除 ---
        try:
            shutil.rmtree(temp_dir_path)
        except Exception as ex:
            Logger.warning(f"一時フォルダ削除失敗 : {temp_dir_path} : {ex}")

        # --- 更新フラグ false に更新 ---
        update_device = {"ID": device.ID, "DeviceUpdateRequired": False}
        DeviceModel.upsert(**update_device)

        Logger.info(f"USBデバイス情報更新 完了 : {device_sn}")
        return True

    except Exception as e:
        Logger.error(f"USBデバイス情報更新 失敗: {device_sn} / {e}")
        return False
    
    
def write_usb_license_info(device_sn, license_key, license_path, drive_letter):
    Logger.info(f"USBライセンス情報更新 開始 : {device_sn} ({drive_letter})")
    try:
        # DBからデバイス情報を取得
        device = DeviceModel.load_by_device_sn(device_sn)
        
        if not device:
            Logger.warning(f"デバイス {device_sn} の情報がデータベースに存在しません。")
            return False

        # ExpireDate 確認
        expire_date = ""
        result, expire_date, err_msg = LicenseService.check(device_sn, license_key, device.TeamDeviceID)
        if not result:
            Logger.error(f"ライセンス期限エラー : {device_sn} : {err_msg}")
            return False

        # LicenseKey 書き込み
        license_key_file_path = Path.get_drive_KEY_file(drive_letter, device_sn)
        subprocess.run(["attrib", "-H", "-R", license_key_file_path], creationflags=subprocess.CREATE_NO_WINDOW)
        shutil.copyfile(license_path, license_key_file_path)
        # with open(license_key_file_path, 'wb') as g: #これだとうまくいかないデバイスがあるため、ファイル差し替えとした
        #     g.write(device.LicenseKey)
        subprocess.check_call(["attrib", "+H", license_key_file_path])
        Logger.info(f"LicenseKey : {license_key_file_path}")

        update_device = {"ID" : device.ID, "LicenseKey" : license_key, "ExpireDate" : expire_date}
        DeviceModel.upsert(**update_device)
        Logger.info(f"USBデバイス情報更新 完了 : {device_sn}")
        return True

    except Exception as e:
        Logger.error(f"USBデバイス情報更新 エラー : {device_sn}", e)
        return False
    

def get_license_key(drive_letter, device_sn):
    license_key = ""
    with open(Path.get_drive_KEY_file(drive_letter, device_sn), 'rb') as g:
        license_key = g.read()
    return license_key
        

def create_license_file(drive_letter, device_sn):
    """
    2030/12/31 の有効期限を持つライセンスファイルを作成する
    """
    # ✅ ライセンス情報を作成
    expire_date = "2030-12-31"
    link_num = "3"        # リンク数
    license_plan = "000"  # ライセンスプラン（仮定）

    # 文字列を 16 バイト長にパディング（不足分を `_` で埋める）
    secret_key_16byte = device_sn
    if len(secret_key_16byte) % 16 != 0:
        secret_key_16byte += "_" * (16 - (len(secret_key_16byte) % 16))
    secret_key_16byte = secret_key_16byte.encode("utf-8")

    # ✅ AES 暗号化（CBC モード）
    iv = "gps_glo_gal_qzss".encode("utf-8")
    crypto = AES.new(secret_key_16byte, AES.MODE_CBC, iv)

    # ✅ ライセンスデータ（長さ16バイトに調整）
    license_data = (expire_date + link_num + license_plan).ljust(16, "_")
    license_data = license_data.encode("utf-8")

    # ✅ 暗号化
    encrypted_license = crypto.encrypt(license_data)

    # ✅ ファイルパス
    license_file_path = Path.get_drive_KEY_file(drive_letter, device_sn)

    # ✅ バイナリファイルとして保存
    with open(license_file_path, "wb") as f:
        f.write(encrypted_license)

    print(f"✅ ライセンスファイルを作成しました: {license_file_path}")

    return license_file_path





