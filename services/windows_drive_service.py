import re
import pandas as pd
import subprocess
from time import sleep
import os
import psutil
from distutils.dir_util import copy_tree, remove_tree
from datetime import datetime, timedelta
import asyncio
from models.registration.device_model import DeviceModel
from models.registration.systemsetting_model import SystemSettingModel
from services.path_serivce import Path
from services.import_file_serivce import ImportFile
from services.logger_service import Logger
from Crypto.Cipher import AES
import asyncio


def run_diskpart_command(command):
    try:
        result = subprocess.run(['diskpart'], input=command, text=True, capture_output=True, check=True, creationflags=subprocess.CREATE_NO_WINDOW)
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error: {e}")
        return None
    

async def run_powershell_command(command):
    """PowerShell コマンドを非同期で実行し、出力を取得"""
    process = await asyncio.create_subprocess_exec(
        'powershell', '-Command', command,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=False,
        creationflags=subprocess.CREATE_NO_WINDOW
    )
    stdout, stderr = await process.communicate()
    if stderr:
        print(f"Error: {stderr}")
    # バイト列の場合、文字列に変換して返す
    return stdout.decode('utf-8') if stdout else ""

async def get_volume_dict(update_progress, progress_start, progress_end):
    Logger.info(f"ボリューム情報取得 開始")
    
    progress = progress_start
    if update_progress:
        await update_progress(progress)

    """すべてのボリュームのリストを取得"""
    command = 'Get-Volume'
    output = await run_powershell_command(command)
    progress += (progress_end - progress_start) / 10
    if update_progress:
        await update_progress(progress)
    
    if output:
        volume_dict = {}
        total_count = len(output.splitlines())
        count = 0        
        step = (progress_end - progress) / total_count
        
        # 出力を行ごとに分割してボリューム情報をリスト化
        for line in output.splitlines():
            progress += step
            if update_progress:
                await update_progress(progress)
            count += 1
            
            # 各行を "FAT32" または "NTFS" で分割
            if "FAT32" in line or "NTFS" in line:

                Logger.info(f"[取得文字列]{line}")
                parts = [part.strip() for part in line.split() if part.strip()]
                
                # "FAT32" または "NTFS" より前の要素のみ取得
                pre_parts = []
                for i, part in enumerate(parts):
                    if part in ['FAT32', 'NTFS']:
                        pre_parts = parts[:i]  # "FAT32" または "NTFS" より前の部分のみを残す
                        post_parts = parts[i:]  # "FAT32" または "NTFS" 以降の部分を取得
                        break

                # 分割数が2なら、1つ目を DriveLetter とし、2つ目を DeviceSN とする
                if len(pre_parts) == 2:
                    # ドライブレターあり
                    drive_letter = pre_parts[0]
                    device_sn = pre_parts[1] 
                else:
                    # ドライブレターなし
                    # 分割数が1なら、DriveLetter は空、DeviceSN として処理
                    drive_letter = ""
                    device_sn = pre_parts[0]

                # DeviceSNが"SN"で始まるボリュームのみをリストに追加
                if device_sn.startswith("SN"):

                    Logger.info(f"=>対象デバイスです : {device_sn} : {drive_letter}")
                    volume = {
                        'DriveLetter': drive_letter,  # ドライブレター
                        'DeviceSN': device_sn,  # ラベル
                        'FileSystemType': post_parts[0],  # FAT32 or NTFS
                        'DriveType': post_parts[1],  # ドライブタイプ
                        'HealthStatus': post_parts[2],  # 健康状態                        
                        'StorageUsedSize': post_parts[-4],  # 残り容量
                        'StorageUsedSizeUnit': post_parts[-3],  # 単位
                        'StorageTotalSize': post_parts[-2],  # 総容量
                        'StorageTotalSizeUnit': post_parts[-1],  # 単位
                        'StorageFileCount': 0,  # 初期値
                        'StorageLatestFileDate': None,  # 初期値
                        'ImportFileCount': 0,  # 初期値
                        'VolumeID' : None,
                    }

                    try:
                        volume['OperationalStatus'] = " ".join(post_parts[3:-4]) # 操作状態
                    except:
                        pass
                        # Error case                    
                        # 'DriveLetter' = 'D'
                        # 'DeviceSN' = 'SN5LDQPEI'
                        # 'FileSystemType' = 'FAT32'
                        # 'DriveType' = 'Removable'
                        # 'HealthStatus' = 'Warning'
                        # 'OperationalStatus' = 'Full Repair Needed'
                        # 'StorageUsedSize' = '27.45'
                        # 'StorageUsedSizeUnit' = 'GB'
                        # 'StorageTotalSize' = '28.45'
                        # 'StorageTotalSizeUnit' = 'GB'
                        # 'StorageFileCount' = 0
                        # 'StorageLatestFileDate' = None
                        # 'ImportFileCount' = 0
                        # 'VolumeID' = None
                    

                    if drive_letter != "":
                        # ボリュームのファイルリストを取得
                        volume['StorageFileCount'], volume['StorageLatestFileDate'], volume['ImportFileCount'] = get_file_list(drive_letter)
                    else:
                        volume['StorageFileCount'], volume['StorageLatestFileDate'], volume['ImportFileCount'] = '?', None, '?'

                    volume_dict[device_sn] = volume
                else:
                    Logger.info(f"=>対象外 : {device_sn} : {drive_letter}")

        Logger.info(f"ボリューム情報取得 終了")
        return volume_dict
    return []

async def get_local_info(device_sn, ):
    
    auto_delete = SystemSettingModel.AutoDeleteLocalDataEnable
    auto_delete_days = SystemSettingModel.AutoDeleteLocalDataDays

    device_file_dir_path = Path.get_devicesn_FILE_dir(device_sn)
    if not os.path.exists(device_file_dir_path):
        os.makedirs(device_file_dir_path)
    
    imported_used_size = 0
    imported_file_count = 0
    calc_file_count = 0
    latest_file_date = None
                
    # FILEフォルダ
    if os.path.exists(device_file_dir_path):
        for root, dirs, files in os.walk(device_file_dir_path):
            for file in files:
                file_path = os.path.join(root, file)
                # --- 削除チェック ---
                if auto_delete:
                    if len(file) >= 10 and file[2:10].isdigit():
                        try:
                            file_date = datetime.strptime(file[2:10], "%Y%m%d").date()
                            threshold_date = datetime.today().date() - timedelta(days=auto_delete_days)
                            if file_date < threshold_date:
                                os.remove(file_path)
                                Logger.info(f"ローカルファイル自動削除 : {file_path}")
                                continue
                        except Exception as ex:
                            Logger.warning(f"ローカルファイル自動削除 日付判定エラー: {file} / {ex}")
                if ImportFile.is_target_file_name(file):
                    imported_used_size += os.path.getsize(file_path)
                    imported_file_count += 1
                    file_date = ImportFile.get_date_str(file, file_path)
                    if latest_file_date is None or file_date > latest_file_date:
                        latest_file_date = file_date                        
                if ImportFile.is_calc_file_name(file):
                    calc_file_count += 1
                    
    # GNSSフォルダ
    device_gnss_dir_path = Path.get_devicesn_GNSS_dir(device_sn)
    if not os.path.exists(device_gnss_dir_path):
        os.makedirs(device_gnss_dir_path)    
    if os.path.exists(device_gnss_dir_path):
        for root, dirs, files in os.walk(device_gnss_dir_path):
            for file in files:
                file_path = os.path.join(root, file)
                # --- 削除チェック ---
                if auto_delete:
                    if len(file) >= 8 and file[:8].isdigit():
                        try:
                            file_date = datetime.strptime(file[:8], "%Y%m%d").date()
                            threshold_date = datetime.today().date() - timedelta(days=auto_delete_days)
                            if file_date < threshold_date:
                                os.remove(file_path)
                                Logger.info(f"ローカルGNSSファイル自動削除 : {file_path}")
                                continue
                        except Exception as ex:
                            Logger.warning(f"ローカルGNSSファイル自動削除 日付判定エラー: {file} / {ex}")
                # --- サイズ集計 ---
                imported_used_size += os.path.getsize(file_path)

    # LIVEフォルダ (削除専用)
    live_dir_path = Path.get_data_live_dir()
    if not os.path.exists(live_dir_path):
        os.makedirs(live_dir_path)    
    if os.path.exists(live_dir_path):
        for root, dirs, files in os.walk(live_dir_path):
            for file in files:
                file_path = os.path.join(root, file)
                # --- 削除チェック ---
                if auto_delete:
                    if len(file) >= 8 and file[:8].isdigit():
                        try:
                            file_date = datetime.strptime(file[:8], "%Y%m%d").date()
                            threshold_date = datetime.today().date() - timedelta(days=auto_delete_days)
                            if file_date < threshold_date:
                                os.remove(file_path)
                                Logger.info(f"ローカルLIVEファイル自動削除 : {file_path}")
                                continue
                        except Exception as ex:
                            Logger.warning(f"ローカルLIVEファイル自動削除 日付判定エラー: {file} / {ex}")

    # 各デバイスのローカル情報を辞書形式で格納
    local_info = {
        'DeviceSN': device_sn,
        'ImportedUsedSize': imported_used_size,
        'ImportedFileCount': imported_file_count,
        'CalcFileCount' : calc_file_count,
        'ImportedLatestFileDate': latest_file_date.strftime("%Y/%m/%d %H:%M") if latest_file_date else None
    }
    
    return local_info


def get_file_list(drive_letter):
    """ボリューム内のファイルリストを取得し、ファイル数と最新ファイルの日付を返す"""
    # ボリュームのパスを指定（例：CドライブならC:/）
    drive_path = f"{drive_letter}:/"
    latest_file_date = None
    file_count = 0
    import_file_count = 0
    
    try:
        for file in os.listdir(drive_path):
            if ImportFile.is_target_file_name(file):
                file_count += 1
                if ImportFile.is_not_copied_file_name(file):
                    import_file_count += 1
                file_path = os.path.join(drive_path, file)
                file_date = ImportFile.get_date_str(file, file_path)
                if latest_file_date is None or file_date > latest_file_date:
                    latest_file_date = file_date
    except Exception as e:
        print(f"Error get_file_list : {e}")

    # 最新のファイルの日付を文字列形式に変換
    if latest_file_date:
        latest_file_date = format_datetime(latest_file_date)

    return file_count, latest_file_date, import_file_count

def format_datetime(dt):
    """datetimeオブジェクトを '2024/10/23 01:07' の形式に変換"""
    try:
        # datetimeオブジェクトを 'YYYY/MM/DD HH:MM' の形式に変換
        return dt.strftime("%Y/%m/%d %H:%M")
    except ValueError as e:
        print(f"Date format error: {e}")
        return None

async def get_volume_id_dicts(update_progress, progress_start, progress_end):
    """Get-Volumeコマンドを実行し、ドライブレターが空のボリュームのVO部分を取得"""
    command = 'Get-Volume | Format-List ObjectID, DriveLetter'
    output = await run_powershell_command(command)

    progress = progress_start
    first_progress = (progress_end - progress_start) / 10
    progress += first_progress
    if update_progress:
        await update_progress(progress)
    
    # 出力を行ごとに処理
    lines = output.split('\r\n\r\n')
    
    drive_letter_volume_id_dict = {}
    no_drive_letter_volume_ids = []
    
    for line in lines:
        # print(line)
        # ドライブレターが空の行を探す
        if line.strip() and "ObjectID" in line and "DriveLetter" in line:
            line_parts = line.split('DriveLetter')
            if len(line_parts) > 1:                
                drive_letter = line_parts[1].replace(':', '').strip()
                # ドライブレターが空で、VO: の後の文字列を取得
                volume_id = line_parts[0].split('VO:')[-1].strip().strip('"').replace('\r\n', '').replace(' ','')
                if drive_letter:
                    drive_letter_volume_id_dict[drive_letter] = volume_id
                else:
                    no_drive_letter_volume_ids.append(volume_id)

    no_drive_letter_devicesn_volume_id_dict = {}
    count = 0
    total_count = len(no_drive_letter_volume_ids)
    progress_step = (progress_end - progress) / total_count
    for volume_id in no_drive_letter_volume_ids:
        count += 1
        command = f'Get-Volume -UniqueId "{volume_id}"'
        output = await run_powershell_command(command)
        for line in output.splitlines():
            if "FAT32" in line or "NTFS" in line:
                parts = [part.strip() for part in line.split() if part.strip()]
                
                # "FAT32" または "NTFS" より前の要素のみ取得
                pre_parts = []
                for i, part in enumerate(parts):
                    if part in ['FAT32', 'NTFS']:
                        pre_parts = parts[:i]  # "FAT32" または "NTFS" より前の部分のみを残す
                        # post_parts = parts[i:]  # "FAT32" または "NTFS" 以降の部分を取得
                        break

                # 分割数が2なら、1つ目を DriveLetter とし、2つ目を DeviceSN とする
                if len(pre_parts) == 2:
                    # 通常はありえない
                    drive_letter = pre_parts[0]
                    device_sn = pre_parts[1]
                else:
                    # 分割数が1なら、DriveLetter は空、DeviceSN として処理
                    drive_letter = ""
                    device_sn = pre_parts[0]
                no_drive_letter_devicesn_volume_id_dict[device_sn] = volume_id

        progress += progress_step
        if update_progress:
            await update_progress(progress)

    return drive_letter_volume_id_dict, no_drive_letter_devicesn_volume_id_dict


def request_drive_letter_by_volume_object_id(object_id, drive_letter):
    remove_drive_access_path(drive_letter)
    if not set_drive_access_path(object_id, drive_letter):
        return False
    return True

def remove_drive_access_path(drive_letter):
    """指定したドライブレターからアクセスパスを削除"""
    # PowerShell コマンドを構築
    command = f'Get-Partition -DriveLetter "{drive_letter}" | Remove-PartitionAccessPath -AccessPath "{drive_letter}:\"'
    
    try:
        # PowerShell コマンドを実行
        result = subprocess.run(
            ['powershell', '-Command', command],
            capture_output=True,
            text=True,
            check=True,
            creationflags=subprocess.CREATE_NO_WINDOW
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error: {e.stderr}")
        return False

def set_drive_access_path(object_id, drive_letter):
    """指定したObjectIdにドライブアクセスパスを設定"""
    # PowerShell コマンドを構築
    command = f"Get-Volume -UniqueId '{object_id}' | Get-Partition | Add-PartitionAccessPath -AccessPath '{drive_letter}:\'"
    
    try:
        # PowerShell コマンドを実行
        result = subprocess.run(
            ['powershell', '-Command', command],
            capture_output=True,
            text=True,
            check=True,
            creationflags=subprocess.CREATE_NO_WINDOW
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error: {e.stderr}")
        return False

