import os
import time
import asyncio
import joblib
import http.client
import shutil
import subprocess
from datetime import datetime, timedelta
import zipfile
import pandas as pd
import pandas.tseries.offsets as offsets
from models.registration.device_model import DeviceModel
from models.registration.config_model import ConfigModel
from models.registration.field_model import FieldModel
from models.registration.systemsetting_model import SystemSettingModel
from models.import_database_manager import ImportDatabaseManager
from models.importdata.import_gnss_trace_config_model import ImportGnssTraceConfigModel
from models.importdata.import_gnss_trace_field_model import ImportGnssTraceFieldModel
from models.importdata.import_gnss_model import ImportGnssModel
from models.importdata.import_gnss_sec_model import ImportGnssSecModel
from models.importdata.import_gnss_field_in_log_model import ImportGnssFieldInLogModel
from models.importdata.importviewdata_model import ImportDeviceProgress, SPECIFIED_DRIVE_LETTER
from services import windows_drive_service
from services.import_file_serivce import ImportFile
from services.import_data_function_create_df10 import CreateDf10
from services import usb_data_service
from services.license_service import LicenseService
from services.field_data_serivce import FieldDataService
from services.localization_service import LocalizationService, _
from services.path_serivce import Path
from services.logger_service import Logger
from scipy.signal import butter, filtfilt



class ImportDataService:
    _instance = None

    def __new__(self):
        if self._instance is None:
            self._instance = super(ImportDataService, self).__new__(self)
            self.initialize()
        return self._instance

    @classmethod
    def initialize(self):
        pass

    @classmethod
    def get_instance(self):
        return self._instance

    @classmethod
    async def update_task(self, device_sn_list, update_progress, show_warning, show_error):
        Logger.info("デバイス情報 取得開始")

        volume_dict, local_dict = {}, {}
        progress = 0

        # local_dict取得
        try:
            progress_target = 0.5
            if len(device_sn_list) > 0:
                progress_step = progress_target / len(device_sn_list)
                for device_sn in device_sn_list:
                    local_info = await windows_drive_service.get_local_info(device_sn)
                    local_dict[device_sn] = local_info
                    progress += progress_step
                    if update_progress:
                        await update_progress(progress)
                    Logger.info(f"{device_sn} ローカル情報取得")
                progress = progress_target
                if update_progress:
                    await update_progress( progress )
        except Exception as e:
            await show_error(f"get_local_info Error : {e}")

        # volume_dict取得
        progress_target = 1
        try:
            progress_start = progress
            progress_end = progress_target
            volume_dict = await windows_drive_service.get_volume_dict(update_progress, progress_start, progress_end)
        except Exception as e:
            await show_error(f"get_volume_dict Error : {e}")

        if update_progress:
            await update_progress( 1 )
            
        Logger.info("デバイス情報 取得終了")
        return volume_dict, local_dict
    
    
    @classmethod
    async def register_new_device_task(self, df_unregistered_device, update_progress, on_device_update, show_warning, show_error):
        Logger.info("新規デバイス情報 取得開始")
        
        progress = 0
        middle_progress = 0.5

        device_dict = df_unregistered_device.set_index('DeviceSN')['DriveLetter'].to_dict()
        volume_id_required = False
        for device_sn in device_dict.keys():
            if device_dict[device_sn] is None or device_dict[device_sn] == "":
                volume_id_required = True

        if volume_id_required:
            try:
                drive_letter_volume_id_dict, no_drive_letter_devicesn_volume_id_dict = await windows_drive_service.get_volume_id_dicts(update_progress, progress, middle_progress)
            except Exception as e:
                await show_error(f"get_volume_id_dicts Error : {e}")
                return 0

        progress = middle_progress
        if update_progress:
            await update_progress( progress )

        count = 0
        total_count = len(device_dict.items())
        if total_count > 0:
            # 空文字を最後に、それ以外はアルファベット順で並び替える
            sorted_device_dict = dict(sorted(device_dict.items(), key=lambda item: (item[1] == "", item[1])))
            progress_step = (1 - middle_progress) / total_count
            for device_sn, drive_letter in sorted_device_dict.items():

                count += 1
                if drive_letter is None or drive_letter == "":
                    if device_sn in no_drive_letter_devicesn_volume_id_dict.keys():
                        volume_id = no_drive_letter_devicesn_volume_id_dict[device_sn]
                        result = windows_drive_service.request_drive_letter_by_volume_object_id(volume_id, SPECIFIED_DRIVE_LETTER)
                        if not result:
                            Logger.warning(f"ドライブ割り当てに失敗しました : {device_sn} : {volume_id} : {SPECIFIED_DRIVE_LETTER}")
                            continue
                        else:
                            drive_letter = SPECIFIED_DRIVE_LETTER
                    else:
                        continue
                try:
                    if not usb_data_service.register_usb_device(device_sn, drive_letter):
                        await show_warning(f"USB Register Warning : {device_sn} from {drive_letter}")
                    # break # Debug
                except Exception as e:
                    await show_error(f"register_usb_device Error : {device_sn} from {drive_letter} : {e}")
                progress += progress_step
                if update_progress:
                    await update_progress(progress)
                
        if update_progress:
            await update_progress( 1 )

        Logger.info("デバイス情報 登録終了")
        return total_count
    
    
    @classmethod
    async def update_license_task(self, zip_path, volume_dict, update_progress, show_warning, show_error):
        Logger.info("ライセンス更新 開始")

        progress = 0
        middle_progress = 0.4

        # 1. .zipファイルかどうか確認
        if not zip_path.lower().endswith(".zip"):
            await show_error("選択されたファイルは .zip ではありません。")
            return
                
        def get_devicesn_licensekey_dict_from_zip(zip_path, dir_path):
            if not os.path.isfile(zip_path):
                return {}
            _devicesn_licensekey_dict = {}  # DeviceSN → テキスト内容
            _devicesn_licensepath_dict = {}
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                for member in zip_ref.namelist():
                    if member.upper().endswith(".KEY"):
                        # ファイル名の先頭が DeviceSN（例: SN4LDQPDW.KEY）
                        device_sn = os.path.splitext(os.path.basename(member))[0]
                        extracted_path = os.path.join(dir_path, os.path.basename(member))
                        _devicesn_licensepath_dict[device_sn] = extracted_path
                        # 書き出してから読み込む
                        with open(extracted_path, 'wb') as f:
                            f.write(zip_ref.read(member))
                        try:
                            with open(extracted_path, 'rb') as g:
                                license_key = g.read()
                                _devicesn_licensekey_dict[device_sn] = license_key
                        except Exception as e:
                            Logger.error(f"LicenseKey Error : {device_sn}", e)
            return _devicesn_licensekey_dict, _devicesn_licensepath_dict
        
        # 2. ZIPファイルを展開して、KEYファイルリスト取得 SN4LDQPDW.KEY
        temp_dir_path = Path.get_data_temp_update_license_dir()
        if not os.path.exists(temp_dir_path):
            os.makedirs(temp_dir_path)
        devicesn_licensekey_dict, devicesn_licensepath_dict = get_devicesn_licensekey_dict_from_zip(zip_path, temp_dir_path)
        
        updated_device_count = 0
        device_sn_list = devicesn_licensekey_dict.keys()
        update_device_count = len(device_sn_list)
        if update_device_count > 0:         
        
            devicesn_driveletter_dict = {}
            for device_sn, volume in volume_dict.items():
                devicesn_driveletter_dict[volume['DeviceSN']] = volume['DriveLetter']
            volume_id_required = False
            for device_sn in device_sn_list:
                if device_sn in devicesn_driveletter_dict.keys():
                    if devicesn_driveletter_dict[device_sn] == "":
                        volume_id_required = True

            devicesn_without_driveletter_volumeid_dict = {}
            if volume_id_required:
                try:
                    drive_letter_volume_id_dict, devicesn_without_driveletter_volumeid_dict = await windows_drive_service.get_volume_id_dicts(update_progress, progress, middle_progress)
                except Exception as e:
                    await show_error(f"get_volume_id_dicts Error : {e}")
                    return False
                
            progress = middle_progress
            if update_progress:
                await update_progress( progress )
            progress_step = (1 - middle_progress) / update_device_count
                
            SPECIFIED_DRIVE_LETTER = "Z"
            # ドライブレターなしを最後にソート
            sorted_device_dict = dict(sorted(devicesn_driveletter_dict.items(), key=lambda item: (item[1] == "", item[1])))
            for device_sn, drive_letter in sorted_device_dict.items():
                if device_sn not in device_sn_list:
                    continue
                
                if drive_letter == "":    
                    # ドライブレターなしはZドライブにマウント         
                    if device_sn in devicesn_without_driveletter_volumeid_dict.keys():
                        volume_id = devicesn_without_driveletter_volumeid_dict[device_sn]
                        result = windows_drive_service.request_drive_letter_by_volume_object_id(volume_id, SPECIFIED_DRIVE_LETTER)
                        if not result:
                            await show_warning(f"request_drive_letter_by_volume_object_id Warning : {device_sn} : {drive_letter} drive")
                            continue
                        else:
                            drive_letter = SPECIFIED_DRIVE_LETTER
                    else:
                        await show_warning(f"No volume ID warning : {device_sn}")
                        continue

                license_key = devicesn_licensekey_dict[device_sn]
                license_path = devicesn_licensepath_dict[device_sn]
                success = usb_data_service.write_usb_license_info(device_sn, license_key, license_path, drive_letter)
                if success:
                    updated_device_count += 1
                progress += progress_step
                if update_progress:
                    await update_progress(progress)                

        if update_progress:
            await update_progress( 1 )
        
        try:
            shutil.rmtree(temp_dir_path)
        except Exception as ex:
            Logger.warning(f"shutil.rmtree error : {temp_dir_path} : {ex}")

        Logger.info(f"ライセンス更新 終了 ({updated_device_count}/{update_device_count} )")
        return updated_device_count
    

    @classmethod
    async def sync_setting_task(self, device_sn_list, volume_dict, update_progress, show_warning, show_error):
        Logger.info("デバイス情報 同期開始")

        progress = 0
        middle_progress = 0.2

        update_device_count = len(device_sn_list)
        if update_device_count > 0:         
        
            devicesn_driveletter_dict = {}
            for device_sn, volume in volume_dict.items():
                devicesn_driveletter_dict[volume['DeviceSN']] = volume['DriveLetter']
            volume_id_required = False
            for device_sn in device_sn_list:
                if device_sn in devicesn_driveletter_dict.keys():
                    if devicesn_driveletter_dict[device_sn] == "":
                        volume_id_required = True

            devicesn_without_driveletter_volumeid_dict = {}
            if volume_id_required:
                try:
                    drive_letter_volume_id_dict, devicesn_without_driveletter_volumeid_dict = await windows_drive_service.get_volume_id_dicts(update_progress, progress, middle_progress)
                except Exception as e:
                    await show_error(f"get_volume_id_dicts Error : {e}")
                    return False
                
            progress = middle_progress
            if update_progress:
                await update_progress( progress )
            progress_step = (1 - middle_progress) / update_device_count
                
            SPECIFIED_DRIVE_LETTER = "Z"
            # ドライブレターなしを最後にソート
            sorted_device_dict = dict(sorted(devicesn_driveletter_dict.items(), key=lambda item: (item[1] == "", item[1])))
            for device_sn, drive_letter in sorted_device_dict.items():
                if device_sn not in device_sn_list:
                    continue
                
                if drive_letter == "":    
                    # ドライブレターなしはZドライブにマウント         
                    if device_sn in devicesn_without_driveletter_volumeid_dict.keys():
                        volume_id = devicesn_without_driveletter_volumeid_dict[device_sn]
                        result = windows_drive_service.request_drive_letter_by_volume_object_id(volume_id, SPECIFIED_DRIVE_LETTER)
                        if not result:
                            await show_warning(f"request_drive_letter_by_volume_object_id Warning : {device_sn} : {drive_letter} drive")
                            continue
                        else:
                            drive_letter = SPECIFIED_DRIVE_LETTER
                    else:
                        await show_warning(f"No volume ID warning : {device_sn}")
                        continue

                success = usb_data_service.write_usb_device_info(device_sn, drive_letter)
                progress += progress_step
                if update_progress:
                    await update_progress(progress)                

        if update_progress:
            await update_progress( 1 )

        Logger.info("デバイス情報 同期終了")
    

    @classmethod
    async def cloud_download(self, selected_devicesn_list, update_progress, on_device_update, show_success, show_warning, show_error):
        Logger.info("クラウドダウンロード 開始")
        sync_dir = Path.get_system_sync_dir()
        sync_token_pkl_file_path = os.path.join(sync_dir,"sync_token.pkl")
        
        #Datalakeにアクセスするための、SASトークンを確認する
        sync_token = joblib.load(sync_token_pkl_file_path)
        
        #インターネットに接続しているか確認
        conn = http.client.HTTPConnection("www.google.com", timeout=2)
        try:
            conn.request("HEAD", "/")
            conn.close()
            print('Internet Connected')
            
            #SASトークンの文字列の長さで簡易的にトークンの確からしさを確認。あくまで簡易的
            if len(sync_token[0]) > 0 and len(sync_token[1]) > 100: 
                
                #Azure Datalakeコンテナにアクセスするための文字列生成
                AzCopy1 = '"https://' + sync_token[0] + '.dfs.core.windows.net/'
                AzCopyList = sync_token[0] + '/' + '1_CloudDataLink'
                AzCopy3 = sync_token[1] + '"'
                
                #GPSログのリストを取得
                AzListCMD \
                = 'azcopy.exe list '\
                + AzCopy1 + AzCopyList + AzCopy3\
                
                #文字列の条件から、GPSログだけを取り出す
                AzureFiles = subprocess.check_output(AzListCMD)
                AzureFiles = str(AzureFiles)
                AzureFiles = AzureFiles.replace(';  Content Length', '')
                AzureFilesList = AzureFiles.split(': ')
                AzureFilesList1 = [Files for Files in AzureFilesList if Files.startswith('A')]
                AzureFilesList2 = [Files for Files in AzureFilesList1 if not Files.startswith('A000')]
                
                #GPSログファイル名に含むデバイスSNと照合して、どのデバイスのログなのか紐づけ
                DeviceList = selected_devicesn_list
                for i in range(len(DeviceList)):
                    DeviceList[i] = DeviceList[i].lstrip('SN4L')
                
                MatchDevices = []
                for elem1 in DeviceList:
                    if any(elem1 in elem2 for elem2 in AzureFilesList2):
                        MatchDevices.append(elem1)                       
                
                MatchFiles = []
                for file in range(len(MatchDevices)):
                    MatchFile = [s for s in AzureFilesList2 if MatchDevices[file] in s]
                    MatchFiles = MatchFiles + MatchFile
                    
                #絞り込まれて、対象の日時・選択されているデバイスのデータとして残ったファイルは、Datalakeコンテナからデータを取得し、DigitalystConsole deviceフォルダの各デバイスのフォルダに保存
                original_cd = Path.get_base_dir()
                for file in range(len(MatchFiles)):
                    print(MatchFiles[file])
                    AzCopyCopy = sync_token[0] + '/' + '1_CloudDataLink/' + MatchFiles[file]
                    AzCopyDestination = original_cd + '\\device\\SN4L' + MatchFiles[file][1:6]
                    
                    AzCopyCMD \
                    = 'azcopy.exe copy '\
                    + AzCopy1 + AzCopyCopy + AzCopy3\
                    + ' "' + AzCopyDestination + '"' \
                    
                    subprocess.call(AzCopyCMD)
                    
                    if MatchFiles[file][9:] == 'zip':
                        os.chdir (AzCopyDestination)
                        shutil.unpack_archive(MatchFiles[file])
                        os.remove(MatchFiles[file])
                        os.chdir (original_cd + '\\system\\sync')
                    
                    # self.m_grid325.SetCellValue(df_active_device[df_active_device.loc[:,'DeviceSN']\
                    # == 'SN4L' + MatchFiles[file][1:6]].index.values[0], 1, SQLsetup.import_[48])
                
                os.chdir (original_cd) #一応戻しておく
                show_success(_("warning_294"))
                return
                
            else:
                await show_error(_("token length error"))
                
        except Exception as e:
            await show_error(_("warning_295") + f" : {e}")
            return  
        Logger.info("クラウドダウンロード 終了")              
        return


    @classmethod
    async def import_task(
            self, copy_flg, calc_flg, cloud_download_flg, imu_calc_enable, output_df10_file,  
            df_active_device, volume_dict, local_dict, config:ConfigModel,
            update_progress, on_device_update, show_success, show_warning, show_error):
        Logger.info("インポート 開始")
        dialog_msg = ""

        # 事前に update_task している分の進捗
        progress = 0.1
        if update_progress:
            await update_progress(progress)
            
        #deviceのSNとなるDevice IDと、各チームがGNSSに付けるTeam Device IDのテーブルを読み込み
        selected_devicesn_list = df_active_device["DeviceSN"].values.tolist()
        devicesn_teamdeviceid_dict = dict(zip(df_active_device['DeviceSN'], df_active_device['TeamDeviceID']))

        if len(selected_devicesn_list) == 0:
            await show_error(f"device is not selected.")
            return None
                
        if copy_flg:
            if cloud_download_flg:
                #クラウドデータ設定にチェックを入れ、『データ取得』ボタンをクリックした場合の処理
                #AzureのDatalakeのコンテナに保存されているGPSログを取得する
                #（GPSログファイルをスマホからdigitalyst_cloud@digitalyst.jpに送ると、Azure App Serviceにて、GPSログをDatalakeのコンテナに移動）
                await self.cloud_download(selected_devicesn_list, update_progress, on_device_update, show_warning, show_error)
                return None
        
            devicesn_driveletter_dict = {}
            for device_sn, volume in volume_dict.items():
                devicesn_driveletter_dict[volume['DeviceSN']] = volume['DriveLetter']
            volume_id_required = False
            for device_sn in selected_devicesn_list:
                if device_sn in devicesn_driveletter_dict.keys():
                    if devicesn_driveletter_dict[device_sn] == "":
                        volume_id_required = True

            get_vol_step = 0.1
            progress_vol_end = progress + get_vol_step
            devicesn_without_driveletter_volumeid_dict = {}
            if volume_id_required:
                try:
                    drive_letter_volume_id_dict, devicesn_without_driveletter_volumeid_dict = await windows_drive_service.get_volume_id_dicts(update_progress, progress, progress_vol_end)
                except Exception as e:
                    await show_error(f"get_volume_id_dicts Error : {e}")
                    return None
                
            progress = progress_vol_end
            if update_progress:
                await update_progress(progress) 
            progress_step = (1 - progress) / len(selected_devicesn_list)

            #-------------------------------------------------------------------------------------------  
            # ライセンスチェック　と　データコピー
            # ドライブレターの切替があるので、ライセンスチェック　と　データコピーをドライブアクセス時に同時に実施する

            SPECIFIED_DRIVE_LETTER = "Z"
            # ドライブレターなしを最後にソート
            sorted_device_dict = dict(sorted(devicesn_driveletter_dict.items(), key=lambda item: (item[1] == "", item[1])))
            field_calc_data = None #ImportDataFunction.get_field_calc_data()
            for device_sn, drive_letter in sorted_device_dict.items():
                if device_sn not in selected_devicesn_list:
                    continue
                
                if drive_letter == "":    
                    # ドライブレターなしはZドライブにマウント
                    if device_sn in devicesn_without_driveletter_volumeid_dict.keys():
                        volume_id = devicesn_without_driveletter_volumeid_dict[device_sn]
                        result = windows_drive_service.request_drive_letter_by_volume_object_id(volume_id, SPECIFIED_DRIVE_LETTER)
                        if not result:
                            await show_warning(f"request_drive_letter_by_volume_object_id Warning : {device_sn} : {drive_letter} drive")
                            continue
                        else:
                            drive_letter = SPECIFIED_DRIVE_LETTER
                    else:
                        await show_warning(f"No volume ID warning : {device_sn}")
                        continue
                    
                team_device_id = devicesn_teamdeviceid_dict[device_sn]
                license_key = usb_data_service.get_license_key(drive_letter, device_sn)
                valid, expire_date, err_msg = LicenseService.check(device_sn, license_key, team_device_id)
                if not valid:
                    await show_error(err_msg)
                    await on_device_update(device_sn, {'Progress' : ImportDeviceProgress.Failed})
                    dialog_msg += err_msg + "\n"
                else:                    
                    await self.copy_and_calculate(copy_flg, calc_flg, imu_calc_enable, output_df10_file,
                            drive_letter, device_sn, config,
                            progress, progress_step, update_progress,
                            on_device_update, show_success, show_warning, show_error)
                
                progress += progress_step
                if update_progress: 
                    await update_progress(progress)

        elif calc_flg:
            # 計算のみの場合はUSBメモリ確認しない
            device_df = DeviceModel.get_df()

            progress_step = (1 - progress) / len(selected_devicesn_list)
            for device_sn in selected_devicesn_list:
                
                team_device_id = devicesn_teamdeviceid_dict[device_sn]
                license_key = device_df.loc[device_df['DeviceSN'] == device_sn, 'LicenseKey'].values[0]
                valid, expire_date, err_msg = LicenseService.check(device_sn, license_key, team_device_id)
                if not valid:
                    await show_error(err_msg)
                    await on_device_update(device_sn, {'Progress' : ImportDeviceProgress.Failed})
                    dialog_msg += "\n" + err_msg
                else:
                    await self.copy_and_calculate(copy_flg, calc_flg, imu_calc_enable, output_df10_file,
                            None, device_sn, config,
                            progress, progress_step, update_progress,
                            on_device_update, show_success, show_warning, show_error)
                
                progress += progress_step
                if update_progress: 
                    await update_progress(progress)
                
        if update_progress: 
            await update_progress( 1 )

        Logger.info("インポート 終了")
        return dialog_msg 


    #-------------------------------------------------------------------------------------------
    #★分析対象ファイルに対する計算処理
    @classmethod
    async def copy_and_calculate(
            self, copy_flg, calc_flg, imu_calc_enable, output_df10_file,
            drive_letter, device_sn, config:ConfigModel,
            progress, progress_step, update_progress,
            on_device_update, show_success, show_warning, show_error):
        
        if copy_flg and calc_flg:
            copy_progress_step = progress_step / 4
            calc_progress_step = progress_step / 4 * 3
        elif copy_flg:
            copy_progress_step = progress_step
        elif calc_flg:
            calc_progress_step = progress_step
        else:
            return
        
        local_dir_path = Path.get_devicesn_FILE_dir(device_sn)   
        if copy_flg:
            Logger.info(f"{device_sn} コピー 開始")
            await on_device_update(device_sn, {'Progress' : ImportDeviceProgress.Copy})
            await asyncio.sleep(0.5)
            await ImportFile.copy_new_files_from_drive(device_sn, drive_letter, local_dir_path, imu_calc_enable, show_warning, show_error)
            if imu_calc_enable:
                await ImportFile.zip_CI_files(local_dir_path, show_warning, show_error)
            progress += copy_progress_step
            if update_progress: 
                await update_progress(progress)
            Logger.info(f"{device_sn} コピー 終了")

        if calc_flg:
            Logger.info(f"{device_sn} 計算 開始")
            await on_device_update(device_sn, {'Progress' : ImportDeviceProgress.Calculate})
            await asyncio.sleep(0.5)
            file_path_list = ImportFile.get_CG_file_path_list(local_dir_path)

            if len(file_path_list) > 0:
                calc_file_progress_step = calc_progress_step / len(file_path_list)
                for file_path in file_path_list:
                    result, err_str = await self.calculate_file(file_path, device_sn, config, output_df10_file, show_success, show_warning, show_error)
                    if os.path.exists(file_path):
                        if result:
                            file_name = os.path.basename(file_path)
                            calculated_zip_file_name = ImportFile.get_calculated_zip_file_name(file_name)
                            calculated_zip_file_path = os.path.join(local_dir_path, calculated_zip_file_name)
                            await ImportFile.zip_and_remove_file(file_path, calculated_zip_file_path, show_warning)
                        else:
                            error_dir = Path.get_data_device_error_dir()
                            if not os.path.exists(error_dir):
                                os.makedirs(error_dir)
                            date_str = datetime.now().strftime('%Y%m%d%H%M%S')
                            file_name = os.path.basename(file_path)
                            error_file_name = f"{date_str}_{err_str}_{file_name}"
                            error_zip_file_name = error_file_name.replace(".TXT", ".ZIP")
                            error_zip_file_path = os.path.join(error_dir, error_zip_file_name)
                            await ImportFile.zip_and_remove_file(file_path, error_zip_file_path, show_warning)     
                            Logger.warning(f"Import Error ファイル保存 : {error_zip_file_path}")                       
                    else:
                        Logger.warning(f"ファイルがなし ZIP化スキップ : {file_path}")
                    progress += calc_file_progress_step
                    if update_progress: 
                        await update_progress(progress)
        
        update_device_info = await windows_drive_service.get_local_info(device_sn)
        if drive_letter:
            update_device_info['StorageFileCount'], update_device_info['StorageLatestFileDate'], update_device_info['ImportFileCount'] = windows_drive_service.get_file_list(drive_letter)
        update_device_info['Progress'] = ImportDeviceProgress.Completed
        await on_device_update(device_sn, update_device_info)
        await asyncio.sleep(0.5)
            
        return True
    
    @classmethod
    async def calculate_file(self, file_path, device_sn, config, output_df10_file, show_success, show_warning, show_error):
        Logger.info(f"{file_path} 計算 開始")

        # 10Hzデータ作成
        start_time = time.time()
        success, df10, err_str = await CreateDf10.do(file_path, device_sn, config, show_success, show_warning, show_error)
        Logger.info(f"CreateDf10.do : {time.time() - start_time:.3f} 秒")

        if not success:
            Logger.info(f"{file_path} 異常終了")
        else:
            if df10 is None or df10.empty:
                Logger.info(f"{file_path} 計算データなし終了")
            else:
                Logger.info(f"{file_path} 計算開始")
                # 念のため日付毎にGroupBy
                df10['Date_str'] = df10['TimeStamp'].dt.strftime('%Y%m%d')
                date_df10_dict = {date: group for date, group in df10.groupby('Date_str')}
                file_name = os.path.basename(file_path)
                for date in date_df10_dict.keys():
                    
                    # DB準備
                    ImportDatabaseManager.initialize(device_sn, date)
                    ImportGnssModel.update_session()
                    ImportGnssSecModel.update_session()
                    ImportGnssFieldInLogModel.update_session()
                    ImportGnssTraceConfigModel.update_session()
                    ImportGnssTraceFieldModel.update_session()


                    # 既にファイル読込済みの場合は過去データ削除
                    items = ImportGnssTraceConfigModel.load_by_file_name(file_name)
                    if items and len(items) > 0:
                        for item in items:
                            try:
                                ImportGnssModel.delete_by_trace_config_ids([item.ID])
                            except:
                                pass
                            try:
                                ImportGnssSecModel.delete_by_trace_config_ids([item.ID])
                            except:
                                pass
                            try:
                                ImportGnssFieldInLogModel.delete_by_trace_config_ids([item.ID])
                            except:
                                pass
                            try:
                                ImportGnssTraceFieldModel.delete_by_trace_config_ids([item.ID])
                            except:
                                pass
                            try:
                                ImportGnssTraceConfigModel.delete_by_id(item.ID)
                            except:
                                pass

                    # 10HzデータDB保存
                    trace_config_item = {"FileName": file_name, "ConfigID": config.selected_id, "ConfigJson" : config.to_json()}
                    trace_config_id = ImportGnssTraceConfigModel.upsert(**trace_config_item)
                    df10_date = date_df10_dict[date]
                    df10_date['TraceConfigID'] = trace_config_id
                    start_time = time.time()
                    ImportGnssModel.save_df(df10_date)
                    Logger.info(f"ImportGnssModel.save_df : ({len(df10_date)}) => {time.time() - start_time:.3f} 秒")

                    # 10HzデータCSV出力
                    if output_df10_file:
                        start = df10_date.iloc[0]["TimeStamp"]
                        file_start_time = start.strftime("%H%M%S")  # 例: "132501"
                        df11 = df10_date[['Time_UTC', 'Lat', 'Lon', 'TotalDist', 'Speed2', 'Acceleration2', 'Hacc', 'HDOP', 'SVNum']] #Total => Distance
                        # 列名は wxpython 形式に合わせている
                        df11 = df11.rename(columns={'TotalDist': 'Distance', 'Speed2': 'Speed', 'Acceleration2': 'Acceleration'})
                        dir_path = Path.get_data_export_dir()
                        if not os.path.exists(dir_path):
                            os.makedirs(dir_path)
                        file_name = f"{date}_{file_start_time}_{device_sn}_10Hz.csv"
                        file_path = os.path.join(dir_path, file_name)
                        df11.to_csv(file_path, encoding='utf_8_sig')
                        Logger.info(f"df11.to_csv : ({file_name}) => {time.time() - start_time:.3f} 秒")

                    # 1秒毎データ保存（グラフ用）
                    start_time = time.time()
                    df10_date_grouped_by_1sec = df10_date.groupby(df10_date.index.floor('S')).agg({
                        'MaxSpeed': 'max',
                        'MaxAccel': 'max',
                        'Hacc': 'min',
                        'HDOP': 'max',
                        'SVNum': 'min',
                        'TraceConfigID': 'first'
                    }).reset_index().rename(columns={'index': 'TimeStamp'})
                    ImportGnssSecModel.save_df(df10_date_grouped_by_1sec)
                    Logger.info(f"ImportGnssSecModel.save_df : {len(df10_date_grouped_by_1sec)} => {time.time() - start_time:.3f} 秒")

                    # デジタルフェンス有効時間保存
                    start_time = time.time()
                    df_field_in_log = FieldDataService.get_df_field_in_log(df10, field_calc_duration_sec=1)
                    if not df_field_in_log.empty:
                        df_field_in_log['TraceConfigID'] = trace_config_id
                        field_id_to_trace_field_id = {}
                        for field_id in set(df_field_in_log['FieldID']):
                            field = FieldModel.load_by_id(field_id)
                            trace_field_item = {"TraceConfigID": trace_config_id, "FieldID": field.ID, "FieldJson": field.to_json()}
                            trace_field_id = ImportGnssTraceFieldModel.upsert(**trace_field_item)
                            field_id_to_trace_field_id[field_id] = trace_field_id
                        df_field_in_log['TraceFieldID'] = df_field_in_log['FieldID'].map(field_id_to_trace_field_id)
                        ImportGnssFieldInLogModel.save_df(df_field_in_log)
                        Logger.info(f"ImportGnssFieldInLogModel.save_df : {len(df_field_in_log)} => {time.time() - start_time:.3f} 秒")

                Logger.info(f"{file_path} 計算 終了")

        return success, err_str