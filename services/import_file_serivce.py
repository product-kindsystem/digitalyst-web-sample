import os
import sys
import shutil
from datetime import datetime, date, timedelta
from models.registration.device_model import DeviceModel
from models.registration.systemsetting_model import SystemSettingModel
from services.path_serivce import Path
from services.logger_service import Logger
import zipfile
import glob


class ImportFile:
    
    @staticmethod
    def get_imported_db_date_list():
        device_sn_list = DeviceModel.get_device_sn_list()
        date_list = []
        for device_sn in device_sn_list:
            import_path = os.path.join(Path.get_base_dir(), "Data", "Device", device_sn, "GNSS")            
            if os.path.exists(import_path):
                for filename in os.listdir(import_path):
                    if filename.endswith(".db") and "_GNSS_" in filename:
                        date_str = filename.split("_GNSS_")[0]
                        try:
                            date_dt = datetime.strptime(date_str, "%Y%m%d")
                            if date_dt not in date_list:
                                date_list.append(date_dt)
                        except ValueError as e:
                            Logger.warning(f"{filename} : {e}")
        return date_list
    
    @staticmethod
    def get_imported_devicesn_list_by_date(date:datetime):
        date_str = date.strftime("%Y%m%d")
        device_sn_list = DeviceModel.get_device_sn_list()
        imported_device_sn_list = []
        for device_sn in device_sn_list:
            imported_date_db_path = os.path.join(Path.get_base_dir(), "Data", "Device", device_sn, "GNSS", f"{date_str}_GNSS_{device_sn}.db") # 20250315_GNSS_SN5LDQPE5.db        
            if os.path.exists(imported_date_db_path):
                imported_device_sn_list.append(device_sn)
        return imported_device_sn_list

    @staticmethod    
    def get_CG_file_path_list(local_dir_path):
        file_path_list = []
        if os.path.exists(local_dir_path):
            for file_name in os.listdir(local_dir_path):
                if ImportFile.is_CG_file(file_name):
                    file_path = os.path.join(local_dir_path, file_name)
                    file_path_list.append(file_path)
        return file_path_list


    @staticmethod    
    async def zip_CI_files(local_dir_path, show_warning, show_error):
        file_name = ""
        if os.path.exists(local_dir_path):
            for file_name in os.listdir(local_dir_path):
                if ImportFile.is_CI_file(file_name):
                    try:
                        file_path = os.path.join(local_dir_path, file_name)

                        # ZIPファイルの名前を決定
                        zip_file_name = os.path.splitext(file_name)[0] + ".ZIP"
                        zip_file_path = os.path.join(local_dir_path, zip_file_name)

                        # ZIP化の処理
                        with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                            zipf.write(file_path, arcname=file_name)  # 元ファイルをZIPに追加

                        Logger.info(f"{file_name} をZIP化しました: {zip_file_name}")

                        # 元のファイルを削除
                        os.remove(file_path)
                        Logger.info(f"{file_name} を削除しました。")
                    except Exception as e:
                        await show_warning(f"File ZIP Warning : {file_name}")
        return True
    
    @staticmethod    
    async def zip_and_remove_file(file_path, zip_file_path, show_warning):
        try:
            # ZIP化の処理
            with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(file_path, arcname=os.path.basename(file_path))  # 元ファイルをZIPに追加

            Logger.info(f"{file_path} をZIP化しました: {zip_file_path}")

            # 元のファイルを削除
            os.remove(file_path)
            Logger.info(f"{file_path} を削除しました。")
        except Exception as e:
            await show_warning(f"File ZIP Warning : {file_path}")
        return True

    @staticmethod    
    async def copy_new_files_from_drive(device_sn, drive_letter, local_dir_path, imu_calc_enable, show_warning, show_error):
        temp_renamed_file_names, failed_file_names, file_name, copy_count = [], [], "", 0
        source_dir_path = f'{drive_letter}:\\'  # 例: 'E:\\'
        # コピー先ディレクトリが存在しない場合は作成
        if not os.path.exists(local_dir_path):
            os.makedirs(local_dir_path)
            Logger.info(f"コピー先フォルダ {local_dir_path} が作成されました。")

        auto_delete = SystemSettingModel.AutoDeleteDeviceDataEnable
        auto_delete_days = SystemSettingModel.AutoDeleteDeviceDataDays

        # ドライブ内のすべてのファイルとフォルダをコピー
        for file_name in os.listdir(source_dir_path):
            source_file_path = os.path.join(source_dir_path, file_name)
            upper_file_name = file_name.upper() # ファイル名：CA20240927_082449.TXT
            
            # CAで始まり、拡張子がTXTのファイルのみコピー            
            if ImportFile.is_not_copied_file_name(upper_file_name):
                if not imu_calc_enable and ImportFile.is_AI_file(upper_file_name):
                    # Skip to move IMU file
                    continue
                copied_file_name = ImportFile.get_copied_file_name(device_sn, upper_file_name, source_file_path)
                dest_file_path = os.path.join(local_dir_path, copied_file_name)
                # レアケースだが同名ファイルが存在するケースがあるため、その場合は末尾に日時を付与
                if os.path.exists(dest_file_path):
                    current_time = datetime.now().strftime("%Y%m%d%H%M%S%f")
                    upper_file_name = upper_file_name.replace(".TXT", f"_{current_time}.TXT")
                    copied_file_name = ImportFile.get_copied_file_name(device_sn, upper_file_name, source_file_path)
                    dest_file_path = os.path.join(local_dir_path, copied_file_name)
                src_rename_file_path = os.path.join(source_dir_path, copied_file_name)
                try:
                    # ファイルのコピー
                    shutil.copy2(source_file_path, dest_file_path)
                    os.rename(source_file_path, src_rename_file_path)
                    Logger.info(f"{source_file_path} を {dest_file_path} にコピーしました。")
                    temp_renamed_file_names.append(copied_file_name)
                    source_file_path = src_rename_file_path
                    copy_count += 1
                except Exception as e:
                    try:
                        if os.path.exists(dest_file_path):
                            os.remove(dest_file_path)
                            Logger.info(f"{dest_file_path} を削除しました。")
                    except Exception as e:
                        Logger.error(f"削除し中にエラーが発生しました", e)
                    try:
                        if os.path.exists(src_rename_file_path):      
                            os.rename(src_rename_file_path, source_file_path)
                            Logger.info(f"{src_rename_file_path} をリネームして元のファイル名に戻しました。")
                    except Exception as e:
                        Logger.error(f"{src_rename_file_path} リネームして元のファイル名に戻し中にエラーが発生しました", e)
                    failed_file_names.append(file_name)
                    await show_warning(f"File Copy Warning : {device_sn} : {drive_letter} drive : {file_name}")
                      
            # デバイスファイル自動削除
            if auto_delete:
                for root, dirs, files in os.walk(source_dir_path):
                    for file in files:
                        file_path = os.path.join(root, file)
                        # --- 削除チェック ---
                        if len(file) >= 10 and file[2:10].isdigit():
                            try:
                                file_date = datetime.strptime(file[2:10], "%Y%m%d").date()
                                threshold_date = datetime.today().date() - timedelta(days=auto_delete_days)
                                if file_date < threshold_date:
                                    os.remove(file_path)
                                    Logger.info(f"デバイスファイル自動削除 : {file_path}")
                                    continue
                            except Exception as ex:
                                Logger.warning(f"デバイスファイル自動削除 日付判定エラー: {file} / {ex}")

        Logger.info(f"{source_dir_path} から {local_dir_path} へのコピーが完了しました。")
        return True

    @staticmethod
    def is_IMU_file(file_name:str):
        return ImportFile.is_AI_file(file_name) or ImportFile.is_CI_file(file_name) or ImportFile.is_DI_file(file_name)

    # for USB drive
    @staticmethod
    def is_target_file_name(file_name:str):
        if ImportFile.is_IMU_file(file_name) and not SystemSettingModel.ImuCalcEnable: return False 
        if ImportFile.is_CA_file(file_name): return False #暫定対応
        return ImportFile.is_A_file(file_name) or ImportFile.is_CA_file(file_name) or ImportFile.is_AG_file(file_name) or ImportFile.is_AI_file(file_name) or ImportFile.is_CG_file(file_name) or ImportFile.is_CI_file(file_name) or ImportFile.is_DG_file(file_name) or ImportFile.is_DI_file(file_name)

    # for USB drive
    @staticmethod
    def is_not_copied_file_name(file_name:str):
        if ImportFile.is_IMU_file(file_name) and not SystemSettingModel.ImuCalcEnable: return False 
        if ImportFile.is_CA_file(file_name): return False #暫定対応
        return ImportFile.is_A_file(file_name) or ImportFile.is_CA_file(file_name) or ImportFile.is_AG_file(file_name) or ImportFile.is_AI_file(file_name)
    
    # for USB drive
    @staticmethod
    def is_copied_file_name(file_name:str):
        if ImportFile.is_IMU_file(file_name) and not SystemSettingModel.ImuCalcEnable: return False 
        return ImportFile.is_CG_file(file_name) or ImportFile.is_CI_file(file_name)

    # for PC local
    @staticmethod
    def is_imported_file_name(file_name:str):
        if ImportFile.is_IMU_file(file_name) and not SystemSettingModel.ImuCalcEnable: return False 
        return ImportFile.is_CG_file(file_name) or ImportFile.is_CI_file(file_name) or ImportFile.is_DG_file(file_name) or ImportFile.is_DI_file(file_name)
    
    # for PC local
    @staticmethod
    def is_calc_file_name(file_name:str):
        if ImportFile.is_IMU_file(file_name) and not SystemSettingModel.ImuCalcEnable: return False 
        return ImportFile.is_CG_file(file_name) or ImportFile.is_CI_file(file_name)   
    
    @staticmethod
    def get_copied_file_name(device_sn, file_name: str, source_file_path):
        copied_file_name = ""

        # CAファイルの場合
        if ImportFile.is_CA_file(file_name):
            # CA日付_時間.TXT
            temp_file_name = file_name.replace("CA", "CG", 1)
            temp_file_name = temp_file_name.upper().replace(".TXT", "")
            copied_file_name = f"{temp_file_name}_{device_sn}.TXT"

        # AGファイルの場合
        elif ImportFile.is_AG_file(file_name):
            # AG日付_時間_デバイスシリアル.TXT
            copied_file_name = file_name.replace("AG", "CG", 1)

        # AIファイルの場合
        elif ImportFile.is_AI_file(file_name):
            # AI日付_時間_デバイスシリアル.TXT
            copied_file_name = file_name.replace("AI", "CI", 1)

        # Aファイルの場合
        elif ImportFile.is_A_file(file_name):
            # 日付時間は、ファイルの最終更新日時から決定する
            # 最終更新日時を取得
            try:
                file_mod_time = os.path.getmtime(source_file_path)
                date_time = datetime.fromtimestamp(file_mod_time).strftime("%Y%m%d_%H%M%S")
                copied_file_name = f"CG{date_time}_{device_sn}.TXT"
            except Exception as e:
                Logger.error(f"ファイルの最終更新日時を取得できませんでした: {source_file_path}", e)
                copied_file_name = ""

        return copied_file_name

    @staticmethod
    def get_date_str(file_name:str, source_file_path):        
        if ImportFile.is_A_file(file_name):
            # 日付時間は、ファイルの最終更新日時から決定する
            # 最終更新日時を取得
            try:
                file_mod_time = os.path.getmtime(source_file_path)
                return datetime.fromtimestamp(file_mod_time)
            except Exception as e:
                Logger.error(f"ファイルの最終更新日時を取得できませんでした: {source_file_path}", e)
                return None
        date_str = file_name[2:17]
        date_time = datetime.strptime(date_str, "%Y%m%d_%H%M%S")
        return date_time
    
    @staticmethod
    def get_calculated_zip_file_name(file_name: str):
        calculated_file_name = ""

        # CGファイルの場合
        if ImportFile.is_CG_file(file_name):
            # CG日付_時間_デバイスシリアル.TXT
            calculated_file_name = file_name.replace("CG", "DG", 1).replace(".TXT", ".ZIP")

        # CIファイルの場合
        elif ImportFile.is_CI_file(file_name):
            # CI日付_時間_デバイスシリアル.TXT
            calculated_file_name = file_name.replace("CI", "DI", 1).replace(".TXT", ".ZIP")

        return calculated_file_name
    
    # Old Raw GNSS file (Not copied)
    # A0000001.TXT
    @staticmethod
    def is_A_file(file_name:str):
        file_name = file_name.upper()
        return file_name.startswith('A') and file_name.endswith('.TXT') and not ImportFile.is_AG_file(file_name) and not ImportFile.is_AI_file(file_name)
    
    # Old CA GNSS file (Not copied)
    # CA20241002_194252.TXT
    @staticmethod
    def is_CA_file(file_name:str):
        file_name = file_name.upper()
        return file_name.startswith('CA') and file_name.endswith('.TXT')
    
    # GNSS file (Not copied)
    # AG20250113_144254_SN5LDQPEH.txt
    @staticmethod
    def is_AG_file(file_name:str):
        file_name = file_name.upper()
        return file_name.startswith('AG') and file_name.endswith('.TXT')
    
    # IMU file (Not copied)
    # AI20250113_144254_SN5LDQPEH.txt
    @staticmethod
    def is_AI_file(file_name:str):
        file_name = file_name.upper()
        return file_name.startswith('AI') and file_name.endswith('.TXT')
    
    # GNSS file (Copied)
    # CG20250113_144254_SN5LDQPEH.txt
    @staticmethod
    def is_CG_file(file_name:str):
        file_name = file_name.upper()
        return file_name.startswith('CG') and (file_name.endswith('.TXT') or file_name.endswith('.ZIP'))
    
    # IMU file (Copied)
    # CI20250113_144254_SN5LDQPEH.txt
    @staticmethod
    def is_CI_file(file_name:str):
        file_name = file_name.upper()
        return file_name.startswith('CI') and (file_name.endswith('.TXT') or file_name.endswith('.ZIP'))
    
    # GNSS file (Calculated)
    # DG20250113_144254_SN5LDQPEH.txt
    @staticmethod
    def is_DG_file(file_name:str):
        file_name = file_name.upper()
        return file_name.startswith('DG') and (file_name.endswith('.TXT') or file_name.endswith('.ZIP'))
    
    # IMU file (Calculated)
    # DI20250113_144254_SN5LDQPEH.txt
    @staticmethod
    def is_DI_file(file_name:str):
        file_name = file_name.upper()
        return file_name.startswith('DI') and (file_name.endswith('.TXT') or file_name.endswith('.ZIP'))
    
    # -----------------------------------------------------------------

    # Zip解凍再計算用    
    @staticmethod
    def get_imported_zip_file_path_devicesn_dict_by_date(date:date):
        date_str = date.strftime("%Y%m%d")
        device_sn_list = DeviceModel.get_device_sn_list()
        dict = {}
        for device_sn in device_sn_list:
            dir_path = Path.get_devicesn_FILE_dir(device_sn)
            if os.path.exists(dir_path) and os.path.isdir(dir_path):
                pattern = os.path.join(dir_path, f"DI{date_str}*.zip")
                for file_path in glob.glob(pattern):
                    dict[file_path] = device_sn
                pattern = os.path.join(dir_path, f"DG{date_str}*.zip")
                for file_path in glob.glob(pattern):
                    dict[file_path] = device_sn
        return dict

    @staticmethod
    def extract_txt_file_from_zip(zip_path, dir_path):
        if not os.path.isfile(zip_path):
            return None
        extracted_path = None
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # ZIP内のファイル一覧から、.TXT拡張子のものだけ
            for member in zip_ref.namelist():
                if member.upper().endswith(".TXT"):
                    # ディレクトリに展開
                    extracted_path = os.path.join(dir_path, os.path.basename(member))
                    with open(extracted_path, 'wb') as f:
                        f.write(zip_ref.read(member))
                    break
        return extracted_path
    

