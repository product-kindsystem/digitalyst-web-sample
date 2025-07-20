import os
import sys


class Path:
    
    @staticmethod
    def get_base_dir():
        if getattr(sys, 'frozen', False):  # exe 化された場合
            base_dir = os.path.dirname(sys.executable)
            return os.path.dirname(base_dir)  # 1つ上の階層を返す
        else:
            return os.getcwd()
        
        
    @staticmethod
    def get_backup_dir():        
        return os.path.join(Path.get_base_dir(), "Backup")
    
    @staticmethod
    def get_system_dir():        
        return os.path.join(Path.get_base_dir(), "System")
    
    @staticmethod
    def get_system_sync_dir():        
        return os.path.join(Path.get_base_dir(), "System", "Sync")
    
    @staticmethod
    def get_data_dir():        
        return os.path.join(Path.get_base_dir(), "Data")
    
    @staticmethod
    def get_data_log_dir():        
        return os.path.join(Path.get_base_dir(), "Data", "Log")
    
    @staticmethod
    def get_data_tracking_dir():        
        return os.path.join(Path.get_base_dir(), "Data", "Tracking")
    
    @staticmethod
    def get_data_tracking_log_dir():        
        return os.path.join(Path.get_base_dir(), "Data", "Tracking", "Log")
    
    @staticmethod
    def get_data_export_dir():        
        return os.path.join(Path.get_base_dir(), "Data", "Export")
    
    @staticmethod
    def get_data_live_dir():        
        return os.path.join(Path.get_base_dir(), "Data", "Live")
    
    @staticmethod
    def get_data_temp_dir():        
        return os.path.join(Path.get_base_dir(), "Data", "Temp")
    
    @staticmethod
    def get_data_temp_recalculate_dir():        
        return os.path.join(Path.get_base_dir(), "Data", "Temp", "Recalculate")
    
    @staticmethod
    def get_data_temp_update_license_dir():        
        return os.path.join(Path.get_base_dir(), "Data", "Temp", "UpdateLicense")
    
    @staticmethod
    def get_data_temp_update_device_info_dir():        
        return os.path.join(Path.get_base_dir(), "Data", "Temp", "UpdateDeviceInfo")
    
    @staticmethod
    def get_data_device_dir():        
        return os.path.join(Path.get_base_dir(), "Data", "Device")
    
    @staticmethod
    def get_data_device_error_dir():        
        return os.path.join(Path.get_base_dir(), "Data", "Device", "Error")
    
    @staticmethod
    def get_devicesn_dir(device_sn):
        return os.path.join(Path.get_base_dir(), "Data", "Device", device_sn)
    
    @staticmethod
    def get_devicesn_ID_dir(device_sn):        
        return os.path.join(Path.get_base_dir(), "Data", "Device", device_sn, ".ID")
    
    @staticmethod
    def get_devicesn_KEY_dir(device_sn):        
        return os.path.join(Path.get_base_dir(), "Data", "Device", device_sn, ".KEY")
    
    @staticmethod
    def get_devicesn_SN_dir(device_sn):        
        return os.path.join(Path.get_base_dir(), "Data", "Device", device_sn, "SN")
    
    @staticmethod
    def get_devicesn_FILE_dir(device_sn):        
        return os.path.join(Path.get_base_dir(), "Data", "Device", device_sn, "FILE")
    
    @staticmethod
    def get_devicesn_GNSS_dir(device_sn):        
        return os.path.join(Path.get_base_dir(), "Data", "Device", device_sn, "GNSS")
    
    @staticmethod
    def get_devicesn_IMU_dir(device_sn):        
        return os.path.join(Path.get_base_dir(), "Data", "Device", device_sn, "IMU")
    
    @staticmethod
    def get_drive_ID_dir(drive_letter):        
        return os.path.join(drive_letter + ":", ".ID")
    
    @staticmethod
    def get_drive_ID_file(drive_letter):        
        return os.path.join(drive_letter + ":", ".ID", ".ID.TXT")
    
    @staticmethod
    def get_drive_TeamDeviceID_file(drive_letter):        
        return os.path.join(drive_letter + ":", ".ID", ".TeamDeviceID.TXT")
    
    @staticmethod
    def get_drive_KEY_dir(drive_letter):        
        return os.path.join(drive_letter + ":", ".KEY")
    
    @staticmethod
    def get_drive_KEY_file(drive_letter, device_sn):        
        return os.path.join(drive_letter + ":", ".KEY", f".{device_sn}.KEY")
    
    @staticmethod
    def get_drive_SN_dir(drive_letter):        
        return os.path.join(drive_letter + ":", "SN")
    
    @staticmethod
    def get_drive_MISC_dir(drive_letter):        
        return os.path.join(drive_letter + ":", "MISC")
    
    @staticmethod
    def get_drive_wifi_setting_file(drive_letter):        
        return os.path.join(drive_letter + ":", "MISC", "settings.txt")