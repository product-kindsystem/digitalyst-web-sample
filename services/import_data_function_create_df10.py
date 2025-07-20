import os
import pandas as pd
import time
from datetime import timedelta
from decimal import ROUND_HALF_UP
import mmap
import subprocess
import pandas.tseries.offsets as offsets
from scipy.signal import butter, filtfilt
import traceback
import numpy as np
from numpy.fft import fftn, ifftn, fftfreq
import joblib
import math
from pytz import timezone
from models.registration.config_model import ConfigModel
from models.registration.threshold_model import ThreEnum
from services import detect_peaks
from services.path_serivce import Path
from services.localization_service import LocalizationService, _
from services.logger_service import Logger



class CreateDf10:    

    async def _create_raw_df10(
            file_path, device_sn, config:ConfigModel,
            show_success, show_warning, show_error):
        
        start_time = time.time()
        timezone_delta = LocalizationService.get_timezone_offset()
        
        #単位、日付、時間変換用の関数
        # def knot2kmh_convert(knot):
        #     kmh = knot * 1.852
        #     return kmh
            
        #NMEAフォーマットの緯度経度を通常の緯度経度に変換する関数
        def nmea_convert(nmea):
            decimal, integer = math.modf(nmea/100.0)
            gnss = integer + decimal / 60.0 * 100.0
            return gnss
                                                
        #ファイル内に存在する行数確認
        f, buf, line_count, size = None, None, 0, 0
        try:
            f = open(file_path, "r")
            size = os.path.getsize(file_path)
            buf = mmap.mmap(f.fileno(), 0, access = mmap.ACCESS_READ)
            readline = buf.readline
            while readline():
                line_count += 1
            buf.close()
            f.close()
            f = None
            line_count = line_count - 1
        except Exception as e:
            try:
                if buf:
                    buf.close()
                    buf = None
            except:
                pass
            try:
                if f:
                    f.close()
                    f = None
            except:
                pass
            err_str = 'MmapValueError'
            Logger.error(f'{err_str} : {file_path}', e)
            return False, None, err_str
        
        #行数が100行以下の場合は、10秒以内のデータであり、有効な分析にはならない為、対象除外
        if line_count < 100 and line_count != 0:
            err_str = 'LineCountLessThan1000Warning'
            Logger.warning(f'{err_str} : {file_path}')
            return False, None, err_str
        
        #読込みできるがline_countが1行になってしまうケースへの対応
        #一旦、読込みTXTに再書き込みして、再度読みだす
        if line_count == 0:                            
            try:
                f = open(file_path, 'r', encoding="utf_8")
                line = f.read()
                f.close()
                f = None
                os.remove(file_path)
                
                f = open(file_path, "w", encoding = "utf_8")
                f.write(line)
                f.close()
                f = None

                f = open(file_path, "r")
                buf = mmap.mmap(f.fileno(), 0, access = mmap.ACCESS_READ)
                line_count = 0
                readline = buf.readline
                while readline():
                    line_count += 1
                buf.close()
                buf = None
                f.close()
                f = None
                line_count = line_count - 1
            except Exception as e:
                # 安全にクローズ・削除
                try:
                    if buf:
                        buf.close()
                        buf = None
                except:
                    pass
                try:
                    if f:
                        f.close()
                        f = None
                except:
                    pass
                err_str = 'LineCount0RetryError'
                Logger.error(f'{err_str} : {file_path}', e)
                return False, None, err_str
                
        #TXTのGPSログをPandasデータフレームに変換する。データ欠損やデータが想定外のフォーマットになっている場合の処理を入れている。
        #上手く取りこめた場合は、import_completeを1にする。
        #過去に色々なデータ異常によって、データ取込みがエラーで止まる or データ取込みが出来ない、という事が発生し、複数のTry処理が貼っている。
        import_complete = False
        
        try:                            
            
            for x in range(0, 40):
                try:
                    df0 = pd.read_csv(file_path, header=None, delimiter=",", usecols=[0,1,2,3,5,7,8,9,11,13,15,18],\
                    dtype = {0:str, 1:str, 2:str, 3:str, 5:str, 7:str, 8:str, 9:str, 11:str, 13:str, 15:str, 18:str},\
                    skiprows = range(0,40+x), nrows = line_count-(41+x), on_bad_lines='warn',\
                    memory_map=True)
                    import_complete = True
                except ValueError:
                    pass                          
            
            if not import_complete:
                temp_file = os.path.join(Path.get_devicesn_dir(device_sn), "A90000000.txt")
                error_file = open(file_path, "r", encoding="utf_8")
                fileobj = open(temp_file, "w", encoding = "utf_8")
                for lines in range(line_count):
                    try:
                        line = error_file.readline()
                        fileobj.write(line)
                    except UnicodeDecodeError:
                        pass
                fileobj.close()
                error_file.close()
                os.remove(file_path)
                os.rename(temp_file, file_path)                            
            
            if not import_complete:
                for x in range(0, 40):
                    try:
                        df0 = pd.read_csv(file_path, header=None, delimiter=",", usecols=[0,1,2,3,5,7,8,9,11,13,15,18],\
                        dtype = {0:str, 1:str, 2:str, 3:str, 5:str, 7:str, 8:str, 9:str, 11:str, 13:str, 15:str, 18:str},\
                        skiprows = range(0,40+x), nrows = line_count-(41+x), on_bad_lines='warn',\
                        memory_map=True)
                        import_complete = True
                    except ValueError:
                        pass                         
            
            if not import_complete: #195r.py
                temp_file = os.path.join(Path.get_devicesn_dir(device_sn), "temporarily.txt")
                for y in range(2):
                    if not import_complete:
                        for x in range(0, 10000):#10000
                            try:
                                df0 = pd.read_csv(file_path, header=None, delimiter=",", usecols=[0,1,2,3,5,7,8,9,11,13,15,18],\
                                dtype = {0:str, 1:str, 2:str, 3:str, 5:str, 7:str, 8:str, 9:str, 11:str, 13:str, 15:str, 18:str},\
                                skiprows = range(0,40), nrows = line_count-(60*x), on_bad_lines='warn',\
                                memory_map=True)
                                import_complete = True #195r.py
                            except ValueError:
                                with open(file_path, "r") as input:
                                #input = open(ifn, "r")
                                    with open(temp_file, "w") as output:
                                        for z in range(100):
                                            try:
                                                for line in input:
                                                    output.write(line)
                                            except UnicodeDecodeError:
                                                pass
                                try:
                                    os.remove(file_path)
                                    os.rename(temp_file, file_path)    
                                except PermissionError:
                                    pass
                                f = open(file_path, "r+")
                                size1 = os.path.getsize(file_path)
                                f.close()
                                if size == size1:
                                    break
                            else:
                                break
                    if size == size1:
                        break                          
            
            if not import_complete: #195r.py
                for x in range(10, 100):
                    try:
                        df0 = pd.read_csv(file_path, header=None, delimiter=",", usecols=[0,1,2,3,5,7,8,9,11,13,15,18],\
                        dtype = {0:str, 1:str, 2:str, 3:str, 5:str, 7:str, 8:str, 9:str, 11:str, 13:str, 15:str, 18:str},\
                        skiprows = range(0,40+x), nrows = line_count-(600*x), on_bad_lines='warn',\
                        memory_map=True)
                        import_complete = True #195r.py
                    except ValueError:
                        pass            
            
            if not import_complete: #195r.py
                for x in range(10, 100):
                    try:
                        df0 = pd.read_csv(file_path, header=None, delimiter=",", usecols=[0,1,2,3,5,7,8,9,11,13,15,18],\
                        dtype = {0:str, 1:str, 2:str, 3:str, 5:str, 7:str, 8:str, 9:str, 11:str, 13:str, 15:str, 18:str},\
                        skiprows = range(0,40+x), nrows = line_count-(6000*x), on_bad_lines='warn',\
                        memory_map=True)
                        import_complete = True #195r.py
                    except ValueError:
                        pass
            
            try:
                df0.columns = ['A','B','C','D','E','F','G','H','I','J','K','L']
            except UnboundLocalError:
                try:
                    df0 = pd.read_csv(file_path, header=None, delimiter=",", usecols=[0,1,2,3,5,7,8,9,11,13,15,18],\
                    dtype = {0:str, 1:str, 2:str, 3:str, 5:str, 7:str, 8:str, 9:str, 11:str, 13:str, 15:str, 18:str},\
                    skiprows = range(0,100+x), skipfooter = 100, engine= 'python', on_bad_lines='warn',  memory_map=True)
                except UnicodeDecodeError as e:
                    line_count = int(subprocess.check_output(['wc', '-l', file_path]).decode().split(',')[0].replace(' ', '').split('A')[0])
                    df0 = pd.read_csv(file_path, header=None, delimiter=",", usecols=[0,1,2,3,5,7,8,9,11,13,15,18],\
                    dtype = {0:str, 1:str, 2:str, 3:str, 5:str, 7:str, 8:str, 9:str, 11:str, 13:str, 15:str, 18:str},\
                    skiprows = range(0,100), nrows = line_count-100, on_bad_lines='warn',  memory_map=True)
                    err_str = 'DataReadUnicodeDecodeError'
                    Logger.error(f'{err_str} : {file_path}', e)
                    return False, None, err_str
                
                else:
                    df0 = pd.read_csv(file_path, header=None, delimiter=",", usecols=[0,1,2,3,5,7,8,9,11,13,15,18],\
                    dtype = {0:str, 1:str, 2:str, 3:str, 5:str, 7:str, 8:str, 9:str, 11:str, 13:str, 15:str, 18:str},\
                    skiprows = range(0,100+x), skipfooter = 100, engine= 'python', on_bad_lines='warn',  memory_map=True)
                    df0_len = len(df0)
                    df0 = pd.read_csv(file_path, header=None, delimiter=",", usecols=[0,1,2,3,5,7,8,9,11,13,15,18],\
                    dtype = {0:str, 1:str, 2:str, 3:str, 5:str, 7:str, 8:str, 9:str, 11:str, 13:str, 15:str, 18:str},\
                    skiprows = range(0,100+x), nrows = df0_len, on_bad_lines='warn',  memory_map=True)
                    df0.columns = ['A','B','C','D','E','F','G','H','I','J','K','L']
            except ValueError:
                df0.columns = ['A','B','C','D','E','F','G','H','I','J','K','L']
        
        except Exception as e:
            err_str = 'DataReadError'
            Logger.error(f'{err_str} : {file_path}', e)
            return False, None, err_str
        
        
        #正常にTXTファイルからdf0に変換できたデータに対する、詳細なデータクレンジング処理
        try:
            #NMEAフォーマットで出力されているレコードにて、PUBX（UbloxのNMEAオプション出力、GNRMCのみを使う）
            df0 = df0[df0.iloc[:,0] != "$GNTXT"]
            df0PUBX = df0[df0.iloc[:,0] == "$PUBX"]
            
            #Navigation Status
            df0PUBX = df0PUBX[(df0PUBX.iloc[:,6].str.match('..', na=False))&\
            (df0PUBX.iloc[:,6].str.len()==2)&\
            (df0PUBX.iloc[:,6] != "NF")]
            
            #df0PUBX.to_csv("df0PUBX.csv")
            
            b_p = len(df0PUBX)
            #msgId
            df0PUBX = df0PUBX[df0PUBX.iloc[:,1] == "00"]
            df0PUBX = df0PUBX.drop(df0PUBX.columns[[0,1,5,6]], axis=1) 
            
            #UTC hhmmss.ss
            df0PUBX = df0PUBX[(df0PUBX.iloc[:,0].str.match('[0-9]{6}\.[0-9]{2}', na=False))&\
            (~df0PUBX.iloc[:,0].str.contains('[a-zA-Z]', na=False))&\
            (~df0PUBX.iloc[:,0].str.contains('&', na=False))&\
            (~df0PUBX.iloc[:,0].str.contains('³', na=False))&\
            (df0PUBX.iloc[:,0].str.len()==9)]
            
            df0PUBX = df0PUBX[df0PUBX.iloc[:,0].str[:2].astype(float) < 24]
            df0PUBX = df0PUBX[df0PUBX.iloc[:,0].str[2:4].astype(float) < 60]
            df0PUBX = df0PUBX[df0PUBX.iloc[:,0].str[4:6].astype(float) < 60]
            df0PUBX = df0PUBX[df0PUBX.iloc[:,0].str[7:8].astype(float) < 10]
            
            #Lat ddmm.mmmmm
            df0PUBX = df0PUBX[(df0PUBX.iloc[:,1].str.match('[0-9]{4}\.[0-9]{5}', na=False))&\
            (~df0PUBX.iloc[:,1].str.contains('[a-zA-Z]', na=False))&\
            (~df0PUBX.iloc[:,1].str.contains('&', na=False))&\
            (~df0PUBX.iloc[:,1].str.contains('³', na=False))&\
            (df0PUBX.iloc[:,1].str.len()==10)]
            
            #Lon dddmm.mmmmm
            df0PUBX = df0PUBX[(df0PUBX.iloc[:,2].str.match('[0-9]{5}\.[0-9]{5}', na=False))&\
            (~df0PUBX.iloc[:,2].str.contains('[a-zA-Z]', na=False))&\
            (~df0PUBX.iloc[:,2].str.contains('&', na=False))&\
            (~df0PUBX.iloc[:,2].str.contains('³', na=False))&\
            (df0PUBX.iloc[:,2].str.len()==11)]
            
            #Horizontal accuracy estimate
            df0PUBX = df0PUBX[(df0PUBX.iloc[:,3].str.match('[0-9]{1,2}\.?[0-9]{0,2}', na=False))&\
            (~df0PUBX.iloc[:,3].str.contains('\.\.', na=False))&\
            (~df0PUBX.iloc[:,3].str.contains('\.[0-9]{0,2}\.', na=False))&\
            (~df0PUBX.iloc[:,3].str.contains('[a-zA-Z]', na=False))&\
            (~df0PUBX.iloc[:,3].str.contains('&', na=False))&\
            (~df0PUBX.iloc[:,3].str.contains('³', na=False))&\
            (df0PUBX.iloc[:,3].str.len()<6)]
            
            #Speed over ground knots
            df0PUBX = df0PUBX[(df0PUBX.iloc[:,4].str.match('[0-9]{1,2}\.?[0-9]{0,3}', na=False))&\
            (~df0PUBX.iloc[:,4].str.contains('\.\.', na=False))&\
            (~df0PUBX.iloc[:,4].str.contains('\.[0-9]{0,3}\.', na=False))&\
            (~df0PUBX.iloc[:,4].str.contains('[a-zA-Z]', na=False))&\
            (~df0PUBX.iloc[:,4].str.contains('&', na=False))&\
            (~df0PUBX.iloc[:,4].str.contains('³', na=False))&\
            (df0PUBX.iloc[:,4].str.len()<7)]
            
            #Vertical velocity m/s
            df0PUBX = df0PUBX[(df0PUBX.iloc[:,5].str.match('-?[0-9]{1,2}\.?[0-9]{0,3}', na=False))&\
            (~df0PUBX.iloc[:,5].str.contains('\.\.', na=False))&\
            (~df0PUBX.iloc[:,5].str.contains('\.[0-9]{0,4}\.', na=False))&\
            (~df0PUBX.iloc[:,5].str.contains('[a-zA-Z]', na=False))&\
            (~df0PUBX.iloc[:,5].str.contains('&', na=False))&\
            (~df0PUBX.iloc[:,5].str.contains('³', na=False))&\
            (df0PUBX.iloc[:,5].str.len()<8)]
            
            #HDOP
            df0PUBX = df0PUBX[(df0PUBX.iloc[:,6].str.match('[0-9]{1,2}\.?[0-9]{0,2}', na=False))&\
            (~df0PUBX.iloc[:,6].str.contains('\.\.', na=False))&\
            (~df0PUBX.iloc[:,6].str.contains('\.[0-9]{0,3}\.', na=False))&\
            (~df0PUBX.iloc[:,6].str.contains('[a-zA-Z]', na=False))&\
            (~df0PUBX.iloc[:,6].str.contains('&', na=False))&\
            (~df0PUBX.iloc[:,6].str.contains('³', na=False))&\
            (df0PUBX.iloc[:,6].str.len()<6)]
            
            #Number of satellites used in the navigationsolution
            df0PUBX = df0PUBX[(df0PUBX.iloc[:,7].str.match('[0-9]{1,2}', na=False))&\
            (~df0PUBX.iloc[:,7].str.contains('[^0-9]{1,2}', na=False))&\
            (~df0PUBX.iloc[:,7].str.contains('[0-9]{1}[^0-9]{1}', na=False))&\
            (~df0PUBX.iloc[:,7].str.contains('[^0-9]{1}[0-9]{1}', na=False))&\
            (~df0PUBX.iloc[:,7].str.contains('[a-zA-Z]', na=False))&\
            (~df0PUBX.iloc[:,7].str.contains('&', na=False))&\
            (~df0PUBX.iloc[:,7].str.contains('³', na=False))&\
            (df0PUBX.iloc[:,7].str.len()<3)]
            
            df0PUBX = df0PUBX.drop_duplicates(subset='C')
            df0PUBX = df0PUBX.reset_index(drop=True)
            a_p = len(df0PUBX)
            
            df0 = df0[df0.iloc[:,0] == "$GNRMC"]
            
            #Data validity status
            df0 = df0[df0.iloc[:,2] == "A" ]
            b_0 = len(df0)
            
            #df0 = df0[df0.iloc[:,8].isnull()]
            df0 = df0[df0.iloc[:,10].isnull()]
            df0 = df0.drop(df0.columns[[0,2,3,4,5,6,8,9,10,11]], axis=1) 
            
            #UTC hhmmss.ss
            df0 = df0[(df0.iloc[:,0].str.match('[0-9]{6}\.[0-9]{2}', na=False))&\
            (~df0.iloc[:,0].str.contains('[a-zA-Z]', na=False))&\
            (~df0.iloc[:,0].str.contains('&', na=False))&\
            (~df0.iloc[:,0].str.contains('³', na=False))&\
            (df0.iloc[:,0].str.len()==9)]
            
            df0 = df0[df0.iloc[:,0].str[:2].astype(float) < 24]
            df0 = df0[df0.iloc[:,0].str[2:4].astype(float) < 60]
            df0 = df0[df0.iloc[:,0].str[4:6].astype(float) < 60]
            df0 = df0[df0.iloc[:,0].str[7:8].astype(float) < 10]
            
            '''
            #Lat ddmm.mmmmm
            df0 = df0[(df0.iloc[:,1].str.match('[0-9]{4}\.[0-9]{5}', na=False))&\
            (~df0.iloc[:,1].str.contains('[a-zA-Z]', na=False))&\
            (~df0.iloc[:,1].str.contains('&', na=False))&\
            (~df0.iloc[:,1].str.contains('³', na=False))&\
            (df0.iloc[:,1].str.len()==10)]
            
            #Lon dddmm.mmmmm
            df0 = df0[(df0.iloc[:,2].str.match('[0-9]{5}\.[0-9]{5}', na=False))&\
            (~df0.iloc[:,2].str.contains('[a-zA-Z]', na=False))&\
            (~df0.iloc[:,2].str.contains('&', na=False))&\
            (~df0.iloc[:,2].str.contains('³', na=False))&\
            (df0.iloc[:,2].str.len()==11)]
            
            #Speed over ground knots
            df0 = df0[(df0.iloc[:,3].str.match('[0-9]{1,2}\.?[0-9]{0,3}', na=False))&\
            (~df0.iloc[:,3].str.contains('\.\.', na=False))&\
            (~df0.iloc[:,3].str.contains('\.[0-9]{0,3}\.', na=False))&\
            (~df0.iloc[:,3].str.contains('[a-zA-Z]', na=False))&\
            (~df0.iloc[:,3].str.contains('&', na=False))&\
            (~df0.iloc[:,3].str.contains('³', na=False))&\
            (df0.iloc[:,3].str.len()<7)]
            '''
            #Date in day, month, year format. ddmmyy
            df0 = df0[df0.iloc[:,1].str.match('[0-9]{6}', na=False)&\
            (~df0.iloc[:,1].str.contains('[a-zA-Z]', na=False))&\
            (~df0.iloc[:,1].str.contains('&', na=False))&\
            (~df0.iloc[:,1].str.contains('³', na=False))&\
            (df0.iloc[:,1].str.len()==6)]
            
            #Dayは32未満、Monthは13未満、Yearは、17（2017）より大きい
            df0 = df0[df0.iloc[:,1].str[:2].astype(float) < 32]
            df0 = df0[df0.iloc[:,1].str[2:4].astype(float) < 13]
            df0 = df0[df0.iloc[:,1].str[4:6].astype(float) > 17]
            
            #DayMonthYearは、UTCの零時を挟まない限りは全ての行で同一になる
            #零時を挟む場合も当日と翌日になるはず
            # if df0.iloc[:,1].nunique()==1:
            #     pass
            # elif df0.iloc[:,1].nunique()==2:
            #     days_list = df0.iloc[:,1].unique().tolist()
            #     if (df0.iloc[:,1].value_counts()[days_list[0]]\
            #     + df0.iloc[:,1].value_counts()[days_list[1]])\
            #     == len(df0):
            #         pass
            #     else:
            #         yesterday = str(mdd - timedelta(days=1))
            #         today = str(mdd)
            #         tomorrow = str(mdd + timedelta(days=1))
            #         str_yesterday = yesterday[8:10] + yesterday[5:7] + yesterday[2:4]
            #         str_today = today[8:10] + today[5:7] + today[2:4]
            #         str_tomorrow = tomorrow[8:10] + tomorrow[5:7] + tomorrow[2:4]
            #         df0 = df0[(df0.iloc[:,1] == str_yesterday) | (df0.iloc[:,1] == str_today) |\
            #         (df0.iloc[:,1] == str_tomorrow)]
                    
            # else:
            #     yesterday = str(mdd - timedelta(days=1))
            #     today = str(mdd)
            #     tomorrow = str(mdd + timedelta(days=1))
            #     str_yesterday = yesterday[8:10] + yesterday[5:7] + yesterday[2:4]
            #     str_today = today[8:10] + today[5:7] + today[2:4]
            #     str_tomorrow = tomorrow[8:10] + tomorrow[5:7] + tomorrow[2:4]
            #     df0 = df0[(df0.iloc[:,1] == str_yesterday) | (df0.iloc[:,1] == str_today) |\
            #     (df0.iloc[:,1] == str_tomorrow)]
            
            df0 = df0.drop_duplicates(subset='B') 
            df0 = df0.reset_index(drop=True)
            a_0 = len(df0)
            
            #df0 or df0PUBX　の行数が少なすぎる or ゼロの場合は、データ数が少ないか、データ異常であるため、ファイルを削除する
            if a_p == 0:
                err_str = 'Df0PUBXCount0Warning'
                Logger.warning(f'{err_str} : line_count {line_count} : {file_path}')
                return False, None, err_str
            if a_0 == 0:
                err_str = 'Df0Count0Warning'
                Logger.warning(f'{err_str} : line_count {line_count} : {file_path}')
                return False, None, err_str            
            
        except Exception as e:
            err_str = 'DataCleansingError'
            Logger.error(f'{err_str} : {file_path}', e)
            return False, None, err_str
        
        
        #データクレンジングを通過したデータに対する処理を実施する
        try:
            #ログファイルの行数を取得
            rows_n = len(df0.index)
            rowsPUBX_n =  len(df0PUBX.index)
            
            #末尾の1から5行目はデータ欠損がある可能性が高いため、取得しない。
            try:
                df1 = df0.drop([rows_n-5,rows_n-4,rows_n-3,rows_n-2,rows_n-1])
            except KeyError:
                pass
                
            try:
                df1PUBX = df0PUBX.drop([rowsPUBX_n-5,rowsPUBX_n-4,rowsPUBX_n-3,rowsPUBX_n-2,rowsPUBX_n-1])
            except KeyError:
                pass
            
            
            '''
            #不要な列を削除。
            try:
                df1 = df0.drop(df0.columns[[0,2,4,6,8,10,11,12,13,14,15,16,17,18,19,20]], axis=1) 
            except IndexError:
                df1 = df0.drop(df0.columns[[0,2,4,6,8,10,11,12]], axis=1)
            #不要な列を削除。
            #df1PUBX = df0PUBX.drop(df0.columns[[0,1,3,4,5,6,7,11,12,14,17,19,20]], axis=1) 
            #df1PUBX.columns = ['Time_raw', 'Status', 'Hacc', 'Vacc', 'VVel', 'HDOP', 'VPOP', 'SVnum']
            
            try:
                df1PUBX = df0PUBX.drop(df0.columns[[0,1,3,4,5,6,7,8,10,11,12,14,16,17,19,20]], axis=1) 
                #df1PUBX = df0PUBX.drop(df0.columns[[0,1,3,4,5,6,7,8,9,10,11,12,14,15,16,17,18,19,20]], axis=1) 
            except IndexError:
                pass
            '''
            
            
            #列の名前を設定。
            try:
                df1.columns = ['Time_raw', 'Date_raw']
            except UnboundLocalError:
                pass
            
            try:
                df1PUBX.columns = ['Time_raw_str', 'Lat0', 'Lon0','Hacc', 'Speed', 'VVel', 'HDOP', 'SVNum']
            except UnboundLocalError:
                pass
            
            
            df1PUBX['Time_raw'] = df1PUBX['Time_raw_str']
            
            #Time_rawをKeyにしてdf1とdf1PUBXを結合
            try:
                df1 = pd.merge(df1PUBX, df1, on='Time_raw', how='left')
            except UnboundLocalError:
                pass
            
            df1 = df1[['Time_raw', 'Lat0', 'Lon0', 'Speed', 'Date_raw', 'Time_raw_str','Hacc','VVel','HDOP', 'SVNum']]
            
            #日付を前の値で埋める
            df1['Date_raw'] = df1['Date_raw'].fillna(method='ffill')
            
            if len(df1['Time_raw_str'][pd.isnull(df1.loc[:, 'Time_raw_str'])]) != 0:
                df1['Time_raw_str'][pd.isnull(df1.loc[:, 'Time_raw_str'])] = df1['Time_raw']
            else:
                pass
            
            df1['Time_raw'] = df1['Time_raw'].astype(float)
            df1['Lat0'] = df1['Lat0'].astype(float)
            df1['Lon0'] = df1['Lon0'].astype(float)
            df1['Speed'] = df1['Speed'].astype(float)
            #df1['Date_raw'] = df1['Date_raw'].astype(float) #165r.py
            df1['Hacc'] = df1['Hacc'].astype(float)
            df1['VVel'] = df1['VVel'].astype(float)
            df1['HDOP'] = df1['HDOP'].astype(float)
            df1['SVNum'] = df1['SVNum'].astype(float)
            
            #水平方向の精度指標が5以上の行は、精度が悪い為に除く
            df1 = df1[df1['Hacc'] < 5] #195r.py
            
            #全てにおいて精度が悪い場合はファイルを削除。システムログにもHacc errorを出力
            if len(df1) == 0:
                err_str = 'HaccWarning'
                Logger.warning(f'{err_str} : {file_path}')
                return False, None, err_str
            
            #NaNの行は削除
            df1 = df1.dropna(subset=['Time_raw'])
            df1 = df1.dropna(subset=['Date_raw'])
            df1 = df1.dropna(subset=['Speed'])
            
            #線形置換で欠損行の値を補間
            df1['Hacc'] = df1['Hacc'].interpolate(method='linear')
            df1['VVel'] = df1['VVel'].interpolate(method='linear')
            df1['HDOP'] = df1['HDOP'].interpolate(method='linear')
            df1['SVNum'] = df1['SVNum'].interpolate(method='linear')
            
            #PUBXのSOGはkm/hのため変換しない
            '''
            #Speedの単位をKontからkm/hに変換
            df1["Speed"] = df1["Speed"]*1.852 
            '''
            
            #VerticalVelocityの単位をm/secからkm/hに変換
            try:
                df1['VVel'] = df1['VVel']*3600/1000
            except KeyError:
                pass
            #Speed(SOG)にVVelを加え、Speed(3D)を計算
            try:
                df1['Speed'] = np.sqrt(np.power(df1['Speed'],2) + np.power(df1['VVel'],2))
            except KeyError:
                pass
                
            #df1.to_csv('df1.csv')
            #VVelの列を削除
            try:
                df1 = df1.drop("VVel", axis=1)
            except KeyError:
                pass
                
            df1 = df1.reset_index(drop=True)
            
            '''
            df1['Time_raw'] = df1['Time_raw'].astype(float)
            df1['Lat0'] = df1['Lat0'].astype(float)
            df1['Lon0'] = df1['Lon0'].astype(float)
            df1['Speed'] = df1['Speed'].astype(float)
            df1['Date_raw'] = df1['Date_raw'].astype(float)
            '''
            
            #外れ値以上の速度を削除。 sot : Speed Outlier Threshold　を取得
            df1['Speed'] = df1['Speed'].where(df1['Speed'] < float(config.General.SpeedOutlierThreshold), None).dropna() 
            
            #Object型になったKnotを、Float型に戻す。
            df1['Speed'] = df1['Speed'] .astype(float).round(3)
            
            #小数点第二位を持つ異常値を取り除く。
            df1["Time_raw2"] = round((df1["Time_raw"].astype(float))*10)/10
            
            #時間及び日付の処理
            df1['Date'] = pd.to_datetime(df1['Date_raw'], format="%d%m%y").dt.date
            
            if len(df1) == 0:
                err_str = 'Df1Count0Warning'
                Logger.warning(f'{err_str} : {file_path}')
                return False, None, err_str
            
            #UTCで日付を跨ぐ場合に日付や時刻を適切に変換
            if len(df1.loc[0,'Date_raw']) == len(df1.loc[len(df1)-1,'Date_raw']):
                pass
            else:
                if len(str(int(df1.loc[0,'Date_raw']))) == 6:
                    df1.loc[:,'Date'][df1.loc[:,'Date_raw'] == df1.loc[0,'Date_raw']] \
                    = pd.to_datetime(df1['Date_raw'][df1.loc[:,'Date_raw'] \
                    == df1.loc[0,'Date_raw']].astype(int).astype(str), format="%d%m%y").dt.date
                    if len(str(int(df1.loc[len(df1)-1,'Date_raw']))) == 6:
                        df1.loc[:,'Date'][df1.loc[:,'Date_raw'] == df1.loc[len(df1)-1,'Date_raw']] \
                        = pd.to_datetime(df1['Date_raw'][df1.loc[:,'Date_raw'] \
                        == df1.loc[len(df1)-1,'Date_raw']].astype(int).astype(str), format="%d%m%y").dt.date
                        
                    else:
                        df1.loc[:,'Date'][df1.loc[:,'Date_raw'] == df1.loc[len(df1)-1,'Date_raw']] \
                        = pd.to_datetime('0' + df1['Date_raw'][df1.loc[:,'Date_raw'] \
                        == df1.loc[len(df1)-1,'Date_raw']].astype(int).astype(str), format="%d%m%y").dt.date
                        
                else:
                    df1.loc[:,'Date'][df1.loc[:,'Date_raw'] == df1.loc[0,'Date_raw']] \
                    = pd.to_datetime('0' + df1['Date_raw'].astype(int).astype(str), format="%d%m%y").dt.date
                    if len(str(int(df1.loc[len(df1)-1,'Date_raw']))) == 6:
                        df1.loc[:,'Date'][df1.loc[:,'Date_raw'] == df1.loc[len(df1)-1,'Date_raw']] \
                        = pd.to_datetime(df1['Date_raw'][df1.loc[:,'Date_raw'] \
                        == df1.loc[len(df1)-1,'Date_raw']].astype(int).astype(str), format="%d%m%y").dt.date
                        
                    else:
                        df1.loc[:,'Date'][df1.loc[:,'Date_raw'] == df1.loc[len(df1)-1,'Date_raw']] \
                        = pd.to_datetime('0' + df1['Date_raw'][df1.loc[:,'Date_raw'] \
                        == df1.loc[len(df1)-1,'Date_raw']].astype(int).astype(str), format="%d%m%y").dt.date
                        
            
            df1['Time_UTC'] = pd.to_datetime(df1['Date_raw'] + df1['Time_raw_str'], format="%d%m%y%H%M%S.%f")
            # if int(df1['Time_UTC'].head(1).dt.hour) > int(df1['Time_UTC'].tail(1).dt.hour):
            #     df1.loc[df1[df1['Time_UTC'].dt.hour.astype(int) <= int(df1['Time_UTC'].tail(1).dt.hour)].index, 'Time_UTC'] = df1['Time_UTC'] + offsets.Hour(24)
            
            #UTCに加えPCのOS時間から取得したローカル時刻を計算
            df1['Time_Local'] = df1['Time_UTC'] + offsets.Hour(timezone_delta)
            
            #ローカル日付を計算
            df1['Date'] = df1['Time_Local'].dt.date
            
            if df1.loc[0,'Date'] != df1.loc[len(df1)-1,'Date']:
                df1 = df1[df1.loc[:,'Date'] >= df1.loc[0,'Date']]
            
            df1['Date_raw'] = df1['Date'].astype(str)
            
            date_raw = df1.loc[0,'Date_raw']
            
            #★ファイル名をCA日付時刻.TXTにするための日時を取得
            # file_date = date_raw[0:4] + date_raw[5:7] + date_raw[8:10] 
            # file_start = str(df1.loc[0,'Time_Local'])
            # file_start_time = file_start[11:13] + file_start[14:16] + file_start[17:19]
            
            #ファイル名を作成
            # CG20240927_082449.TXT
            # new_file_name_without_ext = ('CG' + file_date[0:4] + file_date[4:6] + file_date[6:8] + '_' + file_start_time[0:6])                            
            # if file_path[0:2] == 'CG':
            #     if file_path[:-4] != new_file_name_without_ext:
            #         new_file_name_without_ext = file_path[:-4]
            # new_file_name = new_file_name_without_ext + '.TXT'
            # new_file_path = os.path.join(Path.get_devicesn_FILE_dir(device_sn), new_file_name)
            
            #ファイル名をリネーム
            # A0000000.TXT => CA20240927_082449.TXT
            # try:
            #     os.rename(file_path, new_file_name)
            # except FileExistsError:
            #     #os.remove(ifn)
            #     os.remove(new_file_path)
            #     os.rename(file_path, new_file_name)
            # except OSError: 
            #     #os.remove(ifn)
            #     try:
            #         os.remove(new_file_path)
            #     except FileNotFoundError:
            #         show_error(_("warning_268"))
            #         Logger.debug('f')
            #         return False, None
            #     os.rename(file_path, new_file_name)
            
            #172r.py
        except Exception as e:
            err_str = 'Df1ProcessingError'
            Logger.error(f'{err_str} : {file_path}', e)
            return False, None, err_str
            
            
        try:
            df4 = df1[:-1]
            
            # 何となくやらない方が良い気がするため一旦コメントアウト
            # df4_date_diff = df4[df4['Time_Local'].astype(str).str[:10] != str(mdd)].copy()
            
            # if len(df4_date_diff) != 0:
            #     if df4_date_diff.reset_index(drop=True).loc[0, 'Time_Local'].hour != 0:
            #         df4 = df4[df4['Time_Local'].astype(str).str[:10] == str(mdd)]
                    
            #         df4_date_diff['Time_UTC'] = df4_date_diff['Time_UTC'] - timedelta(days = 1)
            #         df4_date_diff['Time_Local'] = df4_date_diff['Time_Local'] - timedelta(days = 1)
            #         df4_date_diff['Date'] = df4_date_diff['Date'] - timedelta(days = 1)
            #         df4 = pd.concat([df4, df4_date_diff], ignore_index=True, sort=True)[df4.columns.tolist()]
            
            start = df4.loc[0, 'Time_Local']
            start_s = start.second
            start_ms = start.microsecond
            start = start - timedelta(seconds = start_s) - timedelta(microseconds = start_ms)
            end = df4.loc[len(df4)-1, 'Time_Local']
            duration = end - start
            duration_min = int(duration.total_seconds()/60)
            
            #Time_UTCをdf4のIndexに設定
            df4 = df4.set_index('Time_Local') 
            df4_num = len(df4)
            
            #GPSの速度データ異常として、速度変化が大きすぎる場合は、異常値として対象の行を削除
            df4['Speed_diff']= ((df4['Speed'].shift(1)-df4['Speed'])/0.1) * ((df4['Speed']-df4['Speed'].shift(-1))/0.1)
            df4 = df4[(df4['Speed_diff'] > -4000) | (pd.isnull(df4['Speed_diff']))] 
            
            #df4のtop_time:一番上　と bot_time:一番下の時間を取得
            top_time, bot_time = df4.index.min(), df4.index.max() 
            
            #top_timeとbot_timeの間を、0.1秒ずつ再Indexし、データ欠落を補う。
            df5 = df4.reindex(pd.date_range(top_time, bot_time, freq = '100ms')) 
            df5_num = len(df5)
            
            #データ欠落部分を線形補完
            df5[['Time_raw', 'Lat0', 'Lon0', 'Date_raw', 'Time_raw_str', 'Speed', 'Hacc', 'HDOP', 'SVNum', 'Time_raw2', 'Date',]] \
            = df5[['Time_raw', 'Lat0', 'Lon0', 'Date_raw', 'Time_raw_str', 'Speed', 'Hacc', 'HDOP', 'SVNum', 'Time_raw2', 'Date',]].interpolate(method='linear', limit=100)
            
            df5_speed = df5.loc[:,'Speed'].fillna(0)
            df5 = df5.rename(columns={'Speed': 'Speed_Raw'})
            data = df5_speed.to_list()
            
            #Butterworthフィルタを適用
            try:
                # パラメータ設定
                btype = 'lowpass'  # フィルタのタイプ
                cutoff = 1         # カットオフ周波数（Hz）
                fs = 10            # サンプリング周波数（Hz）
                order = 3        # フィルタの次数（一般的には2から5の範囲）

                # 正規化されたカットオフ周波数（ナイキスト周波数で割る）
                nyq = 0.5 * fs
                normal_cutoff = cutoff / nyq

                # フィルタ係数の計算
                if len(df5) < 13:
                    err_str = 'Df5CountLessThan13Warning'
                    Logger.warning(f'{err_str} : {file_path}')
                    return False, None, err_str
                b, a = butter(order, normal_cutoff, btype=btype, analog=False)

                # データにフィルタを適用（例として生成されたサンプルデータを使用）
                butterworth_data = filtfilt(b, a, data)
                df_butterworth = pd.DataFrame({'Speed_BW': butterworth_data})
                
            except NameError as e:
                err_str = 'ButterWorthError'
                Logger.error(f'{err_str} : {file_path}', e)
                Logger.info("Microsoft Visual Studio Update : https://notepm.jp/sharing/3f3c5987-5b5f-4494-9b0b-d4c85f28b01a")
                return False, None, err_str
                
            df_butterworth['Time_Local'] = df5.index
            df_butterworth = df_butterworth.set_index('Time_Local')
            
            df5 = pd.concat([df5, df_butterworth], axis=1)
            df5 = df5.rename(columns={'Speed_BW': 'Speed'})
            df5.loc[df5['Speed'] < 0.05, 'Speed'] = 0
            
            #緯度経度関係の単位変換
            #Lat/Lonの列を抽出
            arr_lat_nmea= df5.loc[:,'Lat0'] 
            arr_lon_nmea = df5.loc[:,'Lon0'] 
            
            #nmea_convert関数を適用。緯度経度を度単位に変換。
            arr_lat = arr_lat_nmea.apply(nmea_convert) 
            arr_lon = arr_lon_nmea.apply(nmea_convert)
            
            #nmea_convert関数の戻り値でDataFrame'df_lat'を作成
            df_lat = pd.DataFrame({'Lat' : arr_lat}) 
            df_lon = pd.DataFrame({'Lon' : arr_lon}) 
            
            #df5にdf_lat/df_lonを結合し、df8作成。
            df6 = pd.concat([df5 , df_lat], axis = 1,sort=True)
            df8 = pd.concat([df6 , df_lon], axis = 1,sort=True)
            
            #SpeedからAccelerationを算出。0.8秒の平均加速度を算出。
            df8['Acceleration'] = (df8['Speed'].shift(-5) - df8['Speed'].shift(5))*1000/3600/(10/float(config.General.DataFrequency))
            
            #SpeedからTotalDistを算出
            df8['TotalDist'] = df8['Speed']*1000/3600*(1/config.General.DataFrequency)
            df8.loc[df8[df8['Speed'] < 0.72].index, 'TotalDist'] = 0
            
            #外れ値以上/以下の加速度を削除。
            df8['Acceleration'] = df8["Acceleration"].where(df8["Acceleration"] < float(config.General.AccelOutlierThresholdUpper), None).dropna()
            df8['Acceleration'] = df8["Acceleration"].where(df8["Acceleration"] > float(config.General.AccelOutlierThresholdLower), None).dropna()
            
            #Object型になったAccelerationを、Float型に戻す。
            df8['Acceleration'] = df8['Acceleration'] .astype(float)
            
            #データ欠落部分を補完
            df8['Acceleration'] = df8['Acceleration'].interpolate(method='linear', limit=3, limit_direction = 'both') 
            df8['Date'] = df8['Date'].fillna(method='ffill') 
            
            df8['Speed2'] = df8['Speed']
            
            df8['Speed2'] = df8['Speed2'].fillna(0)
            
            df8['Acceleration'] = df8["Speed"].where(df8["Speed2"] == 0, df8['Acceleration'])
            
            df8['Acceleration2'] = df8['Acceleration']
            
            df8['Acceleration2'] = df8['Acceleration2'].fillna(0)
            
            #df8.to_csv("df10.csv")
            #不要な列を削除。順番を入れ替え。
            df10 = df8.drop(df8.columns[[0,1,2,4,5,9]], axis=1)
            #Speed_Rawは、生データを残しておくために保存。
            df10 = df10[['Time_UTC', 'Lat', 'Lon', 'Speed', 'Acceleration', 'TotalDist', 'Speed2', 'Acceleration2', 'Hacc', 'HDOP', 'SVNum','Speed_Raw']]
            df10 = df10.round({'Speed': 3, 'Lat':6, 'Lon':6, 'Acceleration': 3, 'Speed2': 3, 'Acceleration2': 3, 'Hacc': 1, 'HDOP':1, 'Speed_Raw': 3,})
            try:
                # df10['SVNum'] = df10['SVNum'].round(0).astype(int)
                df10['SVNum'] = df10['SVNum'].fillna(0).round(0).astype(int)
            except ValueError:
                pass
            # df10['Time_UTC'] = timezone_delta
            df10['TimeStamp'] = df10.index
            df1['Time_UTC'] = df10['TimeStamp'] - offsets.Hour(timezone_delta)
            
        except Exception as e:        
            err_str = 'Df10GenerationError'
            Logger.error(f'{err_str} : {file_path}', e)
            return False, None, err_str
        
        # df10 : ['TimeStamp(index)', 'Time_UTC', 'Lat', 'Lon', 'Speed', 'Acceleration', 'TotalDist', 'Speed2', 'Acceleration2', 'Hacc', 'HDOP', 'SVNum','Speed_Raw']
        Logger.info(f"CreateRaw : {time.time() - start_time:.3f} 秒")
        return True, df10, ""

    #最大速度の算出
    async def _addMaxSpeedColumn(df10, MaxSpeedDuration):
        start_time = time.time()
        max_speed_duration = int(float(MaxSpeedDuration)*10)
        df_max_speed = pd.DataFrame(columns=['MaxSpeed'])
        for msd_ in range(0,max_speed_duration):
            df_max_speed["'" + str(msd_) +"'"] = df10['Speed2'].shift(-msd_)
        df_max_speed['MaxSpeed'] = df_max_speed.min(axis=1)
        df10['MaxSpeed'] = df_max_speed['MaxSpeed']
        #水平精度指標が1.8より悪い場合、何らかの要因で正常な状況ではない可能性があるので、カウントしない。（寝転んで立ち上がった時など）
        df10.loc[df10[df10['Hacc']>1.8].index ,'MaxSpeed'] = 0
        Logger.debug(f"MaxSpeed : {time.time() - start_time:.3f} 秒")

    #最大加速度の算出
    async def _addMaxAccelColumn(df10, MaxAccelDuration):
        start_time = time.time()
        max_acc_duration = int(float(MaxAccelDuration)*10)
        df_max_acc = pd.DataFrame(columns=['MaxAccel'])
        for mad_ in range(0,max_acc_duration):
            df_max_acc["'" + str(mad_) +"'"] = df10['Acceleration2'].shift(-mad_)
        df_max_acc['MaxAccel'] = df_max_acc.min(axis=1)
        df10['MaxAccel'] = df_max_acc['MaxAccel']
        #水平精度指標が1.8より悪い場合、何らかの要因で正常な状況ではない可能性があるので、カウントしない。（寝転んで立ち上がった時など）
        df10.loc[df10[df10['Hacc']>1.8].index, 'MaxAccel'] = 0
        Logger.debug(f"MaxAccel : {time.time() - start_time:.3f} 秒")

    #回数系のカラム計算と列追加(閾値以上でカウント)
    def addOverThresholdCountColumn(df10, col_name, list, list_max, list_Hacc, gps_hacc, threshold, judge_duration_sec, reset_margin):
        start_time = time.time()
        judge_duration = float(judge_duration_sec) * 10
        flag, duration = 0, 0
        df10[col_name] = 0
        count = 0
        if list_max >= float(threshold):
            for x in range(1, len(df10)):  # x=0だと x-1 でエラーになるので 1から開始
                if list_Hacc[x] < gps_hacc:
                    if list[x] >= float(threshold) and list[x-1] < float(threshold):
                        duration += 1
                    elif list[x] >= float(threshold) and list[x-1] >= float(threshold):
                        duration += 1
                        if duration >= judge_duration and flag ==0: #継続時間
                            df10.loc[df10.index[x], col_name] = 1
                            flag = 1
                            count += 1
                    elif list[x] < float(threshold) - reset_margin and list[x-1] >= float(threshold) - reset_margin:
                        flag, duration = 0, 0
                else:
                    flag, duration = 0, 0
        Logger.debug(f"{col_name} : {count} => {time.time() - start_time:.3f} 秒")


    # ★ numpy try
    #回数系のカラム計算と列追加(閾値以上でカウント)
    async def _addOverThresholdCountColumn2(df10, col_name, value_list, list_max, list_Hacc, gps_hacc, threshold, judge_duration_sec, reset_margin):
        start_time = time.time()
        if len(df10) != len(value_list) or len(df10) != len(list_Hacc):
            raise ValueError("Length mismatch: df10 and input lists must have the same length.")

        judge_duration = int(float(judge_duration_sec) * 10)

        # NumPy化
        # values = np.array(value_list)
        # haccs = np.array(list_Hacc)
        
        values = value_list
        haccs = list_Hacc

        # 条件マスク
        over_mask = values >= float(threshold)
        hacc_mask = haccs < gps_hacc
        valid_mask = over_mask & hacc_mask

        df10[col_name] = 0  # 初期化

        # 状態遷移検出
        padded = np.concatenate([[0], valid_mask.astype(int), [0]])
        diff = np.diff(padded)

        starts = np.where(diff == 1)[0]
        ends = np.where(diff == -1)[0]

        count = 0
        for start, end in zip(starts, ends):
            length = end - start
            if length >= judge_duration:
                # 継続していた末尾インデックスに1をセット（df10.index[end-1]）
                df10.at[df10.index[end-1], col_name] = 1
                count += 1

        Logger.debug(f"{col_name} : {count} => {time.time() - start_time:.3f} 秒")
        


        
    #回数系のカラム計算と列追加(閾値以下でカウント)
    async def _addUnderThresholdCountColumn(df10, col_name, list, list_min, list_Hacc, gps_hacc, threshold, judge_duration_sec, reset_margin):
        list = [-1 * x for x in list]
        # list = list * -1 ★ numpy try
        list_max = -1 * list_min
        threshold *= -1
        CreateDf10.addOverThresholdCountColumn(df10, col_name, list, list_max, list_Hacc, gps_hacc, threshold, judge_duration_sec, reset_margin)

    #ベースボールスコアを計算
    async def _addBaseBallScoreColumn(df10, ms_sr, ma_ar, list_Hacc, gps_hacc, list_speed_raw, list_acc_raw):
        start_time = time.time()
        bbs_duration_before = 0
        bbs_duration_after = 0
        bbs_before = 8
        bbs_after = 7
        col_name = 'BaseBallScore'
        df10[col_name] = 0
        count = 0
        if ms_sr >= 5.5 and ma_ar >= 3.5:
            for x in range(2, len(df10)-1):  # エラーにならないように制限
                if list_Hacc[x] < gps_hacc:
                    if list_speed_raw[x] >= 1 and list_speed_raw[x] < 3 and list_speed_raw[x-1] < 1.5:
                    #and list_speed_raw[x-2] < 1.5 and list_speed_raw[x-3] < 1.5:
                        bbs_duration_before = 0
                        bbs_duration_before += 1
                        
                    elif bbs_duration_before >= 1 and bbs_duration_before <= bbs_before:
                        if list_speed_raw[x] >= 5.5:
                            try:
                                if list_acc_raw[x-2] >= 3.5 or list_acc_raw[x-1] >= 3.5\
                                or list_acc_raw[x] >= 3.5 or list_acc_raw[x+1] >= 3.5:
                                    bbs_duration_before = 0
                                    bbs_duration_after += 1
                                else:
                                    bbs_duration_before += 1
                            except IndexError:
                                if list_acc_raw[x-1] >= 3.5 or list_acc_raw[x] >= 3.5:
                                    bbs_duration_before = 0
                                    bbs_duration_after += 1
                                else:
                                    bbs_duration_before += 1
                        elif list_speed_raw[x] < 5.5:
                            bbs_duration_before += 1
                            
                    elif bbs_duration_before > bbs_before:
                        bbs_duration_before = 0
                        
                    elif bbs_duration_after >= 1 and bbs_duration_after <= bbs_after:
                        if list_speed_raw[x] >= 3:
                            bbs_duration_after += 1
                            
                        elif list_speed_raw[x] < 3:
                            df10.at[x, col_name] = 1
                            bbs_duration_after = 0
                            count += 1
                            
                    elif bbs_duration_after > bbs_after:
                        bbs_duration_after = 0
                            
                else:
                    bbs_duration_before = 0
                    bbs_duration_after = 0
        Logger.debug(f"{col_name} : {count} => {time.time() - start_time:.3f} 秒")

    async def _addSpeedZoneDistColumns(df10, sz, sl):
        df10['SpeedZone'] = pd.cut(df10['Speed'], bins=sz, labels=sl, right=False)
        for zone_label in sl:
            df10[f'Speed{zone_label}Dist'] = 0.0 # zone_label : Zone1 ~ Zone7
        for zone_label in sl:
            mask = df10['SpeedZone'] == zone_label
            df10.loc[mask, f'Speed{zone_label}Dist'] = df10.loc[mask, 'TotalDist']

    async def do(
            file_path, device_sn, config:ConfigModel,
            show_success, show_warning, show_error):
        
        # ベースとなる df10 を作成
        result, df10, err_str = await CreateDf10._create_raw_df10(file_path, device_sn, config,show_success, show_warning, show_error)
        # df10 => [
        # 'TimeStamp(index)', 'Time_UTC', 'Lat', 'Lon', 'Speed', 'Acceleration', 'TotalDist', 'Speed2', 'Acceleration2', 'Hacc', 'HDOP', 'SVNum','Speed_Raw',
        # ]

        if df10 is None:
            Logger.info(f"_create_raw_df10 : {result} : df10 None")
        elif df10.empty:
            Logger.info(f"_create_raw_df10 : {result} : df10.empty")
        else:
            Logger.info(f"_create_raw_df10 : {result} : df10 count {len(df10)}")

        if not result or df10 is None or df10.empty:
            return result, df10, err_str
                
        # 事前にメトリクス計算用のカラムを追加
        gps_hacc = config.General.GnssAccuracyFilter
        list_speed = df10.loc[:,'Speed2'].tolist()
        ms_s2 = np.nan_to_num(round(df10['Speed2'].max(),1))
        list_speed_raw = df10.loc[:,'Speed_Raw'].tolist()
        ms_sr = np.nan_to_num(round(df10['Speed_Raw'].max(),1))
        list_acc_raw = df10.loc[:,'Acceleration'].tolist()
        ma_ar = np.nan_to_num(round(df10['Acceleration'].max(),1))
        mi_ar = np.nan_to_num(round(df10['Acceleration'].min(),1))
        list_acc = df10.loc[:, 'Acceleration2'].tolist()
        ma_a2 = np.nan_to_num(round(df10['Acceleration2'].max(),1))
        mi_a2 = np.nan_to_num(round(df10['Acceleration2'].min(),1))
        list_Hacc = df10.loc[:, 'Hacc'].tolist()

        # ★ numpy try
        # list_speed = np.array(list_speed)
        # list_acc = np.array(list_acc)
        # list_Hacc = np.array(list_Hacc)
        
        # MaxSpeed
        await CreateDf10._addMaxSpeedColumn(df10, config.Duration.MaxSpeedDuration)

        # MaxAccel
        await CreateDf10._addMaxAccelColumn(df10, config.Duration.MaxAccelDuration)

        # SprintEffort
        col_name, threshold, duration = "SprintEffort", config.Metric.SprintZone, config.Duration.SprintEffortDuration
        CreateDf10.addOverThresholdCountColumn(df10, col_name, list_speed, ms_s2, list_Hacc, gps_hacc, threshold, duration, 2)

        # HSprintEffort
        col_name, threshold, duration = "HSprintEffort", config.Metric.HSprintZone, config.Duration.HSprintEffortDuration
        CreateDf10.addOverThresholdCountColumn(df10, col_name, list_speed, ms_s2, list_Hacc, gps_hacc, threshold, duration, 2)

        # BaseBallScore
        await CreateDf10._addBaseBallScoreColumn(df10, ms_sr, ma_ar, list_Hacc, gps_hacc, list_speed_raw, list_acc_raw)

        # AccelEffort
        col_name, threshold, duration = "AccelEffort", config.Metric.AccelZone, config.Duration.AccelEffortDuration
        CreateDf10.addOverThresholdCountColumn(df10, col_name, list_acc, ma_a2, list_Hacc, gps_hacc, threshold, duration, 1)

        # QuickAccelEffort
        col_name, threshold, duration = "QuickAccelEffort", config.Metric.QuickAccelZone, config.Duration.QuickAccelEffortDuration
        CreateDf10.addOverThresholdCountColumn(df10, col_name, list_acc, ma_a2, list_Hacc, gps_hacc, threshold, duration, 1)

        # DecelEffort
        col_name, threshold, duration = "DecelEffort", config.Metric.DecelZone, config.Duration.DecelEffortDuration
        await CreateDf10._addUnderThresholdCountColumn(df10, col_name, list_acc, mi_a2, list_Hacc, gps_hacc, threshold, duration, 1)
        
        # QuickDecelEffort
        col_name, threshold, duration = "QuickDecelEffort", config.Metric.QuickDecelZone, config.Duration.QuickDecelEffortDuration
        await CreateDf10._addUnderThresholdCountColumn(df10, col_name, list_acc, mi_a2, list_Hacc, gps_hacc, threshold, duration, 1) 
        
        # SpeedZone?Dist
        ZONE_MAX = 9999999
        sl = ['Zone1', 'Zone2', 'Zone3', 'Zone4', 'Zone5', 'Zone6']
        sz = [
            config.get_value_by_thre_enum(ThreEnum.SpeedZone.SpeedZone1),
            config.get_value_by_thre_enum(ThreEnum.SpeedZone.SpeedZone2),
            config.get_value_by_thre_enum(ThreEnum.SpeedZone.SpeedZone3),
            config.get_value_by_thre_enum(ThreEnum.SpeedZone.SpeedZone4),
            config.get_value_by_thre_enum(ThreEnum.SpeedZone.SpeedZone5),
            config.get_value_by_thre_enum(ThreEnum.SpeedZone.SpeedZone6),
            ZONE_MAX, #config.get_value_by_thre_enum(ThreEnum.SpeedZone.SpeedZone7),
        ]
        await CreateDf10._addSpeedZoneDistColumns(df10, sz, sl)

        # SpeedZone?Effort
        thre_dict = {
            1 : config.get_value_by_thre_enum(ThreEnum.SpeedZone.SpeedZone1),
            2 : config.get_value_by_thre_enum(ThreEnum.SpeedZone.SpeedZone2),
            3 : config.get_value_by_thre_enum(ThreEnum.SpeedZone.SpeedZone3),
            4 : config.get_value_by_thre_enum(ThreEnum.SpeedZone.SpeedZone4),
            5 : config.get_value_by_thre_enum(ThreEnum.SpeedZone.SpeedZone5),
            6 : config.get_value_by_thre_enum(ThreEnum.SpeedZone.SpeedZone6),
        }
        for key, value in thre_dict.items():
            col_name = f"SpeedZone{key}Effort"
            CreateDf10.addOverThresholdCountColumn(df10, col_name, list_speed, ms_s2, list_Hacc, gps_hacc, value, config.Duration.SprintEffortDuration, 2)

        # AccelZone?Effort, QuickAccelZone?Effort
        thre_dict = {
            1 : config.get_value_by_thre_enum(ThreEnum.AccelZone.AccelZone1),
            2 : config.get_value_by_thre_enum(ThreEnum.AccelZone.AccelZone2),
            3 : config.get_value_by_thre_enum(ThreEnum.AccelZone.AccelZone3),
            4 : config.get_value_by_thre_enum(ThreEnum.AccelZone.AccelZone4),
        }
        for key, value in thre_dict.items():
            col_name, duration = f"AccelZone{key}Effort", config.Duration.AccelEffortDuration
            CreateDf10.addOverThresholdCountColumn(df10, col_name, list_acc, ma_a2, list_Hacc, gps_hacc, value, duration, 1)
        for key, value in thre_dict.items():
            col_name, duration = f"QuickAccelZone{key}Effort", config.Duration.QuickAccelEffortDuration
            CreateDf10.addOverThresholdCountColumn(df10, col_name, list_acc, ma_a2, list_Hacc, gps_hacc, value, duration, 1)
            
        # DecelZone?Effort, QuickDecelZone?Effort
        thre_dict = {
            1 : config.get_value_by_thre_enum(ThreEnum.DecelZone.DecelZone1),
            2 : config.get_value_by_thre_enum(ThreEnum.DecelZone.DecelZone2),
            3 : config.get_value_by_thre_enum(ThreEnum.DecelZone.DecelZone3),
            4 : config.get_value_by_thre_enum(ThreEnum.DecelZone.DecelZone4),
        }
        for key, value in thre_dict.items():
            col_name, duration = f"DecelZone{key}Effort", config.Duration.DecelEffortDuration
            await CreateDf10._addUnderThresholdCountColumn(df10, col_name, list_acc, mi_a2, list_Hacc, gps_hacc, value, duration, 1)
        for key, value in thre_dict.items():  
            col_name, duration = f"QuickDecelZone{key}Effort", config.Duration.QuickDecelEffortDuration
            await CreateDf10._addUnderThresholdCountColumn(df10, col_name, list_acc, mi_a2, list_Hacc, gps_hacc, value, duration, 1)
    
        # df10 => [
        # 'TimeStamp(index)', 'Time_UTC', 'Lat', 'Lon', 'Speed', 'Acceleration', 'TotalDist', 'Speed2', 'Acceleration2', 'Hacc', 'HDOP', 'SVNum','Speed_Raw',
        # 'MaxSpeed', 'MaxAccel', 'SprintEffort', 'HSprintEffort', 'BaseBallScore', 'AccelEffort', 'QuickAccelEffort', 'DecelEffort', 'QuickDecelEffort',
        # 'SpeedZone1Dist', 'SpeedZone2Dist', 'SpeedZone3Dist', 'SpeedZone4Dist', 'SpeedZone5Dist', 'SpeedZone6Dist', 
        # 'SpeedZone1Effort', 'SpeedZone2Effort', 'SpeedZone3Effort', 'SpeedZone4Effort', 'SpeedZone5Effort', 'SpeedZone6Effort', 
        # 'AccelZone1Effort', 'AccelZone2Effort', 'AccelZone3Effort', 'AccelZone4Effort',
        # 'QuickAccelZone1Effort', 'QuickAccelZone2Effort', 'QuickAccelZone3Effort', 'QuickAccelZone4Effort',
        # 'DecelZone1Effort', 'DecelZone2Effort', 'DecelZone3Effort', 'DecelZone4Effort',
        # 'QuickDecelZone1Effort', 'QuickDecelZone2Effort', 'QuickDecelZone3Effort', 'QuickDecelZone4Effort',
        # ]

        Logger.info(f"df10 count : {len(df10)}")

        return result, df10, ""
        
    