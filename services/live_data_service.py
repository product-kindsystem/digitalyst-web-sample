import time
import asyncio
import socket
import pandas as pd
import numpy as np
import random
import pandas.tseries.offsets as offsets
from scipy.signal import butter, filtfilt
from datetime import datetime, timezone, timedelta

from models.registration.config_model import ConfigModel, ThreEnum
from models.registration.device_model import DeviceModel
from models.drill.drill_gnss_result_live_model import DrillGnssResultLiveModel
from views.helpers.content_helper import ContentHelper
from services.localization_service import _
from services.logger_service import Logger
from multiprocessing import Pool

import warnings
warnings.filterwarnings('ignore')


class LiveDataService:   
    _DEBUG = False
    _instance = None

    def __new__(self):
        if self._instance is None:
            self._instance = super(LiveDataService, self).__new__(self)
            self.initialize()
        return self._instance

    @classmethod
    def get_instance(self):
        return self._instance

    @classmethod
    def initialize(self):
        self._running = False
        self._drill_id = None
        self._df_drill_detail = None
        self._drill_lock = asyncio.Lock()
        self._connected_device_sn_valid_data_ratio_dict = {}
        self._active_device_sn_list = []
        self._elapsed_time = None
        self._udp_running_status = False
        self._udp_queue = asyncio.Queue()
        self._db_lock = asyncio.Lock()
        self._udp_socket = None

    '''
    @classmethod
    async def receive_udp_data(self, port):
        loop = asyncio.get_running_loop()
        self._udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._udp_socket.bind(('', port))
        self._udp_socket.setblocking(False)

        # _udp_queue を空にする
        while not self._udp_queue.empty():
            await self._udp_queue.get()  # キューの中身をすべて取り除く
        try:
            while self._running:
                data = await loop.sock_recv(self._udp_socket, 1024)
                await self._udp_queue.put(data.decode('utf-8'))
        except:
            pass
    '''

    @classmethod
    def udp_listener(self, port):
        Logger.info(f"start")
        """ 別スレッドで UDP をリスニングする関数（同期処理） """
        self._udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._udp_socket.bind(('', port))
        self._udp_socket.setblocking(True)  # ブロッキングモードに設定
        
        # ここで `get_event_loop()` を使う
        loop = self._loop
        if loop is None:
            Logger.error("イベントループが未設定です。")
            return

        while self._running:
            try:
                data, _ = self._udp_socket.recvfrom(1024)
                if data:
                    # `call_soon_threadsafe` を使い、メインスレッドのイベントループでキューに追加
                    loop.call_soon_threadsafe(self._udp_queue.put_nowait, data.decode('utf-8'))
            except Exception as e:
                if self._running:
                    Logger.warning(f"UDP udp_listener error", e)
                break
        Logger.info(f"end")

    @classmethod
    async def receive_udp_data(self, port):
        Logger.info(f"called")
        """ UDPデータ受信を別スレッドで開始 """
        self._running = True
        self._loop = asyncio.get_event_loop()  # 現在のイベントループを保存
        await asyncio.to_thread(self.udp_listener, port)

    @classmethod
    def get_connected_device_sn_valid_data_ratio_dict(self):
        return self._connected_device_sn_valid_data_ratio_dict
    
    @classmethod
    def get_elapsed_time(self):
        return self._elapsed_time

    @classmethod
    async def start_receiving(self, port_number):
        Logger.info(f"start")
        async with self._drill_lock:
            self._df_drill_detail = None
            self._drill_id = None
        self._running = True
        asyncio.create_task(self.receive_udp_data(port=port_number))
        Logger.info(f"end")

    @classmethod
    async def stop_receiving(self):
        Logger.info(f"stop")
        async with self._drill_lock:
            self._drill_id = None
        self._running = False
        self._elapsed_time = None
        self._connected_device_sn_valid_data_ratio_dict = {}
        if self._udp_socket:
            asyncio.sleep(0.5)
            self._udp_socket.close() # ソケットを閉じて `sock_recv()` を強制終了
            self._udp_socket = None
        Logger.info(f"end")
        
    @classmethod
    async def start_drill(self, drill_id, df_live_player_device, active_device_sn_list):
        Logger.info(f"called")
        async with self._drill_lock:
            self._drill_id = drill_id
            self._df_drill_detail = None
            self._df_live_player_device = df_live_player_device
            self._active_device_sn_list = active_device_sn_list
        
    @classmethod
    async def stop_drill(self):
        Logger.info(f"called")
        async with self._drill_lock:
            self._drill_id = None

    @classmethod
    def is_drill_running(self):
        return self._drill_id is not None

    @classmethod
    async def update_active_device_sn_list(self, active_device_sn_list):
        async with self._drill_lock:
            self._active_device_sn_list = active_device_sn_list

    @classmethod
    async def update_livedata(self, df_live_calculated):
        async with self._drill_lock:
            start_time = time.time()
            from models.livedata.live_gnss_model import LiveGnssModel
            try:
                if df_live_calculated.empty or 'DeviceSN' not in df_live_calculated.columns:
                    self._connected_device_sn_valid_data_ratio_dict = {}
                    return
                
                self._connected_device_sn_valid_data_ratio_dict = dict(zip(df_live_calculated['DeviceSN'], df_live_calculated['ValidDataRatio']))
                
                df_live_calculated['IsActive'] = df_live_calculated['DeviceSN'].isin(self._active_device_sn_list)
                LiveGnssModel.save_df(df_live_calculated)
                
                df_live_calculated = df_live_calculated[df_live_calculated['IsActive']]
                if self._drill_id is not None:            

                    # カラム定義
                    sum_columns = [
                        "DurationSec", "TotalDist", "HIRDist", "HSRDist", "HMLDist",
                        "AccelEffort", "DecelEffort", "SprintEffort", "HSprintEffort",
                        "ValidDataCount", "TotalDataCount",
                    ]
                    max_columns = ["MaxSpeed", "MaxAccel"]
                    overwrite_columns = ["TimeStamp", "MaxSpeed_30s", "MaxAccel_30s", "IsActive", ]

                    # DrillGnssResultLiveModel に含まれるカラムのみに絞る
                    model_columns = DrillGnssResultLiveModel.get_column_names()
                    df_live_calculated = df_live_calculated[[col for col in model_columns if col in df_live_calculated.columns]]

                    # DrillID, TargetDate を追加
                    df_live_calculated[DrillGnssResultLiveModel.DrillID.name] = self._drill_id
                    df_live_calculated[DrillGnssResultLiveModel.TargetDate.name] = df_live_calculated["TimeStamp"].apply(lambda x: x.date())

                    # 初回ならそのまま代入、2回目以降：後ろに追加
                    df_live_calculated = pd.merge(df_live_calculated, self._df_live_player_device, how="left", on="DeviceSN")                    
                    if self._df_drill_detail is None or self._df_drill_detail.empty:
                        self._df_drill_detail = df_live_calculated
                    else:
                        self._df_drill_detail = pd.concat([self._df_drill_detail, df_live_calculated], ignore_index=True)

                    # groupby 集計
                    agg_dict = {col: "sum" for col in sum_columns}
                    agg_dict.update({col: "max" for col in max_columns})
                    agg_dict.update({col: "last" for col in overwrite_columns})

                    # その他は first
                    for col in self._df_drill_detail.columns:
                        if col not in agg_dict and col != "DeviceSN":
                            agg_dict[col] = "first"

                    self._df_drill_detail = self._df_drill_detail.groupby("DeviceSN", as_index=False).agg(agg_dict)

                    # 保存
                    # print(self._df_drill_detail["ID"])
                    self._df_drill_detail = DrillGnssResultLiveModel.save_df(self._df_drill_detail)

            except Exception as ex:
                Logger.error(f"例外が発生しました", ex)
            Logger.debug(f"update_livedata : {(time.time() - start_time):.3f} 秒")
            

    @classmethod
    async def process_udp_data(self, config:ConfigModel, trace_config_id):
        from models.livedata.live_gnss_model import LiveGnssModel
        Logger.info(f"start")
        UTC = timezone(timedelta(hours=0), 'UTC')
        time_local_unix = time.mktime(datetime.now().timetuple())
        time_utc_unix = time.mktime(datetime.now(UTC).timetuple())
        timezone_delta = pd.Timestamp(time_local_unix-time_utc_unix, unit='s').hour
        formatted_date = time.strftime("%d%m%y", time.localtime(time_local_unix))
        formatted_date_utc = time.strftime("%d%m%y", time.localtime(time_utc_unix))
        # 1日（86400秒）後のUTCのUnixタイム
        time_utc_unix_1day = time_utc_unix + 86400
        # 1日後の日付をフォーマット
        formatted_date_utc_1day = time.strftime("%d%m%y", time.localtime(time_utc_unix_1day))

        raw_data_buffer, raw_data_buffer_previous = [], []
        log = ""
        _test_flag = True
        while self._running:
            try:
                await asyncio.sleep(2)  # 2秒ごとにデータを処理

                log = ""
                start_time = time.time()

                if self._DEBUG and _test_flag:                    
                    df_live_player_device = DeviceModel.get_df_player_device(1, True)
                    device_sn_list = df_live_player_device["DeviceSN"].tolist()
                    df_live_calculated = self.generate_random_gnss_data(device_sn_list, 2)                     
                    asyncio.create_task(self.update_livedata(df_live_calculated))
                    self._elapsed_time = time.time() - start_time
                    continue

                raw_data_buffer = []
                while not self._udp_queue.empty():
                    raw_data_buffer.append(await self._udp_queue.get())

                raw_data_buffer_now = raw_data_buffer
                log += f"[1]{time.time() - start_time:.2f}=>"

                if self._udp_running_status == True:
                    try:
                        raw_data_buffer = raw_data_buffer_previous + raw_data_buffer_now
                    except UnboundLocalError:
                        pass
                elif self._udp_running_status == False:
                    self._udp_running_status = True
                    continue

                raw_data_buffer_previous = raw_data_buffer_now

                df_rbuff = pd.DataFrame(raw_data_buffer)

                if len(df_rbuff) == 0:
                    self._connected_device_sn_valid_data_ratio_dict = {}
                    continue
                else:
                    # df_rbuff.to_csv("rbuff.csv")
                    # 1行に複数レコードが混ざっている可能性に対処
                    df_rbuff = df_rbuff.iloc[:, 0].str.split('\n', expand=True).stack().reset_index(drop=True).to_frame(name='raw')
                    # セミコロンで分割
                    split_df = df_rbuff['raw'].str.split(';', expand=True)
                    # 4列以上ある行だけ使う
                    split_df = split_df.loc[split_df.notna().sum(axis=1) >= 4]
                    split_df = split_df.iloc[:, :4]
                    split_df.columns = ['DeviceSN', 'BSSID', 'RSSI', 'GNSS_Record']
                    # 結果に上書き
                    df_rbuff = split_df

                if len(df_rbuff) == 0 or len(df_rbuff) == 1:
                    continue
                else:
                    pass

                df_DeviceSN = df_rbuff['DeviceSN']                
                df_GNSS = df_rbuff['GNSS_Record']
                df_GNSS = df_GNSS.str.split(',', expand=True)

                df_live = pd.concat([df_DeviceSN, df_GNSS,], axis=1)
                df_live = df_live[df_live.iloc[:, 1] != "$GNTXT"]

                df_PUBX = df_live[df_live.iloc[:, 1] == "$PUBX"]
                #df_GNRMC = df_live[df_live.iloc[:, 1] == "$GNRMC"]

                log += f"[2]{time.time() - start_time:.2f}=>"

                # 仮で列を削除している。実データで修正必要。Dateは残さなくて良いか。
                #df_GNRMC = df_GNRMC.drop(df_GNRMC.columns[[1, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21]], axis=1)
                #df_GNRMC.columns = ['DeviceSN', 'Time_raw_str', 'Date_raw_str']
                
                #df_GNRMC['Date_raw_str'] = df_GNRMC['Date_raw_str'].apply(lambda x: x[-2:] + x[2:-2] + x[:2] if len(x) >= 4 else x)

                # df_PUBX = df_PUBX.drop(df_PUBX.columns[[1, 2, 5, 7, 8, 9, 11, 13, 15, 17, 18, 20, 21]], axis=1)
                # df_PUBX.columns = ['DeviceSN', 'Time_raw_str', 'Lat', 'Lon', 'Hacc', 'Speed', 'VVel', 'HDOP', 'NumSU']
                df_PUBX = df_PUBX.drop(df_PUBX.columns[[1]], axis=1)
                try:
                    df_PUBX.columns = ['DeviceSN', 'Time_raw_str', 'Hacc', 'Speed', 'VVel', 'HDOP', 'NumSU']
                except:
                    # DeviceSN 1 2 3 4 5 6 7 8 9 10 11 となっているケースがあった。データは正しそうなため下記対応を入れた
                    df_PUBX = df_PUBX.iloc[:, :7]  # 最初の7列だけを使う
                    df_PUBX.columns = ['DeviceSN', 'Time_raw_str', 'Hacc', 'Speed', 'VVel', 'HDOP', 'NumSU']
                df_PUBX['Date_raw_str'] = str(formatted_date)

                #df0_live = pd.merge(df_GNRMC, df_PUBX, on=['DeviceSN', 'Time_raw_str'], how='right')
                df0_live = df_PUBX
                df0_live = df0_live.sort_values(by=['DeviceSN', 'Date_raw_str',]).reset_index(drop=True)
                # df0_live = df0_live.drop(df_PUBX.columns[[2]], axis=1)

                log += f"[3]{time.time() - start_time:.2f}=>"

                # 仮で列名を変更。これも修正が必要。
                # df0_live.columns=['DeviceSN','Time_raw_str','Lat','Lon','Speed','Hacc','Vacc','VVel',]
                # 数値変換をまとめて適用
                cols_to_convert = [2, 3, 4, 5, 6,] #[4, 5, 6, 7, 8]
                df0_live.iloc[:, cols_to_convert] = df0_live.iloc[:, cols_to_convert].apply(pd.to_numeric, errors='coerce')

                # フィルタリング条件を一括で処理
                pattern = r'^[0-9]{6}\.[0-9]{2}$'
                invalid_patterns = r'[a-zA-Z&³]'

                df0_live = df0_live[
                    df0_live.iloc[:, 1].str.match(pattern, na=False) &  # 数字形式に一致
                    ~df0_live.iloc[:, 1].str.contains(invalid_patterns, na=False) &  # 無効な文字が含まれない
                    (df0_live.iloc[:, 1].str.len() == 9)  # 長さが9文字
                ]

                df0_live['Time_raw'] = df0_live['Time_raw_str']
                df1 = df0_live

                df1['Time_raw'] = df1['Time_raw'].astype(float)
                df1['Speed'] = df1['Speed'].astype(float)
                df1['Date_raw'] = formatted_date_utc
                df1['Hacc'] = df1['Hacc'].astype(float)
                df1['VVel'] = df1['VVel'].astype(float)
                df1['HDOP'] = df1['HDOP'].astype(float)
                df1['NumSU'] = df1['NumSU'].astype(float)

                # df1 = df1[df1['Hacc']<5]

                
                # if len(df1) == 0:
                #     Logger.debug('Hacc > 5 error')
                #     continue
                
                # NMEAの日付('Date_raw')がNaNの列は削除
                df1 = df1.dropna(subset=['Time_raw'])
                df1 = df1.dropna(subset=['Date_raw'])
                df1 = df1.dropna(subset=['Speed'])

                def GNSS_Live_Calculation_df1df5(df1, config):

                    df1['Hacc'] = df1['Hacc'].interpolate(method='linear')
                    df1['VVel'] = df1['VVel'].interpolate(method='linear')
                    df1['HDOP'] = df1['HDOP'].interpolate(method='linear')
                    df1['NumSU'] = df1['NumSU'].interpolate(method='linear')

                    try:
                        df1['VVel'] = df1['VVel']*3600/1000
                    except KeyError:
                        pass
                    # Speed(SOG)にVVelを加え、Speed(3D)を計算
                    try:
                        df1['Speed'] = np.sqrt(np.power(df1['Speed'], 2) + np.power(df1['VVel'], 2))
                    except KeyError:
                        pass
                    # VVelの列を削除

                    try:
                        df1 = df1.drop(['VVel', 'Lat', 'Lon',], axis=1)

                    except KeyError:
                        pass

                    df1 = df1.reset_index(drop=True)

                    if len(df1) == 0:
                        Logger.debug('Hacc error / ' + df1.loc[0, 'DeviceSN'])
                        return
                    
                    df1.loc[df1['Hacc'] > 5, 'Speed'] = None

                    # 外れ値以上の速度を削除。
                    SpeedOutlierThreshold = config.General.SpeedOutlierThreshold
                    df1['Speed'] = df1['Speed'].where(df1['Speed'] < float(SpeedOutlierThreshold), None).dropna()

                    # Object型になったKnotを、Float型に戻す。
                    df1['Speed'] = df1['Speed'] .astype(float)

                    # 小数点第二位を持つ異常値を取り除く。
                    df1["Time_raw2"] = round((df1["Time_raw"].astype(float))*10)/10

                    #UTCで日付を跨ぐ際のデバッグ用
                    '''
                    value_at_20th_row = float(df1.loc[19, "Time_raw2"])
                    df1.loc[20:, "Time_raw2"] = df1.loc[20:, "Time_raw2"].astype(float) - value_at_20th_row
                    time_start = float(23 * 10000 + 59 * 100 + 58)  # 23:59:58.000 → 235958.000
                    df1.loc[:19, "Time_raw2"] = [time_start + i * 0.1 for i in range(20)]
                    df1["Time_raw_str"] = df1["Time_raw2"].apply(lambda x: f"{x:09.3f}")'
                    '''

                    if df1.loc[len(df1) - 1, 'Time_raw2'] - df1.loc[0, 'Time_raw2'] < 0:
                        df1['Date_raw'][df1["Time_raw2"] < 1000] = formatted_date_utc_1day

                    # 時間及び日付の処理
                    # Date_rawの列を抽出
                    df1['Date'] = pd.to_datetime(df1['Date_raw'], format="%d%m%y").dt.date  # 165r.py

                    df1 = df1.reset_index(drop=True)
                    
                    #if len(df1.loc[0, 'Date_raw']) != len(df1.loc[len(df1) - 1, 'Date_raw']):
                    #    df1['Date'] = pd.to_datetime(df1['Date_raw'].astype(int).astype(str).str.zfill(6), format="%d%m%y").dt.date

                    df1['Time_UTC'] = pd.to_datetime(df1['Date_raw'] + df1['Time_raw_str'], format="%d%m%y%H%M%S.%f")
                    df1['Time_Local'] = df1['Time_UTC'] + offsets.Hour(timezone_delta)
                    
                    df1['Date'] = df1['Time_Local'].dt.date
                    df1['Date_raw'] = df1['Date'].astype(str)
                    df4 = df1

                    if df4.empty:
                        return

                    try:
                        end = df4.loc[len(df4)-1, 'Time_Local']
                    except KeyError:
                        pass

                    try:
                        start = df4.loc[0, 'Time_Local']
                    except KeyError:
                        pass

                    start_str = start.strftime('%Y%m%d%H%M%S')[-6:]

                    end_ms = end.microsecond
                    end = end - timedelta(microseconds=end_ms)

                    df4 = df4.set_index('Time_Local')
                    df4 = df4.drop_duplicates(keep='first')
                    df4 = df4.sort_index()

                    DataFrequency = config.General.DataFrequency
                    df4['Speed_diff'] = ((df4['Speed'].shift(1)-df4['Speed'])/(1/DataFrequency)) * \
                        ((df4['Speed']-df4['Speed'].shift(-1))/(1/DataFrequency))
                    df4 = df4[(df4['Speed_diff'] > -4000) | (pd.isnull(df4['Speed_diff']))]

                    # df4のtop_time:一番上　と bot_time:一番下の時間を取得
                    top_time, bot_time = df4.index.min(), df4.index.max()

                    # top_timeとbot_timeの間を、0.1秒ずつ再Indexし、データ欠落を補う。
                    try:
                        df5 = df4.reindex(pd.date_range(top_time, bot_time, freq='100ms'))
                    except ValueError as e:
                        df4 = df4[~df4.index.duplicated(keep='first')]
                        df5 = df4.reindex(pd.date_range(top_time, bot_time, freq='100ms'))

                    df5 = df5[['Time_raw_str', 'Hacc', 'Speed', 'Time_raw', 'Date_raw', 'Time_raw2', 'Date', 'Time_UTC',]]

                    # データ欠落部分を線形補完
                    df5[['Time_raw', 'Date_raw', 'Time_raw_str', 'Speed', 'Hacc', 'Time_raw2', 'Date' ]] \
                        = df5[['Time_raw', 'Date_raw', 'Time_raw_str', 'Speed', 'Hacc', 'Time_raw2', 'Date' ]].interpolate(method='linear', limit=100)
                    

                    # 計算未実施の行は0、計算を実施後に1とする。
                    df5['Calculate_flag'] = 0

                    df5['DeviceSN'] = df1.loc[0, 'DeviceSN']
                    df5 = df5.reset_index()
                    df5 = df5.rename(columns={'index': 'Time_Local'})

                    return df5        
                
                def GNSS_Live_Calculation_df1df5_2(df1, config):
                    from pandas.tseries.offsets import Hour

                    # 欠損値の補完
                    interpolate_cols = ['Hacc', 'VVel', 'HDOP', 'NumSU']
                    df1[interpolate_cols] = df1[interpolate_cols].interpolate(method='linear')

                    # VVel の変換と Speed(3D) の計算
                    if 'VVel' in df1.columns:
                        df1['VVel'] = df1['VVel'] * 3600 / 1000
                        df1['Speed'] = np.sqrt(np.square(df1['Speed']) + np.square(df1['VVel']))

                    # 不要な列の削除
                    drop_cols = ['VVel', 'Lat', 'Lon']
                    df1 = df1.drop(columns=[col for col in drop_cols if col in df1.columns], errors='ignore')
                    df1 = df1.reset_index(drop=True)

                    if len(df1) == 0:
                        Logger.debug('Hacc error / ' + df1.loc[0, 'DeviceSN'])
                        return

                    # 速度の外れ値を削除
                    SpeedOutlierThreshold = config.General.SpeedOutlierThreshold
                    df1['Speed'] = df1['Speed'].where(df1['Speed'] < SpeedOutlierThreshold)

                    # Knot を Float 型に変換
                    df1['Speed'] = df1['Speed'].astype(float)

                    # 小数点第二位を持つ異常値を削除
                    df1['Time_raw2'] = np.round(df1['Time_raw'].astype(float) * 10) / 10

                    # 時間および日付の処理
                    df1['Date'] = pd.to_datetime(df1['Date_raw'], format="%d%m%y").dt.date
                    df1 = df1.reset_index(drop=True)

                    if len(df1.loc[0, 'Date_raw']) != len(df1.loc[len(df1) - 1, 'Date_raw']):
                        df1['Date'] = pd.to_datetime(
                            df1['Date_raw'].astype(int).astype(str).str.zfill(6), format="%d%m%y"
                        ).dt.date

                    # UTC とローカル時間の計算
                    df1['Time_UTC'] = pd.to_datetime(df1['Date_raw'] + df1['Time_raw_str'], format="%d%m%y%H%M%S.%f")
                    df1['Time_Local'] = df1['Time_UTC'] + Hour(timezone_delta)
                    df1['Date'] = df1['Time_Local'].dt.date
                    df1['Date_raw'] = df1['Date'].astype(str)
                    df4 = df1

                    if df4.empty:
                        return

                    # 開始と終了時刻の取得
                    try:
                        start, end = df4.loc[0, 'Time_Local'], df4.loc[len(df4) - 1, 'Time_Local']
                    except KeyError:
                        return

                    start_str = start.strftime('%Y%m%d%H%M%S')[-6:]
                    end = end.replace(microsecond=0)

                    # 時間の再インデックスとデータ補完
                    df4 = df4.set_index('Time_Local').drop_duplicates(keep='first').sort_index()

                    DataFrequency = config.General.DataFrequency
                    df4['Speed_diff'] = ((df4['Speed'].shift(1) - df4['Speed']) / (1 / DataFrequency)) * \
                                        ((df4['Speed'] - df4['Speed'].shift(-1)) / (1 / DataFrequency))
                    df4 = df4[(df4['Speed_diff'] > -4000) | (df4['Speed_diff'].isnull())]

                    # 0.1秒刻みで再インデックス
                    top_time, bot_time = df4.index.min(), df4.index.max()
                    try:
                        df5 = df4.reindex(pd.date_range(top_time, bot_time, freq='100ms'))
                    except ValueError:
                        df4 = df4[~df4.index.duplicated(keep='first')]
                        df5 = df4.reindex(pd.date_range(top_time, bot_time, freq='100ms'))

                    # 必要な列を保持し、欠損値を補完
                    cols_to_keep = ['Time_raw_str', 'Hacc', 'Speed', 'Time_raw', 'Date_raw', 'Time_raw2', 'Date', 'Time_UTC']
                    df5 = df5[cols_to_keep]
                    df5[cols_to_keep] = df5[cols_to_keep].interpolate(method='linear', limit=100)

                    # 計算フラグとデバイス情報の追加
                    df5['Calculate_flag'] = 0
                    df5['DeviceSN'] = df1.loc[0, 'DeviceSN']
                    

                    df5 = df5.reset_index().rename(columns={'index': 'Time_Local'})
                    return df5
        
                
                log += f"[4]{time.time() - start_time:.2f}=>"

                df5_merge = df1.groupby('DeviceSN').apply(lambda x: GNSS_Live_Calculation_df1df5(x, config,)).reset_index(drop=True)
                # df5_merge = df1.groupby('DeviceSN').apply(lambda x: GNSS_Live_Calculation_df1df5_2(x, config,)).reset_index(drop=True) #TRY

                try:
                    df5_merge = pd.concat([df8_previous_merge, df5_merge])
                except UnboundLocalError:
                    pass

                def GNSS_Live_Calculation_df5df8(df5, config):

                    df5_DeviceSN = df5.reset_index(drop=True)

                    df5 = df5.set_index('Time_Local')
                    df5 = df5.sort_index()
                    df5 = df5.sort_values(by=[df5.index.name, 'Calculate_flag'], ascending=[True, True])
                    df5 = df5.loc[~(df5.index.duplicated(keep='last') & (df5['Calculate_flag'] == 0))]

                    df5_speed = df5.loc[:, 'Speed'].fillna(0)
                    df5 = df5.rename(columns={'Speed': 'Speed_Raw'})
                    data = df5_speed.to_list()

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
                        return
                    b, a = butter(order, normal_cutoff, btype=btype, analog=False)

                    # データにフィルタを適用（例として生成されたサンプルデータを使用）
                    butterworth_data = filtfilt(b, a, data)
                    df_butterworth = pd.DataFrame({'Speed_BW': butterworth_data})

                    # Butterworthフィルタに変更
                    df_butterworth['Time_Local'] = df5.index
                    df_butterworth = df_butterworth.set_index('Time_Local')

                    df5 = pd.concat([df5, df_butterworth], axis=1)
                    df5 = df5.rename(columns={'Speed_BW': 'Speed'})
                    df5.loc[df5['Speed'] < 0.05, 'Speed'] = 0
                    df8 = df5

                    # SpeedからAccelerationを算出。0.8秒の平均加速度を算出。
                    DataFrequency = config.General.DataFrequency
                    shifted_speed_m5 = df8['Speed'].shift(-5).fillna(method='ffill')  # 後ろのNaNを前の値で埋める
                    shifted_speed_p5 = df8['Speed'].shift(5).fillna(method='bfill')   # 前のNaNを後ろの値で埋める
                    df8['Acceleration'] = ((shifted_speed_m5 - shifted_speed_p5).shift(5)) * 1000 / 3600 / (10 / float(DataFrequency))
                    df8 = df8.dropna(subset=['Acceleration'])

                    # SpeedからDistanceを算出
                    df8['Distance'] = df8['Speed']*1000/3600*(1/DataFrequency)
                    df8.loc[df8[df8['Speed'] < 0.72].index, 'Distance'] = 0

                    # 外れ値以上/以下の加速度を削除。
                    AccelOutlierThresholdUpper = config.General.AccelOutlierThresholdUpper
                    AccelOutlierThresholdLower = config.General.AccelOutlierThresholdLower
                    df8['Acceleration'] = df8["Acceleration"].where(df8["Acceleration"] < float(AccelOutlierThresholdUpper), None).dropna()
                    df8['Acceleration'] = df8["Acceleration"].where(df8["Acceleration"] > float(AccelOutlierThresholdLower), None).dropna()

                    # Object型になったAccelerationを、Float型に戻す。
                    df8['Acceleration'] = df8['Acceleration'] .astype(float)

                    # データ欠落部分を補完
                    df8['Acceleration'] = df8['Acceleration'].interpolate(method='linear', limit=3, limit_direction='both')
                    df8['Date'] = df8['Date'].ffill()

                    df8['Speed2'] = df8['Speed']
                    df8['Speed2'] = df8['Speed2'].fillna(0)
                    df8['Acceleration'] = df8["Speed"].where(df8["Speed2"] == 0, df8['Acceleration'])
                    df8['Acceleration2'] = df8['Acceleration']
                    df8['Acceleration2'] = df8['Acceleration2'].ffill()

                    df8['RowCount_flag'] = 0
                    df8['DeviceSN'] = df5_DeviceSN.loc[0, 'DeviceSN']
                    df8 = df8.reset_index()
                    df8 = df8.rename(columns={'index': 'Time_Local'})
                    # Time_Localの小数部分だけ抽出
                    fractional = df8['Time_Local'].astype(str).str.split('.').str[1]
                    # 後ろからチェックして、端数だけカウント
                    drop_count = 0
                    df8_raw_count = len(df8)
                    for frac in reversed(fractional[-df8_raw_count :]):
                        if frac is not None and int(frac) < 900:  # 端数が 0～3 なら対象外にする
                            drop_count += 1
                        else:
                            break  # 完全な秒に達したら終わり
                    # 実際に1をセット
                    if drop_count < 620+drop_count and drop_count != 0:
                        df8.iloc[-620+drop_count:-drop_count, df8.columns.get_loc('RowCount_flag')] = 1
                    elif drop_count == 0:
                        df8.iloc[-620+drop_count:, df8.columns.get_loc('RowCount_flag')] = 1
                    
                    df8 = df8.tail(630)
                    return df8
                
                log += f"[5]{time.time() - start_time:.2f}=>"

                df8_merge = df5_merge.groupby('DeviceSN').apply(lambda x: GNSS_Live_Calculation_df5df8(
                    x, config,)).reset_index(drop=True)
                df8_previous_merge = df8_merge.copy()
                df8_merge = df8_merge[df8_merge['RowCount_flag'] == 1]
                df8_previous_merge.loc[:, 'Calculate_flag'][df8_previous_merge['RowCount_flag']==1]=1
                df8_previous_merge = df8_previous_merge.drop(df8_previous_merge.columns[[11, 12, 13, 14, 15, 16]], axis=1)
                df8_previous_merge = df8_previous_merge.rename(columns={'Speed_Raw': 'Speed'})
   
                columns = LiveGnssModel.get_column_names()
                def GNSS_Live_Performance_Calculation(df8, config: ConfigModel):
                    
                    df_live = pd.DataFrame(columns=columns)
                    df8 = df8.reset_index(drop=True)
                    df8_DeviceSN = df8.copy()  # 必要ならコピーを保持
                    df8 = df8.set_index('Time_Local').sort_index()
                    df10 = df8.drop(df8.columns[[0, 3, 4, 5]], axis=1)
                    df10 = df10[['Date', 'Time_UTC', 'Speed', 'Acceleration', 'Distance', 'Speed2', 'Acceleration2',
                                'Hacc', 'Speed_Raw', 'Calculate_flag']]
                    
                    df10['HaccRolling'] = df10['Hacc'].rolling(10, center=True).max()
                    df10['HaccRolling'] = df10['HaccRolling'].ffill()

                    MaxSpeedDuration = int(config.Duration.MaxSpeedDuration * 10)
                    df10['MaxSpeed'] = df10['Speed2'].rolling(MaxSpeedDuration).min()
                    df10.loc[df10['HaccRolling'] > config.General.GnssAccuracyFilter, 'MaxSpeed'] = 0

                    MaxAccelDuration = int(config.Duration.MaxAccelDuration * 10)
                    df10['MaxAccel'] = df10['Acceleration2'].rolling(MaxAccelDuration).min()
                    df10.loc[df10['HaccRolling'] > config.General.GnssAccuracyFilter, 'MaxAccel'] = 0

                    SprintEffortDuration = int(config.Duration.SprintEffortDuration * 10)
                    SprintThreshold = config.Metric.SprintZone
                    df10['SprintRolling'] = df10['Speed2'].rolling(SprintEffortDuration).min()
                    df10['SprintStatus'] = pd.NA
                    df10['SprintStatusLower'] = pd.NA
                    df10.loc[df10[df10['SprintRolling']>SprintThreshold].index, 'SprintStatus'] = 1
                    df10.loc[df10[df10['SprintRolling']>SprintThreshold-2].index, 'SprintStatusLower'] = 1
                    df10.loc[df10[df10['SprintStatusLower']!=1].index, 'SprintStatus'] = 0
                    df10['SprintStatus'] = df10['SprintStatus'].fillna(method='ffill')
                    df10['SprintStatus1'] = df10['SprintStatus']
                    df10.loc[df10[df10['SprintStatus1'] == 0].index, 'SprintStatus1'] = -1
                    df10['SprintEffort'] = df10['SprintStatus1'].shift(0) * df10['SprintStatus1'].shift(1)
                    df10.loc[df10[df10['SprintEffort'] == 1].index, 'SprintEffort'] = pd.NA
                    df10.loc[df10[df10['SprintEffort'] == -1].index, 'SprintEffort'] = 1
                    df10.loc[df10[(df10['SprintEffort'] == 1) & (df10['HaccRolling'] > config.General.GnssAccuracyFilter)].index, 'SprintEffort'] = pd.NA
                    df10.loc[df10[(df10['SprintEffort'] == 1) & (df10['SprintStatus'] == 0)].index, 'SprintEffort'] = pd.NA
                    df10.loc[df10[df10['HaccRolling'] > config.General.GnssAccuracyFilter].index, 'SprintStatus'] = 0
                    df10 = df10.drop(['SprintStatus1', 'SprintStatusLower', 'SprintRolling'], axis=1)


                    AccelEffortDuration = int(config.Duration.AccelEffortDuration * 10)
                    AccelThreshold = config.Metric.AccelZone
                    df10['AccelRolling'] = df10['Acceleration2'].rolling(AccelEffortDuration).min()
                    df10['AccelStatus'] = pd.NA
                    df10['AccelStatusLower'] = pd.NA
                    df10.loc[df10[df10['AccelRolling']>AccelThreshold].index, 'AccelStatus'] = 1
                    df10.loc[df10[df10['AccelRolling']>AccelThreshold-1].index, 'AccelStatusLower'] = 1
                    df10.loc[df10[df10['AccelStatusLower']!=1].index, 'AccelStatus'] = 0
                    df10['AccelStatus'] = df10['AccelStatus'].fillna(method='ffill')
                    df10['AccelStatus1'] = df10['AccelStatus']
                    df10.loc[df10[df10['AccelStatus1'] == 0].index, 'AccelStatus1'] = -1
                    df10['AccelEffort'] = df10['AccelStatus1'].shift(0) * df10['AccelStatus1'].shift(1)
                    df10.loc[df10[df10['AccelEffort'] == 1].index, 'AccelEffort'] = pd.NA
                    df10.loc[df10[df10['AccelEffort'] == -1].index, 'AccelEffort'] = 1
                    df10.loc[df10[(df10['AccelEffort'] == 1) & (df10['HaccRolling'] > config.General.GnssAccuracyFilter)].index, 'AccelEffort'] = pd.NA
                    df10.loc[df10[(df10['AccelEffort'] == 1) & (df10['AccelStatus'] == 0)].index, 'AccelEffort'] = pd.NA
                    df10.loc[df10[df10['HaccRolling'] > config.General.GnssAccuracyFilter].index, 'AccelStatus'] = 0
                    df10 = df10.drop(['AccelStatus1', 'AccelStatusLower', 'AccelRolling'], axis=1)


                    DecelEffortDuration = int(config.Duration.DecelEffortDuration * 10)
                    DecelThreshold = config.Metric.DecelZone
                    df10['DecelRolling'] = df10['Acceleration2'].rolling(DecelEffortDuration).max()
                    df10['DecelStatus'] = pd.NA
                    df10['DecelStatusLower'] = pd.NA
                    df10.loc[df10[df10['DecelRolling']<DecelThreshold].index, 'DecelStatus'] = 1
                    df10.loc[df10[df10['DecelRolling']<DecelThreshold+1].index, 'DecelStatusLower'] = 1
                    df10.loc[df10[df10['DecelStatusLower']!=1].index, 'DecelStatus'] = 0
                    df10['DecelStatus'] = df10['DecelStatus'].fillna(method='ffill')
                    df10['DecelStatus1'] = df10['DecelStatus']
                    df10.loc[df10[df10['DecelStatus1'] == 0].index, 'DecelStatus1'] = -1
                    df10['DecelEffort'] = df10['DecelStatus1'].shift(0) * df10['DecelStatus1'].shift(1)
                    df10.loc[df10[df10['DecelEffort'] == 1].index, 'DecelEffort'] = pd.NA
                    df10.loc[df10[df10['DecelEffort'] == -1].index, 'DecelEffort'] = 1
                    df10.loc[df10[(df10['DecelEffort'] == 1) & (df10['HaccRolling'] > config.General.GnssAccuracyFilter)].index, 'DecelEffort'] = pd.NA
                    df10.loc[df10[(df10['DecelEffort'] == 1) & (df10['HaccRolling'].isnull() == True)].index, 'DecelEffort'] = pd.NA
                    df10.loc[df10[(df10['DecelEffort'] == 1) & (df10['DecelStatus'] == 0)].index, 'DecelEffort'] = pd.NA
                    df10.loc[df10[df10['HaccRolling'] > config.General.GnssAccuracyFilter].index, 'DecelStatus'] = 0
                    df10.loc[df10[df10['HaccRolling'].isnull() == True].index, 'DecelStatus'] = 0
                    df10 = df10.drop(['DecelStatus1', 'DecelStatusLower', 'DecelRolling'], axis=1)


                    df_st = df10[df10['Calculate_flag'] == 0]
                    #df_st.to_csv(str(df8_DeviceSN.loc[0, 'DeviceSN']) +'_live_10Hz.csv', index=False, header=True, encoding='utf_8_sig', mode='a')

                    if len(df_st) == 0:
                        return
                    
                    start = df_st.index[0].to_pydatetime()
                    end = df_st.index[len(df_st)-1]

                    df_st_30s = df10.loc[end-offsets.Second(30): end]
                    total_seconds = (end-start).total_seconds()

                    # 基本のサンプリング間隔を決定
                    if total_seconds <= 1:
                        base_interval_sec = 1
                    elif total_seconds <= 2:
                        base_interval_sec = 2
                    elif total_seconds <= 3:
                        base_interval_sec = 3
                    elif total_seconds <= 4:
                        base_interval_sec = 4
                    elif total_seconds <= 5:
                        base_interval_sec = 5
                    else:
                        base_interval_sec = 5

                    # 5秒ごとに分割
                    chunk_count = 0
                    current_time = start
                    while current_time < end:
                        next_time = current_time + timedelta(seconds=base_interval_sec)
                        if next_time > end:
                            # 端数は調整（残り時間に応じて 1〜4秒にする）
                            remaining_sec = (end - current_time).total_seconds()
                            if remaining_sec <= 1:
                                next_time = current_time + timedelta(seconds=1)
                            elif remaining_sec <= 2:
                                next_time = current_time + timedelta(seconds=2)
                            elif remaining_sec <= 3:
                                next_time = current_time + timedelta(seconds=3)
                            elif remaining_sec <= 4:
                                next_time = current_time + timedelta(seconds=4)
                            else:
                                next_time = end  # 理論上ここには来ないはず
                                
                        df_st_chunk = df_st[(df_st.index >= current_time) & (df_st.index < next_time)]
                        
                        if not df_st_chunk.empty:
                            TotalDist = 0
                            TotalDist = round(df_st_chunk['Distance'].sum(), 1)  # td : Total Distancep
                            TotalDist = np.nan_to_num(TotalDist)

                            MaxSpeed = 0
                            MaxSpeed = round(df_st_chunk['MaxSpeed'].max(), 1)  # ms : Max Speed #198r.py
                            MaxSpeed = np.nan_to_num(MaxSpeed)

                            MaxSpeed_30s = 0
                            MaxSpeed_30s = round(df_st_30s['MaxSpeed'].max(), 1)  # ms : Max Speed #198r.py
                            MaxSpeed_30s = np.nan_to_num(MaxSpeed_30s)

                            MaxAccel = 0
                            MaxAccel = round(df_st_chunk['MaxAccel'].max(), 1)  # ma : Max Acceleration #198r.py
                            MaxAccel = np.nan_to_num(MaxAccel)

                            MaxAccel_30s = 0
                            MaxAccel_30s = round(df_st_30s['MaxAccel'].max(), 1)  # ma : Max Acceleration #198r.py
                            MaxAccel_30s = np.nan_to_num(MaxAccel_30s)

                            HIRThreshold = config.Metric.HIRZone
                            HIRLabel = ['HIR']  # hl : HIR Label
                            HIR = pd.cut(df_st_chunk.Speed, [float(HIRThreshold), 100], labels=HIRLabel)
                            HIRDist = round(float(df_st_chunk.groupby(HIR).Distance.sum()[0]), 1)  # hird : High Intensity Running Distance
                            HIRDist = np.nan_to_num(HIRDist)

                            SprintEffort = 0
                            SprintEffort = df_st_chunk['SprintEffort'].sum()
                            AccelEffort = 0
                            AccelEffort = df_st_chunk['AccelEffort'].sum()
                            DecelEffort = 0
                            DecelEffort = df_st_chunk['DecelEffort'].sum()
                            
                            ValidDataCount = int(df_st_chunk['Time_UTC'].count())
                            TotalDataCount = len(df_st_chunk['Time_UTC'])

                            ValidDataRatio = (ValidDataCount / TotalDataCount * 100) if TotalDataCount > 0 else 0
                            
                            df_live.loc[chunk_count, 'DeviceSN'] = df8_DeviceSN.loc[0, 'DeviceSN']
                            df_live.loc[chunk_count, 'ConfigID'] = config.selected_id
                            df_live.loc[chunk_count, 'TraceConfigID'] = trace_config_id

                            df_live.loc[chunk_count, 'TimeStamp'] = current_time
                            df_live.loc[chunk_count, 'DurationSec'] = float(round(len(df_st_chunk)/config.General.DataFrequency, 1))
                            df_live.loc[chunk_count, 'TotalDist'] = float(round(TotalDist, 2))
                            df_live.loc[chunk_count, 'MaxSpeed'] = float(round(MaxSpeed, 1))
                            df_live.loc[chunk_count, 'MaxAccel'] = float(round(MaxAccel, 1))
                            df_live.loc[chunk_count, 'MaxSpeed_30s'] = float(round(MaxSpeed_30s, 1))
                            df_live.loc[chunk_count, 'MaxAccel_30s'] = float(round(MaxAccel_30s, 1))
                            df_live.loc[chunk_count, 'HIRDist'] = float(round(HIRDist, 2))
                            df_live.loc[chunk_count, 'AccelEffort'] = int(AccelEffort)
                            df_live.loc[chunk_count, 'DecelEffort'] = int(DecelEffort)
                            df_live.loc[chunk_count, 'SprintEffort'] = int(SprintEffort)
                            df_live.loc[chunk_count, 'Hacc'] = float(round(df_st_chunk['Hacc'].mean(), 1))
                            df_live.loc[chunk_count, 'ValidDataCount'] = ValidDataCount
                            df_live.loc[chunk_count, 'TotalDataCount'] = TotalDataCount
                            df_live.loc[chunk_count, 'ValidDataRatio'] = ValidDataRatio
                            
                            
                            
                        chunk_count = chunk_count+1    
                        current_time = next_time

                    '''
                    TotalDist = 0
                    TotalDist = round(df_st['Distance'].sum(), 1)  # td : Total Distancep
                    TotalDist = np.nan_to_num(TotalDist)

                    MaxSpeed = 0
                    MaxSpeed = round(df_st['MaxSpeed'].max(), 1)  # ms : Max Speed #198r.py
                    MaxSpeed = np.nan_to_num(MaxSpeed)

                    MaxSpeed_30s = 0
                    MaxSpeed_30s = round(df_st_30s['MaxSpeed'].max(), 1)  # ms : Max Speed #198r.py
                    MaxSpeed_30s = np.nan_to_num(MaxSpeed_30s)

                    MaxAccel = 0
                    MaxAccel = round(df_st['MaxAccel'].max(), 1)  # ma : Max Acceleration #198r.py
                    MaxAccel = np.nan_to_num(MaxAccel)

                    MaxAccel_30s = 0
                    MaxAccel_30s = round(df_st_30s['MaxAccel'].max(), 1)  # ma : Max Acceleration #198r.py
                    MaxAccel_30s = np.nan_to_num(MaxAccel_30s)

                    HIRThreshold = config.Metric.HIRZone
                    HIRLabel = ['HIR']  # hl : HIR Label
                    HIR = pd.cut(df_st.Speed, [float(HIRThreshold), 100], labels=HIRLabel)
                    HIRDist = round(float(df_st.groupby(HIR).Distance.sum()[0]), 1)  # hird : High Intensity Running Distance
                    HIRDist = np.nan_to_num(HIRDist)

                    SprintEffort = 0
                    SprintEffort = df_st['SprintEffort'].sum()
                    AccelEffort = 0
                    AccelEffort = df_st['AccelEffort'].sum()
                    DecelEffort = 0
                    DecelEffort = df_st['DecelEffort'].sum()

                    ValidDataCount = int(df_st['Time_UTC'].count())
                    TotalDataCount = len(df_st['Time_UTC'])

                    ValidDataRatio = (ValidDataCount / TotalDataCount * 100) if TotalDataCount > 0 else 0

                    df_live.loc[0, 'DeviceSN'] = df8_DeviceSN.loc[0, 'DeviceSN']
                    df_live.loc[0, 'ConfigID'] = config.selected_id
                    df_live.loc[0, 'TraceConfigID'] = trace_config_id

                    df_live.loc[0, 'TimeStamp'] = start
                    df_live.loc[0, 'DurationSec'] = float(round(len(df_st)/config.General.DataFrequency, 1))
                    # df_live.loc[0, 'DurationSec'] = session_time
                    df_live.loc[0, 'TotalDist'] = float(round(TotalDist, 2))
                    df_live.loc[0, 'MaxSpeed'] = float(round(MaxSpeed, 1))
                    df_live.loc[0, 'MaxAccel'] = float(round(MaxAccel, 1))
                    df_live.loc[0, 'MaxSpeed_30s'] = float(round(MaxSpeed_30s, 1))
                    df_live.loc[0, 'MaxAccel_30s'] = float(round(MaxAccel_30s, 1))
                    df_live.loc[0, 'HIRDist'] = float(round(HIRDist, 2))
                    df_live.loc[0, 'AccelEffort'] = int(AccelEffort)
                    df_live.loc[0, 'DecelEffort'] = int(DecelEffort)
                    df_live.loc[0, 'SprintEffort'] = int(SprintEffort)
                    df_live.loc[0, 'Hacc'] = float(round(df_st['Hacc'].mean(), 1))
                    df_live.loc[0, 'ValidDataCount'] = ValidDataCount
                    df_live.loc[0, 'TotalDataCount'] = TotalDataCount
                    df_live.loc[0, 'ValidDataRatio'] = ValidDataRatio
                    '''
                    return df_live
            
                
                log += f"[6]{time.time() - start_time:.2f}=>"

                df_live_calculated = df8_merge.groupby('DeviceSN').apply(lambda x: GNSS_Live_Performance_Calculation(
                    x, config,)).reset_index(drop=True)
                
                self._elapsed_time = time.time() - start_time
                log += f"[Total]{self._elapsed_time:.2f}秒"
                Logger.debug(log)

                asyncio.create_task(self.update_livedata(df_live_calculated))
            except Exception as ex:
                Logger.error("例外が発生しました" , ex)
                await asyncio.sleep(1)
        Logger.info(f"end")

    @classmethod
    def generate_random_gnss_data(self, device_sn_list, count) -> pd.DataFrame:
        records = []

        for device_sn in device_sn_list:
            for i in range(count):
                record = {
                    "ID": None,  # DB挿入時に自動付与
                    "DeviceSN": device_sn,
                    "ConfigID": 1,
                    "TraceConfigID": random.randint(1, 5),
                    "TimeStamp": datetime.now() + timedelta(seconds=i*1),
                    "ValidDataCount": random.randint(10, 30),
                    "TotalDataCount": random.randint(10, 30),
                    "DurationSec": round(random.uniform(1.0, 5.0), 2),
                    "TotalDist": round(random.uniform(0.0, 2.0), 2),
                    "MaxSpeed": round(random.uniform(0.5, 1.5), 2),
                    "MaxAccel": round(random.uniform(-1.0, 1.0), 2),
                    "MaxSpeed_30s": round(random.uniform(0.5, 1.5), 2),
                    "MaxAccel_30s": round(random.uniform(-1.0, 1.0), 2),
                    "HIRDist": round(random.uniform(0.0, 1.0), 2),
                    "HSRDist": round(random.uniform(0.0, 1.0), 2),
                    "HMLDist": round(random.uniform(0.0, 1.0), 2),
                    "AccelEffort": round(random.uniform(0.0, 1.0), 2),
                    "DecelEffort": round(random.uniform(0.0, 1.0), 2),
                    "SprintEffort": round(random.uniform(0.0, 1.0), 2),
                    "HSprintEffort": round(random.uniform(0.0, 1.0), 2),
                    "IsActive": False,
                    "Hacc": round(random.uniform(0.0, 2.0), 2),
                    "HDOP": round(random.uniform(0.0, 2.0), 2),
                    "SVNum": random.randint(4, 20),
                }
                record["ValidDataRatio"] = min(100.0, (record["ValidDataCount"] / record["TotalDataCount"] * 100) if record["TotalDataCount"] > 0 else 0)
                records.append(record)

        return pd.DataFrame(records)


