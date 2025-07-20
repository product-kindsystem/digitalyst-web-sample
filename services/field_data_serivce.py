import math
import pandas as pd
import numpy as np
import requests
import googlemaps
from models.registration.field_model import FieldModel
from shapely.geometry import Point, Polygon


class FieldDataService:

    #座標変換用の定数
    f = 1/298.257223563
    a = 6378137
    e = math.sqrt(f * (2 - f))

    @staticmethod
    def load_gps_data(file_path, show_warning, show_error):  
        a, f, e = FieldDataService.a, FieldDataService.f, FieldDataService.e          
        
        df = pd.read_csv(file_path)
        df = df.round({'Lat': 6, 'Lon': 6})
        df = df[df['SVNum']>8]
        df = df[df['Hacc']<1]
        df = df[df['Speed']<0.15]
        #df = df.reset_index(drop=True)
        
        try:
            df1 = df.assign(count=0).groupby(['Lat','Lon'])['count'].count().reset_index()
        except KeyError as e:
            show_error(f"KeyError : {str(e)}")
            return None
        
        df2 = df1.sort_values('count', ascending=False)
        
        df3 = df2[:30].copy()
        #df3.to_csv("field_auto.csv")
        df3['Calc'] = df3['Lat'] * df3['Lon']
        df4 = df3.sort_values('Calc', ascending=False)
        df4 = df4.round({'Calc': 3})
        
        point_01 = df4[(df4['Calc'] == df4['Calc'].max()) | (df4['Calc'] == df4['Calc'].max()-0.001)].copy()
        point_02 = df4[(df4['Calc'] == df4['Calc'].min()) | (df4['Calc'] == df4['Calc'].min()+0.001)].copy()
        
        df5 = df4[~(df4['Calc'] == df4['Calc'].max())]
        df5 = df5[~(df5['Calc'] == df4['Calc'].max()-0.001)]
        df5 = df5[~(df5['Calc'] == df4['Calc'].min())]
        df5 = df5[~(df5['Calc'] == df4['Calc'].min()+0.001)]
        
        point_03 = df5[(df5['Calc'] == df5['Calc'].max()) | (df5['Calc'] == df5['Calc'].max()-0.001)].copy()
        point_04 = df5[(df5['Calc'] == df5['Calc'].min()) | (df5['Calc'] == df5['Calc'].min()+0.001)].copy()
        
        #point_01.at[:, 'Lat_count'] = point_01['Lat'] * point_01['count']
        #point_01.at[:, 'Lon_count'] = point_01['Lon'] * point_01['count']
        point_01.loc[:, 'Lat_count'] = point_01['Lat'] * point_01['count']
        point_01.loc[:, 'Lon_count'] = point_01['Lon'] * point_01['count']
        Lat_01 = round(point_01['Lat_count'].sum(numeric_only=True) / point_01['count'].sum(numeric_only=True), 7)
        Lon_01 = round(point_01['Lon_count'].sum(numeric_only=True) / point_01['count'].sum(numeric_only=True), 7)
        
        #point_02.at[:, 'Lat_count'] = point_02['Lat'] * point_02['count']
        #point_02.at[:, 'Lon_count'] = point_02['Lon'] * point_02['count']
        point_02.loc[:, 'Lat_count'] = point_02['Lat'] * point_02['count']
        point_02.loc[:, 'Lon_count'] = point_02['Lon'] * point_02['count']
        Lat_02 = round(point_02['Lat_count'].sum(numeric_only=True) / point_02['count'].sum(numeric_only=True), 7)
        Lon_02 = round(point_02['Lon_count'].sum(numeric_only=True) / point_02['count'].sum(numeric_only=True), 7)
        
        #point_03.at[:, 'Lat_count'] = point_03['Lat'] * point_03['count']
        #point_03.at[:, 'Lon_count'] = point_03['Lon'] * point_03['count']
        point_03.loc[:, 'Lat_count'] = point_03['Lat'] * point_03['count']
        point_03.loc[:, 'Lon_count'] = point_03['Lon'] * point_03['count']
        Lat_03 = round(point_03['Lat_count'].sum(numeric_only=True) / point_03['count'].sum(numeric_only=True), 7)
        Lon_03 = round(point_03['Lon_count'].sum(numeric_only=True) / point_03['count'].sum(numeric_only=True), 7)
        
        #point_04.at[:, 'Lat_count'] = point_04['Lat'] * point_04['count']
        #point_04.at[:, 'Lon_count'] = point_04['Lon'] * point_04['count']
        point_04.loc[:, 'Lat_count'] = point_04['Lat'] * point_04['count']
        point_04.loc[:, 'Lon_count'] = point_04['Lon'] * point_04['count']
        Lat_04 = round(point_04['Lat_count'].sum(numeric_only=True) / point_04['count'].sum(numeric_only=True), 7)
        Lon_04 = round(point_04['Lon_count'].sum(numeric_only=True) / point_04['count'].sum(numeric_only=True), 7)
        
        df6 = pd.DataFrame(columns=['Lat', 'Lon'])
        
        df6.at[1, 'Lat'] = Lat_01
        df6.at[1, 'Lon'] = Lon_01
        df6.at[2, 'Lat'] = Lat_02
        df6.at[2, 'Lon'] = Lon_02
        df6.at[3, 'Lat'] = Lat_03
        df6.at[3, 'Lon'] = Lon_03
        df6.at[4, 'Lat'] = Lat_04
        df6.at[4, 'Lon'] = Lon_04
        
        df6 = df6.sort_values('Lat', ascending=False).reset_index(drop=True)
        
        lat_max = df6.loc[:, 'Lat'].max()
        lat_min = df6.loc[:, 'Lat'].min()
        lon_max = df6.loc[:, 'Lon'].max()
        lon_min = df6.loc[:, 'Lon'].min()
        
        lat_mean = (lat_max + lat_min)/2
        lon_mean = (lon_max + lon_min)/2        
        
        r0 = a / (math.sqrt(1 - e*e * math.sin(lat_mean * math.pi / 180) * math.sin(lat_mean * math.pi /180)))
        x0 = r0 * math.cos(lat_mean * math.pi / 180) * math.cos(lon_mean * math.pi / 180)
        y0 = r0 * math.cos(lat_mean * math.pi / 180) * math.sin(lon_mean * math.pi / 180)
        z0 = r0 * (1 - e * e) * math.sin(lat_mean* math.pi / 180)
        
        for i in range(0, 4):
            df_rg = a / (np.sqrt(1 - e * e * np.sin(df6.loc[i, 'Lat'] * np.pi / 180) * np.sin(df6.loc[i, 'Lat'] * np.pi / 180)))
            df_xg = df_rg * np.cos(df6.loc[i, 'Lat'] * np.pi / 180) * np.cos(df6.loc[i, 'Lon'] * np.pi / 180)
            df_yg = df_rg * np.cos(df6.loc[i, 'Lat'] * np.pi / 180) * np.sin(df6.loc[i, 'Lon'] * np.pi / 180)
            df_zg = df_rg * (1 - e * e) * np.sin(df6.loc[i, 'Lat'] * np.pi / 180)
            df6.loc[i, 'E'] = -np.sin(df6.loc[i, 'Lon'] * np.pi / 180) * (df_xg - x0) + np.cos(df6.loc[i, 'Lon'] * np.pi / 180) * (df_yg - y0)
            df6.loc[i, 'N'] = -np.sin(df6.loc[i, 'Lat'] * np.pi / 180) * np.cos(df6.loc[i, 'Lon'] * np.pi / 180) * (df_xg - x0) - np.sin(df6.loc[i, 'Lat'] * np.pi / 180) * np.sin(df6.loc[i, 'Lon'] * np.pi / 180) * (df_yg - y0) + np.cos(df6.loc[i, 'Lat'] * np.pi / 180) * (df_zg - z0)
            
        #df6.to_csv("field_auto.csv")
        
        #df6.at[df6[(df6.loc[:, 'E'] >= 0) & (df6.loc[:, 'N'] >= 0)].index, 'NO'] = 0
        #df6.at[df6[(df6.loc[:, 'E'] < 0) & (df6.loc[:, 'N'] >= 0)].index, 'NO'] = 1
        #df6.at[df6[(df6.loc[:, 'E'] < 0) & (df6.loc[:, 'N'] < 0)].index, 'NO'] = 2
        #df6.at[df6[(df6.loc[:, 'E'] >= 0) & (df6.loc[:, 'N'] < 0)].index, 'NO'] = 3
        df6.loc[df6[(df6.loc[:, 'E'] >= 0) & (df6.loc[:, 'N'] >= 0)].index, 'NO'] = 0
        df6.loc[df6[(df6.loc[:, 'E'] < 0) & (df6.loc[:, 'N'] >= 0)].index, 'NO'] = 1
        df6.loc[df6[(df6.loc[:, 'E'] < 0) & (df6.loc[:, 'N'] < 0)].index, 'NO'] = 2
        df6.loc[df6[(df6.loc[:, 'E'] >= 0) & (df6.loc[:, 'N'] < 0)].index, 'NO'] = 3
        
        df6 = df6.sort_values('NO', ascending=True).reset_index(drop=True)
        print(df6)
        '''
        self.m_textCtrl1.SetValue(str(round(df6.loc[0, 'Lat'],8)))
        self.m_textCtrl2.SetValue(str(round(df6.loc[0, 'Lon'],8)))
        
        self.m_textCtrl17.SetValue(str(round(df6.loc[1, 'Lat'],8)))
        self.m_textCtrl18.SetValue(str(round(df6.loc[1, 'Lon'],8)))
        
        self.m_textCtrl19.SetValue(str(round(df6.loc[2, 'Lat'],8)))
        self.m_textCtrl23.SetValue(str(round(df6.loc[2, 'Lon'],8)))
        
        self.m_textCtrl3.SetValue(str(round(df6.loc[3, 'Lat'],8)))
        self.m_textCtrl4.SetValue(str(round(df6.loc[3, 'Lon'],8)))
        '''

        return {
            "Lat1":round(df6.loc[0, 'Lat'],7),
            "Lon1":round(df6.loc[0, 'Lon'],7),
            "Lat2":round(df6.loc[1, 'Lat'],7),
            "Lon2":round(df6.loc[1, 'Lon'],7),
            "Lat3":round(df6.loc[2, 'Lat'],7),
            "Lon3":round(df6.loc[2, 'Lon'],7),
            "Lat4":round(df6.loc[3, 'Lat'],7),
            "Lon4":round(df6.loc[3, 'Lon'],7),
        }
    
    
    @staticmethod
    def calculate(item: FieldModel, show_warning, show_error):
        a, f, e = FieldDataService.a, FieldDataService.f, FieldDataService.e

        df_ground = pd.DataFrame(columns = ['Lat', 'Lon'])
        for i in FieldModel.LAT_LON_INDEXES:            
            lat = item.get(f"Lat{i}", None)
            lon = item.get(f"Lon{i}", None)
            if lat and lon:
                df_ground.loc[i-1, 'Lat'] = round(float(lat),7)
                df_ground.loc[i-1, 'Lon'] = round(float(lon),7)
        
        if df_ground.empty:
            return item

        googleapikey = 'AIzaSyDJy6f3otu6T8D06svVDAoiCEJNX2tu3Dg'
        gmaps = googlemaps.Client(key=googleapikey)
        
        lat_max = df_ground.loc[:, 'Lat'].max()
        lat_min = df_ground.loc[:, 'Lat'].min()
        lon_max = df_ground.loc[:, 'Lon'].max()
        lon_min = df_ground.loc[:, 'Lon'].min()
        
        lat_mean = (lat_max + lat_min)/2
        lon_mean = (lon_max + lon_min)/2
        
        r0 = a / (math.sqrt(1 - e*e * math.sin(lat_mean * math.pi / 180) * math.sin(lat_mean * math.pi /180)))
        x0 = r0 * math.cos(lat_mean * math.pi / 180) * math.cos(lon_mean * math.pi / 180)
        y0 = r0 * math.cos(lat_mean * math.pi / 180) * math.sin(lon_mean * math.pi / 180)
        z0 = r0 * (1 - e * e) * math.sin(lat_mean* math.pi / 180)
        
        df_g = pd.DataFrame(columns = ['original_E', 'original_N','E', 'N'])
        
        for i in range(0, len(df_ground)):
            df_rg = a / (np.sqrt(1 - e * e * np.sin(df_ground.loc[i, 'Lat'] * np.pi / 180) * np.sin(df_ground.loc[i, 'Lat'] * np.pi / 180)))
            df_xg = df_rg * np.cos(df_ground.loc[i, 'Lat'] * np.pi / 180) * np.cos(df_ground.loc[i, 'Lon'] * np.pi / 180)
            df_yg = df_rg * np.cos(df_ground.loc[i, 'Lat'] * np.pi / 180) * np.sin(df_ground.loc[i, 'Lon'] * np.pi / 180)
            df_zg = df_rg * (1 - e * e) * np.sin(df_ground.loc[i, 'Lat'] * np.pi / 180)
            df_g.loc[i, 'original_E'] = -np.sin(df_ground.loc[i, 'Lon'] * np.pi / 180) * (df_xg - x0) + np.cos(df_ground.loc[i, 'Lon'] * np.pi / 180) * (df_yg - y0)
            df_g.loc[i, 'original_N'] = -np.sin(df_ground.loc[i, 'Lat'] * np.pi / 180) * np.cos(df_ground.loc[i, 'Lon'] * np.pi / 180) * (df_xg - x0) - np.sin(df_ground.loc[i, 'Lat'] * np.pi / 180) * np.sin(df_ground.loc[i, 'Lon'] * np.pi / 180) * (df_yg - y0) + np.cos(df_ground.loc[i, 'Lat'] * np.pi / 180) * (df_zg - z0)
            
        cos_g = (df_g.loc[2, 'original_E'] - df_g.loc[1, 'original_E']) / math.sqrt((df_g.loc[2, 'original_E'] - df_g.loc[1, 'original_E']) ** 2 + (df_g.loc[2, 'original_N'] - df_g.loc[1, 'original_N']) ** 2)
        sin_g = (df_g.loc[2, 'original_N'] - df_g.loc[1, 'original_N']) / math.sqrt((df_g.loc[2, 'original_E'] - df_g.loc[1, 'original_E']) ** 2 + (df_g.loc[2, 'original_N'] - df_g.loc[1, 'original_N']) ** 2)
        
        for i in range(0, len(df_ground)):
            df_g.loc[i, 'E'] = df_g.loc[i, 'original_E'] * cos_g + df_g.loc[i, 'original_N'] * sin_g
            df_g.loc[i, 'N'] = -df_g.loc[i, 'original_E'] * sin_g + df_g.loc[i, 'original_N'] * cos_g
            
        e_max = df_g.loc[:, 'E'].max()
        e_min = df_g.loc[:, 'E'].min()
        n_max = df_g.loc[:, 'N'].max()
        n_min = df_g.loc[:, 'N'].min()
            
        if e_max >= -e_min:
            pass
        else:
            e_max = -e_min
        if n_max >= -n_min:
            pass
        else:
            n_max = -n_min

        df_g_1 = df_g[:1].copy()
        df_g_1_less = df_g[1:].copy()
        df_g_1_less = df_g_1_less.iloc[::-1]
        df_g_1_less.index = range(len(df_g_1_less))
        df_g = pd.concat([df_g_1, df_g_1_less])
        df_g = df_g.reset_index(drop=True)
        
        field_address = ""
        try:
            results = gmaps.reverse_geocode((lat_mean, lon_mean),language='ja')
            field_address = results[0]['formatted_address']
        except requests.exceptions.ConnectionError as e:
            show_warning(f" requests.exceptions.ConnectionError : {str(e)}")
            pass
        except googlemaps.exceptions.HTTPError:
            show_warning(f" googlemaps.exceptions.HTTPError : {str(e)}")        
        
        # Set Calculation Result
        item['E_max'] = float(e_max)
        item['N_max'] = float(n_max)
        item['CenterLat'] = float(lat_mean)
        item['CenterLon'] = float(lon_mean)
        item['COS'] = float(cos_g)
        item['SIN'] = float(sin_g)
        item['Address'] = field_address
        
        for i in FieldModel.LAT_LON_INDEXES:
            item['E' + str(i)] = 0
            item['N' + str(i)] = 0            
        
        for i in range(len(df_g)):
            item['E' + str(i+1)] = float(df_g.loc[i, 'E'])
            item['N' + str(i+1)] = float(df_g.loc[i, 'N'])  

        item['Points'] = int(len(df_g))

        return item
    

    @staticmethod
    def get_df_field_in_log(df10, field_calc_duration_sec=1):
        a, f, e = FieldDataService.a, FieldDataService.f, FieldDataService.e

        #デジタルフェンスの対象となっているフィールドのリストを取得する
        df_field = FieldModel.get_df()
        df_field = df_field[df_field.loc[:, 'PerformanceFlag'] == 1]
        df_field = df_field.reset_index(drop=True)

        #デジタルフェンス用に緯度経度をローカル座標XYに変換する。
        #対象はデジタルフェンス用に選択されているフィールド全て
        df10_column_name = ['Lat', 'Lon']
        df10 = df10[df10_column_name]

        #デジタルフェンス計算
        #選択されているフィールドの中にいる場合にのみ、パフォーマンス計算を有効にする。
        #選手交代を簡易的に位置情報から検出し、選手がフィールドの中にいる時間のみで計算する

        # 出力用 DataFrame
        df_field_in_log = pd.DataFrame(columns=['FieldID', 'StartTime', 'EndTime'])

        # 時間でグルーピング（5秒単位）
        duration_str = f"{field_calc_duration_sec}S"
        agg_dict = {col: 'mean' for col in df10_column_name}
        df_grouped = df10.groupby(df10.index.floor(duration_str)).agg(agg_dict).reset_index().rename(columns={'index': 'Time_Group'})

        # 出力用 DataFrame
        df_field_in_log = pd.DataFrame(columns=['TimeStamp', 'FieldID', 'StartTime', 'EndTime'])

        # 事前に全ポリゴンを shapely の Polygon で作成
        polygons = []
        for _, field_row in df_field.iterrows():
            if pd.notnull(field_row['Lat1']):
                points = []
                for m in range(1, int(field_row['Points']) + 1):
                    lat = field_row.get(f'Lat{m}')
                    lon = field_row.get(f'Lon{m}')
                    if pd.notnull(lat) and pd.notnull(lon):
                        points.append((lat, lon))
                if len(points) >= 3:
                    polygons.append((field_row['ID'], Polygon(points)))

        # 状態管理用フラグ
        field_in_flag = False
        field_in_id = None
        field_in_time = None
        field_out_time = None

        # 各時間グループに対して処理
        for _, row in df_grouped.iterrows():
            
            try:
                if pd.notna(row['Lat']) and pd.notna(row['Lon']):
                    in_flag = False
                    in_id = None
                    point = Point(row['Lat'], row['Lon'])

                    for pid, poly in polygons:
                        if poly.contains(point):
                            in_flag = True
                            in_id = pid
                            break

                    if not field_in_flag and in_flag:
                        try:
                            field_in_flag = True
                            field_in_id = in_id
                            field_in_time = row['Time_Group']
                        except:
                            pass

                    elif field_in_flag and in_flag and field_in_id != in_id:
                        field_out_time = row['Time_Group']
                        df_field_in_log = pd.concat([df_field_in_log, pd.DataFrame([{
                            'FieldID': field_in_id,
                            'StartTime': field_in_time,
                            'EndTime': field_out_time,
                            'DurationSec' : (field_out_time - field_in_time).total_seconds()
                        }])], ignore_index=True)
                        field_in_id = in_id
                        field_in_time = row['Time_Group']

                    elif field_in_flag and not in_flag:
                        field_out_time = row['Time_Group']
                        df_field_in_log = pd.concat([df_field_in_log, pd.DataFrame([{
                            'FieldID': field_in_id,
                            'StartTime': field_in_time,
                            'EndTime': field_out_time,
                            'DurationSec' : (field_out_time - field_in_time).total_seconds()
                        }])], ignore_index=True)
                        field_in_flag = False
                        field_in_id = None
                        field_in_time = None
                        field_out_time = None
            except Exception as e:
                print(f"get_df_field_in_log Error : {str(e)}")
                print(row)
        
        if field_in_flag and field_in_time is not None:
            field_out_time = df_grouped.iloc[-1]['Time_Group']  # 最後の時間を終了時刻とする
            df_field_in_log = pd.concat([df_field_in_log, pd.DataFrame([{
                'FieldID': field_in_id,
                'StartTime': field_in_time,
                'EndTime': field_out_time,
                'DurationSec': (field_out_time - field_in_time).total_seconds()
            }])], ignore_index=True)

        return df_field_in_log
        