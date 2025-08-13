import numpy as np
import pandas as pd
import time
from typing import List
from models.drill.drill_gnss_result_model import DrillGnssResultModel
from models.importdata.import_gnss_model import ImportGnssModel
from models.registration.metric_model import MetricModel, MetricEnum
from models.registration.config_model import ConfigModel
from models.registration.threshold_model import ThreEnum
from models.registration.player_model import PlayerModel
from services.import_data_function_create_df10 import CreateDf10
from services.logger_service import Logger


# ---------------- 各プレイヤーのメトリクス計算 ----------------

def calculate_metrics_for_player(df10, config: ConfigModel, metrics: List[MetricModel], player: PlayerModel, is_live, is_all_result_required = False):
    Logger.debug(f"パフォーマンス計算開始")
    start_time = time.time()

    def is_result_required(metrics_name_list):
        if is_all_result_required:
            return True
        for metric in metrics:
            if metric.Name in metrics_name_list:
                return True
        return False
    
    start_timestamp = df10["TimeStamp"].iloc[0]
    end_timestamp = df10["TimeStamp"].iloc[-1]
    total_duration_sec = (end_timestamp - start_timestamp).total_seconds()
    if total_duration_sec <= 0:
        return {}
    total_duration_min = total_duration_sec / 60

    result = {}

    # --------------------------------------------------------------------------
    # BASIC INFO

    result[DrillGnssResultModel.TotalDist.name] = float(df10[ImportGnssModel.TotalDist.name].sum())
    result[DrillGnssResultModel.ValidDataCount.name] = len(df10)
    result[DrillGnssResultModel.TotalDataCount.name] = len(df10)

    result[DrillGnssResultModel.DurationSec.name] = total_duration_sec
    result[DrillGnssResultModel.MaxSpeed.name] = float(df10[MetricEnum.MaxSpeed.name].max())
    result[DrillGnssResultModel.MaxAccel.name] = float(df10[MetricEnum.MaxAccel.name].max())
    if is_result_required([MetricEnum.AvgDistPerMin.name]):
        result[DrillGnssResultModel.AvgDistPerMin.name] = float(result[DrillGnssResultModel.TotalDist.name] / total_duration_min if total_duration_min != 0 else 0)
    if is_result_required([MetricEnum.PercentMaxSpeed.name]):
        result[DrillGnssResultModel.PercentMaxSpeed.name] = float(result[DrillGnssResultModel.MaxSpeed.name] / player.TopSpeed * 100 if player.TopSpeed != 0 else 0)
    if is_result_required([MetricEnum.PercentMaxAccel.name]):
        result[DrillGnssResultModel.PercentMaxAccel.name] = float(result[DrillGnssResultModel.MaxAccel.name] / player.TopAccel * 100 if player.TopAccel != 0 else 0)
    total_dist = result[DrillGnssResultModel.TotalDist.name]
    Logger.debug(f"BASIC INFO : {time.time() - start_time:.3f} 秒")

    if is_live:
        sum_list = [
            MetricEnum.HIRDist.name, MetricEnum.HSRDist.name, MetricEnum.AccelEffort.name, 
            MetricEnum.DecelEffort.name, MetricEnum.SprintEffort.name, MetricEnum.HSprintEffort.name,
        ]
        for enum_name in sum_list:        
            result[enum_name] = float(df10[enum_name].sum())
        return result

    # --------------------------------------------------------------------------
    # SpeedZone?Dist
    if is_result_required([
            MetricEnum.SpeedZone1Dist.name, MetricEnum.SpeedZone2Dist.name, MetricEnum.SpeedZone3Dist.name, 
            MetricEnum.SpeedZone4Dist.name, MetricEnum.SpeedZone5Dist.name, MetricEnum.SpeedZone6Dist.name,
            MetricEnum.SpeedZone1Ratio.name, MetricEnum.SpeedZone2Ratio.name, MetricEnum.SpeedZone3Ratio.name, 
            MetricEnum.SpeedZone4Ratio.name, MetricEnum.SpeedZone5Ratio.name, MetricEnum.SpeedZone6Ratio.name,
            MetricEnum.HIRDist.name, MetricEnum.HSRDist.name]):
        sum_list = [
            MetricEnum.SpeedZone1Dist.name, MetricEnum.SpeedZone2Dist.name, MetricEnum.SpeedZone3Dist.name, 
            MetricEnum.SpeedZone4Dist.name, MetricEnum.SpeedZone5Dist.name, MetricEnum.SpeedZone6Dist.name,
        ]
        for enum_name in sum_list:        
            result[enum_name] = float(df10[enum_name].sum())

        # DistRatio
        enum_list = [
            MetricEnum.SpeedZone1Ratio, MetricEnum.SpeedZone2Ratio, MetricEnum.SpeedZone3Ratio, MetricEnum.SpeedZone4Ratio, MetricEnum.SpeedZone5Ratio, MetricEnum.SpeedZone6Ratio,
        ]
        for enum in enum_list:
            zone_dist = result[enum.name.replace("Ratio", "Dist")]   
            result[enum.name] = zone_dist / total_dist * 100 if total_dist != 0 else 0
        
        # HIRDist
        zone_num = config.get_value_by_thre_enum(ThreEnum.ZoneSelect.HIRZone)
        result[DrillGnssResultModel.HIRDist.name] = 0
        for i in [1,2,3,4,5,6]:
            if i >= zone_num:
                result[DrillGnssResultModel.HIRDist.name] += result[f"SpeedZone{i}Dist"]
        result[DrillGnssResultModel.HIRRatio.name] = result[DrillGnssResultModel.HIRDist.name] / total_dist * 100 if total_dist != 0 else 0

        # HSRDist
        zone_num = config.get_value_by_thre_enum(ThreEnum.ZoneSelect.HSRZone)
        result[DrillGnssResultModel.HSRDist.name] = 0
        for i in [1,2,3,4,5,6]:
            if i >= zone_num:
                result[DrillGnssResultModel.HSRDist.name] += result[f"SpeedZone{i}Dist"]
        result[DrillGnssResultModel.HSRRatio.name] = result[DrillGnssResultModel.HSRDist.name] / total_dist * 100 if total_dist != 0 else 0
        Logger.debug(f"SpeedZone?Dist : {time.time() - start_time:.3f} 秒")


    # --------------------------------------------------------------------------
    # SpeedZone?Effort
    if is_result_required([
            MetricEnum.SpeedZone1Effort.name, MetricEnum.SpeedZone2Effort.name, MetricEnum.SpeedZone3Effort.name, 
            MetricEnum.SpeedZone4Effort.name, MetricEnum.SpeedZone5Effort.name, MetricEnum.SpeedZone6Effort.name,
            MetricEnum.SprintEffort.name, MetricEnum.HSprintEffort.name]):
        sum_list = [
            MetricEnum.SpeedZone1Effort.name, MetricEnum.SpeedZone2Effort.name, MetricEnum.SpeedZone3Effort.name, 
            MetricEnum.SpeedZone4Effort.name, MetricEnum.SpeedZone5Effort.name, MetricEnum.SpeedZone6Effort.name,
        ]
        for enum_name in sum_list:        
            result[enum_name] = int(pd.to_numeric(df10[enum_name], errors="coerce").fillna(0).sum())
        
        # SprintEffort
        zone_num = config.get_value_by_thre_enum(ThreEnum.ZoneSelect.SprintZone)
        result[DrillGnssResultModel.SprintEffort.name] = result[f"SpeedZone{zone_num}Effort"]
        
        # HSprintEffort
        zone_num = config.get_value_by_thre_enum(ThreEnum.ZoneSelect.HSprintZone)
        result[DrillGnssResultModel.HSprintEffort.name] = result[f"SpeedZone{zone_num}Effort"]
        Logger.debug(f"SpeedZone?Effort : {time.time() - start_time:.3f} 秒")

    # --------------------------------------------------------------------------
    # AccelZone?Effort            
    if is_result_required([
            MetricEnum.AccelZone1Effort.name, MetricEnum.AccelZone2Effort.name, MetricEnum.AccelZone3Effort.name, MetricEnum.AccelZone4Effort.name,
            MetricEnum.AccelEffort.name]):
        sum_list = [
            MetricEnum.AccelZone1Effort.name, MetricEnum.AccelZone2Effort.name, MetricEnum.AccelZone3Effort.name, MetricEnum.AccelZone4Effort.name,
        ]
        for enum_name in sum_list:
            result[enum_name] = int(pd.to_numeric(df10[enum_name], errors="coerce").fillna(0).sum())
    
        # AccelEffort
        zone_num = config.get_value_by_thre_enum(ThreEnum.ZoneSelect.AccelZone)
        result[DrillGnssResultModel.AccelEffort.name] = result[f"AccelZone{zone_num}Effort"]
        result[DrillGnssResultModel.AccelEffortPerMin.name] = result[DrillGnssResultModel.AccelEffort.name] / total_duration_min if total_duration_min != 0 else 0
        Logger.debug(f"AccelZone?Effort : {time.time() - start_time:.3f} 秒")

    # --------------------------------------------------------------------------
    # DecelZone?Effort            
    if is_result_required([
            MetricEnum.DecelZone1Effort.name, MetricEnum.DecelZone2Effort.name, MetricEnum.DecelZone3Effort.name, MetricEnum.DecelZone4Effort.name,
            MetricEnum.DecelEffort.name]):
        sum_list = [
            MetricEnum.DecelZone1Effort.name, MetricEnum.DecelZone2Effort.name, MetricEnum.DecelZone3Effort.name, MetricEnum.DecelZone4Effort.name,
        ]
        for enum_name in sum_list:        
            result[enum_name] = int(pd.to_numeric(df10[enum_name], errors="coerce").fillna(0).sum())

        # DecelEffort
        zone_num = config.get_value_by_thre_enum(ThreEnum.ZoneSelect.DecelZone)
        result[DrillGnssResultModel.DecelEffort.name] = result[f"DecelZone{zone_num}Effort"]
        result[DrillGnssResultModel.DecelEffortPerMin.name] = result[DrillGnssResultModel.DecelEffort.name] / total_duration_min if total_duration_min != 0 else 0
        Logger.debug(f"DecelZone?Effort : {time.time() - start_time:.3f} 秒")

    # --------------------------------------------------------------------------
    # OTHER Effort
    sum_list = [   
        MetricEnum.QuickAccelZone1Effort.name, MetricEnum.QuickAccelZone2Effort.name, MetricEnum.QuickAccelZone3Effort.name, MetricEnum.QuickAccelZone4Effort.name, 
        MetricEnum.QuickDecelZone1Effort.name, MetricEnum.QuickDecelZone2Effort.name, MetricEnum.QuickDecelZone3Effort.name, MetricEnum.QuickDecelZone4Effort.name, 
        MetricEnum.BaseBallScore.name, MetricEnum.BaseBallScoreLow.name, MetricEnum.BaseBallScoreMid.name, MetricEnum.BaseBallScoreHigh.name,
    ]
    for enum_name in sum_list:       
        if is_result_required([enum_name]):
            result[enum_name] = int(pd.to_numeric(df10[enum_name], errors="coerce").fillna(0).sum())
    Logger.debug(f"OTHER Effort : {time.time() - start_time:.3f} 秒")

    # --------------------------------------------------------------------------
    # RHIRDist
    if is_result_required([MetricEnum.RHIRDist.name, MetricEnum.RHIRRatio.name]):
        percent = config.Metric.RHIRZone
        threshold = player.TopSpeed * percent / 100
        result[DrillGnssResultModel.RHIRDist.name] = float(df10.loc[df10[DrillGnssResultModel.MaxSpeed.name] >= threshold, ImportGnssModel.TotalDist.name].sum())
        result[DrillGnssResultModel.RHIRRatio.name] = result[DrillGnssResultModel.RHIRDist.name] / total_dist * 100 if total_dist != 0 else 0
        Logger.debug(f"RHIRDist : {time.time() - start_time:.3f} 秒")
    
    # --------------------------------------------------------------------------
    # RHSRDist
    if is_result_required([MetricEnum.RHSRDist.name, MetricEnum.RHSRRatio.name]):
        percent = config.Metric.RHSRZone
        threshold = player.TopSpeed * percent / 100
        result[DrillGnssResultModel.RHSRDist.name] = float(df10.loc[df10[DrillGnssResultModel.MaxSpeed.name] >= threshold, ImportGnssModel.TotalDist.name].sum())
        result[DrillGnssResultModel.RHSRRatio.name] = result[DrillGnssResultModel.RHSRDist.name] / total_dist * 100 if total_dist != 0 else 0
        Logger.debug(f"RHSRDist : {time.time() - start_time:.3f} 秒")
    
    # --------------------------------------------------------------------------
    # RSpeedZone?Dist
    enum_dict = {
        MetricEnum.RSpeedZone1Dist: ThreEnum.RSpeedZone.RSpeedZone1, 
        MetricEnum.RSpeedZone2Dist: ThreEnum.RSpeedZone.RSpeedZone2, 
        MetricEnum.RSpeedZone3Dist: ThreEnum.RSpeedZone.RSpeedZone3, 
        MetricEnum.RSpeedZone4Dist: ThreEnum.RSpeedZone.RSpeedZone4, 
    }
    for enum, thre in enum_dict.items():
        ratio_enum_name = enum.name.replace("Dist", "Ratio")
        if is_result_required([enum.name, ratio_enum_name]):
            percent = config.get_value_by_thre_enum(thre)
            threshold = player.TopSpeed * percent / 100
            result[enum.name] = float(df10.loc[df10[DrillGnssResultModel.MaxSpeed.name] >= threshold, ImportGnssModel.TotalDist.name].sum())
            result[ratio_enum_name] = result[enum.name] / total_dist * 100 if total_dist != 0 else 0
    Logger.debug(f"RSpeedZone?Dist : {time.time() - start_time:.3f} 秒")


    # --------------------------------------------------------------------------
    # R?Effort
    if is_result_required([MetricEnum.RSprintEffort.name, MetricEnum.RHSprintEffort.name, MetricEnum.RAccelEffort.name, MetricEnum.RAccelEffortPerMin.name]):
        
        # 事前にメトリクス計算用のカラムを追加
        gps_hacc = config.General.GnssAccuracyFilter
        list_speed = df10.loc[:,'Speed2'].tolist()
        ms_s2 = np.nan_to_num(round(df10['Speed2'].max(),1))
        list_acc = df10.loc[:, 'Acceleration2'].tolist()
        ma_a2 = np.nan_to_num(round(df10['Acceleration2'].max(),1))
        list_Hacc = df10.loc[:, 'Hacc'].tolist()
        
        # --------------------------------------------------------------------------
        # RSprintEffort
        if is_result_required([MetricEnum.RSprintEffort.name]):
            enum_name = MetricEnum.RSprintEffort.name        
            col_name, threshold, duration = enum_name, player.TopSpeed * config.Metric.RSprintZone / 100, config.Duration.SprintEffortDuration
            CreateDf10.addOverThresholdCountColumn(df10, col_name, list_speed, ms_s2, list_Hacc, gps_hacc, threshold, duration, 2)
            result[enum_name] = int(pd.to_numeric(df10[enum_name], errors="coerce").fillna(0).sum())

        # --------------------------------------------------------------------------
        # RHSprintEffort
        if is_result_required([MetricEnum.RHSprintEffort.name]):
            enum_name = MetricEnum.RHSprintEffort.name
            col_name, threshold, duration = enum_name, player.TopSpeed * config.Metric.RHSprintZone / 100, config.Duration.SprintEffortDuration
            CreateDf10.addOverThresholdCountColumn(df10, col_name, list_speed, ms_s2, list_Hacc, gps_hacc, threshold, duration, 2)
            result[enum_name] = int(pd.to_numeric(df10[enum_name], errors="coerce").fillna(0).sum())

        # --------------------------------------------------------------------------
        # RAccelEffort
        if is_result_required([MetricEnum.RAccelEffort.name, MetricEnum.RAccelEffortPerMin.name]):
            enum_name = MetricEnum.RAccelEffort.name
            col_name, threshold, duration = enum_name, player.TopAccel * config.Metric.RAccelZone / 100, config.Duration.AccelEffortDuration
            CreateDf10.addOverThresholdCountColumn(df10, col_name, list_acc, ma_a2, list_Hacc, gps_hacc, threshold, duration, 1)
            result[enum_name] = int(pd.to_numeric(df10[enum_name], errors="coerce").fillna(0).sum())
            result[DrillGnssResultModel.RAccelEffortPerMin.name] = result[DrillGnssResultModel.RAccelEffort.name] / total_duration_min if total_duration_min != 0 else 0    

        Logger.debug(f"R?Effort : {time.time() - start_time:.3f} 秒")

    Logger.debug(f"パフォーマンス計算終了 : {time.time() - start_time:.3f} 秒")
    return result


def aggregate_gnss_result_by_player(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    group_key = DrillGnssResultModel.PlayerID.name

    max_cols = [DrillGnssResultModel.MaxSpeed.name, DrillGnssResultModel.MaxAccel.name, DrillGnssResultModel.PercentMaxSpeed.name, DrillGnssResultModel.PercentMaxAccel.name]
    sum_cols = [DrillGnssResultModel.DurationSec.name, DrillGnssResultModel.DurationMin.name]
    first_cols = []
    recalc_ratio_cols = []
    recalc_per_min_cols = []

    for col in df.columns:
        if col == group_key: continue
        if col in max_cols: continue
        if col in sum_cols: continue
        if col in first_cols: continue            
        if col in recalc_ratio_cols: continue 
        if col in recalc_per_min_cols: continue   

        dtype = df[col].dtype.kind
        if dtype in "fi":  # float or int
            if "Ratio" in col:
                recalc_ratio_cols.append(col)
            elif "PerMin" in col:
                recalc_per_min_cols.append(col)
            elif any(kw in col for kw in ["Dist", "Effort", "Score"]):
                sum_cols.append(col)
            else:
                first_cols.append(col)
        else:
            first_cols.append(col)

    # ---- 集計方法定義 ----
    agg_dict = {col: "max" for col in max_cols}
    agg_dict.update({col: "sum" for col in sum_cols})
    agg_dict.update({col: "first" for col in first_cols})

    df_agg = df.groupby(group_key, as_index=False).agg(agg_dict)

    # ---- Ratio再計算（例: SpeedZone1Dist / TotalDist） ----
    for col in recalc_ratio_cols:
        dist_col = col.replace("Ratio", "Dist")
        if dist_col in df_agg.columns and DrillGnssResultModel.TotalDist.name in df_agg.columns:
            df_agg[col] = df_agg[dist_col] / df_agg[DrillGnssResultModel.TotalDist.name] * 100

    # ---- PerMin再計算（例: Effort / DurationSec） ----
    for col in recalc_per_min_cols:
        if col == DrillGnssResultModel.AvgDistPerMin.name:
            effort_col = DrillGnssResultModel.TotalDist.name
        else:
            effort_col = col.replace("PerMin", "")
        if effort_col in df_agg.columns and DrillGnssResultModel.DurationSec.name in df_agg.columns:
            duration_min = df_agg[DrillGnssResultModel.DurationSec.name] / 60
            df_agg[col] = (df_agg[effort_col] / duration_min).where(duration_min > 0, 0)
            df_agg[col] = df_agg[col].fillna(0).round(1)

    return df_agg


def arrange_values_for_display(df: pd.DataFrame) -> pd.DataFrame:

    # ---- 最後に整数に変換するカラムを処理 ----
    int_cols = [
        DrillGnssResultModel.TotalDist.name,
        DrillGnssResultModel.AvgDistPerMin.name,
        DrillGnssResultModel.HIRDist.name,
        DrillGnssResultModel.HSRDist.name,
        DrillGnssResultModel.RHIRDist.name,
        DrillGnssResultModel.RHSRDist.name,
        DrillGnssResultModel.SpeedZone1Dist.name,
        DrillGnssResultModel.SpeedZone2Dist.name,
        DrillGnssResultModel.SpeedZone3Dist.name,
        DrillGnssResultModel.SpeedZone4Dist.name,
        DrillGnssResultModel.SpeedZone5Dist.name,
        DrillGnssResultModel.SpeedZone6Dist.name,
        DrillGnssResultModel.RSpeedZone1Dist.name,
        DrillGnssResultModel.RSpeedZone2Dist.name,
        DrillGnssResultModel.RSpeedZone3Dist.name,
        DrillGnssResultModel.RSpeedZone4Dist.name,
    ]

    for col in int_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0).round(0).astype(int)

    return df
