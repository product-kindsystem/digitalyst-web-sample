import numpy as np
import pandas as pd


def aggregate_gnss_result_by_player(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    group_key = "PlayerID"

    max_cols = ["MaxSpeed", "MaxAccel", "PercentMaxSpeed", "PercentMaxAccel"]
    sum_cols = ["DurationSec", "DurationMin"]
    first_cols = []
    recalc_ratio_cols = []
    recalc_per_min_cols = []

    for col in df.columns:
        if col == group_key:
            continue
        if col in max_cols:
            continue
        if col in sum_cols:
            continue
        if col in first_cols:
            continue
        if col in recalc_ratio_cols:
            continue
        if col in recalc_per_min_cols:
            continue

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
        if dist_col in df_agg.columns and "TotalDist" in df_agg.columns:
            df_agg[col] = df_agg[dist_col] / df_agg["TotalDist"] * 100

    # ---- PerMin再計算（例: Effort / DurationSec） ----
    for col in recalc_per_min_cols:
        if col == "AvgDistPerMin":
            effort_col = "TotalDist"
        else:
            effort_col = col.replace("PerMin", "")
        if effort_col in df_agg.columns and "DurationSec" in df_agg.columns:
            duration_min = df_agg["DurationSec"] / 60
            df_agg[col] = (df_agg[effort_col] / duration_min).where(
                duration_min > 0, 0)
            df_agg[col] = df_agg[col].fillna(0).round(1)

    return df_agg


def arrange_values_for_display(df: pd.DataFrame) -> pd.DataFrame:

    # ---- 最後に整数に変換するカラムを処理 ----
    int_cols = get_int_metrics_names()

    for col in int_cols:
        if col in df.columns:
            df[col] = df[col].astype(float).fillna(0).round(0).astype(int)

    return df


def get_int_metrics_names():
    return [
        "TotalDist",
        "AvgDistPerMin",
        "HIRDist",
        "HSRDist",
        "RHIRDist",
        "RHSRDist",
        "SpeedZone1Dist",
        "SpeedZone2Dist",
        "SpeedZone3Dist",
        "SpeedZone4Dist",
        "SpeedZone5Dist",
        "SpeedZone6Dist",
        "RSpeedZone1Dist",
        "RSpeedZone2Dist",
        "RSpeedZone3Dist",
        "RSpeedZone4Dist",
        "AccelEffort",
        "DecelEffort",
        "RAccelEffort",
        "BaseBallScore",
        "SprintEffort",
        "HSprintEffort",
        "RSprintEffort",
        "RHSprintEffort",
        "SpeedZone1Effort",
        "SpeedZone2Effort",
        "SpeedZone3Effort",
        "SpeedZone4Effort",
        "SpeedZone5Effort",
        "SpeedZone6Effort",
        "AccelZone1Effort",
        "AccelZone2Effort",
        "AccelZone3Effort",
        "AccelZone4Effort",
        "DecelZone1Effort",
        "DecelZone2Effort",
        "DecelZone3Effort",
        "DecelZone4Effort",
        "QuickAccelZone1Effort",
        "QuickAccelZone2Effort",
        "QuickAccelZone3Effort",
        "QuickAccelZone4Effort",
        "QuickDecelZone1Effort",
        "QuickDecelZone2Effort",
        "QuickDecelZone3Effort",
        "QuickDecelZone4Effort",
        "DurationMin",
    ]


# DurationSec = Column(Float, nullable=True)
# TotalDist = Column(Float, nullable=True)
# AvgDistPerMin = Column(Float, nullable=True)

# HIRDist = Column(Float, nullable=True)
# HIRRatio = Column(Float, nullable=True)
# HSRDist = Column(Float, nullable=True)
# HSRRatio = Column(Float, nullable=True)

# RHIRDist = Column(Float, nullable=True)
# RHIRRatio = Column(Float, nullable=True)
# RHSRDist = Column(Float, nullable=True)
# RHSRRatio = Column(Float, nullable=True)

# AccelEffort = Column(Integer, nullable=True)
# AccelEffortPerMin = Column(Float, nullable=True)
# DecelEffort = Column(Integer, nullable=True)
# DecelEffortPerMin = Column(Float, nullable=True)
# RAccelEffort = Column(Integer, nullable=True)
# RAccelEffortPerMin = Column(Float, nullable=True)
# BaseBallScore = Column(Integer, nullable=True)

# SprintEffort = Column(Integer, nullable=True)
# HSprintEffort = Column(Integer, nullable=True)
# RSprintEffort = Column(Integer, nullable=True)
# RHSprintEffort = Column(Integer, nullable=True)

# MaxSpeed = Column(Float, nullable=True)
# PercentMaxSpeed = Column(Float, nullable=True)
# MaxAccel = Column(Float, nullable=True)
# PercentMaxAccel = Column(Float, nullable=True)

# SpeedZone1Dist = Column(Float, nullable=True)
# SpeedZone2Dist = Column(Float, nullable=True)
# SpeedZone3Dist = Column(Float, nullable=True)
# SpeedZone4Dist = Column(Float, nullable=True)
# SpeedZone5Dist = Column(Float, nullable=True)
# SpeedZone6Dist = Column(Float, nullable=True)

# SpeedZone1Effort = Column(Integer, nullable=True)
# SpeedZone2Effort = Column(Integer, nullable=True)
# SpeedZone3Effort = Column(Integer, nullable=True)
# SpeedZone4Effort = Column(Integer, nullable=True)
# SpeedZone5Effort = Column(Integer, nullable=True)
# SpeedZone6Effort = Column(Integer, nullable=True)

# SpeedZone1Ratio = Column(Float, nullable=True)
# SpeedZone2Ratio = Column(Float, nullable=True)
# SpeedZone3Ratio = Column(Float, nullable=True)
# SpeedZone4Ratio = Column(Float, nullable=True)
# SpeedZone5Ratio = Column(Float, nullable=True)
# SpeedZone6Ratio = Column(Float, nullable=True)

# RSpeedZone1Dist = Column(Float, nullable=True)
# RSpeedZone2Dist = Column(Float, nullable=True)
# RSpeedZone3Dist = Column(Float, nullable=True)
# RSpeedZone4Dist = Column(Float, nullable=True)

# RSpeedZone1Ratio = Column(Float, nullable=True)
# RSpeedZone2Ratio = Column(Float, nullable=True)
# RSpeedZone3Ratio = Column(Float, nullable=True)
# RSpeedZone4Ratio = Column(Float, nullable=True)

# AccelZone1Effort = Column(Integer, nullable=True)
# AccelZone2Effort = Column(Integer, nullable=True)
# AccelZone3Effort = Column(Integer, nullable=True)
# AccelZone4Effort = Column(Integer, nullable=True)

# DecelZone1Effort = Column(Integer, nullable=True)
# DecelZone2Effort = Column(Integer, nullable=True)
# DecelZone3Effort = Column(Integer, nullable=True)
# DecelZone4Effort = Column(Integer, nullable=True)

# QuickAccelZone1Effort = Column(Integer, nullable=True)
# QuickAccelZone2Effort = Column(Integer, nullable=True)
# QuickAccelZone3Effort = Column(Integer, nullable=True)
# QuickAccelZone4Effort = Column(Integer, nullable=True)

# QuickDecelZone1Effort = Column(Integer, nullable=True)
# QuickDecelZone2Effort = Column(Integer, nullable=True)
# QuickDecelZone3Effort = Column(Integer, nullable=True)
# QuickDecelZone4Effort = Column(Integer, nullable=True)

# ValidDataRatio = VirtualColumn(name="ValidDataRatio")
# DurationMin = VirtualColumn(name="DurationMin")
# JerseyNum = VirtualColumn(name="JerseyNum")
# PartnerID = VirtualColumn(name="PartnerID")
