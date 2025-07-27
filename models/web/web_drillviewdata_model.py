import os
import json
import pandas as pd
import flet as ft
from urllib.parse import urlencode
from datetime import datetime
from services.web_path_serivce import WebPath
from services.web_url_serivce import WebUrl
from services.logger_service import Logger
import services.performance_calc_service_web as performance_calc_service


class WebDrillDataKeys():
    selected_date = "selected_date"
    session_name = "session_name"
    df_drill_dict = "df_drill_dict"
    drilltag_id_name_dict = "drilltag_id_name_dict"
    metric_names = "metric_names"
    df_group_dict = "df_group_dict"
    df_group_player_dict = "df_group_player_dict"
    df_player_dict = "df_player_dict"
    df_tagset_dict = "df_tagset_dict"
    df_tag_player_dict = "df_tag_player_dict"
    df_tag_dict = "df_tag_dict"
    df_gnss_result_dict = "df_gnss_result_dict"


class WebDrillViewDataModel():

    SessionTotalDrillID = -1

    def __init__(self, team_name):
        self.report_mode = False
        self.hide_setting = False
        self.team_name = team_name
        self.file_name_list = WebPath.uploaded_file_names
        self.selected_file_names = [None, None]
        self.data_dicts = [None, None]
        self.df_gnss_results = [None, None]
        self.df_total_gnss_results = [None, None]
        self.df_graph_gnss_results = [None, None]
        # drill
        self.selected_drill_ids = [None, None]
        self.selected_drill_time_texts = [None, None]
        self.selected_drill_drilltags = [None, None]
        # metric
        self.all_metric_names = []
        self.selected_metric_names = []
        # group
        self.group_id_name_dict = {}
        self.selected_group_id = None
        self.selected_group_player_ids = None
        # player
        self.player_id_model_dict = None
        self.selected_player_ids = []
        # tagset
        self.tagset_id_name_dict = {}
        self.selected_tagset_id = 0
        # tag
        self.selected_tag_id_name_dict = {}
        self.selected_tag_id_color_dict = {}
        self.selected_player_id_tag_id_dict = {}

        self.COLOR_DEBUG = False
        self.tag_color_list = [
            {
                "bgcolor": ft.Colors.LIGHT_GREEN_ACCENT_700,
                "color": ft.Colors.BLACK
            },
            {
                "bgcolor": ft.Colors.YELLOW,
                "color": ft.Colors.BLACK
            },
            {
                "bgcolor": ft.Colors.ORANGE,
                "color": ft.Colors.WHITE
            },
            {
                "bgcolor": ft.Colors.DEEP_ORANGE,
                "color": ft.Colors.WHITE
            },
            {
                "bgcolor": '#ff0000',
                "color": ft.Colors.WHITE
            },
            {
                "bgcolor": ft.Colors.PURPLE,
                "color": ft.Colors.WHITE
            },
            {
                "bgcolor": ft.Colors.BROWN,
                "color": ft.Colors.WHITE
            },
            # {"bgcolor": ft.Colors.GREEN,        "color": ft.Colors.WHITE},
            {
                "bgcolor": ft.Colors.PINK_ACCENT_100,
                "color": ft.Colors.BLACK
            },
            {
                "bgcolor": ft.Colors.ORANGE_ACCENT_100,
                "color": ft.Colors.BLACK
            },
            {
                "bgcolor": ft.Colors.GREEN_ACCENT_100,
                "color": ft.Colors.BLACK
            },
            {
                "bgcolor": ft.Colors.BROWN_100,
                "color": ft.Colors.BLACK
            },
            {
                "bgcolor": ft.Colors.GREY,
                "color": ft.Colors.WHITE
            },
            {
                "bgcolor": ft.Colors.BLACK,
                "color": ft.Colors.WHITE
            },
        ]
        # graph
        self.selected_graph_order_index = 0
        self.selected_graph_zoom_index = 0

    def update_file_name_list(self):
        self.file_name_list = WebPath.uploaded_file_names

    def select_file(self, selected_file_name, i):
        self.selected_file_names[i] = selected_file_name
        try:
            text = WebPath.get_file_as_text(self.selected_file_names[i])
            self.data_dicts[i] = json.loads(text)
        except Exception as ex:
            Logger.error("json parse error", ex)
            self.selected_file_names[i] = None
        self.selected_drill_ids[i] = None
        self.selected_drill_time_texts[i] = None
        self.selected_drill_drilltags[i] = None
        self.update_all_metric_names()
        self.update_group_id_name_dict()
        self.update_player_id_model_dict()
        self.update_tagset_id_name_dict()

        # df_gnss_result, df_total_gnss_result 計算
        df_gnss_result = pd.DataFrame(
            self.data_dicts[i][WebDrillDataKeys.df_gnss_result_dict])
        df_gnss_result = df_gnss_result.sort_values(by=["PlayerID", "DrillID"])
        df_total_gnss_result = performance_calc_service.aggregate_gnss_result_by_player(
            df_gnss_result)
        df_gnss_result = performance_calc_service.arrange_values_for_display(
            df_gnss_result)
        df_total_gnss_result = performance_calc_service.arrange_values_for_display(
            df_total_gnss_result)
        self.df_gnss_results[i] = df_gnss_result
        self.df_total_gnss_results[i] = df_total_gnss_result

    def select_drill(self, drill_id, i):
        if drill_id == WebDrillViewDataModel.SessionTotalDrillID:
            start_time, end_time = None, None
            drilltags_text = ""
            self.selected_drill_ids[i] = drill_id
            for item in self.data_dicts[i][WebDrillDataKeys.df_drill_dict]:
                temp_start_time = datetime.strptime(item["StartTime"],
                                                    "%Y-%m-%d %H:%M:%S")
                if start_time is None:
                    start_time = temp_start_time
                elif temp_start_time < start_time:
                    start_time = temp_start_time
                temp_end_time = datetime.strptime(item["StartTime"],
                                                  "%Y-%m-%d %H:%M:%S")
                if end_time is None:
                    end_time = temp_end_time
                elif temp_end_time < end_time:
                    end_time = temp_end_time

                if item["DrillTagID"] > 0:
                    drilltag_text = self.data_dicts[i][
                        WebDrillDataKeys.drilltag_id_name_dict][str(
                            item["DrillTagID"])]
                    if drilltags_text == "":
                        drilltags_text = drilltag_text
                    else:
                        drilltags_text += "," + drilltag_text
            if start_time is not None and end_time is not None:
                self.selected_drill_time_texts[
                    i] = f"{start_time.hour}:{start_time.minute:02d} - {end_time.hour}:{end_time.minute:02d}"
            else:
                self.selected_drill_time_texts[i] = ""
            self.selected_drill_drilltags[i] = drilltags_text
        else:
            for item in self.data_dicts[i][WebDrillDataKeys.df_drill_dict]:
                if item["ID"] == drill_id:
                    self.selected_drill_ids[i] = drill_id

                    def format_time_h_m(text: str) -> str:
                        try:
                            dt = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
                            return f"{dt.hour}:{dt.minute:02d}"
                        except Exception:
                            return text  # パース失敗時はそのまま返す

                    self.selected_drill_time_texts[
                        i] = f'{format_time_h_m(item["StartTime"])} - {format_time_h_m(item["EndTime"])}'
                    if item["DrillTagID"] > 0:
                        self.selected_drill_drilltags[i] = self.data_dicts[i][
                            WebDrillDataKeys.drilltag_id_name_dict][str(
                                item["DrillTagID"])]
                    else:
                        self.selected_drill_drilltags[i] = ""
                    break

        self.update_all_metric_names()
        self.update_group_id_name_dict()
        self.update_player_id_model_dict()
        self.update_tagset_id_name_dict()

    # metric
    def update_all_metric_names(self):
        merged_metrics = []
        for i in [0, 1]:
            if self.data_dicts[i] is not None:
                merged_metrics.extend(
                    self.data_dicts[i][WebDrillDataKeys.metric_names])
        self.all_metric_names = list(
            dict.fromkeys(merged_metrics))  # 順序を保持したまま重複削除

    def append_selected_metric_name(self, metric_name):
        if metric_name not in self.selected_metric_names:
            self.selected_metric_names.append(metric_name)

    def remove_selected_metric_name(self, metric_name):
        if metric_name in self.selected_metric_names:
            self.selected_metric_names.remove(metric_name)

    # group
    def update_group_id_name_dict(self):
        merged_group_dict = {}
        for i in [0, 1]:
            if self.data_dicts[i] is not None:
                for group in self.data_dicts[i][
                        WebDrillDataKeys.df_group_dict]:
                    if group["ID"] not in merged_group_dict.keys():
                        merged_group_dict[group["ID"]] = group["Name"]
        self.group_id_name_dict = merged_group_dict

    def select_group_id(self, group_id):
        self.selected_group_id = group_id
        self.selected_group_player_ids = None
        if self.selected_group_id is not None and self.selected_group_id > 0:
            self.selected_group_player_ids = []
            for i in [0, 1]:
                if self.data_dicts[i] is not None:
                    for group_player in self.data_dicts[i][
                            WebDrillDataKeys.df_group_player_dict]:
                        if group_player["GroupID"] == self.selected_group_id:
                            self.selected_group_player_ids.append(
                                group_player["PlayerID"])

    # player
    def update_player_id_model_dict(self):
        merged_player_ids = set()
        for i in [0, 1]:
            if self.data_dicts[i] is not None:
                df_gnss_result_dict = self.data_dicts[i][
                    WebDrillDataKeys.df_gnss_result_dict]
                drill_id = self.selected_drill_ids[i]
                for item in df_gnss_result_dict:
                    if drill_id == WebDrillViewDataModel.SessionTotalDrillID or item[
                            "DrillID"] == drill_id:
                        merged_player_ids.add(item["PlayerID"])
        all_player_ids = list(merged_player_ids)

        player_id_model_dict = {}
        for i in [0, 1]:
            if self.data_dicts[i] is not None:
                df_player_dict = self.data_dicts[i][
                    WebDrillDataKeys.df_player_dict]
                for player in df_player_dict:
                    pid = player["ID"]
                    if pid in all_player_ids and pid not in player_id_model_dict.keys(
                    ):
                        player_id_model_dict[pid] = player
        # DisplayOrder で昇順ソート
        sorted_players = sorted(player_id_model_dict.items(),
                                key=lambda x: x[1].get("DisplayName", 0))
        self.player_id_model_dict = dict(sorted_players)

    def append_selected_player_id(self, player_id):
        if player_id not in self.selected_player_ids:
            self.selected_player_ids.append(player_id)

    def remove_selected_player_id(self, player_id):
        if player_id in self.selected_player_ids:
            self.selected_player_ids.remove(player_id)

    # tagset
    def update_tagset_id_name_dict(self):
        tagset_id_name_dict = {}
        for i in [0, 1]:
            if self.data_dicts[i] is not None:
                df_tagset_dict = self.data_dicts[i][
                    WebDrillDataKeys.df_tagset_dict]
                for tagset in df_tagset_dict:
                    tagset_id = tagset["ID"]
                    if tagset_id not in tagset_id_name_dict.keys():
                        tagset_id_name_dict[tagset_id] = tagset["Name"]
        self.tagset_id_name_dict = tagset_id_name_dict

    # tag
    def select_tagset_id(self, tagset_id):
        self.selected_tagset_id = tagset_id
        self.selected_tag_id_name_dict = {}
        self.selected_tag_id_color_dict = {}
        self.selected_player_id_tag_id_dict = {}
        if tagset_id > 0:
            if self.data_dicts[0] is None or self.data_dicts[0][
                    WebDrillDataKeys.df_tag_dict] is None:
                df_tag_dict = []
            else:
                df_tag_dict = self.data_dicts[0][WebDrillDataKeys.df_tag_dict]
            if self.data_dicts[1] is None or self.data_dicts[1][
                    WebDrillDataKeys.df_tag_dict] is None:
                df_tag_dict1 = []
            else:
                df_tag_dict1 = self.data_dicts[1][WebDrillDataKeys.df_tag_dict]
            df_tag_dict = (df_tag_dict + df_tag_dict1)
            for tag in df_tag_dict:
                if tag["TagsetID"] == self.selected_tagset_id:
                    if tag["ID"] not in self.selected_tag_id_name_dict.keys():
                        self.selected_tag_id_name_dict[tag["ID"]] = tag["Name"]
            for i, id in enumerate(self.selected_tag_id_name_dict.keys()):
                self.selected_tag_id_color_dict[id] = self.tag_color_list[
                    i % len(self.tag_color_list)]

            player_id_tag_id_dict = {}
            for i in [0, 1]:
                if self.data_dicts[i] is not None:
                    df_tag_player_dict = self.data_dicts[i][
                        WebDrillDataKeys.df_tag_player_dict]
                    for item in df_tag_player_dict:
                        tag_id = item["TagID"]
                        player_id = item["PlayerID"]
                        if tag_id in self.selected_tag_id_name_dict.keys():
                            if player_id not in player_id_tag_id_dict.keys():
                                player_id_tag_id_dict[player_id] = tag_id
            self.selected_player_id_tag_id_dict = player_id_tag_id_dict

    # graph
    def update_df_graph_gnss_results(self):
        for i in [0, 1]:
            drill_id = self.selected_drill_ids[i]
            if drill_id is None or self.df_gnss_results[i] is None:
                self.df_graph_gnss_results[i] = None
            else:
                if drill_id > 0:
                    df = self.df_gnss_results[i]
                    df_filtered = df[df["DrillID"] == drill_id]
                else:
                    df_filtered = self.df_total_gnss_results[i]

                # 共通の PlayerID リストでフィルタ
                if self.selected_player_ids:
                    df_filtered = df_filtered[df_filtered["PlayerID"].isin(
                        self.selected_player_ids)]

                self.df_graph_gnss_results[i] = df_filtered.copy()

    # link
    def get_report_link(self, hide_setting=True):
        # パラメータを辞書形式で定義
        params = {
            "report_mode": True,
            "hide_setting": hide_setting,
            "team_name": self.team_name,
            "file_name": self.selected_file_names[0] or "",
            "file_name2": self.selected_file_names[1] or "",
            "drill_id": self.selected_drill_ids[0] or "",
            "drill_id2": self.selected_drill_ids[1] or "",
            "metric_names": ",".join(self.selected_metric_names),
            "group_id": self.selected_group_id or "",
            "player_ids": ",".join(map(str, self.selected_player_ids)),
            "tagset_id": self.selected_tagset_id,
            "graph_order_index": self.selected_graph_order_index,
        }
        # クエリ文字列を生成
        query = urlencode(params)
        # 完成したURL
        link = f"{WebUrl.HTTPS_URL}/{WebUrl.TEAM_NAME}?{query}"
        return link

    def set_report_param(self, params: dict):
        report_mode = params.get("report_mode", 'False')
        self.report_mode = report_mode == 'True'

        if self.report_mode:
            self.team_name = params.get("team_name", None)
            hide_setting = params.get("hide_setting", 'False')
            self.hide_setting = hide_setting == 'True'

            for i, key in enumerate(["file_name", "file_name2"]):
                file_name = params.get(key)
                if file_name:
                    self.select_file(file_name, i)

            for i, key in enumerate(["drill_id", "drill_id2"]):
                drill_id = params.get(key)
                if drill_id:
                    self.select_drill(int(drill_id), i)

            self.selected_metric_names = (params.get(
                "metric_names", "").split(",") if params.get("metric_names")
                                          else [])
            self.selected_group_id = (int(params["group_id"])
                                      if params.get("group_id") else None)
            self.selected_player_ids = (list(
                map(int, params["player_ids"].split(",")))
                                        if params.get("player_ids") else [])
            self.selected_tagset_id = (int(params["tagset_id"])
                                       if params.get("tagset_id") else 0)
            self.selected_graph_order_index = (int(
                params["graph_order_index"]) if params.get("graph_order_index")
                                               else 0)
