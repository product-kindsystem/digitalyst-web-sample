import flet as ft
import pandas as pd
import math
from models.web.web_drillviewdata_model import WebDrillViewDataModel, WebDrillDataKeys
from services.localization_service import _
import services.performance_calc_service_web as performance_calc_service


class WebChartBase:

    def __init__(self, title, scale_steps, graph_width, graph_height, avg_text_row_width):
        self.title = title
        self.title.size = 20
        self.is_first_load = True
        self.bar_width = 50
        self.graph_height = graph_height
        self.bar_chart_rods = []
        self.bar_chart_rods2 = []
        self.bar_chart_labels = []
        self.bar_chart_value_labels = []
        self.bar_chart_value_labels2 = []
        self.scale_steps = scale_steps
        self.graph_width = graph_width
        self.avg_text_row_width = avg_text_row_width
        self.FIRST_COLOR = ft.Colors.BLUE
        self.SECOND_COLOR = ft.Colors.BLUE_900

    def build(self):

        # 棒グラフ
        self.chart = ft.BarChart(
            bar_groups=[],
            border=ft.border.all(1, ft.Colors.GREY_400),
            top_axis=ft.ChartAxis(labels=[], ),
            left_axis=ft.ChartAxis(
                title=ft.Container(content=self.title,
                                   padding=ft.Padding(left=45,
                                                      right=0,
                                                      top=0,
                                                      bottom=0)),
                title_size=50,
                labels_size=40,
            ),
            bottom_axis=ft.ChartAxis(
                labels=[],
                labels_size=60,
            ),
            horizontal_grid_lines=ft.ChartGridLines(color=ft.Colors.GREY_300,
                                                    width=1,
                                                    dash_pattern=[3, 3]),
            tooltip_bgcolor=ft.Colors.with_opacity(0.8, ft.Colors.GREY_300),
            max_y=50,
            min_y=0,
            interactive=False,
            height=self.graph_height,
            # expand=True,
            width=self.graph_width,
            )
        self.chart.horizontal_grid_lines.interval = self.chart.max_y / 5

        # 平均線用のLineChart設定
        self.line_chart_data = ft.LineChartData(
            data_points=[
                ft.LineChartDataPoint(0, 0),
                ft.LineChartDataPoint(1, 0),
            ],
            color=self.FIRST_COLOR,
            dash_pattern=[5, 5],
            stroke_width=3,
            # stroke_cap_round=True,
        )
        self.line_chart_data_bg = ft.LineChartData(
            data_points=[
                ft.LineChartDataPoint(0, 0),
                ft.LineChartDataPoint(1, 0),
            ],
            color=ft.Colors.WHITE,
            stroke_width=3,
        )
        self.line_chart_data2 = ft.LineChartData(
            data_points=[
                ft.LineChartDataPoint(0, 0),
                ft.LineChartDataPoint(1, 0),
            ],
            color=self.SECOND_COLOR,
            dash_pattern=[5, 5],
            stroke_width=3,
            # stroke_cap_round=True,
            visible=False,
        )
        self.line_chart_data_bg2 = ft.LineChartData(
            data_points=[
                ft.LineChartDataPoint(0, 0),
                ft.LineChartDataPoint(1, 0),
            ],
            color=ft.Colors.WHITE,
            stroke_width=3,
            visible=False,
        )
        self.line_chart = ft.LineChart(
            data_series=[
                self.line_chart_data_bg, self.line_chart_data,
                self.line_chart_data_bg2, self.line_chart_data2
            ],
            top_axis=ft.ChartAxis(
                labels=[
                    ft.ChartAxisLabel(value=0, label=ft.Text(" ", size=16)),
                    ft.ChartAxisLabel(value=1, label=ft.Text(" ", size=16)),
                ],
                labels_interval=1,
            ),
            left_axis=ft.ChartAxis(
                title=ft.Text(" "),
                labels=[
                    ft.ChartAxisLabel(value=0, label=ft.Text(" ", size=16)),
                    ft.ChartAxisLabel(value=1, label=ft.Text(" ", size=16)),
                ],
                title_size=50,
                labels_interval=1,
                labels_size=40,
            ),
            bottom_axis=ft.ChartAxis(
                labels=[
                    ft.ChartAxisLabel(value=0, label=ft.Text(" ")),
                    ft.ChartAxisLabel(value=1, label=ft.Text(" ")),
                ],
                labels_interval=1,
                labels_size=60,
            ),
            max_y=50,
            min_y=0,
            min_x=0,
            max_x=1,
            interactive=False,
            height=self.graph_height,
            # expand=True,
            width=self.graph_width,
            bgcolor=ft.Colors.TRANSPARENT,
            visible=False,
        )

        # 平均値表示
        self.avg_text = ft.Text(
            value=" ",
            size=18,
            weight=ft.FontWeight.BOLD,
            text_align=ft.TextAlign.CENTER,
            color=self.FIRST_COLOR,
        )
        self.avg_text2 = ft.Text(
            value=" ",
            size=18,
            weight=ft.FontWeight.BOLD,
            text_align=ft.TextAlign.CENTER,
            color=self.SECOND_COLOR,
        )
        self.avg_text_col = ft.Column(
            [
                self.avg_text,
            ],
            spacing=0,
            alignment=ft.MainAxisAlignment.CENTER,
        )
        self.avg_text_content = ft.Container(
            content=ft.FloatingActionButton(
                content=self.avg_text_col,
                width=120,
                height=50,
                bgcolor=ft.Colors.YELLOW_ACCENT_100,
                disabled=True,
            ),
            padding=ft.Padding(left=0, right=40, top=40, bottom=0),
        )
        self.avg_text_row = ft.Row([self.avg_text_content],
                                   alignment=ft.MainAxisAlignment.END,
                                #    width=self.avg_text_row_width,
                                   expand=True)

        self.chart_container = ft.Container(
            content=ft.Stack(
                [
                    ft.Row(
                        [
                            ft.Container(
                                ft.Stack([
                                    self.chart,
                                    self.line_chart,
                                ], ),
                                margin=ft.margin.only(bottom=15),
                            )
                        ],
                        scroll=ft.ScrollMode.ALWAYS,
                    ),
                    self.avg_text_row,
                ],
            ),
            # expand=True,
            width=self.graph_width,
        )
        return self.chart_container

    async def update(self, data: WebDrillViewDataModel):
        self.chart.bar_groups.clear()
        self.chart.bottom_axis.labels.clear()
        self.chart.top_axis.labels.clear()
        self.bar_chart_rods.clear()
        self.bar_chart_rods2.clear()
        self.bar_chart_labels.clear()
        self.bar_chart_value_labels.clear()
        self.bar_chart_value_labels2.clear()

        # 棒グラフ
        for i, player_id in enumerate(data.selected_player_ids):

            bar_chart_rod = ft.BarChartRod(
                from_y=0,
                to_y=0,
                width=self.bar_width,
                color=self.FIRST_COLOR,
                tooltip="",
                border_radius=1,
            )
            bar_chart_rod2 = ft.BarChartRod(
                from_y=0,
                to_y=0,
                width=self.bar_width,
                color=self.SECOND_COLOR,
                tooltip="",
                border_radius=1,
                visible=False,
            )
            bar_chart_group = ft.BarChartGroup(
                x=i,
                bar_rods=[
                    bar_chart_rod,
                    bar_chart_rod2,
                ],
            )
            self.chart.bar_groups.append(bar_chart_group)
            self.bar_chart_rods.append(bar_chart_rod)
            self.bar_chart_rods2.append(bar_chart_rod2)

            # Bottomラベル
            bar_chart_label = ft.CircleAvatar(
                content=ft.Text(
                    data.player_id_model_dict[player_id]["DisplayName"],
                    size=16,
                    weight=ft.FontWeight.BOLD,
                    color=ft.Colors.BLACK,
                ),
                radius=30,
                bgcolor=ft.Colors.ORANGE_ACCENT_100,
            )
            chart_axis_label = ft.ChartAxisLabel(value=i,
                                                 label=ft.CircleAvatar(
                                                     content=bar_chart_label,
                                                     radius=30))
            self.chart.bottom_axis.labels.append(chart_axis_label)
            self.bar_chart_labels.append(bar_chart_label)

            # Topラベル
            bar_chart_value_label = ft.Text("",
                                            size=16,
                                            color=self.FIRST_COLOR,
                                            weight=ft.FontWeight.BOLD,
                                            visible=False)
            bar_chart_value_label2 = ft.Text("",
                                             size=16,
                                             color=self.SECOND_COLOR,
                                             weight=ft.FontWeight.BOLD,
                                             visible=False)
            bar_chart_value_column = ft.Column(
                controls=[bar_chart_value_label, bar_chart_value_label2],
                horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                spacing=0)
            chart_axis_value_label = ft.ChartAxisLabel(
                value=i, label=bar_chart_value_column)
            self.chart.top_axis.labels.append(chart_axis_value_label)
            self.bar_chart_value_labels.append(bar_chart_value_label)
            self.bar_chart_value_labels2.append(bar_chart_value_label2)

        self.avg_text.value = "AVG 0.0"
        self.line_chart.visible = False

    async def update_values(self, data: WebDrillViewDataModel, x_axis, y_axis):
        if data.df_graph_gnss_results[0] is not None:
            df_graph_gnss_result = data.df_graph_gnss_results[0]
            df_graph_gnss_result2 = data.df_graph_gnss_results[1]
        else:
            df_graph_gnss_result = data.df_graph_gnss_results[1]
            df_graph_gnss_result2 = None

        player_id_x_dict = self.create_sorted_dict_from_df(
            df_graph_gnss_result, "PlayerID", x_axis)
        player_id_y_dict = self.create_sorted_dict_from_df(
            df_graph_gnss_result, "PlayerID", y_axis, True)

        player_id_y_dict2 = None
        if df_graph_gnss_result2 is not None:
            player_id_y_dict2 = self.create_sorted_dict_from_df(
                df_graph_gnss_result2, "PlayerID", y_axis, True)

        if player_id_y_dict2:
            for player_id in player_id_y_dict2.keys():
                if player_id not in player_id_y_dict.keys():
                    player_id_y_dict[player_id] = 0
                    player_id_x_dict[player_id] = data.player_id_model_dict[
                        player_id]["DisplayName"]

        is_int_metric = False
        if y_axis in performance_calc_service.get_int_metrics_names():
            is_int_metric = True

        i = 0
        max_y = 0
        sum_y = 0
        cnt_y = 0
        sum_y2 = 0
        cnt_y2 = 0
        active_player_num = len(player_id_y_dict.items())
        player_id_y_list = list(player_id_y_dict.items())
        if data.selected_graph_order_index == 0:
            player_id_y_list = sorted(
                player_id_y_dict.items(),
                key=lambda item: data.player_id_model_dict.get(item[0], {
                }).get("DisplayOrder", 9999))
        for i, bar_chart_label in enumerate(self.bar_chart_labels):
            bar_chart_rod = self.bar_chart_rods[i]
            bar_chart_rod2 = self.bar_chart_rods2[i]
            bar_chart_value_label = self.bar_chart_value_labels[i]
            bar_chart_value_label2 = self.bar_chart_value_labels2[i]
            if i < active_player_num:
                (player_id, y) = player_id_y_list[i]
                if y is not None and y != "" and not (isinstance(y, float)
                                                      and math.isnan(y)):
                    bar_chart_rod.to_y = y
                    bar_chart_value_label.value = (str(int(y)) if is_int_metric
                                                   else f"{y:.1f}")
                    sum_y += y
                    cnt_y += 1
                    if y > max_y:
                        max_y = y
                player_display_name = player_id_x_dict[player_id]
                bar_chart_rod.tooltip = player_display_name
                bar_chart_label.content.value = player_display_name
                if player_id in data.selected_player_id_tag_id_dict.keys():
                    tag_id = data.selected_player_id_tag_id_dict[player_id]
                    if tag_id in data.selected_tag_id_color_dict.keys():
                        color = data.selected_tag_id_color_dict[tag_id]
                        bar_chart_label.bgcolor = color["bgcolor"]
                        bar_chart_label.content.color = color["color"]
                bar_chart_value_label.visible = True
                bar_chart_rod.visible = True
                bar_chart_label.visible = True
                if player_id_y_dict2:
                    y2 = player_id_y_dict2.get(player_id, None)
                    if y2 is not None and y2 != "" and not (isinstance(
                            y2, float) and math.isnan(y2)):
                        bar_chart_rod2.to_y = y2
                        bar_chart_value_label2.value = (str(
                            int(y2)) if is_int_metric else f"{y2:.1f}")
                        sum_y2 += y2
                        cnt_y2 += 1
                        if y2 > max_y:
                            max_y = y2
                    else:
                        bar_chart_rod2.to_y = 0
                        bar_chart_value_label2.value = "-"
                    bar_chart_rod2.visible = True
                    half_width = bar_chart_rod.width / 2
                    bar_chart_rod.width = half_width
                    bar_chart_rod2.width = half_width
                    bar_chart_value_label2.visible = True
            else:
                bar_chart_value_label.visible = False
                bar_chart_value_label2.visible = False
                bar_chart_rod.visible = False
                bar_chart_rod2.visible = False
                player_display_name = ""
                bar_chart_label.value = player_display_name

        # 平均値を更新
        if cnt_y > 1:
            avg_y = round(sum_y / cnt_y, 1) if cnt_y > 0 else 0
            self.avg_text.value = "AVG " + (str(int(avg_y)) if is_int_metric
                                            else f"{avg_y:.1f}")
            self.avg_text_content.visible = True

            if avg_y == 0.0:
                self.line_chart.visible = False
            else:
                self.line_chart_data.data_points[0].y = avg_y
                self.line_chart_data.data_points[1].y = avg_y
                self.line_chart_data_bg.data_points[0].y = avg_y
                self.line_chart_data_bg.data_points[1].y = avg_y
                self.line_chart.visible = True

            if cnt_y2 > 0:
                avg_y2 = round(sum_y2 / cnt_y2, 1) if cnt_y2 > 0 else 0
                self.avg_text2.value = "AVG " + (str(
                    int(avg_y2)) if is_int_metric else f"{avg_y2:.1f}")
                self.avg_text.size = 16
                self.avg_text2.size = 16
                self.avg_text_col.controls.append(self.avg_text2)
                self.avg_text_content.padding.top += 20

                if avg_y2 != 0.0:
                    self.line_chart_data2.data_points[0].y = avg_y2
                    self.line_chart_data2.data_points[1].y = avg_y2
                    self.line_chart_data_bg2.data_points[0].y = avg_y2
                    self.line_chart_data_bg2.data_points[1].y = avg_y2
                    self.line_chart_data2.visible = True
                    self.line_chart_data_bg2.visible = True
        else:
            self.avg_text_content.visible = False

        # self.scale_stepsの最大値を確認
        max_scale_step = max(self.scale_steps)

        # max_y の値によりてするスケールの選択
        if max_y > max_scale_step * 0.9:
            # max_y を超える 10000 で割り分ける値を計算
            step = ((int(max_y) // 10000) + 1) * 10000
        else:
            # スケールのステップを現在のスケールステップから選択
            for step in self.scale_steps:
                if max_y <= step * 0.9:
                    break

        # 次のスケールに進む条件: max_yが現在のスケールの90%以上になった場合
        if step != self.chart.max_y:
            self.chart.max_y = step
            self.chart.horizontal_grid_lines.interval = self.chart.max_y / 5
            self.line_chart.max_y = step

    def create_sorted_dict_from_df(self,
                                   df,
                                   key_column,
                                   value_column,
                                   is_value_numeric=False):
        if key_column not in df.columns or value_column not in df.columns:
            raise ValueError(
                f"指定した列名 '{key_column}' または '{value_column}' がDataFrameに存在しません。"
            )
        if is_value_numeric:
            # 値を数値型に変換して正確にソート
            df[value_column] = pd.to_numeric(df[value_column], errors='coerce')
        sorted_df = df[[key_column, value_column]].sort_values(by=value_column,
                                                               ascending=False)
        result_dict = dict(zip(sorted_df[key_column], sorted_df[value_column]))
        return result_dict
