import flet as ft
from typing import (
    List, )
from views.web.web_chart_base import WebChartBase
from models.web.web_drillviewdata_model import WebDrillViewDataModel
from services.localization_service import _
from views.helpers.snackbar_manager import SnackBarManager


class GraphDisplayPanel:

    def __init__(self, page: ft.Page, data: WebDrillViewDataModel,
                 on_update_graph_click):
        self.page = page
        self.data = data
        self.async_wait_time = 0.1
        self.page.on_resize = self.on_page_resize
        self.charts: List[WebChartBase] = []
        self.on_update_graph_click = on_update_graph_click

    def build(self):

        self.update_graph_btn = ft.ElevatedButton(
            # width=150,
            content=ft.Container(
                content=ft.Text(_("Button_UpdateGraph"),
                                weight=ft.FontWeight.BOLD),
                padding=ft.padding.only(left=15, right=15, top=2),
            ),
            data=False,
            on_click=self._on_update_graph_click,
        )

        async def _on_icon_hover(e):
            if e.data == "true" and not e.control.disabled:
                e.control.bgcolor = ft.Colors.with_opacity(
                    0.2, ft.colors.WHITE)
            else:
                e.control.bgcolor = None
            await e.control.update_async()

        self.graph_player_order_icon = ft.Container(
            ft.Icon(ft.Icons.DIRECTIONS_RUN_OUTLINED,
                    color=ft.Colors.WHITE,
                    size=24),
            padding=ft.padding.all(6),
            alignment=ft.alignment.center,
            on_hover=_on_icon_hover)
        self.graph_value_order_icon = ft.Container(
            ft.Icon(ft.Icons.STACKED_BAR_CHART, color=ft.Colors.WHITE,
                    size=24),
            padding=ft.padding.all(6),
            alignment=ft.alignment.center,
            on_hover=_on_icon_hover)
        self.order_select_btn = ft.CupertinoSlidingSegmentedButton(
            selected_index=0,
            thumb_color=ft.Colors.BLUE,
            bgcolor=ft.Colors.GREY_400,
            on_change=self.on_order_select_change,
            padding=ft.padding.symmetric(0, 0),
            width=100,
            controls=[
                self.graph_player_order_icon,
                self.graph_value_order_icon,
            ],
        )

        self.preview_icon_btn = ft.ElevatedButton(
            content=ft.Row(
                [
                    ft.Icon(name=ft.icons.PREVIEW),
                ],
                spacing=0,
                alignment=ft.MainAxisAlignment.CENTER,
            ),
            data=True,
            width=45,
            on_click=self._on_preview_click)

        self.get_link_icon_btn = ft.ElevatedButton(
            content=ft.Row(
                [
                    ft.Icon(name=ft.icons.LINK),
                ],
                spacing=0,
                alignment=ft.MainAxisAlignment.CENTER,
            ),
            data=True,
            width=45,
            on_click=self._on_get_link_click)

        self.tagset_select = ft.Dropdown(
            label=_("WebDrill_TagsetSelect"),
            text_size=14,
            width=200,
            height=40,
            options=[],
            value=0,
            content_padding=ft.padding.only(left=20),
            on_change=self.on_tagset_select_change,
            visible=True,
            options_fill_horizontally=True,
            border_color=ft.Colors.GREY_500,
        )

        self.button_row = ft.Row(
            [
                self.update_graph_btn,
                self.order_select_btn,
                self.preview_icon_btn,
                self.get_link_icon_btn,
                self.tagset_select,
                # ...avator
            ],
            alignment=ft.MainAxisAlignment.START,
            vertical_alignment=ft.CrossAxisAlignment.CENTER,
            wrap=True,
            expand=True,
            spacing=10,
        )

        self.graph_column = ft.Row(
            controls=[],
            spacing=10,
            visible=True,
            wrap=True,
            expand=True,
        )

        self.panel = ft.Column(
            [
                self.button_row,
                self.graph_column,
            ],
            alignment=ft.MainAxisAlignment.START,
            spacing=40,
            visible=True,
        )

        return ft.Container(self.panel, margin=ft.margin.only(right=10))

    async def on_order_select_change(self, e):
        self.data.selected_graph_order_index = int(e.data)
        await self.update_async()

    async def update_async(self):
        self.panel.visible = self.data.selected_drill_ids[
            0] is not None or self.data.selected_drill_ids[1] is not None
        if self.panel.visible:
            self.tagset_select.options = [ft.dropdown.Option(
                key=0, text=" ")] + [
                    ft.dropdown.Option(key=id, text=name)
                    for id, name in self.data.tagset_id_name_dict.items()
                ]
            self.tagset_select.value = self.data.selected_tagset_id
            self.order_select_btn.selected_index = self.data.selected_graph_order_index

        if self.data.report_mode and self.data.hide_setting:
            for option in self.tagset_select.options:
                if option.key != self.tagset_select.value:
                    option.visible = False
            self.tagset_select.on_change = None
            if self.data.selected_tagset_id:
                await self.select_tagset(self.data.selected_tagset_id)
            else:
                self.tagset_select.visible = False
            self.update_graph_btn.visible = False
            self.order_select_btn.visible = False
            self.preview_icon_btn.visible = False
            self.get_link_icon_btn.visible = False

        await self.panel.update_async()

    async def _on_get_link_click(self, e):
        report_link_text = self.data.get_report_link()
        await self.page.set_clipboard_async(report_link_text)
        await SnackBarManager.show_success_async(
            self.page, _("Message_Link_Copy_Success"))

    async def _on_preview_click(self, e):
        report_link_text = self.data.get_report_link()
        await self.page.launch_url_async(report_link_text,
                                         web_window_name="_blank"
                                         )  # ← これで別タブで開く

    async def on_tagset_select_change(self, e):
        tagset_id = int(e.data)
        await self.select_tagset(tagset_id)

    async def select_tagset(self, tagset_id):
        self.data.select_tagset_id(tagset_id)
        self.button_row.controls.clear()
        self.button_row.controls.append(self.update_graph_btn)
        self.button_row.controls.append(self.order_select_btn)
        self.button_row.controls.append(self.preview_icon_btn)
        self.button_row.controls.append(self.get_link_icon_btn)
        self.button_row.controls.append(self.tagset_select)
        for id, name in self.data.selected_tag_id_name_dict.items():
            color = self.data.selected_tag_id_color_dict[id]
            self.button_row.controls.append(
                ft.CircleAvatar(content=ft.Text(name,
                                                size=12,
                                                weight=ft.FontWeight.BOLD),
                                radius=20,
                                color=color["color"],
                                bgcolor=color["bgcolor"]), )
        if self.data.COLOR_DEBUG:
            for color in self.data.tag_color_list:
                self.button_row.controls.append(
                    ft.CircleAvatar(content=ft.Text("TS",
                                                    size=12,
                                                    weight=ft.FontWeight.BOLD),
                                    radius=20,
                                    color=color["color"],
                                    bgcolor=color["bgcolor"]), )
        await self.button_row.update_async()

    async def _on_update_graph_click(self, e):
        await self.update_graph_async(e)
        report_link_text = self.data.get_report_link(hide_setting=False)
        print(f"Show Graph:{report_link_text}")

    async def update_graph_async(self, e):
        if len(self.data.selected_metric_names) > 0 and len(
                self.data.selected_player_ids) > 0:
            await self.on_update_graph_click(e)
            self.data.update_df_graph_gnss_results()

            graph_width = len(self.data.selected_player_ids) * 75 + 100
            self.graph_column.controls.clear()
            self.charts.clear()
            scale_steps = [
                3, 5, 10, 20, 50, 100, 250, 500, 1000, 2000, 4000, 6000, 8000,
                10000, 15000, 20000, 25000, 30000
            ]
            avg_text_row_width = min(self.page.width, graph_width)
            for metric_name in self.data.selected_metric_names:
                chart = WebChartBase(title=ft.Text(
                    _(f"MetricModel_MetricEnum_{metric_name}")),
                                     scale_steps=scale_steps,
                                     graph_width=graph_width,
                                     avg_text_row_width=avg_text_row_width)
                chart_container = chart.build()
                await chart.update(self.data)
                await chart.update_values(self.data, "PlayerDisplayName",
                                          metric_name)

                self.graph_column.controls.append(chart_container)
                self.charts.append(chart)

            await self.graph_column.update_async()

    async def on_page_resize(self, e: ft.WindowResizeEvent):
        if len(self.charts) > 0:
            for chart in self.charts:
                chart.avg_text_row.width = e.width
        await self.graph_column.update_async()
