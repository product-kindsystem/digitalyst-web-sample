import flet as ft
from models.web.web_drillviewdata_model import WebDrillViewDataModel
from services.localization_service import _
from views.helpers.color_helper import ColorHelper


class MetricSelectPanel(ft.UserControl):

    def __init__(self, data: WebDrillViewDataModel, initially_expanded=True):
        super().__init__()
        self.data: WebDrillViewDataModel = data
        self.initially_expanded = initially_expanded

    def build(self):
        self.check_all_icon = ft.Icon(name=ft.icons.CHECK_BOX)
        self.check_all_btn = ft.ElevatedButton(
            content=ft.Row(
                [
                    self.check_all_icon,
                ],
                spacing=0,
                alignment=ft.MainAxisAlignment.START,
            ),
            data=True,
            on_click=self.on_check_all_click,
        )
        self.metric_count_text = ft.Text(value="", width=20)
        self.metric_chip_controls = ft.Row(
            spacing=10,
            wrap=True,
            expand=True,
            alignment=ft.MainAxisAlignment.START)
        self.expansion_tile = ft.ExpansionTile(
            title=ft.Container(
                content=ft.Row(
                    [
                        ft.Text(_("WebDrill_MetricSelect_Title"), width=120),
                        ft.Row(
                            [
                                ft.Icon(name=ft.icons.BAR_CHART,
                                        color=ft.Colors.PRIMARY,
                                        offset=ft.Offset(x=0, y=0)),
                                self.metric_count_text,
                            ],
                            spacing=10,
                        ),
                        self.check_all_btn,
                    ],
                    spacing=20,
                ),
                height=32,
                padding=ft.padding.all(0),
                margin=ft.margin.all(0),
            ),
            affinity=ft.TileAffinity.LEADING,
            initially_expanded=self.initially_expanded,
            collapsed_bgcolor=ColorHelper.get_table_header_bgcolor(),
            collapsed_text_color=ft.Colors.BLUE,
            controls_padding=ft.padding.all(10),
            controls=[
                ft.Column(
                    [
                        ft.Container(self.metric_chip_controls,
                                     expand=True,
                                     padding=ft.padding.only(top=10,
                                                             bottom=10),
                                     alignment=ft.alignment.top_left),
                    ],
                    expand=True,
                )
            ],
            on_change=self.on_expand_change,
            expand=True,
            min_tile_height=50,
        )
        self.title_back_color = ft.Container(
            height=self.expansion_tile.min_tile_height,
            expand=True,
            bgcolor=ColorHelper.get_table_header_bgcolor(),
        )
        self.panel = ft.Stack(
            [
                self.title_back_color,
                self.expansion_tile,
            ],
            expand=True,
            visible=False,
        )
        return ft.Container(self.panel, margin=ft.margin.only(right=10))

    async def update_async(self):

        self.panel.visible = self.data.selected_drill_ids[
            0] is not None or self.data.selected_drill_ids[1] is not None
        if self.panel.visible:
            # 選択数カウント
            self.metric_count_text.value = f"{len(self.data.selected_metric_names)}"
            await self.metric_count_text.update_async()

            # 選択
            self.metric_chip_controls.controls.clear()
            for metric_name in self.data.all_metric_names:
                self.metric_chip_controls.controls.append(
                    ft.Container(
                        content=ft.Chip(
                            label=ft.Text(
                                value=_(
                                    f"MetricModel_MetricEnum_{metric_name}"),
                                width=200,
                                text_align=ft.TextAlign.CENTER,
                            ),
                            selected=metric_name
                            in self.data.selected_metric_names,
                            on_select=self._toggle_metric,
                            data=metric_name,
                            disabled=False,
                        ),
                        width=230,
                    ))
            await self.update_all_check_icon()

        if self.data.report_mode and self.data.hide_setting:
            self.panel.visible = False

        await self.panel.update_async()

    async def on_expand_change(self, e):
        pass

    async def on_check_all_click(self, e):
        for chip in self.metric_chip_controls.controls:
            if not chip.content.disabled:
                metric_name = chip.content.data
                if self.check_all_icon.name == ft.icons.CHECK_BOX:
                    self.data.append_selected_metric_name(metric_name)
                else:
                    self.data.remove_selected_metric_name(metric_name)
        await self.update_all_check_icon()
        await self.update_async()

    async def update_all_check_icon(self):
        any_check = False
        for chip in self.metric_chip_controls.controls:
            if not chip.content.disabled:
                if chip.content.selected:
                    any_check = True
        if any_check:
            self.check_all_icon.name = ft.icons.CLEAR
        else:
            self.check_all_icon.name = ft.icons.CHECK_BOX
        await self.check_all_icon.update_async()

    async def _toggle_metric(self, e):
        metric_name = e.control.data
        if e.control.selected:
            self.data.append_selected_metric_name(metric_name)
        else:
            self.data.remove_selected_metric_name(metric_name)
        self.metric_count_text.value = f"{len(self.data.selected_metric_names)}"
        await self.metric_count_text.update_async()
        await self.update_all_check_icon()
