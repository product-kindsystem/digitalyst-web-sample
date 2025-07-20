import flet as ft
from models.web.web_drillviewdata_model import WebDrillViewDataModel
from views.web.panel_file_drill_select import FileDrillSelectPanel
from views.web.panel_metric_select import MetricSelectPanel
from views.web.panel_player_select import PlayerSelectPanel
from views.web.panel_graph_display import GraphDisplayPanel
from views.helpers.content_helper import ContentHelper
from services.localization_service import _


class WebDrillView:

    def __init__(self, data: WebDrillViewDataModel,
                 file_drill_select_panel: FileDrillSelectPanel,
                 metric_select_panel: MetricSelectPanel,
                 player_select_panel: PlayerSelectPanel,
                 graph_display_panel: GraphDisplayPanel):
        self.data = data
        self.is_first_load = True
        self.file_drill_select_panel = file_drill_select_panel
        self.metric_select_panel = metric_select_panel
        self.player_select_panel = player_select_panel
        self.graph_display_panel = graph_display_panel

    def get_navigation_item(self):
        return dict(
            icon=ft.icons.VIEW_TIMELINE_OUTLINED,
            selected_icon=ft.icons.VIEW_TIMELINE,
            label="Drills",
        )

    def build(self):
        self.waiting_indicator = ft.Container(
            content=ContentHelper.get_waiting_indicator(),
            alignment=ft.alignment.center,
            bgcolor=ft.Colors.with_opacity(0.3, ft.Colors.WHITE),
            expand=True,
            visible=False,
        )
        self.panels = ft.Column(
            controls=[
                self.file_drill_select_panel.build(),
                self.metric_select_panel.build(),
                self.player_select_panel.build(),
                self.graph_display_panel.build(),
            ],
            alignment=ft.MainAxisAlignment.START,
        )
        self.content = ft.Column(
            [
                self.panels,
            ],
            expand=True,
            visible=True,
            scroll=ft.ScrollMode.AUTO,
        )
        return self.content

    async def update_async(self):
        await self.file_drill_select_panel.update_async()
        await self.metric_select_panel.update_async()
        await self.player_select_panel.update_async()
        await self.graph_display_panel.update_async()

    async def collapse_panels(self):
        self.metric_select_panel.initially_expanded = False
        self.player_select_panel.initially_expanded = False
        new_controls = [
            self.panels.controls[0],
            self.metric_select_panel.build(),
            self.player_select_panel.build(), self.panels.controls[3]
        ]
        self.panels.controls.clear()
        self.panels.controls = new_controls
        await self.content.update_async()
        await self.metric_select_panel.update_async()
        await self.player_select_panel.update_async()

    async def start_waiting_indicator_async(self):
        self.waiting_indicator.visible = True
        await self.content.update_async()

    async def end_waiting_indicator_async(self):
        self.waiting_indicator.visible = False
        await self.content.update_async()
