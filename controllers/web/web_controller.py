import flet as ft
from pathlib import Path
from models.web.web_drillviewdata_model import WebDrillViewDataModel
from views.helpers.snackbar_manager import SnackBarManager
from services.json_serivce import JsonService
from services.web_url_serivce import WebUrl
from services.localization_service import _
from services.logger_service import Logger


class WebController:

    def __init__(self, page: ft.Page, team_name):
        self.DEBUG = False
        self.is_top_display = False
        self.is_updating = False

        from views.web.web_drill_view import WebDrillView
        from views.web.panel_file_drill_select import FileDrillSelectPanel
        from views.web.panel_metric_select import MetricSelectPanel
        from views.web.panel_player_select import PlayerSelectPanel
        from views.web.panel_graph_display import GraphDisplayPanel

        self.page = page
        self.team_name = team_name
        self.data = WebDrillViewDataModel(self.team_name)
        self.is_cancel = False

        self.file_drill_select_panel = FileDrillSelectPanel(
            page,
            self.data,
            self.on_file_select_change,
            self.on_file_select_change2,
            self.on_drill_select_change,
            self.on_drill_select_change2,
        )
        self.metric_select_panel = MetricSelectPanel(self.data)
        self.player_select_panel = PlayerSelectPanel(self.data)
        self.graph_display_panel = GraphDisplayPanel(
            self.page, self.data, self.on_update_graph_click)
        self.view = WebDrillView(self.data, self.file_drill_select_panel,
                                 self.metric_select_panel,
                                 self.player_select_panel,
                                 self.graph_display_panel)
        self.called_by_on_drill_connect = False
        self.initialize_request = True
        self.df_last_update = None

        self.data.set_report_param(WebUrl.PARAMS)

    async def on_page_close(self):
        pass

    async def update_async(self):
        await self.view.update_async()
        if self.data.report_mode:
            await self.graph_display_panel.update_graph_async(None)

    async def on_file_upload_click(self, e):
        #Logger.manual_event_info()
        await JsonService.web_import_json_async(self.page,
                                                self.on_file_upload_success)

    async def on_file_upload_success(self, e):
        #Logger.manual_event_info()
        self.data.update_file_name_list()
        await self.update_async()
        await SnackBarManager.show_data_save_success_async(self.page)

    async def on_file_select_change(self, e):
        #Logger.manual_event_info()
        selected_file_name = e.data
        self.data.select_file(selected_file_name, 0)
        await self.update_async()

    async def on_file_select_change2(self, e):
        #Logger.manual_event_info()
        selected_file_name = e.data
        self.data.select_file(selected_file_name, 1)
        await self.update_async()

    async def on_drill_select_change(self, e):
        #Logger.manual_event_info()
        drill_id = int(e.data)
        self.data.select_drill(drill_id, 0)
        await self.update_async()

    async def on_drill_select_change2(self, e):
        #Logger.manual_event_info()
        drill_id = int(e.data)
        self.data.select_drill(drill_id, 1)
        await self.update_async()

    async def on_update_graph_click(self, e):
        # await self.view.collapse_panels() # 少し動作がカクカクするのでやめた
        pass

    async def update_progress(self, value):
        await self.file_select_panel.update_progress(value)

    async def show_success(self, message):
        await SnackBarManager.show_success_async(self.page, message)

    async def show_warning(self, message):
        await SnackBarManager.show_warning_async(self.page, message)

    async def show_error(self, message):
        await SnackBarManager.show_error_async(self.page, message)
