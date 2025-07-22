import flet as ft
from datetime import datetime
from models.web.web_drillviewdata_model import WebDrillViewDataModel, WebDrillDataKeys
from services.localization_service import _


class FileDrillSelectPanel:

    def __init__(self, page: ft.Page, data: WebDrillViewDataModel,
                 on_file_select_change, on_file_select_change2,
                 on_drill_select_change, on_drill_select_change2):
        self.page, self.data = page, data
        self.on_file_select_change = on_file_select_change
        self.on_file_select_change2 = on_file_select_change2
        self.on_drill_select_change = on_drill_select_change
        self.on_drill_select_change2 = on_drill_select_change2
        self.async_wait_time = 0.1

    def build(self):

        # file_select
        self.file_select = ft.Dropdown(
            label=_("WebDrill_FileSelect"),
            text_size=14,
            width=300,
            height=40,
            options=[],
            value=0,
            content_padding=ft.padding.only(left=20),
            on_change=self.on_file_select_change,
            visible=True,
            options_fill_horizontally=True,
            border_color=ft.Colors.GREY_500,
        )
        self.file_select2 = ft.Dropdown(
            label=_("WebDrill_FileSelect"),
            text_size=14,
            width=300,
            height=40,
            options=[],
            value=0,
            content_padding=ft.padding.only(left=20),
            on_change=self.on_file_select_change2,
            visible=True,
            options_fill_horizontally=True,
            border_color=ft.Colors.GREY_500,
        )
        self.file_selects = [self.file_select, self.file_select2]

        # drill_select
        self.drill_select = ft.Dropdown(
            label=_("WebDrill_DrillSelect"),
            text_size=14,
            width=260,
            height=40,
            options=[],
            value=0,
            content_padding=ft.padding.only(left=20),
            on_change=self.on_drill_select_change,
            visible=True,
            options_fill_horizontally=True,
            expand=True,
            border_color=ft.Colors.GREY_500,
        )
        self.drill_select2 = ft.Dropdown(
            label=_("WebDrill_DrillSelect"),
            text_size=14,
            width=260,
            height=40,
            options=[],
            value=0,
            content_padding=ft.padding.only(left=20),
            on_change=self.on_drill_select_change2,
            visible=True,
            options_fill_horizontally=True,
            expand=True,
            border_color=ft.Colors.GREY_500,
        )
        self.drill_selects = [self.drill_select, self.drill_select2]

        self.info_button = ft.IconButton(
            icon=ft.Icons.INFO,
            on_click=self.show_info,
            width=40,
        )
        self.info_button2 = ft.IconButton(
            icon=ft.Icons.INFO,
            on_click=self.show_info2,
            width=40,
        )

        self.panel = ft.Row(
            [
                ft.Row(
                    [
                        self.file_select,
                        ft.Row(
                            controls=[
                                self.drill_select,
                                self.info_button,
                            ],
                            alignment=ft.MainAxisAlignment.START,
                            vertical_alignment=ft.CrossAxisAlignment.CENTER,
                            spacing=5,
                            width=300,
                        )
                    ],
                    alignment=ft.MainAxisAlignment.START,
                    vertical_alignment=ft.CrossAxisAlignment.CENTER,
                    wrap=True,
                    expand=True,
                ),
                ft.Row(
                    [
                        self.file_select2,
                        ft.Row(
                            controls=[
                                self.drill_select2,
                                self.info_button2,
                            ],
                            alignment=ft.MainAxisAlignment.START,
                            vertical_alignment=ft.CrossAxisAlignment.CENTER,
                            spacing=5,
                            width=300,
                        )
                    ],
                    alignment=ft.MainAxisAlignment.START,
                    vertical_alignment=ft.CrossAxisAlignment.CENTER,
                    wrap=True,
                    expand=True,
                ),
            ],
            alignment=ft.MainAxisAlignment.START,
            vertical_alignment=ft.CrossAxisAlignment.START,
            spacing=40,
        )

        return ft.Container(self.panel,
                            margin=ft.margin.only(top=10, right=10))

    async def update_async(self):
        for i in [0, 1]:
            file_select = self.file_selects[i]
            file_select.options = [
                ft.dropdown.Option(key=file_name, text=file_name)
                for file_name in self.data.file_name_list
            ]
            file_select.value = self.data.selected_file_names[i]

            self.drill_selects[i].disabled = self.data.data_dicts[i] is None
            if self.data.data_dicts[i]:
                self.drill_selects[i].options = [
                    ft.dropdown.Option(
                        key=WebDrillViewDataModel.SessionTotalDrillID,
                        text=_("WebDrill_DrillSelect_SessionTotal"))
                ] + [
                    ft.dropdown.Option(key=drill["ID"], text=drill["Name"])
                    for drill in self.data.data_dicts[i][
                        WebDrillDataKeys.df_drill_dict]
                ]
                self.drill_selects[i].value = self.data.selected_drill_ids[i]

            if self.data.report_mode and self.data.hide_setting:
                for option in file_select.options:
                    if option.key != file_select.value:
                        option.visible = False
                file_select.on_change = None
                for option in self.drill_selects[i].options:
                    if option.key != self.drill_selects[i].value:
                        option.visible = False
                self.drill_selects[i].on_change = None

        await self.panel.update_async()

    def show_info(self, e):
        self.show_info_dialog(0)

    def show_info2(self, e):
        self.show_info_dialog(1)

    def show_info_dialog(self, i):
        selected_date_text, session_name_text, drill_time_text, drilltag_text = "", "", "", ""
        if self.data.data_dicts[i]:
            date_str = self.data.data_dicts[i][WebDrillDataKeys.selected_date]
            try:
                date_obj = datetime.strptime(date_str,
                                             "%Y-%m-%d %H:%M:%S").date()
                selected_date_text = f"{date_obj.year}/{date_obj.month}/{date_obj.day}"
            except ValueError:
                selected_date_text = date_str  # フォーマット不正時はそのまま
            session_name_text = self.data.data_dicts[i][
                WebDrillDataKeys.session_name]
            if self.data.selected_drill_ids[i]:
                drill_time_text = self.data.selected_drill_time_texts[i]
                drilltag_text = self.data.selected_drill_drilltags[i]

        dlg_modal = ft.CupertinoAlertDialog(
            modal=True,
            title=ft.Text("Information"),
            content=ft.Column([
                ft.Divider(),
                ft.Row(
                    controls=[
                        ft.Icon(
                            name=ft.Icons.CALENDAR_MONTH,
                            color=ft.Colors.PRIMARY,
                        ),
                        ft.Text(selected_date_text),
                    ],
                    spacing=5,
                    alignment=ft.MainAxisAlignment.START,
                    vertical_alignment=ft.CrossAxisAlignment.CENTER,
                    width=120,
                ),
                ft.Row(
                    controls=[
                        ft.Icon(
                            name=ft.Icons.VIEW_TIMELINE,
                            color=ft.Colors.PRIMARY,
                        ),
                        ft.Text(session_name_text),
                    ],
                    spacing=5,
                    alignment=ft.MainAxisAlignment.START,
                    vertical_alignment=ft.CrossAxisAlignment.CENTER,
                    width=120,
                ),
                ft.Row(
                    controls=[
                        ft.Icon(
                            name=ft.Icons.ACCESS_TIME,
                            color=ft.Colors.PRIMARY,
                        ),
                        ft.Text(drill_time_text),
                    ],
                    spacing=5,
                    alignment=ft.MainAxisAlignment.START,
                    vertical_alignment=ft.CrossAxisAlignment.CENTER,
                    width=120,
                ),
                ft.Row(
                    controls=[
                        ft.Icon(
                            name=ft.Icons.TAG,
                            color=ft.Colors.PRIMARY,
                        ),
                        ft.Text(drilltag_text),
                    ],
                    spacing=5,
                    alignment=ft.MainAxisAlignment.START,
                    vertical_alignment=ft.CrossAxisAlignment.CENTER,
                    width=120,
                ),
            ]),
            actions=[
                ft.CupertinoDialogAction(
                    _("Button_OK"),
                    is_destructive_action=False,
                    on_click=lambda e: self.page.close(dlg_modal),
                )
            ],
        )
        self.page.open(dlg_modal)
