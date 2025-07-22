import flet as ft
from models.web.web_drillviewdata_model import WebDrillViewDataModel
from services.localization_service import _
from views.helpers.color_helper import ColorHelper

class PlayerSelectPanel(ft.UserControl):

    def __init__(self, data:WebDrillViewDataModel, initially_expanded=True):
        super().__init__()
        self.data:WebDrillViewDataModel = data
        self.initially_expanded = initially_expanded

    def build(self):
        self.group_select = ft.Dropdown(
            label=_("WebDrill_GroupSelect"),
            text_size=14, 
            width=180, 
            height=40, 
            options=[],
            value=0,
            content_padding=ft.padding.only(left=20),
            on_change=self.on_group_change,
            options_fill_horizontally=True,
            border_color=ft.Colors.GREY_500,
        )
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
        self.player_count_text = ft.Text(value="", width=20)
        self.player_chip_controls = ft.Row(spacing=10, wrap=True, expand=True, alignment=ft.MainAxisAlignment.START)
        self.expansion_tile = ft.ExpansionTile(
            title=ft.Container(
                content=ft.Row(
                    [
                        ft.Text(_("WebDrill_PlayerSelect_Title"), width=120),
                        ft.Row(
                            [
                                ft.Icon(name=ft.icons.DIRECTIONS_RUN_OUTLINED, color=ft.Colors.PRIMARY, offset=ft.Offset(x=0, y=-0.1)),
                                self.player_count_text,
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
                        ft.Row([self.group_select],),
                        ft.Container(self.player_chip_controls, expand=True, padding=ft.padding.only(bottom=10), alignment=ft.alignment.top_left),
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

        self.panel.visible = self.data.selected_drill_ids[0] is not None or self.data.selected_drill_ids[1] is not None
        if self.panel.visible:

            # グループ
            self.group_select.options.clear()
            self.group_select.options = [ft.dropdown.Option(key=0, text=" ")] + [
                ft.dropdown.Option(key=id, text=name)
                for id, name in self.data.group_id_name_dict.items()
            ]
            self.group_select.value = self.data.selected_group_id
            await self.group_select.update_async()

            # 選択済みプレイヤーの中から、グループに属するものだけ残す
            if self.data.selected_group_player_ids is not None:
                self.data.selected_player_ids = [
                    pid for pid in self.data.selected_player_ids if pid in self.data.selected_group_player_ids
                ]

            # プレイヤーカウント
            self.player_count_text.value = f"{len(self.data.selected_player_ids)}"
            await self.player_count_text.update_async()

            # プレイヤー選択
            self.player_chip_controls.controls.clear()
            for player_id, player in self.data.player_id_model_dict.items():
                if self.data.selected_group_player_ids is None or player_id in self.data.selected_group_player_ids:
                    self.player_chip_controls.controls.append(
                        ft.Container(
                            content=ft.Chip(
                                label=ft.Text(
                                    value=f'{player["JerseyNum"]}: {player["Name"]}', width=120),
                                selected=player_id in self.data.selected_player_ids,
                                on_select=self._toggle_player,
                                data=player_id,
                                disabled=False,
                            ),
                            width=150,
                        )
                    )
            await self.update_all_check_icon()

        if self.data.report_mode and self.data.hide_setting:
            self.panel.visible = False

        await self.panel.update_async()

    async def on_expand_change(self, e):
        pass

    async def on_group_change(self, e):
        self.data.select_group_id(int(self.group_select.value))
        await self.update_async()

    async def on_check_all_click(self, e):
        for chip in self.player_chip_controls.controls:
            if not chip.content.disabled:
                player_id = chip.content.data
                if self.check_all_icon.name == ft.icons.CHECK_BOX:
                    self.data.append_selected_player_id(player_id)
                else:
                    self.data.remove_selected_player_id(player_id)
        await self.update_all_check_icon()
        await self.update_async()

    async def update_all_check_icon(self):
        any_check = False
        for chip in self.player_chip_controls.controls:
            if not chip.content.disabled:
                if chip.content.selected:
                    any_check = True
        if any_check:
            self.check_all_icon.name = ft.icons.CLEAR
        else:
            self.check_all_icon.name = ft.icons.CHECK_BOX
        await self.check_all_icon.update_async()

    async def _toggle_player(self, e):
        player_id = e.control.data
        if e.control.selected:
            self.data.append_selected_player_id(player_id)
        else:
            self.data.remove_selected_player_id(player_id)
        self.player_count_text.value = f"{len(self.data.selected_player_ids)}"
        await self.player_count_text.update_async()
        await self.update_all_check_icon()