import flet as ft
import time
from models.registration.systemsetting_model import SystemSettingModel
from views.helpers.content_helper import ContentHelper
from services.localization_service import _


class PageManager:
    _instance = None
    _page: ft.Page = None

    def __new__(self, page: ft.Page = None):
        if self._instance is None:
            self._instance = super(PageManager, self).__new__(self)
            if page:
                self._page = page
        return self._instance

    @classmethod
    def get_instance(self):
        return self._instance

    def initialize(self):
        self._main_content_width = 1000
        self._page.fonts = {
            "Meiryo": "https://github.com/huiyuan/forum/blob/master/fonts/Meiryo.ttf",
            "Open Sans": "fonts/OpenSans-Regular.ttf",
        }

        def view_pop(handler):
            self._page.views.pop()
            self._page.update()
        self._page.on_view_pop = view_pop
        self._page.auto_scroll = True
        self._page.window.maximized = True
        # self._page.overlay.append(ft.ProgressBar(visible=False))
        # page.on_resized = page_resize
        # App_page_Width = page.window.width
        # App_page_Height = page.window.height

    def update_theme(self, theme_mode, theme_base_color):
        self._page.theme_mode = theme_mode
        self._page.theme = ft.Theme(
            font_family="Meiryo",
            color_scheme_seed=SystemSettingModel.get_theme_base_color(theme_base_color)
        )

    def toggle_darklight(self):
        # item = ContentHelper.get_waiting_indicator()
        # self._page.overlay.append(item)
        self._page.theme_mode = "light" if self._page.theme_mode == "dark" else "dark"
        self._page.update()
        time.sleep(0.5)
        # self._page.overlay.pop(item)
        self._page.update()

    async def toggle_darklight_async(self):
        # item = ContentHelper.get_waiting_indicator()
        # self._page.overlay.append(item)
        self._page.theme_mode = "light" if self._page.theme_mode == "dark" else "dark"
        await self._page.update_async()
        time.sleep(0.5)
        # self._page.overlay.pop(item)
        await self._page.update_async()

    def set_main_content_width(self, value):
        self._main_content_width = value

    def get_main_content_width(self):
        return self._main_content_width

    def append_update_view(self, content):
        self._page.views.append(content)
        self._page.update()

    async def append_update_view_async(self, content):
        self._page.views.append(content)
        await self._page.update_async()

    def pop_update_view(self):
        self._page.views.pop()
        self._page.update()

    async def pop_update_view_async(self):
        self._page.views.pop()
        await self._page.update_async()
