import flet as ft
from services.localization_service import _


class ContentHelper:

    @staticmethod
    def get_waiting_indicator():
        # return ft.CupertinoActivityIndicator(radius=32)
        return ft.ProgressRing()
