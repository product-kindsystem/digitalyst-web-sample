import flet as ft


# https://flet-controls-gallery.fly.dev/colors/themecolors
class ColorHelper:

    @staticmethod
    def get_table_header_bgcolor():
        return ft.Colors.with_opacity(0.05, ft.Colors.SECONDARY)

    @staticmethod
    def get_table_header_border_color():
        return ft.Colors.with_opacity(0.5, ft.Colors.SECONDARY)
