import flet as ft
from services.localization_service import _
from services.logger_service import Logger


class SnackBarManager:

    @staticmethod
    def show_data_save_success(page: ft.Page):
        message = _("Message_Data_Save_Success")
        SnackBarManager._show_snackbar(page,
                                       message,
                                       bgcolor="green",
                                       text_color="white")
        Logger.snack_bar_info(f"Success : {message}")

    @staticmethod
    def show_data_copy_success(page: ft.Page):
        message = _("Message_Data_Copy_Success")
        SnackBarManager._show_snackbar(page,
                                       message,
                                       bgcolor="green",
                                       text_color="white")
        Logger.snack_bar_info(f"Success : {message}")

    @staticmethod
    def show_data_export_success(page: ft.Page):
        message = _("Message_Data_Export_Success")
        SnackBarManager._show_snackbar(page,
                                       message,
                                       bgcolor="green",
                                       text_color="white")
        Logger.snack_bar_info(f"Success : {message}")

    @staticmethod
    def show_data_import_success(page: ft.Page):
        message = _("Message_Data_Import_Success")
        SnackBarManager._show_snackbar(page,
                                       message,
                                       bgcolor="green",
                                       text_color="white")
        Logger.snack_bar_info(f"Success : {message}")

    @staticmethod
    def show_data_delete_success(page: ft.Page):
        message = _("Message_Data_Delete_Success")
        SnackBarManager._show_snackbar(page,
                                       message,
                                       bgcolor="green",
                                       text_color="white")
        Logger.snack_bar_info(f"Success : {message}")

    @staticmethod
    def show_data_reset_info(page: ft.Page):
        message = _("Message_Data_Reset_Success")
        SnackBarManager._show_snackbar(page,
                                       message,
                                       bgcolor="blue",
                                       text_color="white")
        Logger.snack_bar_info(f"Info : {message}")

    @staticmethod
    def show_data_export_error(page: ft.Page, err: Exception):
        message = f'{_("Message_Data_Export_Error")} : {err}'
        SnackBarManager._show_snackbar(page,
                                       message,
                                       bgcolor="red",
                                       text_color="white",
                                       duration=60000)
        Logger.snack_bar_error(f"Error : {message}")

    @staticmethod
    def show_data_import_error(page: ft.Page, err: Exception):
        message = f'{_("Message_Data_Import_Error")} : {err}'
        SnackBarManager._show_snackbar(page,
                                       message,
                                       bgcolor="red",
                                       text_color="white",
                                       duration=60000)
        Logger.snack_bar_error(f"Error : {message}")

    @staticmethod
    def show_success(page: ft.Page, message: str):
        SnackBarManager._show_snackbar(page,
                                       message,
                                       bgcolor="green",
                                       text_color="white")
        Logger.snack_bar_info(f"Success : {message}")

    @staticmethod
    def show_warning(page: ft.Page, message: str):
        SnackBarManager._show_snackbar(page,
                                       message,
                                       bgcolor="orange",
                                       text_color="white")
        Logger.snack_bar_warning(f"Warning : {message}")

    @staticmethod
    def show_error(page: ft.Page, message: str):
        SnackBarManager._show_snackbar(page,
                                       message,
                                       bgcolor="red",
                                       text_color="white",
                                       duration=60000)
        Logger.snack_bar_error(f"Error : {message}")

    @staticmethod
    def show_info(page: ft.Page, message: str):
        SnackBarManager._show_snackbar(page,
                                       message,
                                       bgcolor="blue",
                                       text_color="white")
        Logger.snack_bar_info(f"Info : {message}")

    @staticmethod
    def _show_snackbar(page: ft.Page,
                       message: str,
                       bgcolor: str,
                       text_color: str,
                       duration=3000):
        page.snack_bar = ft.SnackBar(content=ft.Text(message,
                                                     color=text_color),
                                     bgcolor=bgcolor,
                                     duration=duration,
                                     show_close_icon=True)
        page.snack_bar.open = True
        page.update()

    # -----------------------------------------------------------------------------

    @staticmethod
    async def show_data_save_success_async(page: ft.Page):
        message = _("Message_Data_Save_Success")
        await SnackBarManager._show_snackbar_async(page,
                                                   message,
                                                   bgcolor="green",
                                                   text_color="white")
        Logger.snack_bar_info(f"Success : {message}")

    @staticmethod
    async def show_data_copy_success_async(page: ft.Page):
        message = _("Message_Data_Copy_Success")
        await SnackBarManager._show_snackbar_async(page,
                                                   message,
                                                   bgcolor="green",
                                                   text_color="white")
        Logger.snack_bar_info(f"Success : {message}")

    @staticmethod
    async def show_data_export_success_async(page: ft.Page):
        message = _("Message_Data_Export_Success")
        await SnackBarManager._show_snackbar_async(page,
                                                   message,
                                                   bgcolor="green",
                                                   text_color="white")
        Logger.snack_bar_info(f"Success : {message}")

    @staticmethod
    async def show_data_import_success_async(page: ft.Page):
        message = _("Message_Data_Import_Success")
        await SnackBarManager._show_snackbar_async(page,
                                                   message,
                                                   bgcolor="green",
                                                   text_color="white")
        Logger.snack_bar_info(f"Success : {message}")

    @staticmethod
    async def show_data_delete_success_async(page: ft.Page):
        message = _("Message_Data_Delete_Success")
        await SnackBarManager._show_snackbar_async(page,
                                                   message,
                                                   bgcolor="green",
                                                   text_color="white")
        Logger.snack_bar_info(f"Success : {message}")

    @staticmethod
    async def show_data_reset_info_async(page: ft.Page):
        message = _("Message_Data_Reset_Success")
        await SnackBarManager._show_snackbar_async(page,
                                                   message,
                                                   bgcolor="blue",
                                                   text_color="white")
        Logger.snack_bar_info(f"Info : {message}")

    @staticmethod
    async def show_data_export_error_async(page: ft.Page, err: Exception):
        message = f'{_("Message_Data_Export_Error")} : {err}'
        await SnackBarManager._show_snackbar_async(page,
                                                   message,
                                                   bgcolor="red",
                                                   text_color="white",
                                                   duration=60000)
        Logger.snack_bar_error(f"Error : {message}")

    @staticmethod
    async def show_data_import_error_async(page: ft.Page, err: Exception):
        message = f'{_("Message_Data_Import_Error")} : {err}'
        await SnackBarManager._show_snackbar_async(page,
                                                   message,
                                                   bgcolor="red",
                                                   text_color="white",
                                                   duration=60000)
        Logger.snack_bar_error(f"Error : {message}")

    @staticmethod
    async def show_success_async(page: ft.Page, message: str):
        await SnackBarManager._show_snackbar_async(page,
                                                   message,
                                                   bgcolor="green",
                                                   text_color="white")
        Logger.snack_bar_info(f"Success : {message}")

    @staticmethod
    async def show_warning_async(page: ft.Page, message: str):
        await SnackBarManager._show_snackbar_async(page,
                                                   message,
                                                   bgcolor="orange",
                                                   text_color="white")
        Logger.snack_bar_warning(f"Warning : {message}")

    @staticmethod
    async def show_error_async(page: ft.Page, message: str):
        await SnackBarManager._show_snackbar_async(page,
                                                   message,
                                                   bgcolor="red",
                                                   text_color="white",
                                                   duration=60000)
        Logger.snack_bar_error(f"Error : {message}")

    @staticmethod
    async def show_info_async(page: ft.Page, message: str):
        await SnackBarManager._show_snackbar_async(page,
                                                   message,
                                                   bgcolor="blue",
                                                   text_color="white")
        Logger.snack_bar_info(f"Info : {message}")

    @staticmethod
    async def _show_snackbar_async(page: ft.Page,
                                   message: str,
                                   bgcolor: str,
                                   text_color: str,
                                   duration=3000):
        page.snack_bar = ft.SnackBar(content=ft.Text(message,
                                                     color=text_color),
                                     bgcolor=bgcolor,
                                     duration=duration,
                                     show_close_icon=True)
        page.snack_bar.open = True
        await page.update_async()
