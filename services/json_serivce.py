import json
import flet as ft
import os
from time import sleep
from datetime import datetime, timedelta, date
from services.web_path_serivce import WebPath
import pandas as pd
from views.helpers.snackbar_manager import SnackBarManager


class JsonService:

    @staticmethod
    async def export_json_async(page: ft.Page, data_dict: dict,
                                default_file_name: str):

        async def on_file_picked_export(e: ft.FilePickerResultEvent):
            if e.path:
                file_path = e.path
                if not file_path.endswith(".json"):
                    file_path += ".json"
                try:
                    with open(file_path, "w", encoding="utf-8") as f:
                        json.dump(data_dict, f, ensure_ascii=False, indent=2)
                    await SnackBarManager.show_data_export_success_async(page)
                except Exception as err:
                    await SnackBarManager.show_data_export_error_async(
                        page, err)

        try:
            file_picker_export = ft.FilePicker(on_result=on_file_picked_export)
            page.overlay.append(file_picker_export)
            await page.update_async()
            await file_picker_export.save_file_async(
                file_name=default_file_name,
                allowed_extensions=["json"],
            )
        except Exception as err:
            await SnackBarManager.show_data_export_error_async(page, err)

    @staticmethod
    async def web2_import_json_async(page: ft.Page, save_dir, on_success=None):
        files_column = ft.Column()
        upload_button = ft.ElevatedButton("Upload", disabled=True)
        file_picker = ft.FilePicker()

        page.overlay.append(file_picker)

        selected_file = {"name": ""}  # 共有変数

        def file_picker_result(e: ft.FilePickerResultEvent):
            if e.files:
                selected_file["name"] = e.files[0].name
                files_column.controls.clear()
                files_column.controls.append(
                    ft.Text(f"Selected: {e.files[0].name}"))
                upload_button.disabled = False
                page.update()

        async def on_upload_progress(e: ft.FilePickerUploadEvent):
            print(f"Uploading {e.file_name}: {e.progress * 100:.0f}%")

        async def upload_files(e):
            if file_picker.result and file_picker.result.files:
                upload_files = [
                    ft.FilePickerUploadFile(
                        file.name,
                        upload_url=page.get_upload_url(f"{file.name}", 60),
                    ) for file in file_picker.result.files
                ]
                file_picker.upload(upload_files)

                # 待機後にファイル読み込み処理
                await page.wait_async(1.0)  # ←ファイル書き込み完了まで少し待つ（重要）

                file_path = os.path.join(save_dir, selected_file["name"])
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        data_dict = json.load(f)
                    if callable(on_success):
                        await on_success(data_dict)
                    await page.snack_bar.open("JSONインポート成功！")
                except Exception as err:
                    await page.snack_bar.open(f"読み込み失敗: {err}")

        file_picker.on_result = file_picker_result
        file_picker.on_upload = on_upload_progress

        page.add(
            ft.ElevatedButton("Select JSON File",
                              on_click=lambda _: file_picker.pick_files(
                                  allowed_extensions=["json"])),
            files_column,
            upload_button,
        )
        upload_button.on_click = upload_files

    @staticmethod
    async def web_import_json_async(page: ft.Page, on_success=None):
        file_picker = None

        async def on_file_upload(e):
            if e.progress == 1:
                upload_file_name = e.file_name
                uploaded_file_path = WebPath.get_uploaded_team_file_path(
                    upload_file_name)
                WebPath.upload_file(uploaded_file_path, upload_file_name)
                os.remove(uploaded_file_path)
                if on_success:
                    await on_success(e)

        async def on_file_picked_import(e: ft.FilePickerResultEvent):
            if file_picker.result and file_picker.result.files and len(
                    file_picker.result.files) > 0:
                file = file_picker.result.files[0]
                upload_file_name = file.name
                upload_files = [
                    ft.FilePickerUploadFile(
                        name=upload_file_name,
                        upload_url=page.get_upload_url(
                            f"{WebPath.TEAM_NAME}/{upload_file_name}", 60),
                    )
                ]
                print("[アップロードURL]", [f.upload_url for f in upload_files])
                print("[アップロードファイル]", [f.name for f in upload_files])
                file_picker.on_upload = on_file_upload
                await file_picker.upload_async(upload_files)

        try:
            file_picker = ft.FilePicker(on_result=on_file_picked_import)
            page.overlay.append(file_picker)
            await page.update_async()
            await file_picker.pick_files_async(allowed_extensions=["json"])
        except Exception as err:
            await SnackBarManager.show_data_import_error_async(page, err)

    @staticmethod
    async def import_json_async(page: ft.Page, on_success=None):

        async def on_file_picked_import(e: ft.FilePickerResultEvent):
            if e.files and e.files[0].path:
                file_path = e.files[0].path
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        data_dict = json.load(f)
                    if callable(on_success):
                        await on_success(data_dict)
                    await SnackBarManager.show_data_import_success_async(page)
                except Exception as err:
                    await SnackBarManager.show_data_import_error_async(
                        page, err)

        try:
            file_picker_import = ft.FilePicker(on_result=on_file_picked_import)
            page.overlay.append(file_picker_import)
            await page.update_async()
            await file_picker_import.pick_files_async(
                allowed_extensions=["json"])
        except Exception as err:
            await SnackBarManager.show_data_import_error_async(page, err)

    @staticmethod
    def export_json(page: ft.Page, data_dict: dict, default_file_name: str):

        def on_file_picked_export(e: ft.FilePickerResultEvent):
            if e.path:
                file_path = e.path
                if not file_path.endswith(".json"):
                    file_path += ".json"
                try:
                    dict = JsonService.convert_datetime_to_str(data_dict)
                    with open(file_path, "w", encoding="utf-8") as f:
                        json.dump(dict, f, ensure_ascii=False, indent=2)
                    SnackBarManager.show_data_export_success(page)
                except Exception as err:
                    SnackBarManager.show_data_export_error(page, err)

        try:
            file_picker_export = ft.FilePicker(on_result=on_file_picked_export)
            page.overlay.append(file_picker_export)
            page.update()
            file_picker_export.save_file(
                file_name=default_file_name,
                allowed_extensions=["json"],
            )
        except Exception as err:
            SnackBarManager.show_data_export_error(page, err)

    @staticmethod
    def convert_datetime_to_str(obj):
        if isinstance(obj, (datetime, date, pd.Timestamp)):
            if pd.isna(obj):
                return None  # または "" にするなど用途に応じて
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(obj, dict):
            return {
                k: JsonService.convert_datetime_to_str(v)
                for k, v in obj.items()
            }
        elif isinstance(obj, list):
            return [JsonService.convert_datetime_to_str(i) for i in obj]
        else:
            return obj

    @staticmethod
    def import_json(page: ft.Page, on_success=None):

        def on_file_picked_import(e: ft.FilePickerResultEvent):
            if e.files and e.files[0].path:
                file_path = e.files[0].path
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        data_dict = json.load(f)
                    if callable(on_success):
                        on_success(data_dict)
                    SnackBarManager.show_data_import_success(page)
                except Exception as err:
                    SnackBarManager.show_data_import_error(page, err)

        try:
            file_picker_import = ft.FilePicker(on_result=on_file_picked_import)
            page.overlay.append(file_picker_import)
            page.update()
            file_picker_import.pick_files(allowed_extensions=["json"])
        except Exception as err:
            SnackBarManager.show_data_import_error(page, err)
