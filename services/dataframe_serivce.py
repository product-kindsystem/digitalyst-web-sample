import flet as ft
import pandas as pd
from views.helpers.snackbar_manager import SnackBarManager
from services.csv_service import CsvService
from services.localization_service import _


class DataFrameService:

    @staticmethod
    def get_safe_value(df, row, col):
        return df.loc[row, col] if col in df.columns and not pd.isna(df.loc[row, col]) else ""

    @staticmethod
    def create_empty_df(model, exclude_columns=None):
        columns = [col.name for col in model.__table__.columns]
        if exclude_columns:
            columns = [col for col in columns if col not in exclude_columns]
        return pd.DataFrame(columns=columns)

    @staticmethod
    def convert_to_df(model, items, exclude_columns=[]):
        """
        SQLAlchemy セッションとモデルからデータを取得し、Pandas DataFrame に変換します。

        Args:
            session (Session): SQLAlchemy セッションオブジェクト。
            model (Base): SQLAlchemy モデルクラス。

        Returns:
            pd.DataFrame: モデルデータを含む DataFrame。
        """
        df = pd.DataFrame([item.__dict__ for item in items])
        if '_sa_instance_state' in df.columns:
            df.drop('_sa_instance_state', axis=1, inplace=True)

        if df.empty:
            df = DataFrameService.create_empty_df(model, exclude_columns=exclude_columns)
        else:
            columns = [col.name for col in model.__table__.columns]
            df = df[columns]

            if len(exclude_columns) > 0:
                df = df.drop(columns=exclude_columns, errors='ignore')
        return df

    @staticmethod
    def convert_to_items(df: pd.DataFrame, model_class):
        """
        DataFrame をモデルのアイテムリストに変換。
        NaN 値は None に置き換えられます。
        """
        items = []
        model_columns = {col.name for col in model_class.__table__.columns}

        for _, row in df.iterrows():
            item = {}
            for col_name, value in row.items():
                if col_name in model_columns:
                    if pd.isna(value):  # NaN を None に置き換え
                        item[col_name] = None
                    else:
                        item[col_name] = value
            items.append(item)
        return items

    @staticmethod
    def export_csv(page: ft.Page, df, default_file_name, select_columns=None):
        def on_file_picked_export(e: ft.FilePickerResultEvent):
            if e.path:
                file_path = e.path
                if not file_path.endswith(".csv"):
                    file_path += ".csv"
                try:
                    df.to_csv(file_path, index=False, encoding='utf-8-sig')
                    SnackBarManager.show_data_export_success(page)
                except Exception as err:
                    SnackBarManager.show_data_export_error(page, err)
        try:
            file_picker_export = ft.FilePicker(on_result=on_file_picked_export)
            page.controls.append(file_picker_export)
            page.update()
            file_picker_export.save_file(
                file_name=default_file_name,
                allowed_extensions=["csv"],
            )
        except Exception as err:
            SnackBarManager.show_data_export_error(page, err)

    @staticmethod
    def import_csv(page: ft.Page, model, on_success=None):
        def on_file_picked_import(e: ft.FilePickerResultEvent):
            if e.files and e.files[0].path:
                file_path = e.files[0].path
                try:
                    df = CsvService.import_csv(file_path)
                    if df.empty:
                        raise ValueError(_("Message_Data_Import_ValueErrorMessage"))
                    model.save_df(df)
                    SnackBarManager.show_data_import_success(page)
                    if callable(on_success):
                        on_success()
                except Exception as err:
                    SnackBarManager.show_data_import_error(page, err)
        try:
            file_picker_import = ft.FilePicker(on_result=on_file_picked_import)
            page.controls.append(file_picker_import)
            page.update()
            file_picker_import.pick_files(allowed_extensions=["csv"])
        except Exception as err:
            SnackBarManager.show_data_import_error(page, err)

    @staticmethod
    async def export_csv_async(page: ft.Page, df, default_file_name, select_columns=None):
        async def on_file_picked_export(e: ft.FilePickerResultEvent):
            if e.path:
                file_path = e.path
                if not file_path.endswith(".csv"):
                    file_path += ".csv"
                try:
                    df.to_csv(file_path, index=False, encoding='utf-8-sig')
                    await SnackBarManager.show_data_export_success_async(page)
                except Exception as err:
                    await SnackBarManager.show_data_export_error_async(page, err)
        try:
            file_picker_export = ft.FilePicker(on_result=on_file_picked_export)
            page.controls.append(file_picker_export)
            await page.update_async()
            await file_picker_export.save_file_async(
                file_name=default_file_name,
                allowed_extensions=["csv"],
            )
        except Exception as err:
            await SnackBarManager.show_data_export_error_async(page, err)

    @staticmethod
    async def import_csv_async(page: ft.Page, model, on_success=None):
        async def on_file_picked_import(e: ft.FilePickerResultEvent):
            if e.files and e.files[0].path:
                file_path = e.files[0].path
                try:
                    df = CsvService.import_csv(file_path)
                    if df.empty:
                        raise ValueError(_("Message_Data_Import_ValueErrorMessage"))
                    model.save_df(df)
                    if callable(on_success):
                        await on_success(df)
                    await SnackBarManager.show_data_import_success_async(page)
                except Exception as err:
                    await SnackBarManager.show_data_import_error_async(page, err)
        try:
            file_picker_import = ft.FilePicker(on_result=on_file_picked_import)
            page.controls.append(file_picker_import)
            await page.update_async()
            await file_picker_import.pick_files_async(allowed_extensions=["csv"])
        except Exception as err:
            await SnackBarManager.show_data_import_error_async(page, err)

    @staticmethod
    def import_df(page: ft.Page, on_success=None):
        def on_file_picked_import(e: ft.FilePickerResultEvent):
            if e.files and e.files[0].path:
                file_path = e.files[0].path
                try:
                    df = CsvService.import_csv(file_path)
                    if callable(on_success):
                        on_success(df)
                except Exception as err:
                    SnackBarManager.show_data_import_error(page, err)
        try:
            file_picker_import = ft.FilePicker(on_result=on_file_picked_import)
            page.controls.append(file_picker_import)
            page.update()
            file_picker_import.pick_files()
        except Exception as err:
            SnackBarManager.show_data_import_error(page, err)

    @staticmethod
    def upsert_df(session, model, df):
        """
        Pandas DataFrame から SQLAlchemy モデルにデータを挿入します。

        Args:
            session (Session): SQLAlchemy セッションオブジェクト。
            model (Base): SQLAlchemy モデルクラス。
            df (pd.DataFrame): 挿入するデータを含む DataFrame。

        Returns:
            None
        """
        for _, row in df.iterrows():
            item = model(**row.to_dict())
            session.merge(item)  # 存在すれば更新、なければ挿入
        session.commit()
