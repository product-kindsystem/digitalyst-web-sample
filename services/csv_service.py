import flet as ft
import pandas as pd
import os


class CsvService:
    @staticmethod
    def import_csv(file_path: str) -> pd.DataFrame:
        """
        インポート用の静的関数。指定されたファイルパスからCSVを読み込み、Pandas DataFrameを返す。
        :param file_path: 読み込むCSVファイルのパス
        :return: 読み込んだPandas DataFrame
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"ファイルが見つかりません: {file_path}")
        try:
            df = pd.read_csv(file_path)
            return df
        except Exception as e:
            raise Exception(f"CSVのインポート中にエラーが発生しました: {e}")

    @staticmethod
    def export_csv(data: pd.DataFrame, file_path: str) -> None:
        """
        エクスポート用の静的関数。Pandas DataFrameを指定されたファイルパスにCSVとして保存する。
        :param data: エクスポートするPandas DataFrame
        :param file_path: 保存先のCSVファイルパス
        """
        try:
            data.to_csv(file_path, index=False, encoding='utf-8-sig')
            print(f"CSVが正常にエクスポートされました: {file_path}")
        except Exception as e:
            raise Exception(f"CSVのエクスポート中にエラーが発生しました: {e}")

    @staticmethod
    def get_csv_path(page: ft.Page, title, on_success=None):
        def on_file_picked(e: ft.FilePickerResultEvent):
            if e.files and e.files[0].path:
                file_path = e.files[0].path
                try:
                    if callable(on_success):
                        on_success(file_path)
                except Exception as err:
                    pass
        try:
            file_picker = ft.FilePicker(on_result=on_file_picked)
            page.controls.append(file_picker)
            page.update()
            file_picker.pick_files(dialog_title=title, allowed_extensions=["csv"])
        except Exception as err:
            pass
