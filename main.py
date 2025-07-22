import flet as ft
import os
import asyncio
from controllers.web.web_controller import WebController
from services.web_path_serivce import WebPath
from services.web_url_serivce import WebUrl
from services.localization_service import LocalizationService, _
from dotenv import load_dotenv

VERSION = "0.0.0.1"
load_dotenv()

async def main(page: ft.Page):

    if "ping" in page.route:
        return

    # ルートに応じた処理をここで行う（例：ページ表示切り替え）
    async def on_route_change(e: ft.RouteChangeEvent):
        e.page.controls.clear()
        e.page.appbar = None
        await main(e.page)

    async def on_page_resized(e):
        print(f"page.width : {e.width}")
        pass

    page.on_route_change = on_route_change  # イベントハンドラ登録
    page.on_resized = on_page_resized
    print(f"page.width : {page.width}")

    # 初期表示
    page.window.maximized = True
    page.title = f"Digitalyst V.{VERSION}"
    page.window.icon = os.path.join(WebPath.get_web_dir(), "digitalyst.ico")

    page.overlay.append(
        ft.Container(ft.ProgressRing(width=48,
                                     height=48,
                                     color=ft.Colors.BLUE,
                                     expand=True,
                                     semantics_label="Loading.."),
                     expand=True,
                     alignment=ft.alignment.center))
    page.update()
    await asyncio.sleep(1)

    # ルート確認
    WebUrl.set_url(page)
    team_name = WebUrl.TEAM_NAME
    print(f"team_name: {team_name}")

    if team_name not in [
            "a", "team01", "team02", "team03", "team04", "team05"
    ]:
        return

    WebPath.set_team_name(team_name)
    file_path = os.path.join(WebPath.get_web_dir(), "WebLocalization.xlsx")
    LocalizationService.load_localization(file_path)
    LocalizationService.set_language_setting("ja")

    # 画面表示
    page.overlay.clear()

    controller = WebController(page, team_name)
    upload_file_icon_btn = ft.ElevatedButton(
        content=ft.Icon(name=ft.Icons.CLOUD_UPLOAD_OUTLINED),
        data=True,
        on_click=controller.on_file_upload_click,
        visible=not WebUrl.PARAMS.get("report_mode", False))
    title_text = ft.Text(value=f'{_("WebDrill_Title")} ({team_name})', size=20)
    page.appbar = ft.CupertinoAppBar(
        leading=ft.Icon(ft.Icons.VIEW_TIMELINE),
        bgcolor=ft.Colors.SURFACE_CONTAINER_HIGHEST,
        trailing=upload_file_icon_btn,
        middle=title_text,
    )
    page.add(controller.view.build())
    await controller.update_async()


if __name__ == "__main__":
    # WebPath.set_base_dir(os.path.dirname(__file__))
    WebPath.set_base_dir(os.path.abspath(os.path.dirname(__file__)))
    port = int(os.environ.get("PORT", 80))
    os.environ["FLET_SECRET_KEY"] = "your_secure_key"  # 必ず app() を呼ぶ前に設定する
    upload_dir = WebPath.get_upload_dir()
    os.makedirs(upload_dir, exist_ok=True)
    ft.app(main, view=ft.AppView.WEB_BROWSER, port=port, upload_dir=upload_dir)
