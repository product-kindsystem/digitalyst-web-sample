import flet as ft
import os
from urllib.parse import urlparse, parse_qs


class WebUrl:

    HTTP_URL = None
    HTTPS_URL = None
    TEAM_NAME = None
    PARAMS = {}

    @staticmethod
    def set_url(page: ft.Page):

        # URLを分解
        parsed = urlparse(page.url)
        if parsed.port:
            WebUrl.HTTP_URL = f"http://{parsed.hostname}:{parsed.port}"
            WebUrl.HTTPS_URL = f"https://{parsed.hostname}:{parsed.port}"
        else:
            WebUrl.HTTP_URL = f"http://{parsed.hostname}"
            WebUrl.HTTPS_URL = f"https://{parsed.hostname}"

        # 2. team_name をパスから抽出
        parsed = urlparse(page.route)
        WebUrl.TEAM_NAME = parsed.path.lstrip("/")
        WebUrl.PARAMS = {k: v[0] for k, v in parse_qs(parsed.query).items()}
