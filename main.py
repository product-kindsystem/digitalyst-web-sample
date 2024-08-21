import flet as ft 
import webbrowser
def main(page:ft.page):
    page.title="Flet + Vercel"
    



    def mudar_tema(e):
        if page.theme_mode=="light":
            page.theme_mode="dark"
            appbar.bgcolor=ft.colors.PURPLE_800
        else:
            page.theme_mode="light"
            appbar.bgcolor=ft.colors.PURPLE_200
        page.update()

    appbar=ft.AppBar(leading=ft.Icon(ft.icons.CODE),
                          title=ft.Text("Flet + Vercel"),
                          actions=[ft.IconButton(icon=ft.icons.SUNNY,on_click=mudar_tema)],
                          bgcolor=ft.colors.PURPLE_800
                          )
    page.appbar=appbar
    def yt(e):
        webbrowser.open("https://www.youtube.com/@ghost404-")
    page.add(ft.Row(controls=[
        ft.Column(controls=[
            ft.Text("Flet + Vercel",size=30,weight="bold"),
        ft.CupertinoButton("subscribe on my youtube", bgcolor=ft.colors.RED_700,on_click=yt),
        

        ])
    ],alignment=ft.MainAxisAlignment.CENTER))
ft.app(target=main,view=ft.WEB_BROWSER,port=8000)