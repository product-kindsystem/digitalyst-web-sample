# Youtube how to depoloy
[https://www.youtube.com/watch?v=RN-pPq8sq_o]
[https://dash.cloudflare.com/953af55af2652d71fbf09758e371971d/r2/default/buckets/digtalyst-storage]

# Storage
① Cloudflare R2 のセットアップ
Cloudflare にログイン（無料アカウントOK）
左メニュー「R2」→「Create bucket」
バケット名を指定（例: myapp-storage）
「Create」押下

② アクセスキーを作成
Cloudflare ダッシュボード右上「My Profile」→「API Tokens」
R2 API Tokens タブを選択
「Create API Token」 → 「Edit Cloudflare R2 Storage」テンプレート選択
必要なバケット名を指定
トークン作成 → Access key / Secret key を控える


# Deploy Flet on Render
Use this repo as a template to deploy a Python [Flet](https://flet.dev) service on Render.

## Demo
[https://flet-deploy-render.onrender.com]
<img src="screendemo.png" width=800/>

follow the steps below:

## Manual Steps

1. [create your own repository from this template - by Ghost04](https://github.com/diguijoaquim/flet-deploy-render/generate) if you'd like to customize the code.
2. Create a new Web Service on Render.
3. Specify the URL to your new repository or this repository.
4. Render will automatically detect that you are deploying a Python service and use `pip` to download the dependencies.
5. Specify the following as the Start Command.

    ```shell
    #its can take time to delect the port but will find
    python main.py
    ```

6. Click Create Web Service.


## Thanks

Thanks to [Harish](https://harishgarg.com) for the [inspiration to create a FastAPI quickstart for Render](https://twitter.com/harishkgarg/status/1435084018677010434) and for some sample code!
