import os
import boto3
from botocore.client import Config


class WebPath:
    BASE_DIR_PATH = ""
    TEAM_NAME = ""
    uploaded_file_names = []

    # Cloudflare R2 接続設定（S3互換）
    s3 = boto3.client(
        "s3",
        endpoint_url=f"https://{os.environ.get('R2_ACCOUNT_ID')}.r2.cloudflarestorage.com",
        aws_access_key_id=os.environ.get("R2_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("R2_SECRET_KEY"),
        config=Config(signature_version="s3v4"),
        region_name="auto"
    )
    BUCKET_NAME = os.environ.get("R2_BUCKET_NAME")

    @staticmethod
    def set_base_dir(base_dir_path):
        WebPath.BASE_DIR_PATH = base_dir_path

    @staticmethod
    def set_team_name(team_name):
        WebPath.TEAM_NAME = team_name
        WebPath._list_uploaded_file_names()

    @staticmethod
    def get_web_dir():
        return os.path.join(WebPath.BASE_DIR_PATH, "Web")

    @staticmethod
    def get_upload_url():
        return "Web/Uploads"

    @staticmethod
    def get_upload_dir():
        return os.path.join(WebPath.BASE_DIR_PATH, "Web", "Uploads")

    @staticmethod
    def get_uploaded_team_file_path(file_name):
        return os.path.join(WebPath.BASE_DIR_PATH, "Web", "Uploads", WebPath.TEAM_NAME, file_name)

    @staticmethod
    def get_object_path(filename):
        return f"Uploads/{WebPath.TEAM_NAME}/{filename}"

    @staticmethod
    def _list_uploaded_file_names():
        prefix = f"Uploads/{WebPath.TEAM_NAME}/"
        resp = WebPath.s3.list_objects_v2(Bucket=WebPath.BUCKET_NAME, Prefix=prefix)
        WebPath.uploaded_file_names = [
            obj["Key"].replace(prefix, "") for obj in resp.get("Contents", [])
        ] if "Contents" in resp else []

    @staticmethod
    def get_uploaded_file_names():
        return WebPath.uploaded_file_names

    @staticmethod
    def get_file_as_text(filename):
        object_path = WebPath.get_object_path(filename)
        resp = WebPath.s3.get_object(Bucket=WebPath.BUCKET_NAME, Key=object_path)
        return resp['Body'].read().decode('utf-8')

    @staticmethod
    def upload_file(local_path, filename):
        object_path = WebPath.get_object_path(filename)
        WebPath.s3.upload_file(local_path, WebPath.BUCKET_NAME, object_path)
        WebPath._list_uploaded_file_names()
        print(f"Uploaded {local_path} to {object_path}")
