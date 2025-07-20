import os
from replit.object_storage import Client


class WebPath:

    BASE_DIR_PATH = ""
    TEAM_NAME = ""
    client = Client()
    uploaded_file_names = []

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
        return os.path.join(WebPath.BASE_DIR_PATH, "Web", "Uploads",
                            WebPath.TEAM_NAME, file_name)

    @staticmethod
    def _list_uploaded_file_names():
        prefix = f"Uploads/{WebPath.TEAM_NAME}/"
        print(f"prefix: {prefix}")
        list = WebPath.client.list()
        print(f"list: {list}")
        WebPath.uploaded_file_names = [
            obj.name.replace(prefix, "") for obj in list
            if obj.name.startswith(prefix)
        ]

    @staticmethod
    def get_uploaded_file_names():
        return WebPath.uploaded_file_names

    @staticmethod
    def get_object_path(filename):
        return f"Uploads/{WebPath.TEAM_NAME}/{filename}"

    @staticmethod
    def get_file_as_text(filename):
        object_path = WebPath.get_object_path(filename)
        return WebPath.client.download_as_text(object_path)

    @staticmethod
    def upload_file(local_path, filename):
        object_path = WebPath.get_object_path(filename)
        WebPath.client.upload_from_filename(object_path,
                                            local_path)  #存在していたら上賀き
        WebPath._list_uploaded_file_names()
        print(f"Uploaded {local_path} to {object_path}")

