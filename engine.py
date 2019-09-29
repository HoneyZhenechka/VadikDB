import os
import json
import zipfile
from pathlib import Path


class DBManager:
    current_directory = ""

    def __init__(self):
        self.current_directory = Path.cwd()
        db_directory = self.current_directory / "db/"
        if not db_directory.exists():
            os.makedirs(str(db_directory))
        self.current_directory = db_directory

    def create_db(self, name):
        db_file = name + ".vdb"
        db_file_path = self.current_directory / db_file
        if db_file_path.exists():
            print("Overwriting database file! ", db_file_path)
        db_archive = zipfile.ZipFile(db_file_path, "w")
        data_file = open("data.json", "w+")
        data_file.close()
        db_archive.write("data.json")
        os.remove("data.json")
        meta_file = open("meta.json", "w+")
        json.dump({
            "name": name,
            "tables": []
        }, meta_file)
        db_archive.write("meta.json")
        meta_file.close()
        os.remove("meta.json")
        db_archive.close()