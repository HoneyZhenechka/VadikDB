import os
import json
import zipfile
import exception
from pathlib import Path


class DBManager:
    __current_directory = ""
    __db_file_path = ""
    __db_list = []

    def __init__(self):
        self.__current_directory = Path.cwd()
        db_directory = self.__current_directory / "db/"
        if not db_directory.exists():
            os.makedirs(str(db_directory))
        self.__current_directory = db_directory

    def create_db(self, db_name):
        db_file = db_name + ".vdb"
        self.__db_file_path = self.__current_directory / db_file
        if self.__db_file_path.exists():
            print("Overwriting database file! ", self.__db_file_path)
        db_archive = zipfile.ZipFile(self.__db_file_path, "w")
        with open("data.json", "w+") as data_file:
            json.dump({}, data_file)
        db_archive.write("data.json")
        os.remove("data.json")
        with open("meta.json", "w+") as meta_file:
            json.dump({
                "name": db_name,
                "tables": []
            }, meta_file)
        db_archive.write("meta.json")
        os.remove("meta.json")
        db_archive.close()
        self.__db_list.append(db_name)

    def create_table(self, db_name, table_name):
        if db_name not in self.__db_list:
            raise exception.DBNotExists(db_name)
        db_archive = zipfile.ZipFile(self.__db_file_path, "a")
        db_archive.extractall()
        db_archive.close()
        os.remove(self.__db_file_path)
        with open("meta.json", "r") as meta_file:
            meta_data = json.load(meta_file)
            if table_name in meta_data["tables"]:
                raise exception.TableAlreadyExists(table_name)
            meta_data["tables"].append(table_name)
        with open("meta.json", "w") as meta_file:
            json.dump(meta_data, meta_file)
        with open("data.json", "r") as data_file:
            data_json = json.load(data_file)
            data_json[table_name] = {}
        with open("data.json", "w") as data_file:
            json.dump(data_json, data_file)
        db_archive = zipfile.ZipFile(self.__db_file_path, "w")
        db_archive.write("meta.json")
        db_archive.write("data.json")
        os.remove("meta.json")
        os.remove("data.json")
