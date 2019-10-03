import os
import json
import zipfile
import exception
from pathlib import Path


class DBManager:
    __current_directory = ""
    __current_db = ""
    __db_file_path = ""
    __db_list = []
    __is_exception = False

    def __init__(self):
        self.__current_directory = Path.cwd()
        db_directory = self.__current_directory / "db/"
        if not db_directory.exists():
            os.makedirs(str(db_directory))
        self.__current_directory = db_directory
        for file in os.listdir(self.__current_directory):
            if file.endswith(".vdb"):
                self.__db_list.append(str(file).replace(".vdb", ""))

    def __exists_db(self, db_name):
        if db_name not in self.__db_list:
            self.__is_exception = True
            exception.DBNotExists(db_name)
        if self.__is_exception:
            return "ERROR"

    def __extract_db(self):
        with zipfile.ZipFile(self.__db_file_path, "a") as db_archive:
            db_archive.extractall()
        os.remove(self.__db_file_path)

    def __write_db(self, table_meta_files=[]):
        with zipfile.ZipFile(self.__db_file_path, "w") as db_archive:
            db_archive.write("db_meta.json")
            db_archive.write("data.json")
            if len(table_meta_files) > 0:
                for file in table_meta_files:
                    db_archive.write(file)
            os.remove("db_meta.json")
            os.remove("data.json")
            for file in table_meta_files:
                os.remove(file)

    def __get_files_tables_list(self, metadata):
        tables_list = metadata["tables"]
        files_list = []
        for table in tables_list:
            files_list.append("table_" + table + "_meta.json")
        return files_list

    def create_db(self, db_name):
        db_filename = db_name + ".vdb"
        self.__current_db = db_name
        self.__db_file_path = self.__current_directory / db_filename
        if self.__db_file_path.exists():
            print("Overwriting database file! ", self.__db_file_path)
        with zipfile.ZipFile(self.__db_file_path, "w") as db_archive:
            with open("data.json", "w+") as data_file:
                json.dump({}, data_file)
            db_archive.write("data.json")
            with open("db_meta.json", "w+") as meta_file:
                json.dump({
                    "name": db_name,
                    "tables": []
                }, meta_file)
            db_archive.write("db_meta.json")
        os.remove("data.json")
        os.remove("db_meta.json")
        self.__db_list.append(db_name)

    def change_db(self, db_name):
        self.__exists_db(db_name)
        self.__current_db = db_name
        db_filename = db_name + ".vdb"
        self.__db_file_path = self.__current_directory / db_filename

    def create_table(self, table_name, fields):
        self.__exists_db(self.__current_db)
        self.__extract_db()
        with open("db_meta.json", "r") as meta_file:
            meta_data = json.load(meta_file)
            if table_name in meta_data["tables"]:
                self.__is_exception = True
                exception.TableAlreadyExists(table_name)
            if self.__is_exception:
<<<<<<< HEAD
                self.__write_db(files_tables_list)
                return "ERROR"
=======
                return
>>>>>>> parent of 201ba33... remove files in engine
            meta_data["tables"].append(table_name)
        with open("db_meta.json", "w") as meta_file:
            json.dump(meta_data, meta_file)
        table_meta_file = "table_" + table_name + "_meta.json"
        with open(table_meta_file, "w+") as meta_file:
            json.dump({
                "name": table_name,
                "fields": {}
            }, meta_file)
        with open(table_meta_file, "r") as meta_file:
            table_meta = json.load(meta_file)
        with open("data.json", "r") as data_file:
            data_json = json.load(data_file)
            data_json[table_name] = {}
            if not len(fields) == 0:
                for i in range(len(fields)):
                    data_json[table_name][fields[i][0]] = "null"
                    table_meta["fields"][fields[i][0]] = fields[i][1]  # {name:type}
        with open("data.json", "w") as data_file:
            json.dump(data_json, data_file)
        with open(table_meta_file, "w") as meta_file:
            json.dump(table_meta, meta_file)
        files_tables_list = self.__get_files_tables_list(meta_data)
        self.__write_db(files_tables_list)
        return "NOT ERROR"

    def show_create_table(self, table_name):
        self.__exists_db(self.__current_db)
        self.__extract_db()
        with open("db_meta.json", "r") as table_file:
            meta_data = json.load(table_file)
            if table_name not in meta_data["tables"]:
                self.__is_exception = True
                exception.TableNotExists(table_name)
        if self.__is_exception:
<<<<<<< HEAD
            self.__write_db(files_tables_list)
            return "ERROR"
=======
            return
>>>>>>> parent of 201ba33... remove files in engine
        table_meta_file = "table_" + table_name + "_meta.json"
        with open(table_meta_file, "r") as table_file:
            table_meta = json.load(table_file)
        fields = table_meta["fields"]
        fields_str = ""
        for key in fields:
            fields_str += key + " " + fields[key] + ", "
        fields_str = fields_str[:-2]
        files_tables_list = self.__get_files_tables_list(meta_data)
        self.__write_db(files_tables_list)
        query = (
                "CREATE TABLE " + table_name + " (\n" +
                "\t\t\t\t\t" + fields_str + "\n\t\t\t\t);"
        )
        print(
                "===================================================\n" +
                '\t\tTable:\t"' + table_name + '"\n' +
                "Create Table:\t" + query + "\n" 
                "==================================================="
        )
        return "NOT ERROR"

    def drop_table(self, table_name):
        self.__exists_db(self.__current_db)
        self.__extract_db()
        with open("db_meta.json", "r") as meta_file:
            meta_data = json.load(meta_file)
            if table_name not in meta_data["tables"]:
                self.__is_exception = True
                exception.TableNotExists(table_name)
            if self.__is_exception:
<<<<<<< HEAD
                self.__write_db(files_tables_list)
                return "ERROR"
=======
                return
>>>>>>> parent of 201ba33... remove files in engine
            meta_data["tables"].remove(table_name)
        table_meta_file = "table_" + table_name + "_meta.json"
        os.remove(table_meta_file)
        with open("db_meta.json", "w") as meta_file:
            json.dump(meta_data, meta_file)
        with open("data.json", "r") as data_file:
            data_json = json.load(data_file)
            data_json.pop(table_name)
        with open("data.json", "w") as data_file:
            json.dump(data_json, data_file)
        files_tables_list = self.__get_files_tables_list(meta_data)
        self.__write_db(files_tables_list)
        return "NOT ERROR"

