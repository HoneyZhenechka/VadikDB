import os
import json
import exception
from pathlib import Path


class Database:
    __current_directory = ""

    def __init__(self, database_name):
        self.__current_directory = Path.cwd()
        db_directory = self.__current_directory / database_name
        if not db_directory.exists():
            os.makedirs(str(db_directory))
        self.__current_directory = db_directory
        with open(self.__current_directory / "data.json", "w") as data_file:
            json.dump({}, data_file)
        with open(self.__current_directory / "db_meta.json", "w") as meta_file:
            json.dump({
                "name": database_name,
                "tables": []
            }, meta_file)

    def create_table(self, table_name, fields):
        with open(self.__current_directory / "db_meta.json", "r") as meta_file:
            meta_data = json.load(meta_file)
            if table_name in meta_data["tables"]:
                raise exception.TableAlreadyExists(table_name)
        meta_data["tables"].append(table_name)
        with open(self.__current_directory / "db_meta.json", "w") as meta_file:
            json.dump(meta_data, meta_file)
        table_meta_file = "table_" + table_name + "_meta.json"
        with open(self.__current_directory / table_meta_file, "w") as meta_file:
            json.dump({
                "name": table_name,
                "fields": {}
            }, meta_file)
        with open(self.__current_directory / table_meta_file, "r") as meta_file:
            table_meta = json.load(meta_file)
        with open(self.__current_directory / "data.json", "r") as data_file:
            data_json = json.load(data_file)
            data_json[table_name] = {}
            if not len(fields) == 0:
                for i in range(len(fields)):
                    data_json[table_name][fields[i][0]] = "null"
                    table_meta["fields"][fields[i][0]] = fields[i][1]  # {name:type}
        with open(self.__current_directory / "data.json", "w") as data_file:
            json.dump(data_json, data_file)
        with open(self.__current_directory / table_meta_file, "w") as meta_file:
            json.dump(table_meta, meta_file)

    def show_create_table(self, table_name):
        with open(self.__current_directory / "db_meta.json", "r") as table_file:
            meta_data = json.load(table_file)
            if table_name not in meta_data["tables"]:
                raise exception.TableNotExists(table_name)
        table_meta_file = "table_" + table_name + "_meta.json"
        with open(self.__current_directory / table_meta_file, "r") as table_file:
            table_meta = json.load(table_file)
        fields = table_meta["fields"]
        fields_str = ""
        for key in fields:
            fields_str += key + " " + fields[key] + ", "
        fields_str = fields_str[:-2]
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

    def drop_table(self, table_name):
        with open(self.__current_directory / "db_meta.json", "r") as meta_file:
            meta_data = json.load(meta_file)
            if table_name not in meta_data["tables"]:
                raise exception.TableNotExists(table_name)
        meta_data["tables"].remove(table_name)
        table_meta_file = "table_" + table_name + "_meta.json"
        os.remove(self.__current_directory / table_meta_file)
        with open(self.__current_directory / "db_meta.json", "w") as meta_file:
            json.dump(meta_data, meta_file)
        with open(self.__current_directory / "data.json", "r") as data_file:
            data_json = json.load(data_file)
            data_json.pop(table_name)
        with open(self.__current_directory / "data.json", "w") as data_file:
            json.dump(data_json, data_file)
