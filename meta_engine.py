import os
import json
import table
import exception
from pathlib import Path


class Database:
    __current_directory = ""

    def __to_bool(self, value):
        if value.lower() in ("true", "1"):
            return True
        if value.lower() in ("false", "0"):
            return False
        raise Exception('Invalid value for boolean conversion: ' + value)

    def __check_type(self, table_meta, fields, values):
        value_index = 0
        if len(fields) == 0:
            for key, value in table_meta["fields"].items():
                if value == "int":
                    try:
                        int(values[value_index])
                    except Exception:
                        raise exception.InvalidDataType()
                if value == "bool":
                    try:
                        self.__to_bool(values[value_index])
                    except Exception:
                        raise exception.InvalidDataType()
                value_index = value_index + 1
        else:
            for field in fields:
                for key, value in table_meta["fields"].items():
                    if (value == "int") and (key == field):
                        try:
                            int(values[value_index])
                        except Exception:
                            raise exception.InvalidDataType()
                    if (value == "bool") and (key == field):
                        try:
                            self.__to_bool(values[value_index])
                        except Exception:
                            raise exception.InvalidDataType()
                    value_index = value_index + 1

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
                    data_json[table_name][fields[i][0]] = []
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

    def insert(self, table_name, fields, values):
        with open(self.__current_directory / "db_meta.json", "r") as meta_file:
            meta_data = json.load(meta_file)
            if table_name not in meta_data["tables"]:
                raise exception.TableNotExists(table_name)
        with open(self.__current_directory / "data.json", "r") as data_file:
            data_json = json.load(data_file)
        for field in fields:
            if field not in data_json[table_name]:
                raise exception.FieldNotExists(field)
        table_meta_file = "table_" + table_name + "_meta.json"
        with open(self.__current_directory / table_meta_file, "r") as table_file:
            table_meta = json.load(table_file)
        self.__check_type(table_meta, fields, values)
        value_index = 0
        if len(fields) == 0:
            for key in data_json[table_name]:
                data_json[table_name][key].append(values[value_index])
                value_index = value_index + 1
        else:
            for key in data_json[table_name]:
                if key not in fields:
                    data_json[table_name][key].append("NULL")
                else:
                    data_json[table_name][key].append(values[value_index])
                    value_index = value_index + 1
        with open(self.__current_directory / "data.json", "w") as data_file:
            json.dump(data_json, data_file)

    def delete(self, table_name, where_field="", where_value=""):
        current_table = table.Table()
        current_table.load_from_file(table_name, self.__current_directory)
        if(where_field == "") and (where_value == ""):
            for i in range(1, len(current_table.rows), 1):
                current_table.rows.pop()
        else:
            field_index = current_table.rows[0].index(where_field)
            i = 1
            while i < len(current_table.rows):
                if current_table.rows[i][field_index] == where_value:
                    current_table.rows.pop(i)
                    i = i - 1
                i = i + 1
        current_table.save_to_file(self.__current_directory)
