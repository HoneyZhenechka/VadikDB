import os
import json
import table
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
                    data_json[table_name][fields[i][0]] = []
                    table_meta["fields"][fields[i][0]] = fields[i][1]  # {name:type}
        with open(self.__current_directory / "data.json", "w") as data_file:
            json.dump(data_json, data_file)
        with open(self.__current_directory / table_meta_file, "w") as meta_file:
            json.dump(table_meta, meta_file)

    def show_create_table(self, table_name):
        current_table = table.Table()
        current_table.load_from_file(table_name, self.__current_directory)
        fields_str = ", ".join(current_table.table_matrix[0])
        query = (
                "CREATE TABLE " + current_table.table_name + " (\n" +
                "\t\t\t\t\t" + fields_str + "\n\t\t\t\t);"
        )
        print(
                "===================================================\n" +
                '\t\tTable:\t"' + current_table.table_name + '"\n' +
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
        current_table = table.Table()
        current_table.load_from_file(table_name, self.__current_directory)
        values_types = []
        if len(fields) == 0:
            values_types = current_table.types
        else:
            for field in fields:
                if field not in current_table.table_matrix[0]:
                    raise exception.FieldNotExists(field)
                values_types.append(current_table.get_type(field))
        for i in range(len(values)):
            current_table.check_value(values[i], values_types[i])
        if len(fields) == 0:
            current_table.table_matrix.append(values)
        else:
            inserted_index = 0
            inserted_values = []
            for i in range(len(current_table.table_matrix[0])):
                if current_table.table_matrix[0][i] not in fields:
                    inserted_values.append("NULL")
                else:
                    inserted_values.append(values[inserted_index])
                    inserted_index = inserted_index + 1
            current_table.table_matrix.append(inserted_values)
        current_table.save_to_file(self.__current_directory)

    def delete(self, table_name, where_field="", where_value=""):
        current_table = table.Table()
        current_table.load_from_file(table_name, self.__current_directory)
        if(where_field == "") and (where_value == ""):
            for i in range(1, len(current_table.table_matrix), 1):
                current_table.table_matrix.pop()
        else:
            field_index = current_table.table_matrix[0].index(where_field)
            i = 1
            while i < len(current_table.table_matrix):
                if current_table.table_matrix[i][field_index] == where_value:
                    current_table.table_matrix.pop(i)
                    i = i - 1
                i = i + 1
        current_table.save_to_file(self.__current_directory)
