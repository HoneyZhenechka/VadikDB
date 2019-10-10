import os
import json
import table
import exception
from pathlib import Path


class Database:
    __current_directory = ""

    def __check_duplicate_fields(self, fields):
        set_fields = set(fields)
        if len(set_fields) != len(fields):
            raise exception.DuplicateFields(fields)

    def __print_matrix(self, s):
        for j in range(len(s[0])):
            print("%s " % s[0][j], end="")
        print()
        for j in range(len(s[0])):
            print("------", end="")
        print()
        for i in range(1, len(s), 1):
            for j in range(len(s[1])):
                print("%s " % (s[i][j]), end="")
            print()

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
        fields_str = ", ".join(current_table.matrix[0])
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
            self.__check_duplicate_fields(fields)
            for field in fields:
                if field not in current_table.matrix[0]:
                    raise exception.FieldNotExists(field)
                values_types.append(current_table.get_type(field))
        for i in range(len(values)):
            current_table.check_value(values[i], values_types[i])
        if len(fields) == 0:
            current_table.matrix.append(values)
        else:
            inserted_index = 0
            inserted_values = []
            for i in range(len(current_table.matrix[0])):
                if current_table.matrix[0][i] not in fields:
                    inserted_values.append("NULL")
                else:
                    inserted_values.append(values[inserted_index])
                    inserted_index = inserted_index + 1
            current_table.matrix.append(inserted_values)
        current_table.save_to_file(self.__current_directory)

    def delete(self, table_name, where_field="", where_value=""):
        current_table = table.Table()
        current_table.load_from_file(table_name, self.__current_directory)
        if(where_field == "") and (where_value == ""):
            for i in range(1, len(current_table.matrix), 1):
                current_table.matrix.pop()
        else:
            try:
                field_index = current_table.matrix[0].index(where_field)
            except ValueError:
                raise exception.FieldNotExists
            where_type = current_table.get_type(where_field)
            current_table.check_value(where_value, where_type)
            i = 1
            value_exists = False
            while i < len(current_table.matrix):
                if current_table.matrix[i][field_index] == where_value:
                    value_exists = True
                    current_table.matrix.pop(i)
                    i = i - 1
                i = i + 1
        if not value_exists:
            raise exception.FieldNotExists(where_value)
        current_table.save_to_file(self.__current_directory)

    def update(self, table_name, fields, values, where_field="", where_value=""):
        current_table = table.Table()
        current_table.load_from_file(table_name, self.__current_directory)
        self.__check_duplicate_fields(fields)
        fields_types = []
        for i in range(len(fields)):
            fields_types.append(current_table.get_type(fields[i]))
        for i in range(len(values)):
            current_table.check_value(values[i], fields_types[i])
        fields_indexes = []
        for field in fields:
            fields_indexes.append(current_table.matrix[0].index(field))
        if(where_field == "") and (where_value == ""):
            for i in range(1, len(current_table.matrix), 1):
                for j in range(len(fields_indexes)):
                    current_table.matrix[i][fields_indexes[j]] = values[j]
        else:
            rows_indexes = current_table.get_rows_indexes(where_field, where_value)
            for index in rows_indexes:
                for j in range(len(fields_indexes)):
                    current_table.matrix[index][fields_indexes[j]] = values[j]
        current_table.save_to_file(self.__current_directory)

    def select(self, table_name, fields=[], for_print=False, all_rows=False, where_field="", where_value=""):
        current_table = table.Table()
        current_table.load_from_file(table_name, self.__current_directory)
        result_table = table.Table()
        if all_rows:
            result_table.types = current_table.types
            if (where_field == "") and (where_value == ""):
                result_table.matrix = current_table.matrix
            else:
                result_table.matrix[0] = current_table.matrix[0]
                where_type = current_table.get_type(where_field)
                current_table.check_value(where_value, where_type)
                rows_indexes = current_table.get_rows_indexes(where_field, where_value)
                result_index = 1
                for index in rows_indexes:
                    result_table.matrix.append([])
                    for j in range(len(current_table.matrix[0])):
                        result_table.matrix[result_index].append(current_table.matrix[index][j])
                    result_index = result_index + 1
        else:
            self.__check_duplicate_fields(fields)
            for field in fields:
                if field not in current_table.matrix[0]:
                    raise exception.FieldNotExists(field)
            fields_indexes = []
            for i in range(len(fields)):
                if fields[i] in current_table.matrix[0]:
                    fields_indexes.append(current_table.matrix[0].index(fields[i]))
            for index in fields_indexes:
                result_table.types.append(current_table.types[index])
            result_table.matrix[0] = fields
            if(where_field == "") and (where_value == ""):
                for i in range(1, len(current_table.matrix), 1):
                    result_table.matrix.append([])
                    for j in range(len(fields_indexes)):
                        result_table.matrix[i].append(current_table.matrix[i][fields_indexes[j]])
            else:
                rows_indexes = current_table.get_rows_indexes(where_field, where_value)
                result_matrix = []
                row_index = 0
                while row_index < len(rows_indexes):
                    column_index = 0
                    temp_list = []
                    while column_index < len(fields_indexes):
                        temp_list.append(current_table.matrix[rows_indexes[row_index]][fields_indexes[column_index]])
                        column_index = column_index + 1
                    result_matrix.append(temp_list)
                    row_index = row_index + 1
                for result_list in result_matrix:
                    result_table.matrix.append(result_list)
        if for_print:
            self.__print_matrix(result_table.matrix)
        return result_table

    def get_cursor(self, selected_table):
        return table.Cursor(selected_table)
