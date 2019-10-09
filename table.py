import json
import exception


class Table:
    def __init__(self):
        self.types = []
        self.matrix = [[]]
        self.table_name = ""

    def __to_bool(self, value):
        if value.lower() in ("true", "1"):
            return True
        if value.lower() in ("false", "0"):
            return False
        raise Exception('Invalid value for boolean conversion: ' + value)

    def load_from_file(self, table_name, path):
        self.table_name = table_name
        table_meta_file = "table_" + self.table_name + "_meta.json"
        try:
            with open(path / table_meta_file, "r") as meta_file:
                table_meta = json.load(meta_file)
        except OSError:
            raise exception.TableNotExists(table_name)
        with open(path / "data.json", "r") as data_file:
            data_json = json.load(data_file)
        for key, value in table_meta["fields"].items():
            self.types.append(value)
            self.matrix[0].append(key)
        for i in data_json[table_name][self.matrix[0][0]]:
            self.matrix.append([])
        temp_list = []
        for key, value in data_json[table_name].items():
            temp_list.append(value)
        for i in range(len(temp_list)):
            records_index = 1
            for j in range(len(temp_list[i])):
                self.matrix[records_index].append(temp_list[i][j])
                records_index = records_index + 1

    def get_type(self, field):
        return self.types[self.matrix[0].index(field)]

    def get_rows_indexes(self, field, field_value):
        field_type = self.get_type(field)
        self.check_value(field_value, field_type)
        where_index = self.matrix[0].index(field)
        rows_indexes = []
        for i in range(1, len(self.matrix), 1):
            if self.matrix[i][where_index] == field_value:
                rows_indexes.append(i)
        return rows_indexes

    def check_value(self, value, field_type):
        if value == "NULL":
            return
        if field_type.lower() == "int":
            try:
                int(value)
            except ValueError:
                raise exception.InvalidDataType()
        if field_type.lower() == "bool":
            try:
                self.__to_bool(value)
            except Exception:
                raise exception.InvalidDataType()

    def save_to_file(self, path):
        table_meta_file = "table_" + self.table_name + "_meta.json"
        with open(path / table_meta_file, "r") as meta_file:
            table_meta = json.load(meta_file)
        with open(path / "data.json", "r") as data_file:
            data_json = json.load(data_file)
        fields_dict = {}
        for i in range(len(self.matrix[0])):
            fields_dict[self.matrix[0][i]] = self.types[i]
        data_dict = {}
        temp_list = []
        for i in range(len(self.matrix[0])):
            temp_list.append([])
            for j in range(1, len(self.matrix), 1):
                temp_list[i].append(self.matrix[j][i])
        for i in range(len(self.matrix[0])):
            data_dict[self.matrix[0][i]] = temp_list[i]
        table_meta["fields"] = fields_dict
        data_json[self.table_name] = data_dict
        with open(path / table_meta_file, "w") as meta_file:
            json.dump(table_meta, meta_file)
        with open(path / "data.json", "w") as data_file:
            json.dump(data_json, data_file)


class Cursor:
    __row_index = 1

    def __init__(self, selected_table):
        self.__current_table = selected_table
        self.__table_length = len(selected_table.matrix)

    def next(self):
        self.__row_index = self.__row_index + 1
        if self.__row_index == self.__table_length:
            self.close()

    def update(self, fields, values):
        fields_indexes = []
        for i in range(len(fields)):
            if fields[i] in self.__current_table.matrix[0]:
                fields_indexes.append(self.__current_table.matrix[0].index(fields[i]))
        fields_types = []
        for field in fields:
            fields_types.append(self.__current_table.get_type(field))
        for i in range(len(values)):
            self.__current_table.check_value(values[i], fields_types[i])
        i = 0
        for index in fields_indexes:
            self.__current_table.matrix[self.__row_index][index] = values[i]
            i = i + 1

    def delete(self):
        del self.__current_table.matrix[self.__row_index]

    def commit(self):
        return self.__current_table

    def close(self):
        del self
