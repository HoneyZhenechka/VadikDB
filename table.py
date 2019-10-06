import json


class Table:
    types = []
    rows = [[]]
    table_name = ""

    def __init__(self):
        pass

    def load_from_file(self, table_name, path):
        self.table_name = table_name
        table_meta_file = "table_" + self.table_name + "_meta.json"
        with open(path / table_meta_file, "r") as meta_file:
            table_meta = json.load(meta_file)
        with open(path / "data.json", "r") as data_file:
            data_json = json.load(data_file)
        for key, value in table_meta["fields"].items():
            self.types.append(value)
            self.rows[0].append(key)
        for i in data_json[table_name][self.rows[0][0]]:
            self.rows.append([])
        temp_list = []
        for key, value in data_json[table_name].items():
            temp_list.append(value)
        for i in range(len(temp_list)):
            records_index = 1
            for j in range(len(temp_list[i])):
                self.rows[records_index].append(temp_list[i][j])
                records_index = records_index + 1

    def save_to_file(self, path):
        table_meta_file = "table_" + self.table_name + "_meta.json"
        with open(path / table_meta_file, "r") as meta_file:
            table_meta = json.load(meta_file)
        with open(path / "data.json", "r") as data_file:
            data_json = json.load(data_file)
        fields_dict = {}
        for i in range(len(self.rows[0])):
            fields_dict[self.rows[0][i]] = self.types[i]
        data_dict = {}
        temp_list = []
        for i in range(len(self.rows[0])):
            temp_list.append([])
            for j in range(1, len(self.rows), 1):
                temp_list[i].append(self.rows[j][i])
        for i in range(len(self.rows[0])):
            data_dict[self.rows[0][i]] = temp_list[i]
        table_meta["fields"] = fields_dict
        data_json[self.table_name] = data_dict
        with open(path / table_meta_file, "w") as meta_file:
            json.dump(table_meta, meta_file)
        with open(path / "data.json", "w") as data_file:
            json.dump(data_json, data_file)
