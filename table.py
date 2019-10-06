import json


class Table:
    types = []
    records = [[]]

    def __init__(self, table_name, path):
        self.table_name = table_name
        table_meta_file = "table_" + self.table_name + "_meta.json"
        with open(path / table_meta_file, "r") as meta_file:
            self.table_meta = json.load(meta_file)
        with open(path / "data.json", "r") as data_file:
            self.data_json = json.load(data_file)
        for key, value in self.table_meta["fields"].items():
            self.types.append(value)
            self.records[0].append(key)
        for i in self.data_json[table_name][self.records[0][0]]:
            self.records.append([])
        temp_list = []
        for key, value in self.data_json[table_name].items():
            temp_list.append(value)
        for i in range(len(temp_list)):
            records_index = 1
            for j in range(len(temp_list[i])):
                self.records[records_index].append(temp_list[i][j])
                records_index = records_index + 1