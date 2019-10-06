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
        for field, row in self.data_json[self.table_name].items():
            self.records.append(row)

    def get_rows(self, fields = [], expr = ""):
        pass