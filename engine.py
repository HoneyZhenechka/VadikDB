import meta_engine


class DatabaseManager:
    def __init__(self, db_name):
        self.meta_db = meta_engine.Database(db_name)

    def create_table(self, table_name, fields):
        try:
            self.meta_db.create_table(table_name, fields)
        except Exception:
            return "ERROR"

    def show_create_table(self, table_name):
        try:
            self.meta_db.show_create_table(table_name)
        except Exception:
            return "ERROR"

    def drop_table(self, table_name):
        try:
            self.meta_db.drop_table(table_name)
        except Exception:
            return "ERROR"

    def insert(self, table_name, fields, values):
        try:
            self.meta_db.insert(table_name, fields, values)
        except Exception:
            return "ERROR"

    def delete(self, table_name, where_field="", where_value=""):
        try:
            self.meta_db.delete(table_name, where_field, where_value)
        except Exception:
            return "ERROR"

    def update(self, table_name, fields, values, where_field="", where_value=""):
        try:
            self.meta_db.update(table_name, fields, values, where_field, where_value)
        except Exception:
            return "ERROR"
