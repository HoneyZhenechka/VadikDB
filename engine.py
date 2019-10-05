import meta_engine
import exception


class DatabaseManager:
    def __init__(self, db_name):
        self.meta_db = meta_engine.Database(db_name)

    def create_table(self, table_name, fields):
        try:
            self.meta_db.create_table(table_name, fields)
        except exception.TableAlreadyExists:
            return "ERROR"

    def show_create_table(self, table_name):
        try:
            self.meta_db.show_create_table(table_name)
        except exception.TableNotExists:
            return "ERROR"

    def drop_table(self, table_name):
        try:
            self.meta_db.drop_table(table_name)
        except exception.TableNotExists:
            return "ERROR"