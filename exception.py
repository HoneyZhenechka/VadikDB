class DBNotExists(Exception):
    def __init__(self, db_name):
        print("Database " + db_name + "not exists!")


class TableAlreadyExists(Exception):
    def __init__(self, table_name):
        print("Table " + table_name + "already exists!")