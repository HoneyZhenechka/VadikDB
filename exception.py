class TableAlreadyExists(Exception):
    def __init__(self, table_name):
        print("Table " + table_name + " already exists!")


class TableNotExists(Exception):
    def __init__(self, table_name):
        print("Table " + table_name + " not exists!")


class IncorrectSyntax(Exception):
    def __init__(self):
        print("Incorrect Syntax")