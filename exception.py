class TableAlreadyExists(Exception):
    def __init__(self, table_name):
        print("Table " + table_name + " already exists!")


class TableNotExists(Exception):
    def __init__(self, table_name):
        print("Table " + table_name + " not exists!")


class FieldNotExists(Exception):
    def __init__(self, field_name):
        print("Table " + field_name + " not exists!")

class InvalidDataType(Exception):
    def __init__(self):
        print("Invalid Data Type")

class IncorrectSyntax(Exception):
    def __init__(self):
        print("Incorrect Syntax")