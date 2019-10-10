class DBException(Exception):
    error_code = ""


class TableAlreadyExists(DBException):
    def __init__(self, table_name):
        self.error_code = "00"
        print("Error code: " + self.error_code + " -- Table " + table_name + " already exists!")


class TableNotExists(DBException):
    def __init__(self, table_name):
        self.error_code = "01"
        print("Error code: " + self.error_code + " -- Table " + table_name + " not exists!")


class FieldNotExists(DBException):
    def __init__(self, field_name):
        self.error_code = "02"
        print("Error code: " + self.error_code + " -- Table " + field_name + " not exists!")


class InvalidDataType(DBException):
    def __init__(self):
        self.error_code = "03"
        print("Error code: " + self.error_code + " -- Invalid Data Type")


class IncorrectSyntax(DBException):
    def __init__(self):
        self.error_code = "04"
        print("Error code: " + self.error_code + " -- Code Incorrect Syntax")

class DuplicateFields(DBException):
    def __init__(self, fields):
        self.error_code = "05"
        print("Error code: " + self.error_code + " -- Code Incorrect Syntax: " + str(fields))

