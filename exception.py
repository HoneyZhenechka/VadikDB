class DBException(Exception):
    error_code = ""


class WrongFileFormat(DBException):
    def __init__(self):
        self.error_code = "00"
        print("Error code: " + self.error_code + " -- Wrong file format!")


class TableAlreadyExists(DBException):
    def __init__(self, table_name):
        self.error_code = "01"
        print("Error code: " + self.error_code + " -- Table " + table_name + " already exists!")


class TableNotExists(DBException):
    def __init__(self, table_name):
        self.error_code = "02"
        print("Error code: " + self.error_code + " -- Table " + table_name + " not exists!")


class FieldNotExists(DBException):
    def __init__(self, field_name):
        self.error_code = "03"
        print("Error code: " + self.error_code + " -- Field " + field_name + " not exists!")


class InvalidDataType(DBException):
    def __init__(self):
        self.error_code = "04"
        print("Error code: " + self.error_code + " -- Invalid Data Type")


class IncorrectSyntax(DBException):
    def __init__(self, pos):
        self.error_code = "05"
        print("Error code: " + self.error_code + " -- Code Incorrect Syntax")
        print("The position of error: " + str(pos))


class DuplicateFields(DBException):
    def __init__(self, field):
        self.error_code = "06"
        print("Error code: " + self.error_code + " -- Duplicate field: " + str(field))


class ValueNotExists(DBException):
    def __init__(self, value):
        self.error_code = "07"
        print("Error code: " + self.error_code + " -- Value not exists: " + str(value))


class WrongFieldType(DBException):
    def __init__(self, field):
        self.error_code = "08"
        print("Error code: " + self.error_code + " -- Wrong Field Type: " + str(field[0]) + ":" + str(field[1]))


class DifferentCount(DBException):
    def __init__(self):
        self.error_code = "09"
        print("Error code: " + self.error_code + " -- Different count fields and types!")


class TypeNotExists(DBException):
    def __init__(self, type_name):
        self.error_code = "10"
        print("Error code: " + self.error_code + " -- Type not exists: " + str(type_name))
