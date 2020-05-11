
class DBExceptionForClient(Exception):

    def __init__(self):
        self.error_code = ""

    def DBFileNotExists(self):
        self.error_code = "00"
        return "Error code: " + self.error_code + " -- DB file not exists!"

    def WrongFileFormat(self):
        self.error_code = "01"
        return "Error code: " + self.error_code + " -- Wrong file format!"

    def WrongSignature(self):
        self.error_code = "02"
        return "Error code: " + self.error_code + " -- Wrong signature!"

    def TableAlreadyExists(self, table_name):
        self.error_code = "03"
        return "Error code: " + self.error_code + " -- Table " + table_name + " already exists!"

    def TableNotExists(self, table_name):
        self.error_code = "04"
        return "Error code: " + self.error_code + " -- Table " + table_name + " not exists!"

    def FieldNotExists(self, field_name):
        self.error_code = "05"
        return "Error code: " + self.error_code + " -- Field " + field_name + " not exists!"

    def InvalidDataType(self):
        self.error_code = "06"
        return  "Error code: " + self.error_code + " -- Invalid Data Type"

    def IncorrectSyntax(self, pos, token):
        self.error_code = "07"
        return "Error code: " + self.error_code + " -- Code Incorrect Syntax" + "\n" \
               + "The position of error: " + str(pos) + "\nToken: " + str(token)

    def DuplicateFields(self, field_name):
        self.error_code = "08"
        return "Error code: " + self.error_code + " -- Duplicate field: " + str(field_name)

    def ValueNotExists(self, value):
        self.error_code = "09"
        return "Error code: " + self.error_code + " -- Value not exists: " + str(value)

    def WrongFieldType(self, field):
        self.error_code = "10"
        return "Error code: " + self.error_code + " -- Wrong Field Type: " + str(field[0]) + ":" + str(field[1])

    def DifferentCount(self):
        self.error_code = "11"
        return "Error code: " + self.error_code + " -- Different count fields and types!"

    def TypeNotExists(self, type_name):
        self.error_code = "12"
        return "Error code: " + self.error_code + " -- Type not exists: " + str(type_name)

    def DifferentNumberOfColumns(self):
        self.error_code = "13"
        return "Error code: " + self.error_code + " -- The used SELECT statements have a different number of columns"

    def NoTableSpecified(self, field):
        self.error_code = "14"
        return "Error code: " + self.error_code + " -- The field: " + field + " does not have a table"

    def TransactionNotDefined(self, user_index):
        self.error_code = "15"
        return "Error code: " + self.error_code + " -- User(user_index: " + str(user_index) + ") not defined transaction"

    def IndexAlreadyExists(self, table_name):
        self.error_code = "16"
        return "Error code: " + self.error_code + " -- Index " + table_name + " already exists!"

    def IndexNotExists(self, table_name):
        self.error_code = "17"
        return "Error code: " + self.error_code + " -- Index " + table_name + " not exists!"
