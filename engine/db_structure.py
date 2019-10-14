import engine.bin_file
import exception


class Database:
    def __init__(self, file: engine.bin_file.BinFile):
        self.tables_count = 0
        self.signature = "#VDBSignature"
        self.tables = []
        self.file = file

    def write_file(self):
        signature_len = 13
        self.file.write_integer(signature_len, 0, 1)
        self.file.write_str(self.signature, 1, 13)
        self.file.write_integer(self.tables_count, 14, 2)
        for table in self.tables:
            table.write_file()

    def write_table_count(self, count):
        self.file.write_integer(count, 14, 2)
        self.tables_count = count

    def read_file(self):
        signature_len = self.file.read_integer(0, 1)
        signature_result = self.file.read_str(1, signature_len)
        if self.signature != signature_result:
            raise exception.WrongFileFormat()
        for i in range(self.tables_count):
            table_obj = Table(self.file)
            table_obj.index = 16 + i * table_obj.size
            table_obj.read_file()
            self.tables.append(table_obj)

class Table:
    def __init__(self, file: engine.bin_file.BinFile):
        fields_count = 14
        self.rows_length = 0
        self.index = -1
        self.name = ""
        self.file = file
        self.fields = []
        self.types = []
        self.positions = {}
        self.size = 32 + 22 + fields_count * 24

    def write_file(self):
        pass

    def read_file(self):
        pass

    def get_fields(self, fields=[], replace=False):
        return fields


class Page:
    def __init__(self, start_index, table: Table):
        self.table = table
        self.page_size = 512
        self.rows_count = 0
        self.previous_page = 0
        self.next_page = 0
        self.index = start_index

    def get_write_position(self):
        if self.rows_count >= self.page_size:
            return False
        else:
            start_pos = self.index + 12
            new_pos = self.rows_count * self.table.rows_length
            return start_pos + new_pos

    def update_file(self):
        self.table.file.write_integer(self.table.index, self.index, 3)
        self.table.file.write_integer(self.rows_count, self.index + 3, 3)
        self.table.file.write_integer(self.previous_page, self.index + 6, 3)
        self.table.file.write_integer(self.next_page, self.index + 9, 3)

    def write_file(self):
        self.update_file()
        current_page_size = 512 * self.table.rows_length
        self.table.file.write_integer(0, self.index, current_page_size)

    def read_file(self):
        self.rows_count = self.table.file.read_integer(self.index + 3, 3)
        self.previous_page = self.table.file.read_integer(self.index + 6, 3)
        self.next_page = self.table.file.read_integer(self.index + 9, 3)


class Row:
    def __init__(self, table: Table, index=0):
        self.row_index = index
        self.table = table
        self.values = []
        self.row_id = 0
        self.previous_index = 0
        self.next_index = 0
        self.row_available = 0

    def write_info(self):
        row_size = self.row_index + self.table.rows_length
        self.table.file.write_integer(self.row_available, self.row_index, 1)
        self.table.file.write_integer(self.row_id, self.row_index + 1, 3)
        self.table.file.write_integer(self.previous_index, row_size - 3, 3)
        self.table.file.write_integer(self.next_index, row_size - 6, 3)

    def read_info(self):
        row_size = self.row_index + self.table.rows_length
        self.row_available = self.table.file.read_integer(self.row_index, 1)
        self.row_id = self.table.file.read_integer(self.row_index + 1, 3)
        self.previous_index = self.table.file.read_integer(row_size - 3, 3)
        self.next_index = self.table.file.read_integer(row_size - 6, 3)

    def drop_row(self):
        if self.next_index:
            next_row = Row(self.table, self.next_index)
            next_row.read_info()
            next_row.previous_index = self.previous_index
            next_row.write_info()
        if self.previous_index:
            previous_row = Row(self.table, self.previous_index)
            previous_row.read_info()
            previous_row.next_index = self.next_index
            previous_row.write_info()

    def write_row_to_file(self):
        self.write_info()
        for value in self.values:
            value_index = self.table.fields.index(value)
            value_type = self.table.types[value_index]
            value_position = self.table.positions[value]
            self.table.file.write_by_type(value_type, value, self.row_index + value_position, value_type.size)

    def read_row_from_file(self, fields):
        fields = self.table.get_fields(fields, replace=True)
        self.read_info()
        for field, pos in self.table.positions.items():
            if field not in fields:
                continue
            index = self.table.fields.index(field)
            value_type = self.table.types[index]
            self.values = []
            self.values.append(self.table.file.read_by_type(value_type.name, self.row_index + pos, value_type.size))


class Type:
    def __init__(self):
        pass
