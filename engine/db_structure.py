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
        self.size = 32 + 22 + fields_count * 24

    def write_file(self):
        pass

    def read_file(self):
        pass


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
    def __init__(self):
        pass
