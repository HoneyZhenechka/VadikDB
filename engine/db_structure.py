import engine.bin_file


class Database:
    def __init__(self):
        pass


class Table:
    def __init__(self, name, file: engine.bin_file.BinFile):
        self.rows_length = 0
        self.index = -1
        self.name = name
        self.file = file


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
        self.table.file.write_integer(self.next_page, sself.index + 9, 3)

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
