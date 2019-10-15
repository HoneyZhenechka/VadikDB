import engine.bin_file
import exception


class Database:
    def __init__(self):
        self.tables_count = 0
        self.signature = "#VDBSignature"
        self.tables = []
        self.file = engine.bin_file.BinFile("zhavoronkov.vdb")

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
        max_fields_count = 14
        self.row_length = 0
        self.removed_rows_count = 0
        self.index = -1
        self.name = ""
        self.file = file
        self.first_page_index = 0
        self.first_element_index = 0
        self.last_element_index = 0
        self.last_removed_index = 0
        self.fields = []
        self.fields_count = 0
        self.row_count = 0
        self.types = []
        self.types_dict = {"bool": Type("bool", 1), "int": Type("int", 4), "str": Type("str", 256)}
        self.positions = {}
        self.size = 32 + 22 + max_fields_count * 24

    def create_page(self):
        pages = self.get_pages()
        self.file.seek(0, 2)
        previous_index = 0
        page_index = self.file.tell()
        if not len(pages):
            self.first_page_index = page_index
            self.update_pages_info()
        else:
            last_page = pages[-1]
            last_page.next_page = page_index
            last_page.update_file()
            previous_index = last_page.index
        result_page = Page(page_index, self)
        result_page.previous_page = previous_index
        result_page.write_file()
        return result_page

    def get_pages(self):
        pages = []
        page_index = self.first_page_index
        while page_index != 0:
            current_page = Page(page_index, self)
            current_page.read_file()
            page_index = current_page.next_page
            pages.append(current_page)
        return pages

    def update_pages_info(self):
        pass

    def get_write_position(self):
        for page in self.get_pages():
            position = page.get_write_position()
            if position:
                return position, page
        else:
            new_page = self.create_page()
            return new_page.get_write_position(), new_page

    def write_file(self):
        self.file.write_str(self.name, self.index, 32)
        self.update_pages_info()
        self.file.write_integer(self.row_length, self.index + 32 + 18, 2)
        self.file.write_integer(self.fields_count, self.index + 32 + 20, 2)
        current_position = self.index + 32 + 22
        for index, field in enumerate(self.fields):
            self.file.write_str(field + self.types[index].name, current_position, 24)
            current_position += 24
        bytes_count = self.size - (current_position - self.index)
        self.file.write_str("", current_position, bytes_count)

    def read_file(self):
        self.name = self.file.read_str(self.index, 32)
        self.row_count = self.file.read_integer(self.index + 32, 3)
        self.removed_rows_count = self.file.read_integer(self.index + 32 + 3, 3)
        self.first_page_index = self.file.read_integer(self.index + 32 + 6, 3)
        self.first_element_index = self.file.read_integer(self.index + 32 + 9, 3)
        self.last_element_index = self.file.read_integer(self.index + 32 + 12, 3)
        self.last_removed_index = self.file.read_integer(self.index + 32 + 15, 3)
        self.row_length = self.file.read_integer(self.index + 32 + 18, 2)
        self.fields_count = self.file.read_integer(self.index + 32 + 20, 2)
        current_position = self.index + 32 + 22
        field_position = 4
        for i in range(self.fields_count):
            field = self.file.read_str(current_position + i * 24, 21)
            field_type = self.types_dict[self.file.read_str(current_position + i * 24 + 21, 3)]
            self.fields.append(field)
            self.types.append(field_type)
            self.positions[field] = field_position
            field_position += field_type.size

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
            new_pos = self.rows_count * self.table.row_length
            return start_pos + new_pos

    def update_file(self):
        self.table.file.write_integer(self.table.index, self.index, 3)
        self.table.file.write_integer(self.rows_count, self.index + 3, 3)
        self.table.file.write_integer(self.previous_page, self.index + 6, 3)
        self.table.file.write_integer(self.next_page, self.index + 9, 3)

    def write_file(self):
        self.update_file()
        current_page_size = 512 * self.table.row_length
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
        row_size = self.row_index + self.table.row_length
        self.table.file.write_integer(self.row_available, self.row_index, 1)
        self.table.file.write_integer(self.row_id, self.row_index + 1, 3)
        self.table.file.write_integer(self.previous_index, row_size - 3, 3)
        self.table.file.write_integer(self.next_index, row_size - 6, 3)

    def read_info(self):
        row_size = self.row_index + self.table.row_length
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
    def __init__(self, name, size):
        self.name = name
        self.size = size


