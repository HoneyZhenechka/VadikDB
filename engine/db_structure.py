import engine.bin_file as bin_py
import exception


class Database:
    def __init__(self):
        self.tables_count = 0
        self.signature = "#VDBSignature"
        self.tables = []
        self.file = bin_py.BinFile("zhavoronkov.vdb")

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
            table_obj.index_in_file = 16 + i * table_obj.size
            table_obj.read_file()
            self.tables.append(table_obj)

    def create_table(self, table_name, tables_count, fields, recreate_table=False):
        if recreate_table or not self.file.is_file_exist():
            self.file.open("w+")
            self.file.close()
        self.file.open("r+")
        self.file.seek(0, 2)
        new_table = Table(self.file)
        new_table.index_in_file = 16 + tables_count * new_table.size
        new_table.fill_table_fields(fields)
        new_table.calc_row_size()
        new_table.write_file()
        self.tables.append(new_table)
        self.write_table_count(tables_count + 1)
        table_index = self.tables.index(new_table)
        self.tables[table_index].create_block()


class Table:
    def __init__(self, file: bin_py.BinFile, table_count):
        max_fields_count = 14
        self.size = 32 + 22 + max_fields_count * 24
        self.row_length = 0
        self.removed_rows_count = 0
        self.index_in_file = -1
        self.name = ""
        self.file = file
        self.first_block_index = 0
        self.first_row_index = 0
        self.last_row_index = 0
        self.last_removed_index = 0
        self.fields = []
        self.rows = []
        self.fields_count = 0
        self.row_count = 0
        self.types = []
        self.types_dict = {"bool": Type("bool", 1), "int": Type("int", 4), "str": Type("str", 256)}
        self.positions = {"row_id": 1}

    def create_block(self):
        blocks = self.get_blocks()
        self.file.seek(0, 2)
        previous_index = 0
        block_index = self.file.tell()
        if not len(blocks):
            self.first_block_index = block_index
            self.write_meta_info()
        else:
            last_block = blocks[-1]
            last_block.next_block = block_index
            last_block.update_file()
            previous_index = last_block.index_in_file
        result_block = Block(block_index, self)
        result_block.previous_block = previous_index
        result_block.write_file()
        return result_block

    def get_blocks(self):
        blocks = []
        block_index = self.first_block_index
        while block_index != 0:
            current_block = Block(block_index, self)
            current_block.read_file()
            block_index = current_block.next_block
            blocks.append(current_block)
        return blocks

    def get_write_position(self):
        for block in self.get_blocks():
            position = block.get_write_position()
            if position:
                return position, block
        else:
            new_block = self.create_block()
            return new_block.get_write_position(), new_block

    def write_meta_info(self):
        self.file.write_integer(self.row_count, self.index_in_file + 32, 3)
        self.file.write_integer(self.removed_rows_count, self.index_in_file + 32 + 3, 3)
        self.file.write_integer(self.first_block_index, self.index_in_file + 32 + 6, 3)
        self.file.write_integer(self.first_row_index, self.index_in_file + 32 + 9, 3)
        self.file.write_integer(self.last_row_index, self.index_in_file + 32 + 12, 3)
        self.file.write_integer(self.last_removed_index, self.index_in_file + 32 + 15, 3)

    def write_file(self):
        # Table meta
        self.file.write_str(self.name, self.index_in_file, 32)
        self.write_meta_info()
        self.file.write_integer(self.row_length, self.index_in_file + 32 + 18, 2)
        self.file.write_integer(self.fields_count, self.index_in_file + 32 + 20, 2)
        current_position = self.index_in_file + 32 + 22
        for index, field in enumerate(self.fields):
            self.file.write_str(field + self.types[index].name, current_position, 24)
            current_position += 24
        bytes_count = self.size - (current_position - self.index_in_file)
        self.file.write_str("", current_position, bytes_count)

    def read_file(self):
        # Table meta
        self.name = self.file.read_str(self.index_in_file, 32)
        self.row_count = self.file.read_integer(self.index_in_file + 32, 3)
        self.removed_rows_count = self.file.read_integer(self.index_in_file + 32 + 3, 3)
        self.first_block_index = self.file.read_integer(self.index_in_file + 32 + 6, 3)
        self.first_row_index = self.file.read_integer(self.index_in_file + 32 + 9, 3)
        self.last_row_index = self.file.read_integer(self.index_in_file + 32 + 12, 3)
        self.last_removed_index = self.file.read_integer(self.index_in_file + 32 + 15, 3)
        self.row_length = self.file.read_integer(self.index_in_file + 32 + 18, 2)
        self.fields_count = self.file.read_integer(self.index_in_file + 32 + 20, 2)
        current_position = self.index_in_file + 32 + 22
        field_position = 4
        for i in range(self.fields_count):
            field = self.file.read_str(current_position + i * 24, 21)
            field_type = self.types_dict[self.file.read_str(current_position + i * 24 + 21, 3)]
            self.fields.append(field)
            self.types.append(field_type)
            self.positions[field] = field_position
            field_position += field_type.size

    def show_create(self):
        fields = [
            "'" + v + "' " + self.types[i].name
            for i, v in enumerate(self.fields)
        ]
        result_string = "--------------------------------------------------------\n"
        result_string += "Create table: \n"
        result_string += "CREATE TABLE '" + self.name + "' ("
        result_string += ", ".join(fields) + ")\n"
        result_string += "--------------------------------------------------------"
        return result_string

    def delete(self, rows_indexes=[]):
        if not len(rows_indexes):
            row_index = self.first_row_index
            while row_index != 0:
                current_row = Row(self, row_index)
                current_row.read_info()
                self.delete_row(current_row)
                row_index = current_row.next_index
        else:
            for index in rows_indexes:
                current_row = Row(self, index)
                current_row.read_info()
                self.delete_row(current_row)

    def select(self, fields, rows):
        selected_rows = []
        for row in rows:
            row.select_row(fields)
            selected_rows.append(row)
        return selected_rows

    def update(self, fields, values, rows):
        updated_rows = []
        for row in rows:
            row.select_row(fields)
            row.update_row(fields, values)
            updated_rows.append(row)
        return updated_rows

    def insert(self, fields=[], values=[], insert_index=-1):
        position = self.get_free_row()
        if insert_index == -1:
            insert_index = self.last_row_index
        saved_next_index = 0
        if self.first_row_index == 0:
            self.first_row_index = position
            self.write_meta_info()
        if insert_index != 0:
            previous_row = Row(self, insert_index)
            previous_row.read_info()
            saved_next_index = previous_row.next_index
            previous_row.next = position
            previous_row.write_info()
        if saved_next_index != 0:
            next_row = Row(self, saved_next_index)
            next_row.read_info()
            next_row.previous_index = position
            next_row.write_info()
        new_row = Row(self, position)
        new_row.row_id = self.row_count
        new_row.row_available = 1
        new_row.next = saved_next_index
        new_row.previous_index = insert_index
        new_row.fields_values_dict = {field: values[index] for index, field in enumerate(fields)}
        new_row.write_row_to_file()
        if self.last_row_index == insert_index:
            self.last_row_index = position
            self.write_meta_info()
        self.row_count += 1
        return new_row, position

    def iter_rows(self):
        row_index = self.first_row_index
        while row_index != 0:
            current_row = Row(self, row_index)
            current_row.read_info()
            current_row.read_row_from_file()
            yield current_row

    def get_rows(self):
        for row in self.iter_rows():
            self.rows.append(row)
        return self.rows

    def delete_row(self, row):
        if row.index_in_file == self.first_row_index:
            self.first_row_index = row.next_index
        if row.index_in_file == self.last_row_index:
            self.last_row_index = row.previous_index
        row.read_info()
        row.drop_row()
        row.row_available = 2
        row.previous_index = 0
        row.next_index = self.last_removed_index
        row.write_info()
        if self.last_removed_index:
            previous_row = Row(self, self.last_removed_index)
            previous_row.read_info()
            previous_row.previous_index = row.index_in_file
            previous_row.write_info()
        self.row_count -= 1
        self.removed_rows_count += 1
        self.last_removed_index = row.index_in_file
        self.write_meta_info()

    def get_free_row(self):
        if not self.last_removed_index:
            position, block = self.get_write_position()
            block.rows_count += 1
            block.update_file()
        else:
            removed_row = Row(self, self.last_removed_index)
            removed_row.read_info()
            if removed_row.next_index:
                next_row = Row(self, removed_row.next_index)
                next_row.read_info()
                next_row.previous_index = 0
                next_row.write_info()
            position = self.last_removed_index
            self.last_removed_index = removed_row.next_index
        return position

    def calc_row_size(self):
        self.row_length = 4
        self.positions = {"row_id": 1}
        for index, field in enumerate(self.fields):
            self.positions[field] = self.row_length
            self.row_length += self.types[index].size
        self.row_length += 6

    def fill_table_fields(self, fields_dict={}):
        fields_list = list(fields_dict.keys())
        types_list = list(fields_dict.values())
        if len(types_list) != len(fields_list):
            raise exception.DifferentCount()
        self.types = types_list
        self.fields = fields_list
        for index, type_name in enumerate(self.types):
            if type_name in self.types_dict:
                self.types[index] = self.types_dict[type_name]
            else:
                raise exception.TypeNotExists(type_name)
        self.fields_count = len(self.fields)

    def get_fields(self, fields=[], replace_fields=False):
        is_all = replace_fields and (not fields or type(fields) != list)
        if ("*" in fields) or is_all:
            return self.fields
        result_fields = []
        for field in fields:
            if (field in ["id", "row_id"]) or (field in self.fields):
                result_fields.append(field)
        return result_fields


class Block:
    def __init__(self, start_index, table: Table):
        self.table = table
        self.block_size = 512
        self.rows_count = 0
        self.previous_block = 0
        self.next_block = 0
        self.index_in_file = start_index

    def get_write_position(self):
        if self.rows_count >= self.block_size:
            return False
        else:
            start_pos = self.index_in_file + 12
            new_pos = self.rows_count * self.table.row_length
            return start_pos + new_pos

    def update_file(self):
        self.table.file.write_integer(self.table.index_in_file, self.index_in_file, 3)
        self.table.file.write_integer(self.rows_count, self.index_in_file + 3, 3)
        self.table.file.write_integer(self.previous_block, self.index_in_file + 6, 3)
        self.table.file.write_integer(self.next_block, self.index_in_file + 9, 3)

    def write_file(self):
        self.update_file()
        current_block_size = 512 * self.table.row_length
        self.table.file.write_integer(0, self.index_in_file, current_block_size)

    def read_file(self):
        self.rows_count = self.table.file.read_integer(self.index_in_file + 3, 3)
        self.previous_block = self.table.file.read_integer(self.index_in_file + 6, 3)
        self.next_block = self.table.file.read_integer(self.index_in_file + 9, 3)


class Row:
    def __init__(self, table: Table, index=0):
        self.index_in_file = index
        self.table = table
        self.fields_values_dict = {}
        self.row_id = 0
        self.previous_index = 0
        self.next_index = 0
        self.row_available = 0

    def write_info(self):
        row_size = self.index_in_file + self.table.row_length
        self.table.file.write_integer(self.row_available, self.index_in_file, 1)
        self.table.file.write_integer(self.row_id, self.index_in_file + 1, 3)
        self.table.file.write_integer(self.previous_index, row_size - 3, 3)
        self.table.file.write_integer(self.next_index, row_size - 6, 3)

    def read_info(self):
        row_size = self.index_in_file + self.table.row_length
        self.row_available = self.table.file.read_integer(self.index_in_file, 1)
        self.row_id = self.table.file.read_integer(self.index_in_file + 1, 3)
        self.previous_index = self.table.file.read_integer(row_size - 3, 3)
        self.next_index = self.table.file.read_integer(row_size - 6, 3)

    def select_row(self, fields=[]):
        fields = self.table.get_fields(fields)
        result = {}
        for field in fields:
            if fields in self.fields_values_dict:
                result[field] = self.fields_values_dict[field]
        self.fields_values_dict = result

    def update_row(self, fields=[], values=[]):
        for index, field in enumerate(fields):
            self.fields_values_dict[field] = values[index]
        self.write_row_to_file()

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
        for field in self.fields_values_dict:
            field_index = self.table.fields.index(field)
            field_type = self.table.types[field_index]
            value_position = self.table.positions[field]
            self.table.file.write_by_type(field_type, self.fields_values_dict[field],
                                          self.index_in_file + value_position, field_type.size)

    def read_row_from_file(self, fields=[]):
        fields = self.table.get_fields(fields, True)
        self.read_info()
        for field, pos in self.table.positions.items():
            if field not in fields:
                continue
            index = self.table.fields.index(field)
            field_type = self.table.types[index]
            self.fields_values_dict[field] = self.table.file.read_by_type(field_type.name, self.index_in_file + pos,
                                                                          field_type.size)


class Type:
    def __init__(self, name, size):
        self.name = name
        self.size = size
