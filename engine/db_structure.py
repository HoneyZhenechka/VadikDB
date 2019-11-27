import engine.bin_file as bin_py
import threading
import typing
import exception
import os

threading_lock = threading.Lock()


class Database:
    def __init__(self, create_default_file: bool = True, db_filename: str = ""):
        self.tables_count = 0
        self.signature = "#VDBSignature"
        self.tables = []
        self.filename = ""
        self.file = None
        if create_default_file:
            self.filename = "zhavoronkov.vdb"
            self.file = bin_py.BinFile(self.filename)
            self.file.open("w+")
            self.write_file()
            self.write_table_count(self.tables_count)
            self.file.close()
        elif db_filename != "":
            self.filename = db_filename
            if os.path.isfile(self.filename):
                self.connect_to_db(self.filename)
            else:
                self.file = bin_py.BinFile(self.filename)
                self.file.open("w+")
                self.write_file()
                self.write_table_count(self.tables_count)
                self.file.close()

    def __check_journal(self) -> bool:
        if os.path.isfile("journal.log"):
            return True
        return False

    def __update_table_metadata(self, table) -> typing.NoReturn:
        table.last_block_index = table.get_blocks()[-1].index_in_file
        table.last_row_index = table.get_rows(False)[-1].index_in_file
        table.row_count = len(table.get_rows(False))
        table.write_meta_info()

    def wide_rollback(self) -> typing.NoReturn:
        rollback_obj = RollbackLog(self.file, 0)
        rollback_obj.open_file()
        journal_file_size = rollback_obj.file.read_integer(0, 16)
        if journal_file_size < os.stat(self.filename).st_size:
            os.truncate(self.filename, journal_file_size)
        rollback_obj.get_blocks()
        rollback_obj.restore_blocks()
        rollback_obj.close_file()
        os.remove("journal.log")
        for table in self.tables:
            self.__update_table_metadata(table)

    def write_file(self) -> typing.NoReturn:
        signature_len = 13
        self.file.write_integer(signature_len, 0, 1)
        self.file.write_str(self.signature, 1, 13)
        self.file.write_integer(self.tables_count, 14, 2)
        for table in self.tables:
            table.write_file()

    def write_table_count(self, count: int) -> typing.NoReturn:
        self.file.write_integer(count, 14, 2)
        self.tables_count = count

    def read_file(self) -> typing.NoReturn:
        signature_len = self.file.read_integer(0, 1)
        signature_result = self.file.read_str(1, signature_len)
        if self.signature != signature_result:
            raise exception.WrongFileFormat()
        self.tables_count = self.file.read_integer(14, 2)
        for i in range(self.tables_count):
            table_obj = Table(self.file)
            table_obj.index_in_file = 16 + i * table_obj.size
            table_obj.read_file()
            self.tables.append(table_obj)

    def connect_to_db(self, filename: str) -> typing.NoReturn:
        file = bin_py.BinFile(filename)
        if not file.is_file_exist():
            raise exception.DBFileNotExists()
        self.file = file
        self.file.open("r+")
        signature_len = self.file.read_integer(0, 1)
        signature_str = self.file.read_str(1, signature_len)
        if signature_str != self.signature:
            raise exception.WrongSignature()
        self.read_file()
        if self.__check_journal():
            self.wide_rollback()

    def create_table(self, table_name: str, tables_count: int, fields: typing.Dict) -> typing.List:
        self.file.open("r+")
        self.file.seek(0, 2)
        new_table = Table(self.file)
        new_table.name = table_name
        new_table.index_in_file = 16 + tables_count * new_table.size
        new_table.fill_table_fields(fields)
        new_table.calc_row_size()
        new_table.write_file()
        self.tables.append(new_table)
        self.write_table_count(tables_count + 1)
        table_index = self.tables.index(new_table)
        self.tables[table_index].create_block()
        return self.tables[table_index]

    def close_db(self) -> typing.NoReturn:
        self.file.close()
        self.file = None
        self.tables_count = 0
        self.tables = []


class Table:
    def __init__(self, file: bin_py.BinFile):
        max_fields_count = 14
        self.size = 32 + 22 + max_fields_count * 24
        self.row_length = 0
        self.index_in_file = -1
        self.name = ""
        self.file = file
        self.first_block_index = 0
        self.current_block_index = 0
        self.last_block_index = 0
        self.first_row_index = 0
        self.last_row_index = 0
        self.last_removed_index = 0
        self.fields = []
        self.rows = []
        self.fields_count = 0
        self.row_count = 0
        self.types = []
        self.types_dict = {"bool": Type("bool", 1), "int": Type("int", 4), "float": Type("float", 8),
                           "str": Type("str", 256)}
        self.positions = {"row_id": 1}
        self.is_transaction = False
        self.transaction_obj = None

    def __eq__(self, other) -> bool:
        if not isinstance(other, Table):
            return NotImplemented
        return (self.name, self.fields, self.fields_count, self.types, self.positions, self.row_length) == \
               (other.name, other.fields, other.fields_count, other.types, other.positions, other.row_length)

    def __iter_blocks(self) -> typing.Iterable:
        current_index = self.first_block_index
        while current_index != 0:
            current_block = Block(current_index, self)
            current_block.read_file()
            current_index = current_block.next_block
            yield current_block

    def __create_local_rollback_journal(self):
        rollback_obj = RollbackLog(self.file, self.row_length)
        rollback_obj.create_file()
        return rollback_obj

    def __close_local_rollback_journal(self, rollback_obj) -> typing.NoReturn:
        rollback_obj.close_file()
        os.remove("journal.log")

    def start_transaction(self) -> typing.NoReturn:
        self.is_transaction = True
        self.transaction_obj = Transaction(self)
        self.transaction_obj.rollback_journal.create_file()

    def end_transaction(self, is_rollback: bool = False) -> typing.NoReturn:
        self.transaction_obj.commit()
        self.transaction_obj.rollback_journal.file.close()
        self.transaction_obj = None
        self.is_transaction = False
        if not is_rollback:
            os.remove("journal.log")

    def rollback_transaction(self) -> typing.NoReturn:
        rollback_obj = Transaction(self)
        rollback_obj.rollback()

    def create_block(self):
        self.file.seek(0, 2)
        previous_index = 0
        block_index = self.file.tell()
        if not self.last_block_index:
            self.first_block_index = block_index
            self.write_meta_info()
        else:
            last_block = Block(self.last_block_index, self)
            last_block.next_block = block_index
            last_block.update_file()
            previous_index = last_block.index_in_file
        self.last_block_index = block_index
        result_block = Block(block_index, self)
        result_block.previous_block = previous_index
        result_block.write_file()
        return result_block

    def get_blocks(self) -> typing.List:
        blocks = []
        for block in self.__iter_blocks():
            blocks.append(block)
        return blocks

    def get_write_position(self):
        for block in self.get_blocks():
            position = block.get_write_position()
            if position:
                self.current_block_index = block.index_in_file
                return position, block
        else:
            new_block = self.create_block()
            self.current_block_index = new_block.index_in_file
            return new_block.get_write_position(), new_block

    def get_block_index_for_row(self, row) -> int:
        if len(self.get_blocks()):
            return self.first_block_index
        else:
            for block in self.get_blocks():
                if block.next_block > row.index_in_file > block.previous_block:
                    return block.index_in_file

    def write_meta_info(self) -> typing.NoReturn:
        self.file.write_integer(self.row_count, self.index_in_file + 32, 3)
        self.file.write_integer(self.first_block_index, self.index_in_file + 32 + 3, 3)
        self.file.write_integer(self.last_block_index, self.index_in_file + 32 + 6, 3)
        self.file.write_integer(self.first_row_index, self.index_in_file + 32 + 9, 3)
        self.file.write_integer(self.last_row_index, self.index_in_file + 32 + 12, 3)
        self.file.write_integer(self.last_removed_index, self.index_in_file + 32 + 15, 3)

    def write_file(self) -> typing.NoReturn:
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
        self.file.seek(current_position, 0)

    def read_file(self) -> typing.NoReturn:
        # Table meta
        self.name = self.file.read_str(self.index_in_file, 32)
        self.row_count = self.file.read_integer(self.index_in_file + 32, 3)
        self.first_block_index = self.file.read_integer(self.index_in_file + 32 + 6, 3)
        self.last_block_index = self.file.read_integer(self.index_in_file + 32 + 6, 3)
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

    def show_create(self) -> str:
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

    def __delete_row_and_add_block(self, row) -> typing.NoReturn:
        if self.is_transaction:
            command = DBMethod(self.__delete_row, row)
            self.transaction_obj.append(command)
            self.transaction_obj.rollback_journal.add_block(self.get_block_index_for_row(row))
        if not self.is_transaction:
            rollback_obj = self.__create_local_rollback_journal()
            rollback_obj.add_block(self.get_block_index_for_row(row))
            self.__delete_row(row)
            self.__close_local_rollback_journal(rollback_obj)

    def delete(self, rows_indexes: typing.Tuple[int] = ()):
        if not len(rows_indexes):
            row_index = self.first_row_index
            while row_index != 0:
                current_row = Row(self, row_index)
                current_row.read_info()
                self.__delete_row_and_add_block(current_row)
                row_index = current_row.next_index
        else:
            for index in rows_indexes:
                current_row = Row(self, index)
                current_row.read_info()
                self.__delete_row_and_add_block(current_row)

    def select(self, fields: typing.Tuple[str], rows: typing.Tuple) -> typing.List:
        selected_rows = []
        for row in rows:
            row.select_row(fields)
            selected_rows.append(row)
        return selected_rows

    def update(self, fields: typing.Tuple[str], values: typing.Tuple, rows: typing.Tuple) -> typing.NoReturn:
        threading_lock.acquire()
        for row in rows:
            if self.is_transaction:
                first_update_command = DBMethod(row.select_row, fields)
                self.transaction_obj.append(first_update_command)
                second_update_command = DBMethod(row.update_row, fields, values)
                self.transaction_obj.append(second_update_command)
                self.transaction_obj.rollback_journal.add_block(self.get_block_index_for_row(row))
            else:
                rollback_obj = self.__create_local_rollback_journal()
                rollback_obj.add_block(self.get_block_index_for_row(row))
                row.select_row(fields)
                row.update_row(fields, values)
                self.__close_local_rollback_journal(rollback_obj)
        threading_lock.release()

    def insert(self, fields: typing.Tuple[str] = (), values: typing.Tuple = (), insert_index: int = -1,
               test_rollback: bool = False) -> typing.NoReturn:
        if self.is_transaction:
            method = DBMethod(self.__insert, fields, values, insert_index, test_rollback)
            self.transaction_obj.append(method)
        else:
            self.__insert(fields, values, insert_index, test_rollback)

    def __insert(self, fields: typing.Tuple[str] = (), values: typing.Tuple = (), insert_index: int = -1,
                 test_rollback: bool = False) -> typing.NoReturn:
        local_rollback_obj = None
        position = self.get_free_row()
        if not self.is_transaction:
            local_rollback_obj = self.__create_local_rollback_journal()
            local_rollback_obj.add_block(self.get_block_index_for_row(self.current_block_index))
        if self.is_transaction:
            self.transaction_obj.rollback_journal.add_block(self.current_block_index)
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
            previous_row.next_index = position
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
        if test_rollback:
            new_row.write_row_to_file(True)
            local_rollback_obj.file.close()
            return
        new_row.write_row_to_file()
        if self.last_row_index == insert_index:
            self.last_row_index = position
            self.write_meta_info()
        self.row_count += 1
        if not self.is_transaction:
            self.__close_local_rollback_journal(local_rollback_obj)

    def __iter_rows(self) -> typing.Iterable:
        row_index = self.first_row_index
        while row_index != 0:
            current_row = Row(self, row_index)
            current_row.read_info()
            current_row.read_row_from_file()
            row_index = current_row.next_index
            yield current_row

    def get_rows(self, check: bool = True) -> typing.List:
        new_rows_list = []
        for row in self.__iter_rows():
            new_rows_list.append(row)
        if check:
            self.rows = new_rows_list
        return new_rows_list

    def __delete_row(self, row) -> typing.NoReturn:
        if row.index_in_file == self.first_row_index:
            self.first_row_index = row.next_index
        if row.index_in_file == self.last_row_index:
            self.last_row_index = row.previous_index
        row.read_info()
        row.drop_row()
        row.row_available = 2
        row.previous_index = 0
        row.next_index = 0
        row.write_info()
        if self.last_removed_index:
            previous_row = Row(self, self.last_removed_index)
            previous_row.read_info()
            previous_row.previous_index = row.index_in_file
            previous_row.write_info()
        self.row_count -= 1
        self.last_removed_index = row.index_in_file
        self.write_meta_info()

    def get_free_row(self) -> int:
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

    def calc_row_size(self) -> typing.NoReturn:
        self.row_length = 4
        self.positions = {"row_id": 1}
        for index, field in enumerate(self.fields):
            self.positions[field] = self.row_length
            self.row_length += self.types[index].size
        self.row_length += 6

    def fill_table_fields(self, fields_dict: typing.Dict = {}) -> typing.NoReturn:
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

    def get_fields(self, fields: typing.Tuple[str] = (), replace_fields: bool = False) -> typing.List[str]:
        is_all = replace_fields and (not fields or type(fields) != list)
        if ("*" in fields) or is_all:
            return self.fields
        result_fields = []
        for field in fields:
            if (field in ["id", "row_id"]) or (field in self.fields):
                result_fields.append(field)
        return result_fields


class Block:
    def __init__(self, start_index: int, table: Table) -> typing.NoReturn:
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

    def update_file(self) -> typing.NoReturn:
        self.table.file.write_integer(self.table.index_in_file, self.index_in_file, 3)
        self.table.file.write_integer(self.rows_count, self.index_in_file + 3, 3)
        self.table.file.write_integer(self.previous_block, self.index_in_file + 6, 3)
        self.table.file.write_integer(self.next_block, self.index_in_file + 9, 3)

    def write_file(self) -> typing.NoReturn:
        self.update_file()
        current_block_size = 512 * self.table.row_length
        self.table.file.write_integer(0, self.index_in_file, current_block_size)

    def read_file(self) -> typing.NoReturn:
        self.rows_count = self.table.file.read_integer(self.index_in_file + 3, 3)
        self.previous_block = self.table.file.read_integer(self.index_in_file + 6, 3)
        self.next_block = self.table.file.read_integer(self.index_in_file + 9, 3)


class Row:
    def __init__(self, table: Table, index: int = 0):
        self.index_in_file = index
        self.table = table
        self.fields_values_dict = {}
        self.previous_index = 0
        self.next_index = 0
        self.row_available = 0

    def write_info(self) -> typing.NoReturn:
        row_size = self.index_in_file + self.table.row_length
        self.table.file.write_integer(self.row_available, self.index_in_file, 1)
        self.table.file.write_integer(self.previous_index, row_size - 3, 3)
        self.table.file.write_integer(self.next_index, row_size - 6, 3)

    def read_info(self) -> typing.NoReturn:
        row_size = self.index_in_file + self.table.row_length
        self.row_available = self.table.file.read_integer(self.index_in_file, 1)
        self.previous_index = self.table.file.read_integer(row_size - 3, 3)
        self.next_index = self.table.file.read_integer(row_size - 6, 3)

    def select_row(self, fields: typing.Tuple[str] = ()) -> typing.NoReturn:
        fields = self.table.get_fields(fields)
        result = {}
        for field in fields:
            if field in self.fields_values_dict:
                result[field] = self.fields_values_dict[field]
        self.fields_values_dict = result

    def update_row(self, fields: typing.Tuple[str] = (), values: typing.Tuple = ()) -> typing.NoReturn:
        for index, field in enumerate(fields):
            self.fields_values_dict[field] = values[index]
        self.write_row_to_file()

    def drop_row(self) -> typing.NoReturn:
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

    def write_row_to_file(self, is_test: bool = False):
        self.write_info()
        for field in self.fields_values_dict:
            field_index = self.table.fields.index(field)
            field_type = self.table.types[field_index]
            value_position = self.table.positions[field]
            if is_test and field_index:
                return
            self.table.file.write_by_type(field_type.name, self.fields_values_dict[field],
                                          self.index_in_file + value_position, field_type.size)

    def read_row_from_file(self, fields: typing.Tuple[str] = ()) -> typing.NoReturn:
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
    def __init__(self, name: str, size: int):
        self.name = name
        self.size = size

    def __eq__(self, other) -> bool:
        if not isinstance(other, Type):
            return NotImplemented
        return self.__dict__ == other.__dict__


class DBMethod:
    def __init__(self, method, *args):
        self.method = method
        self.args = args

    def __call__(self):
        result = self.method(*self.args)
        self.method = None
        self.args = None
        return result


class Transaction:
    def __init__(self, table: Table):
        self.commands = []
        self.table = table
        self.rollback_journal = RollbackLog(self.table.file, self.table.row_length)

    def remove(self, command: DBMethod) -> typing.NoReturn:
        self.commands.remove(command)

    def append(self, command: DBMethod) -> typing.NoReturn:
        self.commands.append(command)

    def __iter__(self) -> typing.Iterable:
        for command in self.commands:
            yield command

    def commit(self) -> typing.NoReturn:
        for command in self.commands:
            command()
        self.commands = []

    def rollback(self) -> typing.NoReturn:
        self.rollback_journal.open_file()
        journal_file_size = self.rollback_journal.file.read_integer(0, 16)
        if journal_file_size < os.stat(self.table.file.filename).st_size:
            os.truncate(self.table.file.filename, journal_file_size)
        self.rollback_journal.get_blocks()
        self.rollback_journal.restore_blocks()
        self.rollback_journal.close_file()
        self.table.last_block_index = self.table.get_blocks()[-1].index_in_file
        self.table.last_row_index = self.table.get_rows(False)[-1].index_in_file
        self.table.row_count = len(self.table.get_rows(False))
        self.table.write_meta_info()
        os.remove("journal.log")


class RollbackLog:
    def __init__(self, db_file: bin_py.BinFile, row_length: int = 0):
        self.file = bin_py.BinFile("journal.log")
        self.blocks = []
        self.first_rollback_index = 16
        self.block_count = 0
        self.db_file = db_file
        self.block_size = 0
        if row_length:
            self.block_size = 12 + 512 * row_length

    def check_original_indexes(self, index: int) -> bool:
        for block in self.blocks:
            if block.original_index == index:
                return True
        return False

    def create_file(self) -> typing.NoReturn:
        self.file.open("w+")
        self.file.write_integer(os.stat(self.db_file.filename).st_size, 0, 16)

    def open_file(self) -> typing.NoReturn:
        self.file.open("r+")

    def close_file(self) -> typing.NoReturn:
        self.file.close()

    def add_block(self, block_index: int) -> typing.NoReturn:
        if self.check_original_indexes(block_index):
            return
        block_num = self.db_file.read_integer(block_index, self.block_size)
        new_rollback_index = self.first_rollback_index + self.block_count * (self.block_size + 9)
        self.block_count += 1
        if len(self.blocks):
            self.blocks[-1].next_index = new_rollback_index
            self.blocks[-1].write_block(self.file)
        new_block = RollbackBlock(new_rollback_index, self.block_size, block_num, block_index)
        new_block.write_block(self.file)
        self.blocks.append(new_block)

    def get_blocks(self) -> typing.NoReturn:
        current_index = self.first_rollback_index
        while current_index != 0:
            current_block = RollbackBlock(current_index, 0, 0, 0)
            current_block.read_block(self.file)
            current_index = current_block.next_index
            self.blocks.append(current_block)

    def restore_blocks(self) -> typing.NoReturn:
        for block in self.blocks:
            self.db_file.write_integer(block.block_int, block.original_index, block.block_size)


class RollbackBlock:
    def __init__(self, rollback_index: int, size: int, block_int: int, original_index: int):
        self.block_size = size
        self.block_int = block_int
        self.index_in_file = rollback_index
        self.next_index = 0
        self.original_index = original_index

    def write_block(self, file: bin_py.BinFile) -> typing.NoReturn:
        if self.block_size:
            file.write_integer(self.block_size, self.index_in_file, 3)
            file.write_integer(self.block_int, self.index_in_file + 3, self.block_size)
            file.write_integer(self.next_index, self.index_in_file + 3 + self.block_size, 3)
            file.write_integer(self.original_index, self.index_in_file + self.block_size + 6, 3)

    def read_block(self, file: bin_py.BinFile) -> typing.NoReturn:
        self.block_size = file.read_integer(self.index_in_file, 3)
        self.block_int = file.read_integer(self.index_in_file + 3, self.block_size)
        self.next_index = file.read_integer(self.index_in_file + 3 + self.block_size, 3)
        self.original_index = file.read_integer(self.index_in_file + self.block_size + 6, 3)
