import engine.bin_file as bin_py
from datetime import datetime
import time
import threading
import typing
import random
import string
import exception
import fnmatch
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

    def __get_journal_files(self) -> typing.List[str]:
        filename_list = []
        for file in os.listdir('.'):
            if fnmatch.fnmatch(file, 'rollback_journal*'):
                filename_list.append(file)
        return filename_list

    def __check_journal(self) -> bool:
        filename_list = self.__get_journal_files()
        for filename in filename_list:
            if os.path.isfile(filename):
                return True
        return False

    def __update_table_metadata(self, table) -> typing.NoReturn:
        table.last_block_index = table.get_blocks_indexes()[-1]
        table.last_row_index = table.get_last_row_index()
        table.row_count = table.count_rows()
        table.write_meta_info()

    def wide_rollback(self) -> typing.NoReturn:
        filename_list = self.__get_journal_files()
        for filename in filename_list:
            rollback_obj = RollbackLog(self.file, 0, filename)
            rollback_obj.open_file()
            journal_file_size = rollback_obj.file.read_integer(0, 16)
            if journal_file_size < os.stat(self.filename).st_size:
                os.truncate(self.filename, journal_file_size)
            rollback_obj.restore_blocks()
            rollback_obj.close_file()
            os.remove(filename)
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

    def create_table(self, table_name: str, fields: typing.Dict) -> typing.List:
        self.file.open("r+")
        self.file.seek(0, 2)
        new_table = Table(self.file)
        new_table.name = table_name
        new_table.index_in_file = 16 + self.tables_count * new_table.size
        new_table.fill_table_fields(fields)
        new_table.calc_row_size()
        new_table.write_file()
        self.tables.append(new_table)
        self.tables_count += 1
        self.write_table_count(self.tables_count)
        table_index = self.tables.index(new_table)
        self.tables[table_index].create_block()
        return self.tables[table_index]

    def close_db(self) -> typing.NoReturn:
        self.file.close()
        self.file = None
        self.tables_count = 0
        self.tables = []


def get_random_string(length: int) -> str:
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))


def get_current_timestamp() -> float:
    return datetime.now().timestamp()


def convert_timestamp_to_datetime(timestamp: float) -> datetime:
    return datetime.fromtimestamp(timestamp)


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
        self.fields_count = 0
        self.row_count = 0
        self.types = []
        self.types_dict = {"boo": Type("bool", 1), "int": Type("int", 4), "flo": Type("float", 8),
                           "str": Type("str", 256)}
        self.positions = {"row_id": 1}
        self.transactions = {}
        self.max_transaction_id = 0
        self.rollback_filenames = []

    def __eq__(self, other) -> bool:
        if not isinstance(other, Table):
            return NotImplemented
        return (self.name, self.fields, self.fields_count, self.types, self.positions, self.row_length) == \
               (other.name, other.fields, other.fields_count, other.types, other.positions, other.row_length)

    def iter_blocks(self) -> typing.Iterable:
        current_index = self.first_block_index
        while current_index != 0:
            current_block = Block(current_index, self)
            current_block.read_file()
            current_index = current_block.next_block
            yield current_block

    def get_last_row_index(self) -> int:
        for block in self.iter_blocks():
            for row in block.iter_rows():
                if (row.next_index == 0) and (row.row_available == 1):
                    return row.index_in_file
                return 0

    def count_rows(self) -> int:
        result = 0
        for block in self.iter_blocks():
            for row in block.iter_rows():
                if row.row_available == 1:
                    result += 1
        return result

    def __create_local_rollback_journal(self, name: str):
        rollback_obj = RollbackLog(self.file, self.row_length, name)
        rollback_obj.create_file()
        return rollback_obj

    def __close_local_rollback_journal(self, rollback_obj) -> typing.NoReturn:
        rollback_obj.close_file()
        os.remove(rollback_obj.file.filename)

    def start_transaction(self) -> int:
        transaction_obj = Transaction(self)
        self.transactions[transaction_obj.id] = transaction_obj
        self.transactions[transaction_obj.id].transaction_start = get_current_timestamp()
        self.transactions[transaction_obj.id].rollback_journal.create_file()
        return transaction_obj.id

    def ___update_end_timestamp_in_rows(self, transaction_id: int, timestamp: int) -> typing.NoReturn:
        for block in self.iter_blocks():
            for row in block.iter_rows():
                if row.transaction_id == transaction_id:
                    row.transaction_end = timestamp
                    row.write_info()

    def end_transaction(self, transaction_id: int, is_rollback: bool = False) -> typing.NoReturn:
        self.transactions[transaction_id].commit(is_rollback)
        self.transactions[transaction_id].transactions_end = get_current_timestamp()
        self.___update_end_timestamp_in_rows(transaction_id, self.transactions[transaction_id].transactions_end)

    def rollback_transaction(self, transaction_id: int) -> typing.NoReturn:
        self.transactions[transaction_id].rollback()

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

    def get_blocks_count(self) -> int:
        blocks_count = 0
        for _ in self.iter_blocks():
            blocks_count += 1
        return blocks_count

    def get_blocks_indexes(self) -> typing.List[int]:
        result_list = []
        for block in self.iter_blocks():
            result_list.append(block.index_in_file)
        return result_list

    def get_write_position(self):
        for block in self.iter_blocks():
            position = block.get_write_position()
            if position:
                self.current_block_index = block.index_in_file
                return position, block
        else:
            new_block = self.create_block()
            self.current_block_index = new_block.index_in_file
            return new_block.get_write_position(), new_block

    def get_block_index_for_row(self, row) -> int:
        if self.get_blocks_count() == 1:
            return self.first_block_index
        else:
            for block in self.iter_blocks():
                start_index = block.index_in_file
                end_index = start_index + block.block_size
                if end_index > row.index_in_file > start_index:
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
            self.file.write_str(field + self.types[index].name[:3], current_position, 24)
            current_position += 24
        bytes_count = self.size - (current_position - self.index_in_file)
        self.file.write_str("", current_position, bytes_count)
        self.file.seek(current_position, 0)

    def read_file(self) -> typing.NoReturn:
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

    def get_random_filename(self) -> str:
        while True:
            random_string = get_random_string(10)
            filename = "rollback_journal_" + random_string + ".log"
            if filename not in self.rollback_filenames:
                self.rollback_filenames.append(filename)
                break
        return filename

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

    def __delete_row_and_add_block(self, row, transaction_id: int = 0) -> typing.NoReturn:
        if transaction_id > 0:
            self.transactions[transaction_id].rollback_journal.add_block(self.get_block_index_for_row(row))
            row.transaction_start = self.transactions[transaction_id].transaction_start
            row.transaction_id = self.transactions[transaction_id].id
            row.write_info()
            self.__delete_row(row)
        else:
            row.transaction_start = get_current_timestamp()
            rollback_obj = self.__create_local_rollback_journal(self.get_random_filename())
            rollback_obj.add_block(self.get_block_index_for_row(row))
            self.__delete_row(row)
            self.__close_local_rollback_journal(rollback_obj)
            row.transaction_end = get_current_timestamp()

    def delete(self, rows_indexes: typing.Tuple[int] = (), transaction_id: int = 0) -> typing.NoReturn:
        threading_lock.acquire()
        if not len(rows_indexes):
            for block in self.iter_blocks():
                for row in block.iter_rows():
                    if row.row_available == 1:
                        self.__delete_row_and_add_block(row, transaction_id)
        else:
            for index in rows_indexes:
                current_row = Row(self, index)
                current_row.read_info()
                if current_row.row_available == 1:
                    self.__delete_row_and_add_block(current_row, transaction_id)
        threading_lock.release()

    def __get_unique_rows(self, rows_list: typing.List) -> typing.List:
        unique_rows = rows_list.copy()
        removed_dict = {}
        for i in range(len(unique_rows)):
            removed_dict[i] = 0
        for i in range(len(unique_rows)):
            for j in range(len(unique_rows)):
                if i != j:
                    if ((unique_rows[i].row_id == unique_rows[j].row_id) and
                            (unique_rows[i].index_in_file < unique_rows[j].index_in_file)):
                        removed_dict[i] = 1
        for i in range(len(unique_rows)):
            if removed_dict[i] == 1:
                del unique_rows[i]
        return unique_rows

    def select(self, fields: typing.Tuple[str], rows: typing.Tuple, transaction_id: int = 0) -> typing.List:
        selected_rows = []
        if transaction_id > 0:
            transaction_start_datetime = convert_timestamp_to_datetime(
                self.transactions[transaction_id].transaction_start
            )
            commited_rows = []
            for block in self.iter_blocks():
                for row in block.iter_rows():
                    row_tr_end_datetime = convert_timestamp_to_datetime(row.transaction_end)
                    if (row.row_available in [1, 3]) and (row_tr_end_datetime < transaction_start_datetime):
                        commited_rows.append(row)
            selected_rows = self.__get_unique_rows(commited_rows)
        else:
            for row in rows:
                row.select_row(fields)
                selected_rows.append(row)
        return selected_rows

    def __copy_row(self, row_index: int):
        old_row = Row(self, row_index)
        old_row.read_row_from_file()
        old_row.row_available = 3
        old_row.write_info()
        fields = []
        values = []
        for field, value in old_row.fields_values_dict.items():
            fields.append(field)
            values.append(value)
        new_row = self.__insert(tuple(fields), tuple(values), is_copy=True)
        return new_row

    def update(self, fields: typing.Tuple[str], values: typing.Tuple,
               rows: typing.Tuple, transaction_id: int = 0) -> typing.NoReturn:
        threading_lock.acquire()
        for i in range(len(rows)):
            if transaction_id > 0:
                self.transactions[transaction_id].rollback_journal.add_block(self.get_block_index_for_row(rows[i]))
                new_row = self.__copy_row(rows[i].index_in_file)
                new_row.transaction_id = self.transactions[transaction_id].id
                new_row.transaction_start = self.transactions[transaction_id].transaction_start
                new_row.update_row(fields, values[i])
            else:
                rollback_obj = self.__create_local_rollback_journal(self.get_random_filename())
                rollback_obj.add_block(self.get_block_index_for_row(rows[i]))
                new_row = self.__copy_row(rows[i].index_in_file)
                new_row.transaction_start = get_current_timestamp()
                new_row.update_row(fields, values[i])
                self.__close_local_rollback_journal(rollback_obj)
                new_row.transaction_end = get_current_timestamp()
                new_row.write_info()
        threading_lock.release()

    def insert(self, fields: typing.Tuple = (), values: typing.Tuple = (), insert_index: int = -1,
               test_rollback: bool = False, transaction_id: int = 0) -> typing.NoReturn:
        threading_lock.acquire()
        self.__insert(fields, values, insert_index, test_rollback, transaction_id)
        threading_lock.release()

    def __insert(self, fields: typing.Tuple = (), values: typing.Tuple = (), insert_index: int = -1,
                 test_rollback: bool = False, transaction_id: int = 0, is_copy: bool = False):
        local_rollback_obj = None
        position = self.get_free_row()
        if not transaction_id:
            local_rollback_obj = self.__create_local_rollback_journal(self.get_random_filename())
            local_rollback_obj.add_block(self.get_block_index_for_row(self.current_block_index))
        else:
            self.transactions[transaction_id].rollback_journal.add_block(self.current_block_index)
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
        if not is_copy:
            new_row.row_id = self.row_count
        if not is_copy:
            if transaction_id > 0:
                new_row.transaction_id = self.transactions[transaction_id].id
                new_row.transaction_start = self.transactions[transaction_id].transaction_start
            else:
                new_row.transaction_start = get_current_timestamp()
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
        if not is_copy:
            self.row_count += 1
        if transaction_id == 0:
            new_row.transaction_end = get_current_timestamp()
            self.__close_local_rollback_journal(local_rollback_obj)
        return new_row

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
        position, block = self.get_write_position()
        block.rows_count += 1
        block.update_file()
        return position

    def calc_row_size(self) -> typing.NoReturn:
        self.row_length = 4
        self.positions = {"row_id": 1}
        for index, field in enumerate(self.fields):
            self.positions[field] = self.row_length
            self.row_length += self.types[index].size
        self.row_length += 41

    def __check_type_name(self, typename) -> bool:
        for key in self.types_dict:
            if typename == self.types_dict[key].name:
                return True
        return False

    def fill_table_fields(self, fields_dict: typing.Dict) -> typing.NoReturn:
        fields_list = list(fields_dict.keys())
        types_list = list(fields_dict.values())
        if len(types_list) != len(fields_list):
            raise exception.DifferentCount()
        self.types = types_list
        self.fields = fields_list
        for index, type_name in enumerate(self.types):
            if self.__check_type_name(type_name):
                self.types[index] = self.types_dict[type_name[:3]]
            else:
                raise exception.TypeNotExists(type_name)
        self.fields_count = len(self.fields)


class Block:
    def __init__(self, start_index: int, table: Table) -> typing.NoReturn:
        self.table = table
        self.max_row_len = 512
        self.index_in_file = start_index
        self.block_size = 12 + 512 * self.table.row_length
        self.first_row_index = self.index_in_file + 12
        self.rows_count = 0
        self.previous_block = 0
        self.next_block = 0

    def get_write_position(self):
        if self.rows_count >= self.max_row_len:
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

    def iter_rows(self) -> typing.Iterable:
        current_index = self.first_row_index
        while current_index < self.index_in_file + self.block_size:
            current_row = Row(self.table, current_index)
            current_row.read_row_from_file()
            current_index += self.table.row_length
            if current_row.row_available != 0:
                yield current_row
            if not current_row.row_available:
                break


class Row:
    def __init__(self, table: Table, index: int = 0):
        self.index_in_file = index
        self.table = table
        self.fields_values_dict = {}
        self.previous_index = 0
        self.next_index = 0
        self.row_available = 0
        self.transaction_start = 0
        self.transaction_end = 0
        self.transaction_id = 0
        self.row_id = 0

    def write_info(self) -> typing.NoReturn:
        row_size = self.index_in_file + self.table.row_length
        self.table.file.write_integer(self.row_available, self.index_in_file, 1)
        self.table.file.write_integer(self.previous_index, row_size - 3, 3)
        self.table.file.write_integer(self.next_index, row_size - 6, 3)
        self.table.file.write_float(self.transaction_start, row_size - 14)
        self.table.file.write_float(self.transaction_end, row_size - 22)
        self.table.file.write_integer(self.transaction_id, row_size - 36, 14)
        self.table.file.write_integer(self.row_id, row_size - 40, 4)

    def read_info(self) -> typing.NoReturn:
        row_size = self.index_in_file + self.table.row_length
        self.row_available = self.table.file.read_integer(self.index_in_file, 1)
        self.previous_index = self.table.file.read_integer(row_size - 3, 3)
        self.next_index = self.table.file.read_integer(row_size - 6, 3)
        self.transaction_start = self.table.file.read_float(row_size - 14)
        self.transaction_end = self.table.file.read_float(row_size - 22)
        self.transaction_id = self.table.file.read_integer(row_size - 36, 14)
        self.row_id = self.table.file.read_integer(row_size - 40, 4)

    def select_row(self, fields: typing.Tuple[str]) -> typing.NoReturn:
        result = {}
        for field in fields:
            if field in self.fields_values_dict:
                result[field] = self.fields_values_dict[field]
        self.fields_values_dict = result

    def update_row(self, fields: typing.Tuple[str], values: typing.Tuple) -> typing.NoReturn:
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

    def read_row_from_file(self) -> typing.NoReturn:
        fields = self.table.fields
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


class Transaction:
    def __init__(self, table: Table):
        table.max_transaction_id += 1
        self.id = table.max_transaction_id
        self.filename = f"rollback_journal_{self.id}.log"
        self.table = table
        self.transaction_start = 0
        self.transaction_end = 0
        self.rollback_journal = RollbackLog(self.table.file, self.table.row_length, self.filename)

    def commit(self, is_rollback: bool) -> typing.NoReturn:
        self.rollback_journal.file.close()
        if not is_rollback:
            os.remove(self.filename)
        self.table.is_transaction = False

    def rollback(self) -> typing.NoReturn:
        self.rollback_journal.open_file()
        journal_file_size = self.rollback_journal.file.read_integer(0, 16)
        if journal_file_size < os.stat(self.table.file.filename).st_size:
            os.truncate(self.table.file.filename, journal_file_size)
        self.rollback_journal.restore_blocks()
        self.rollback_journal.close_file()
        self.table.last_block_index = self.table.get_blocks_indexes()[-1]
        self.table.last_row_index = self.table.get_last_row_index()
        self.table.row_count = self.table.count_rows()
        self.table.write_meta_info()
        os.remove(self.filename)


class RollbackLog:
    def __init__(self, db_file: bin_py.BinFile, row_length: int = 0, filename: str = ""):
        self.file = bin_py.BinFile("rollback_journal.log")
        if filename != "":
            self.file = bin_py.BinFile(filename)
        self.first_rollback_index = 16
        self.block_count = 0
        self.db_file = db_file
        self.block_size = 0
        if row_length:
            self.row_length = row_length
            self.block_size = 12 + 512 * row_length

    def check_original_indexes(self, index: int) -> bool:
        for block in self.iter_blocks():
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
        if self.block_count > 1:
            for block in self.iter_blocks():
                if block.next_block == 0:
                    block.next_index = new_rollback_index
                    block.write_block(self.file)
        new_block = RollbackBlock(new_rollback_index, self.block_size, block_num, block_index)
        new_block.write_block(self.file)

    def iter_blocks(self) -> typing.Iterable:
        current_index = self.first_rollback_index
        while current_index != 0:
            current_block = RollbackBlock(current_index, 0, 0, 0)
            current_block.read_block(self.file)
            current_index = current_block.next_index
            yield current_block

    def restore_blocks(self) -> typing.NoReturn:
        for block in self.iter_blocks():
            self.db_file.write_integer(block.block_int, block.original_index, block.block_size)


class RollbackBlock:
    def __init__(self, rollback_index: int, size: int, block_int: int, original_index: int):
        self.block_size = size
        self.block_int = block_int
        self.index_in_file = rollback_index
        self.first_row_index = self.index_in_file + 3 + 12
        self.next_index = 0
        self.original_index = original_index
        self.rows_indexes = []

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
