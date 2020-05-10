import engine.bin_file as bin_py
from datetime import datetime
from sortedcontainers import SortedDict
import cacheout
import typing
import random
import string
import exception
import fnmatch
import os


class Database:
    def __init__(self, create_default_file: bool = True, db_filename: str = ""):
        self.tables_count = 0
        self.signature = "#VDBSignature"
        self.tables = []
        self.meta_db_filename = ""
        self.file = None
        if create_default_file:
            self.meta_db_filename = "zhavoronkov.vdb.db_meta"
            self.file = bin_py.BinFile(self.meta_db_filename)
            self.file.open("w+")
            self.write_file()
            self.write_table_count(self.tables_count)
        elif db_filename != "":
            self.meta_db_filename = db_filename
            if os.path.isfile(self.meta_db_filename + ".db_meta"):
                self.connect_to_db(self.meta_db_filename + ".db_meta")
            else:
                self.file = bin_py.BinFile(self.meta_db_filename + ".db_meta")
                self.file.open("w+")
                self.write_file()
                self.write_table_count(self.tables_count)

    def __get_files_by_mask(self, mask: str) -> typing.List[str]:
        filename_list = []
        for file in os.listdir('.'):
            if fnmatch.fnmatch(file, mask):
                filename_list.append(file)
        return filename_list

    def __check_journal(self) -> bool:
        filename_list = self.__get_files_by_mask('rollback_journal*')
        for filename in filename_list:
            if os.path.isfile(filename):
                return True
        return False

    def __update_table_metadata(self, table) -> typing.NoReturn:
        table.last_block_index = table.get_blocks_indexes()[-1]
        table.last_row_index = table.get_last_row_index()
        table.row_count = table.count_rows()
        table.write_meta_info()

    def get_io_count(self) -> int:
        if isinstance(self.file, bin_py.BinFile):
            return self.file.io_count

    def wide_rollback(self) -> typing.NoReturn:
        filename_list = self.__get_files_by_mask('rollback_journal*')
        for filename in filename_list:
            rollback_obj = RollbackLog(self.file, 0, filename)
            rollback_obj.open_file()
            journal_file_size = rollback_obj.file.read_integer(0, 16)
            if journal_file_size < os.stat(self.meta_db_filename).st_size:
                os.truncate(self.meta_db_filename, journal_file_size)
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
        metafiles_list = self.__get_files_by_mask("*.table_meta")
        for metafile in metafiles_list:
            table_obj = Table()
            table_obj.metafile_name = metafile
            table_obj.metafile = bin_py.BinFile(table_obj.metafile_name)
            table_obj.metafile.open("r+")
            table_obj.storage_name = table_obj.metafile_name.replace(".table_meta", ".storage")
            table_obj.storage_file = bin_py.BinFile(table_obj.storage_name)
            table_obj.storage_file.open("r+")
            table_obj.read_file()
            table_obj.open_transaction_registry()
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

    def create_table(self, table_name: str, fields: typing.Dict, is_versioning: bool = False) -> typing.List:
        new_table = Table()
        new_table.name = table_name
        new_table.metafile_name = self.meta_db_filename + "_"+ new_table.name + ".table_meta"
        new_table.metafile = bin_py.BinFile(new_table.metafile_name)
        new_table.metafile.open("w+")
        new_table.storage_name = self.meta_db_filename + "_" + table_name + ".storage"
        new_table.storage_file = bin_py.BinFile(new_table.storage_name)
        new_table.storage_file.open("w+")
        if is_versioning:
            new_table.is_versioning = True
        new_table.fill_table_fields(fields)
        new_table.calc_row_size()
        new_table.create_transaction_registry()
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
    if timestamp == -1.0:
        return datetime.max
    else:
        return datetime.fromtimestamp(timestamp)


class Table:
    def __init__(self):
        max_fields_count = 14
        self.size = 32 + 26 + max_fields_count * 24
        self.row_length = 0
        self.meta_index = 0
        self.name = ""
        self.metafile_name = ""
        self.metafile = None
        self.storage_name = ""
        self.storage_file = None
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
        self.is_versioning = False
        self.max_transaction_id = 0
        self.rollback_filenames = []
        self.indexes = []
        self.max_index_id = 0
        self.transaction_registry = None
        self.cache = cacheout.lfu.LFUCache(maxsize=16)
        self.is_locked = False

    def __eq__(self, other) -> bool:
        if not isinstance(other, Table):
            return NotImplemented
        return ((self.name, self.size, self.fields_count, self.row_length) ==
                (other.name, other.size, other.fields_count, other.row_length))

    def __hash__(self):
        return hash(self.name) ^ hash(self.size) ^ hash(self.fields_count) ^ hash(self.row_length)

    def get_block_by_index(self, index: int):
        current_block = Block(index, self)
        row_count_dict = current_block.count_rows()
        cache_key = (index, row_count_dict["available"], row_count_dict["removed"], row_count_dict["updated"])
        if self.cache.get(cache_key) is None:
            current_block.read_file()
            current_block.get_rows()
            self.cache.set(cache_key, current_block)
        else:
            current_block = self.cache.get(cache_key)
        return current_block

    def iter_blocks(self) -> typing.Iterable:
        current_index = self.first_block_index
        while current_index != 0:
            current_block = self.get_block_by_index(current_index)
            current_index = current_block.next_block
            yield current_block

    def get_last_row_index(self) -> int:
        for block in self.iter_blocks():
            for row in block.rows:
                if (row.next_index == 0) and (row.status == 1):
                    return row.index_in_file
                return 0

    def count_rows(self) -> int:
        result = 0
        for block in self.iter_blocks():
            for row in block.rows:
                if row.status == 1:
                    result += 1
        return result

    def __create_local_rollback_journal(self, name: str):
        rollback_obj = RollbackLog(self.storage_file, self.row_length, name)
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

    def end_transaction(self, transaction_id: int, is_rollback: bool = False) -> typing.NoReturn:
        self.transactions[transaction_id].commit(is_rollback)
        self.transactions[transaction_id].transaction_end = get_current_timestamp()
        self.transaction_registry.insert_transaction_info(transaction_id,
                                                          self.transactions[transaction_id].transaction_start,
                                                          self.transactions[transaction_id].transaction_end)

    def rollback_transaction(self, transaction_id: int) -> typing.NoReturn:
        self.transactions[transaction_id].rollback()

    def create_transaction_registry(self) -> typing.NoReturn:
        self.transaction_registry = TransactionRegistry(self)
        self.transaction_registry.create_file()

    def open_transaction_registry(self) -> typing.NoReturn:
        self.transaction_registry = TransactionRegistry(self)
        self.transaction_registry.open_file()

    def create_block(self):
        self.storage_file.seek(0, 2)
        previous_index = 0
        block_index = self.storage_file.tell()
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

    def get_row_by_id(self, id: int):
        for block in self.iter_blocks():
            for row in block.rows:
                if (row.status == 1) and (row.row_id == id):
                    return row
            return False

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
        self.metafile.write_integer(self.row_count, self.meta_index + 32, 3)
        self.metafile.write_integer(self.first_block_index, self.meta_index + 32 + 3, 3)
        self.metafile.write_integer(self.last_block_index, self.meta_index + 32 + 6, 3)
        self.metafile.write_integer(self.first_row_index, self.meta_index + 32 + 9, 3)
        self.metafile.write_integer(self.last_row_index, self.meta_index + 32 + 12, 3)
        self.metafile.write_integer(self.last_removed_index, self.meta_index + 32 + 15, 3)
        self.metafile.write_integer(self.max_transaction_id, self.meta_index + 32 + 18, 3)

    def write_file(self) -> typing.NoReturn:
        # Table meta
        self.metafile.write_str(self.name, self.meta_index, 32)
        self.write_meta_info()
        self.metafile.write_integer(self.row_length, self.meta_index + 32 + 21, 2)
        self.metafile.write_integer(self.fields_count, self.meta_index + 32 + 23, 2)
        self.metafile.write_bool(self.is_versioning, self.meta_index + 32 + 25)
        current_position = self.meta_index + 32 + 26
        for index, field in enumerate(self.fields):
            self.metafile.write_str(field + self.types[index].name[:3], current_position, 24)
            current_position += 24

    def read_file(self) -> typing.NoReturn:
        self.name = self.metafile.read_str(self.meta_index, 32)
        self.row_count = self.metafile.read_integer(self.meta_index + 32, 3)
        self.first_block_index = self.metafile.read_integer(self.meta_index + 32 + 6, 3)
        self.last_block_index = self.metafile.read_integer(self.meta_index + 32 + 6, 3)
        self.first_row_index = self.metafile.read_integer(self.meta_index + 32 + 9, 3)
        self.last_row_index = self.metafile.read_integer(self.meta_index + 32 + 12, 3)
        self.last_removed_index = self.metafile.read_integer(self.meta_index + 32 + 15, 3)
        self.max_transaction_id = self.metafile.read_integer(self.meta_index + 32 + 18, 3)
        self.row_length = self.metafile.read_integer(self.meta_index + 32 + 21, 2)
        self.fields_count = self.metafile.read_integer(self.meta_index + 32 + 23, 2)
        self.is_versioning = self.metafile.read_bool(self.meta_index + 32 + 25)
        current_position = self.meta_index + 32 + 26
        field_position = 4
        for i in range(self.fields_count):
            field = self.metafile.read_str(current_position + i * 24, 21)
            field_type = self.types_dict[self.metafile.read_str(current_position + i * 24 + 21, 3)]
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
            self.__delete_row(row, transaction_id)
        else:
            start_time = get_current_timestamp()
            rollback_obj = self.__create_local_rollback_journal(self.get_random_filename())
            rollback_obj.add_block(self.get_block_index_for_row(row))
            self.max_transaction_id += 1
            self.__delete_row(row, self.max_transaction_id)
            self.__close_local_rollback_journal(rollback_obj)
            end_time = get_current_timestamp()
            self.transaction_registry.insert_transaction_info(self.max_transaction_id, start_time, end_time)

    def delete(self, rows_indexes: typing.Tuple[int] = (), transaction_id: int = 0) -> typing.NoReturn:
        if not len(rows_indexes):
            for block in self.iter_blocks():
                for row in block.rows:
                    if row.status == 1:
                        self.__delete_row_and_add_block(row, transaction_id)
        else:
            for index in rows_indexes:
                current_row = Row(self, index)
                current_row.read_info()
                if current_row.status == 1:
                    self.__delete_row_and_add_block(current_row, transaction_id)

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

    def select(self, fields: typing.Tuple[str], rows: typing.Tuple, transaction_id: int = 0,
               start_time: datetime = None, end_time: datetime = None) -> typing.List:
        selected_rows = []
        if self.is_versioning and (isinstance(start_time, datetime)) and (isinstance(end_time, datetime)):
            for transaction_info in self.transaction_registry.iter_transactions():
                tr_id = transaction_info["tr_id"]
                tr_start_time = convert_timestamp_to_datetime(transaction_info["tr_start"])
                tr_end_time = convert_timestamp_to_datetime(transaction_info["tr_end"])
                for block in self.iter_blocks():
                    for row in block.rows:
                        if tr_id == row.transaction_start:
                            joined_pair = [(tr_id, tr_start_time, tr_end_time), row]
                            if joined_pair[0][1] < end_time:
                                joined_pair[1].select_row(fields)
                                selected_rows.append(joined_pair[1])
                        if tr_id == row.transaction_end:
                            joined_pair = [(tr_id, tr_start_time, tr_end_time), row]
                            if joined_pair[0][2] <= start_time:
                                row.select_row(fields)
                                selected_rows.remove(row)
            return selected_rows
        if transaction_id > 0:
            commited_rows = []
            for block in self.iter_blocks():
                for row in block.rows:
                    if (row.status in [1, 3]) and (row.transaction_start < transaction_id):
                        row.select_row(fields)
                        commited_rows.append(row)
            selected_rows = self.__get_unique_rows(commited_rows)
        else:
            for row in rows:
                row.select_row(fields)
                selected_rows.append(row)
        return selected_rows

    def __copy_row(self, row_index: int, transaction_id: int):
        old_row = Row(self, row_index)
        old_row.read_row_from_file()
        old_row.transaction_end = transaction_id
        old_row.status = 3
        self.__delete_row_from_indexes(old_row)
        old_row.write_info()
        fields = []
        values = []
        for field, value in old_row.fields_values_dict.items():
            fields.append(field)
            values.append(value)
        new_row = self.__insert(tuple(fields), tuple(values), is_copy=True)
        new_row.row_id = old_row.row_id
        new_row.write_info()
        return new_row

    def update(self, fields: typing.Tuple[str], values: typing.Tuple,
               rows: typing.Tuple, transaction_id: int = 0) -> typing.NoReturn:
        for i in range(len(rows)):
            if transaction_id > 0:
                self.transactions[transaction_id].rollback_journal.add_block(self.get_block_index_for_row(rows[i]))
                new_row = self.__copy_row(rows[i].index_in_file, transaction_id)
                new_row.transaction_id = self.transactions[transaction_id].id
                new_row.transaction_start = transaction_id
                new_row.update_row(fields, values[i])
                self.__add_row_to_indexes(new_row)
            else:
                start_time = get_current_timestamp()
                rollback_obj = self.__create_local_rollback_journal(self.get_random_filename())
                rollback_obj.add_block(self.get_block_index_for_row(rows[i]))
                self.max_transaction_id += 1
                new_row = self.__copy_row(rows[i].index_in_file, self.max_transaction_id)
                new_row.transaction_start = self.max_transaction_id
                new_row.update_row(fields, values[i])
                self.__add_row_to_indexes(new_row)
                self.__close_local_rollback_journal(rollback_obj)
                end_time = get_current_timestamp()
                self.max_transaction_id += 1
                self.transaction_registry.insert_transaction_info(self.max_transaction_id, start_time, end_time)
                new_row.write_info()

    def insert(self, fields: typing.Tuple = (), values: typing.Tuple = (), insert_index: int = -1,
               transaction_id: int = 0) -> typing.NoReturn:
        self.__insert(fields, values, insert_index, transaction_id)

    def __add_row_to_indexes(self, row) -> typing.NoReturn:
        for index in self.indexes:
            key_list = []
            for field in index.fields:
                key_list.append(row.fields_values_dict[field])
            key_tuple = tuple(key_list)
            if key_tuple not in index.data_dict:
                index.data_dict[key_tuple] = []
                index.data_dict[key_tuple].append(row.index_in_file)
            else:
                index.data_dict[key_tuple].append(row.index_in_file)

    def __insert(self, fields: typing.Tuple = (), values: typing.Tuple = (), insert_index: int = -1,
                 transaction_id: int = 0, is_copy: bool = False):
        local_rollback_obj = None
        start_time = get_current_timestamp()
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
                new_row.transaction_start = transaction_id
            else:
                self.max_transaction_id += 1
                new_row.transaction_start = self.max_transaction_id
        new_row.status = 1
        new_row.next = saved_next_index
        new_row.previous_index = insert_index
        new_row.fields_values_dict = {field: values[index] for index, field in enumerate(fields)}
        new_row.write_row_to_file()
        if not is_copy:
            self.__add_row_to_indexes(new_row)
        if self.last_row_index == insert_index:
            self.last_row_index = position
            self.write_meta_info()
        if not is_copy:
            self.row_count += 1
        end_time = get_current_timestamp()
        if transaction_id == 0:
            if not is_copy:
                self.transaction_registry.insert_transaction_info(self.max_transaction_id, start_time, end_time)
            self.__close_local_rollback_journal(local_rollback_obj)
        return new_row

    def __delete_row_from_indexes(self, row) -> typing.NoReturn:
        for index in self.indexes:
            key_list = []
            for field in index.fields:
                key_list.append(row.fields_values_dict[field])
            key_tuple = tuple(key_list)
            if len(index.data_dict[key_tuple]) > 1:
                index.data_dict[key_tuple].remove(row.index_in_file)
            else:
                index.data_dict[key_tuple].remove(row.index_in_file)
                del index.data_dict[key_tuple]

    def __delete_row(self, row, transaction_id: int) -> typing.NoReturn:
        if row.index_in_file == self.first_row_index:
            self.first_row_index = row.next_index
        if row.index_in_file == self.last_row_index:
            self.last_row_index = row.previous_index
        row.read_row_from_file()
        self.__delete_row_from_indexes(row)
        row.drop_row()
        row.transaction_end = transaction_id
        row.status = 2
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

    def create_index(self, indexed_fields: typing.Tuple[str]) -> int:
        for field in indexed_fields:
            if field not in self.fields:
                raise exception.FieldNotExists
        self.max_index_id += 1
        index_id = self.max_index_id
        new_index = Index(index_id, indexed_fields)
        for block in self.iter_blocks():
            for row in block.rows:
                if row.status == 1:
                    values_list = []
                    for field_name in indexed_fields:
                        values_list.append(row.fields_values_dict[field_name])
                    values_tuple = tuple(values_list)
                    if values_tuple not in new_index.data_dict:
                        new_index.data_dict[values_tuple] = []
                        new_index.data_dict[values_tuple].append(row.index_in_file)
                    else:
                        new_index.data_dict[values_tuple].append(row.index_in_file)
        self.indexes.append(new_index)
        return index_id

    def delete_index(self, index_id: int) -> typing.NoReturn:
        for index in self.indexes:
            if index.id == index_id:
                self.indexes.remove(index)
                break


class Block:
    def __init__(self, start_index: int, table: Table) -> typing.NoReturn:
        self.table = table
        self.max_row_len = 512
        self.index_in_file = start_index
        self.block_size = 12 + 512 * self.table.row_length
        self.first_row_index = self.index_in_file + 12
        self.rows = []
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
        self.table.storage_file.write_integer(0, self.index_in_file, 3)
        self.table.storage_file.write_integer(self.rows_count, self.index_in_file + 3, 3)
        self.table.storage_file.write_integer(self.previous_block, self.index_in_file + 6, 3)
        self.table.storage_file.write_integer(self.next_block, self.index_in_file + 9, 3)

    def write_file(self) -> typing.NoReturn:
        self.update_file()
        current_block_size = 512 * self.table.row_length
        self.table.storage_file.write_integer(0, self.index_in_file, current_block_size)

    def read_file(self) -> typing.NoReturn:
        self.rows_count = self.table.storage_file.read_integer(self.index_in_file + 3, 3)
        self.previous_block = self.table.storage_file.read_integer(self.index_in_file + 6, 3)
        self.next_block = self.table.storage_file.read_integer(self.index_in_file + 9, 3)

    def iter_rows(self) -> typing.Iterable:
        current_index = self.first_row_index
        while current_index < self.index_in_file + self.block_size:
            current_row = Row(self.table, current_index)
            current_row.read_row_from_file()
            current_index += self.table.row_length
            if current_row.status != 0:
                yield current_row
            if not current_row.status:
                break

    def count_rows(self) -> typing.Dict[str, int]:
        count_dict = {"available": 0, "removed": 0, "updated": 0}
        for row in self.iter_rows():
            if row.status == 1:
                count_dict["available"] += 1
            if row.status == 2:
                count_dict["removed"] += 1
            if row.status == 3:
                count_dict["updated"] += 1
        return count_dict

    def get_rows(self) -> typing.NoReturn:
        for row in self.iter_rows():
            self.rows.append(row)


class Row:
    def __init__(self, table: Table, index: int = 0):
        self.index_in_file = index
        self.table = table
        self.fields_values_dict = {}
        self.previous_index = 0
        self.next_index = 0
        self.status = 0
        self.transaction_start = -1
        self.transaction_end = -1
        self.transaction_id = 0
        self.row_id = 0

    def write_info(self) -> typing.NoReturn:
        row_size = self.index_in_file + self.table.row_length
        self.table.storage_file.write_integer(self.status, self.index_in_file, 1)
        self.table.storage_file.write_integer(self.previous_index, row_size - 3, 3)
        self.table.storage_file.write_integer(self.next_index, row_size - 6, 3)
        self.table.storage_file.write_long_long(self.transaction_start, row_size - 14)
        self.table.storage_file.write_long_long(self.transaction_end, row_size - 22)
        self.table.storage_file.write_integer(self.transaction_id, row_size - 36, 14)
        self.table.storage_file.write_integer(self.row_id, row_size - 40, 4)

    def read_info(self) -> typing.NoReturn:
        row_size = self.index_in_file + self.table.row_length
        self.status = self.table.storage_file.read_integer(self.index_in_file, 1)
        self.previous_index = self.table.storage_file.read_integer(row_size - 3, 3)
        self.next_index = self.table.storage_file.read_integer(row_size - 6, 3)
        self.transaction_start = self.table.storage_file.read_long_long(row_size - 14)
        self.transaction_end = self.table.storage_file.read_long_long(row_size - 22)
        self.transaction_id = self.table.storage_file.read_integer(row_size - 36, 14)
        self.row_id = self.table.storage_file.read_integer(row_size - 40, 4)

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

    def write_row_to_file(self):
        self.write_info()
        for field in self.fields_values_dict:
            field_index = self.table.fields.index(field)
            field_type = self.table.types[field_index]
            value_position = self.table.positions[field]
            self.table.storage_file.write_by_type(field_type.name, self.fields_values_dict[field],
                                                  self.index_in_file + value_position, field_type.size)

    def read_row_from_file(self) -> typing.NoReturn:
        fields = self.table.fields
        self.read_info()
        for field, pos in self.table.positions.items():
            if field not in fields:
                continue
            index = self.table.fields.index(field)
            field_type = self.table.types[index]
            self.fields_values_dict[field] = self.table.storage_file.read_by_type(field_type.name,
                                                                                  self.index_in_file + pos,
                                                                                  field_type.size)


class Type:
    def __init__(self, name: str, size: int):
        self.name = name
        self.size = size

    def __eq__(self, other) -> bool:
        if not isinstance(other, Type):
            return NotImplemented
        return self.__dict__ == other.__dict__


class Index:
    def __init__(self, index_id: int, fields: typing.Tuple[str]):
        self.id = index_id
        self.fields = fields
        self.data_dict = SortedDict()


class Transaction:
    def __init__(self, table: Table):
        if not table.max_transaction_id:
            table.read_file()
        table.max_transaction_id += 1
        table.write_meta_info()
        self.id = table.max_transaction_id
        self.filename = f"rollback_journal_{self.id}.log"
        self.table = table
        self.transaction_start = 0
        self.transaction_end = 0
        self.rollback_journal = RollbackLog(self.table.storage_file, self.table.row_length, self.filename)

    def commit(self, is_rollback: bool) -> typing.NoReturn:
        self.rollback_journal.file.close()
        if not is_rollback:
            os.remove(self.filename)
        self.table.is_transaction = False

    def rollback(self) -> typing.NoReturn:
        self.rollback_journal.open_file()
        journal_file_size = self.rollback_journal.file.read_integer(0, 16)
        if journal_file_size < os.stat(self.table.storage_file.filename).st_size:
            os.truncate(self.table.storage_file.filename, journal_file_size)
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


class TransactionRegistry:
    def __init__(self, table: Table):
        self.table = table
        self.registry_filename = f"transaction_{self.table.name}.rg"
        self.registry_file = bin_py.BinFile(self.registry_filename)
        self.row_size = 20
        self.rows_count = 0

    def create_file(self) -> typing.NoReturn:
        self.registry_file.open("w+")
        self.registry_file.write_fixed_integer(self.rows_count, 0)

    def open_file(self) -> typing.NoReturn:
        self.registry_file.open("r+")
        self.rows_count = self.registry_file.read_fixed_integer(0)

    def insert_transaction_info(self, tr_id: int, tr_start: float, tr_end: float) -> typing.NoReturn:
        position = self.rows_count * self.row_size + 4
        self.registry_file.write_fixed_integer(tr_id, position)
        self.registry_file.write_float(tr_start, position + 4)
        self.registry_file.write_float(tr_end, position + 12)
        self.rows_count += 1

    def get_transaction_info(self, row_num: int) -> typing.Dict:
        position = row_num * self.row_size + 4
        transaction_dict = {"tr_id": self.registry_file.read_fixed_integer(position),
                            "tr_start": self.registry_file.read_float(position + 4),
                            "tr_end": self.registry_file.read_float(position + 12)}
        return transaction_dict

    def iter_transactions(self) -> typing.Iterable:
        counter = 0
        while counter < self.rows_count:
            result_transaction_info = self.get_transaction_info(counter)
            counter += 1
            yield result_transaction_info
