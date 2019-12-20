import os
import struct
import typing


class BinFile:
    def __init__(self, filename: str):
        self.__file = None
        self.filename = filename
        self.io_count = 0

    def is_file_exist(self) -> bool:
        return os.path.isfile(self.filename)

    def open(self, filemode: str) -> typing.NoReturn:
        self.__file = open(self.filename, filemode + "b")

    def close(self) -> typing.NoReturn:
        self.__file.close()

    def read(self, count_bytes: int) -> typing.NoReturn:
        self.__file.read(count_bytes)

    def tell(self) -> int:
        return self.__file.tell()

    def seek(self, offset: int, from_what: int) -> typing.NoReturn:
        self.__file.seek(offset, from_what)

    def write_integer(self, int_num: int, start_pos: int, count_bytes: int) -> typing.NoReturn:
        if start_pos >= 0:
            self.seek(start_pos, 0)
        byte_num = int_num.to_bytes(count_bytes, byteorder="little")
        self.__file.write(byte_num)
        self.io_count += 1

    def read_integer(self, start_pos: int, count_bytes: int) -> int:
        if start_pos >= 0:
            self.seek(start_pos, 0)
        int_bytes = bytes(self.__file.read(count_bytes))
        self.io_count += 1
        return int.from_bytes(int_bytes, "little")

    def write_fixed_integer(self, int_num: int, start_pos: int) -> typing.NoReturn:
        if start_pos >= 0:
            self.seek(start_pos, 0)
        int_bytes = struct.pack("<i", int_num)
        self.io_count += 1
        self.__file.write(int_bytes)

    def read_fixed_integer(self, start_pos: int) -> int:
        if start_pos >= 0:
            self.seek(start_pos, 0)
        int_bytes = bytes(self.__file.read(4))
        self.io_count += 1
        return struct.unpack("<i",int_bytes)[0]

    def write_bool(self, bool_num: bool, start_pos: int) -> typing.NoReturn:
        if start_pos >= 0:
            self.seek(start_pos, 0)
        bool_bytes = struct.pack("<?", bool_num)
        self.io_count += 1
        self.__file.write(bool_bytes)

    def read_bool(self, start_pos: int) -> bool:
        if start_pos >= 0:
            self.seek(start_pos, 0)
        bool_bytes = bytes(self.__file.read(1))
        self.io_count += 1
        return struct.unpack("<?", bool_bytes)[0]

    def write_float(self, float_num: float, start_pos: int) -> typing.NoReturn:
        if start_pos >= 0:
            self.seek(start_pos, 0)
        float_bytes = struct.pack("<d", float_num)
        self.io_count += 1
        self.__file.write(float_bytes)

    def read_float(self, start_pos: int) -> float:
        if start_pos >= 0:
            self.seek(start_pos, 0)
        float_bytes = bytes(self.__file.read(8))
        self.io_count += 1
        return struct.unpack("<d", float_bytes)[0]

    def write_str(self, string: str, start_pos: int, count_bytes: int) -> typing.NoReturn:
        if start_pos >= 0:
            self.seek(start_pos, 0)
        string = string[:count_bytes]
        zero_bytes = b"\x00" * (count_bytes - len(string))
        self.io_count += 1
        self.__file.write(zero_bytes + string.encode())

    def read_str(self, start_pos: int, count_bytes: int) -> str:
        if start_pos >= 0:
            self.seek(start_pos, 0)
        result = self.__file.read(count_bytes).replace(b"\x00", b"")
        self.io_count += 1
        return result.decode()

    def write_by_type(self, value_type: str, value, start_pos: int, count_bytes: int):
        if value_type == "int":
            self.write_fixed_integer(value, start_pos)
        if value_type == "bool":
            self.write_bool(value, start_pos)
        if value_type == "str":
            self.write_str(value, start_pos, count_bytes)
        if value_type == "float":
            self.write_float(value, start_pos)

    def read_by_type(self, value_type: str, start_pos: int, count_bytes: int):
        if value_type == "int":
            return self.read_fixed_integer(start_pos)
        if value_type == "bool":
            return self.read_bool(start_pos)
        if value_type == "str":
            return self.read_str(start_pos, count_bytes)
        if value_type == "float":
            return self.read_float(start_pos)