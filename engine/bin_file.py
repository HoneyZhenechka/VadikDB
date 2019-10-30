import os


class BinFile:
    def __init__(self, filename):
        self.__file = None
        self.__filename = filename

    def is_file_exist(self):
        return os.path.isfile(self.__filename)

    def open(self, filemode):
        self.__file = open(self.__filename, filemode + "b")

    def close(self):
        self.__file.close()

    def read(self, count_bytes):
        self.__file.read(count_bytes)

    def tell(self):
        return self.__file.tell()

    def seek(self, offset, from_what):
        self.__file.seek(offset, from_what)

    def write_integer(self, int_num, start_pos, count_bytes):
        if start_pos >= 0:
            self.seek(start_pos, 0)
        byte_num = int_num.to_bytes(count_bytes, byteorder="little")
        self.__file.write(byte_num)

    def read_integer(self, start_pos, count_bytes):
        if start_pos >= 0:
            self.seek(start_pos, 0)
        int_bytes = bytes(self.__file.read(count_bytes))
        return int.from_bytes(int_bytes, "little")

    def write_bool(self, bool_num, start_pos, count_bytes):
        self.write_integer(int(bool_num), start_pos, count_bytes)

    def read_bool(self, start_pos, count_bytes):
        return bool(self.read_integer(start_pos, count_bytes))

    def write_str(self, string, start_pos, count_bytes):
        if start_pos >= 0:
            self.seek(start_pos, 0)
        string = string[:count_bytes]
        zero_bytes = b"\x00" * (count_bytes - len(string))
        self.__file.write(zero_bytes + string.encode())

    def read_str(self, start_pos, count_bytes):
        if start_pos >= 0:
            self.seek(start_pos, 0)
        result = self.__file.read(count_bytes).replace(b"\x00", b"")
        return result.decode()

    def write_by_type(self, value_type, value, start_pos, count_bytes):
        if value_type == "int":
            self.write_integer(value, start_pos, count_bytes)
        if value_type == "bool":
            self.write_bool(value, start_pos, count_bytes)
        if value_type == "str":
            self.write_str(value, start_pos, count_bytes)

    def read_by_type(self, value_type, start_pos, count_bytes):
        if value_type == "int":
            return self.read_integer(start_pos, count_bytes)
        if value_type == "bool":
            return self.read_bool(start_pos, count_bytes)
        if value_type == "str":
            return self.read_str(start_pos, count_bytes)