import engine.bin_file as bin_py
import engine.db_structure as db_py
import os


def test_binfile():
    test_file = bin_py.BinFile("test.bin")
    test_file.open("w+")
    test_file.write_integer(22, 0, 1)
    test_file.write_bool(False, 1, 1)
    test_file.write_str("vadik", 2, 32)
    result_int = test_file.read_integer(0, 1)
    result_bool = test_file.read_bool(1, 1)
    result_str = test_file.read_str(2, 32)
    test_file.close()
    os.remove("test.bin")
    assert result_int == 22
    assert not result_bool
    assert result_str == "vadik"


def test_create():
    db = db_py.Database()
    excepted_table = db_py.Table(db.file)
    excepted_table.name = "vadik_table"
    excepted_table.fields = ["zhenya1", "zhenya2"]
    excepted_table.fields_count = 2
    excepted_table.types = [db_py.Type("int", 4), db_py.Type("str", 256)]
    excepted_table.positions = {"row_id": 1, "zhenya1": 4, "zhenya2": 8}
    excepted_table.row_length = 270
    result_table = db.create_table("vadik_table", 0, {"zhenya1": "int", "zhenya2": "str"}, True)
    assert excepted_table == result_table


def test_insert():
    db = db_py.Database()
    db.create_table("vadik_table", 0, {"f1": "int", "f2": "str"}, True)
    db.tables[0].insert(["f2"], ["test_string_123"])
    db.tables[0].insert(["f1", "f2"], [99, "test_string_123"])
    db.tables[0].get_rows()
    assert len(db.tables[0].rows) == 2