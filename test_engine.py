import engine.bin_file as bin_py
import engine.db_structure as db_py
import os


db = db_py.Database()


def test_binfile():
    test_file = bin_py.BinFile("test.bin")
    test_file.open("w+")
    test_file.write_integer(22, 0, 1)
    test_file.write_bool(False, 1)
    test_file.write_str("vadik", 2, 32)
    result_int = test_file.read_integer(0, 1)
    result_bool = test_file.read_bool()
    result_str = test_file.read_str(2, 32)
    test_file.close()
    os.remove("test.bin")
    assert result_int == 22
    assert not result_bool
    assert result_str == "vadik"


def test_create():
    excepted_table = db_py.Table(db.file)
    excepted_table.name = "vadik_table"
    excepted_table.fields = ["zhenya1", "zhenya2"]
    excepted_table.fields_count = 2
    excepted_table.types = [db_py.Type("int", 4), db_py.Type("str", 256)]
    excepted_table.positions = {"row_id": 1, "zhenya1": 4, "zhenya2": 8}
    excepted_table.row_length = 270
    result_table = db.create_table("vadik_table", 0, {"zhenya1": "int", "zhenya2": "str"}, True)
    assert excepted_table == result_table


def test_show_create():
    fields_names = ["zhenya1", "zhenya2"]
    types_names = ["int", "str"]
    fields = [
        "'" + v + "' " + types_names[i]
        for i, v in enumerate(fields_names)
    ]
    table_name = "vadik_table"
    excepted_string = "--------------------------------------------------------\n"
    excepted_string += "Create table: \n"
    excepted_string += "CREATE TABLE '" + table_name + "' ("
    excepted_string += ", ".join(fields) + ")\n"
    excepted_string += "--------------------------------------------------------"
    result_string = db.tables[0].show_create()
    assert result_string == excepted_string


def test_insert():
    db.tables[0].insert(["zhenya2"], ["test_string_123"])
    db.tables[0].insert(["zhenya1", "zhenya2"], [99, "test_string_123"])
    db.tables[0].get_rows()
    assert db.tables[0].rows[1].fields_values_dict["zhenya2"] == "test_string_123"
    assert len(db.tables[0].rows) == 2


def test_delete():
    db.tables[0].delete([db.tables[0].rows[0].index_in_file])
    db.tables[0].get_rows()
    assert len(db.tables[0].rows)
    assert not db.tables[0].rows[0].next_index
    db.tables[0].delete()
    db.tables[0].get_rows()
    assert not len(db.tables[0].rows)


def test_update():
    db.tables[0].insert(["zhenya1", "zhenya2"], [99, "test_string_123"])
    db.tables[0].get_rows()
    assert db.tables[0].rows[0].fields_values_dict["zhenya2"] == "test_string_123"
    db.tables[0].update(["zhenya2"], ["lovetsov"], [db.tables[0].rows[0]])
    db.tables[0].get_rows()
    assert db.tables[0].rows[0].fields_values_dict["zhenya1"] == 99
    assert db.tables[0].rows[0].fields_values_dict["zhenya2"] == "lovetsov"


def test_select():
    db.tables[0].insert(["zhenya1", "zhenya2"], [218, "vadik_vadik"])
    db.tables[0].get_rows()
    result_rows_1 = db.tables[0].select(db.tables[0].fields, db.tables[0].rows)
    assert len(result_rows_1) == 2
    result_rows_2 = db.tables[0].select(["zhenya1"], [db.tables[0].rows[1]])
    assert len(result_rows_2) == 1
    assert result_rows_2[0].fields_values_dict["zhenya1"] == 218


def test_transaction():
    db.tables[0].start_transaction()
    db.tables[0].update(["zhenya2"], ["lovetsov"], [db.tables[0].rows[0]])
    db.tables[0].update(["zhenya1"], [98], [db.tables[0].rows[0]])
    db.tables[0].end_transaction()
    db.tables[0].get_rows()
    assert db.tables[0].rows[0].fields_values_dict["zhenya2"] == "lovetsov"
    assert db.tables[0].rows[0].fields_values_dict["zhenya1"] == 98
