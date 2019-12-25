import engine.db_structure as db_py
import os


filename = "crud.vdb"
if os.path.isfile(filename):
    os.remove(filename)
db = db_py.Database(False, filename)


def test_create():
    excepted_table = db_py.Table(db.file)
    excepted_table.name = "vadik_table"
    excepted_table.fields = ["zhenya1", "zhenya2"]
    excepted_table.fields_count = 2
    excepted_table.types = [db_py.Type("int", 4), db_py.Type("str", 256)]
    excepted_table.positions = {"row_id": 1, "zhenya1": 4, "zhenya2": 8}
    excepted_table.row_length = 313
    result_table = db.create_table("vadik_table", {"zhenya1": "int", "zhenya2": "str"})
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
    db.tables[0].insert(["zhenya1", "zhenya2"], [5000, "b"])
    db.tables[0].insert(["zhenya1", "zhenya2"], [929, "a"])
    assert db.tables[0].count_rows() == 2


def test_delete():
    db.tables[0].delete([db.tables[0].get_row_by_id(0).index_in_file])
    assert db.tables[0].count_rows() == 1
    db.tables[0].delete()
    assert db.tables[0].count_rows() == 0


def test_update():
    db.tables[0].insert(["zhenya1", "zhenya2"], [99, "test_string_123"])
    assert db.tables[0].get_row_by_id(0).fields_values_dict["zhenya2"] == "test_string_123"
    db.tables[0].update(["zhenya2"], [["lovetsov"]], [db.tables[0].get_row_by_id(0)])
    assert db.tables[0].get_row_by_id(0).fields_values_dict["zhenya1"] == 99
    assert db.tables[0].get_row_by_id(0).fields_values_dict["zhenya2"] == "lovetsov"


def test_select():
    db.tables[0].insert(["zhenya1", "zhenya2"], [218, "vadik_vadik"])
    max_id = db.tables[0].count_rows()
    rows_list = []
    for id in range(max_id):
        rows_list.append(db.tables[0].get_row_by_id(id))
    result_rows_1 = db.tables[0].select(db.tables[0].fields, rows_list)
    assert len(result_rows_1) == 2
    result_rows_2 = db.tables[0].select(["zhenya1"], [db.tables[0].get_row_by_id(1)])
    assert len(result_rows_2) == 1
    assert result_rows_2[0].fields_values_dict["zhenya1"] == 218