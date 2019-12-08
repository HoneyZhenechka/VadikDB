import engine.db_structure as db_py

db = db_py.Database()


def get_block_rows(block):
    rows_list = []
    for row in block.iter_rows():
        rows_list.append(row)
    return rows_list


def get_all_rows_list():
    all_rows_list = []
    for block in db.tables[0].iter_blocks():
        all_rows_list.append(get_block_rows(block))
    return all_rows_list


def test_create():
    excepted_table = db_py.Table(db.file)
    excepted_table.name = "vadik_table"
    excepted_table.fields = ["zhenya1", "zhenya2"]
    excepted_table.fields_count = 2
    excepted_table.types = [db_py.Type("int", 4), db_py.Type("str", 256)]
    excepted_table.positions = {"row_id": 1, "zhenya1": 4, "zhenya2": 8}
    excepted_table.row_length = 270
    result_table = db.create_table("vadik_table", 0, {"zhenya1": "int", "zhenya2": "str"})
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
    rows_list = get_all_rows_list()
    assert len(rows_list) == 1
    assert len(rows_list[0]) == 2


def test_delete():
    rows_list = get_all_rows_list()
    db.tables[0].delete([rows_list[0][0].index_in_file])
    rows_list = get_all_rows_list()
    assert len(rows_list[0]) == 1
    db.tables[0].delete()
    rows_list = get_all_rows_list()
    assert len(rows_list[0]) == 0


def test_update():
    db.tables[0].insert(["zhenya1", "zhenya2"], [99, "test_string_123"])
    rows_list = get_all_rows_list()
    assert rows_list[0][0].fields_values_dict["zhenya2"] == "test_string_123"
    db.tables[0].update(["zhenya2"], [["lovetsov"]], [rows_list[0][0]])
    rows_list = get_all_rows_list()
    assert rows_list[0][0].fields_values_dict["zhenya1"] == 99
    assert rows_list[0][0].fields_values_dict["zhenya2"] == "lovetsov"


def test_select():
    db.tables[0].insert(["zhenya1", "zhenya2"], [218, "vadik_vadik"])
    rows_list = get_all_rows_list()
    result_rows_1 = db.tables[0].select(db.tables[0].fields, rows_list[0])
    assert len(result_rows_1) == 2
    result_rows_2 = db.tables[0].select(["zhenya1"], [rows_list[0][0]])
    assert len(result_rows_2) == 1
    assert result_rows_2[0].fields_values_dict["zhenya1"] == 218