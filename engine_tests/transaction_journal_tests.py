import engine.db_structure as db_py
import os

filename = "transaction.vdb"
if os.path.isfile(filename):
    os.remove(filename)
db = db_py.Database(False, filename)
db.create_table("vadik_table", {"zhenya1": "int", "zhenya2": "str"})


def test_transaction():
    id = db.tables[0].start_transaction()
    db.tables[0].insert(["zhenya1", "zhenya2"], [99, "test_string_123"], transaction_id=id)
    db.tables[0].insert(["zhenya1", "zhenya2"], [992, "test_string_321"], transaction_id=id)
    db.tables[0].update(["zhenya2"], [["lovetsov"]], [db.tables[0].get_row_by_id(0)], id)
    db.tables[0].delete([db.tables[0].get_row_by_id(1).index_in_file], id)
    db.tables[0].end_transaction(id)
    assert db.tables[0].count_rows() == 1
    assert db.tables[0].get_row_by_id(0).fields_values_dict["zhenya2"] == "lovetsov"


def test_read_commited():
    id = db.tables[0].start_transaction()
    db.tables[0].update(["zhenya2"], [["anime"]], [db.tables[0].get_row_by_id(0)], id)
    result_rows = db.tables[0].select(["zhenya2"], [db.tables[0].get_row_by_id(0)], id)
    db.tables[0].end_transaction(id)
    result_value = result_rows[0].fields_values_dict["zhenya2"]
    assert result_value == "lovetsov"


def test_rollback():
    id = db.tables[0].start_transaction()
    db.tables[0].insert(["zhenya1", "zhenya2"], [992, "test_string_321"], transaction_id=id)
    db.tables[0].insert(["zhenya1", "zhenya2"], [992, "tesssst_string_321"], transaction_id=id)
    db.tables[0].insert(["zhenya1", "zhenya2"], [992, "tesssttttt_string_321"], transaction_id=id)
    db.tables[0].end_transaction(id, True)
    db.tables[0].rollback_transaction(id)
    assert db.tables[0].count_rows() == 1
    assert db.tables[0].get_row_by_id(0).fields_values_dict["zhenya2"] == "anime"


def test_wide_rollback():
    id = db.tables[0].start_transaction()
    db.tables[0].update(["zhenya2"], [["xxx"]], [db.tables[0].get_row_by_id(0)], id)
    db.tables[0].end_transaction(id, True)
    db.close_db()
    db.connect_to_db("zhavoronkov.vdb")
    assert db.tables[0].get_row_by_id(0).fields_values_dict["zhenya2"] == "anime"


def test_durability():
    db.tables[0].insert(["zhenya1", "zhenya2"], [992, "tesssst_string_321"], test_rollback=True)
    db.close_db()
    db.connect_to_db("zhavoronkov.vdb")
    assert db.tables[0].count_rows() == 1
