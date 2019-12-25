import engine.db_structure as db_py
from datetime import datetime
import os

filename = "transaction.vdb"
if os.path.isfile(filename):
    os.remove(filename)
db = db_py.Database(False, filename)
db.create_table("vadik_table", {"zhenya1": "int", "zhenya2": "str"}, True)


def test_insert_get():
    first_time = datetime.now()
    db.tables[0].insert(["zhenya1", "zhenya2"], [929, "a"])
    second_time = datetime.now()
    db.tables[0].insert(["zhenya1", "zhenya2"], [50200, "bdas"])
    db.tables[0].insert(["zhenya1", "zhenya2"], [92429, "affff"])
    third_time = datetime.now()
    first_result = db.tables[0].select(db.tables[0].fields, [], start_time=second_time, end_time=third_time)
    assert len(first_result) == 2
    assert first_result[0].fields_values_dict["zhenya1"] == 50200
    second_result = db.tables[0].select(db.tables[0].fields, [], start_time=first_time, end_time=third_time)
    assert len(second_result) == 3