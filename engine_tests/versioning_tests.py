import engine.db_structure as db_py
from datetime import datetime
import os

filename = "versioning.vdb"
if os.path.isfile(filename):
    os.remove(filename)
db = db_py.Database(False, filename)
db.create_table("vadik_table", {"zhenya1": "int", "zhenya2": "str"}, True)
time_list = []


def test_insert_get():
    db.tables[0].insert(["zhenya1", "zhenya2"], [929, "a"])
    db.tables[0].insert(["zhenya1", "zhenya2"], [50200, "bdas"])
    db.tables[0].insert(["zhenya1", "zhenya2"], [92429, "affff"])
    time_list.append(datetime.now())
    time_list.append(datetime.now())
    first_result = db.tables[0].select(db.tables[0].fields, [], start_time=time_list[0], end_time=time_list[1])
    assert len(first_result) == 3


def test_delete_get():
    db.tables[0].delete()
    time_list.append(datetime.now())
    time_list.append(datetime.now())
    empty_result = db.tables[0].select(db.tables[0].fields, [], start_time=time_list[2], end_time=time_list[3])
    assert len(empty_result) == 0

