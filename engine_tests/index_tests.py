import engine.db_structure as db_py
from sortedcontainers import SortedDict
import os

filename = "index.vdb"
if os.path.isfile(filename):
    os.remove(filename)
db = db_py.Database(False, filename)
db.create_table("vadik_table", {"zhenya1": "int", "zhenya2": "str"})


def test_create_index():
    db.tables[0].insert(["zhenya1", "zhenya2"], [5000, "b"])
    db.tables[0].insert(["zhenya1", "zhenya2"], [929, "a"])
    db.tables[0].create_index(["zhenya1"])
    ideal_dict = SortedDict({(929,): [735], (5000,): [422]})
    assert ideal_dict == db.tables[0].indexes[0].data_dict


def test_update_index():
    db.tables[0].update(["zhenya1"], [[6000]], [db.tables[0].get_row_by_id(1)])
    ideal_dict = SortedDict({(5000,): [422], (6000,): [1048]})
    assert ideal_dict == db.tables[0].indexes[0].data_dict


def test_delete_index():
    db.tables[0].insert(["zhenya1", "zhenya2"], [1, "le"])
    db.tables[0].delete([db.tables[0].get_row_by_id(1).index_in_file])
    ideal_dict = SortedDict({(1,): [1361], (5000,): [422]})
    assert ideal_dict == db.tables[0].indexes[0].data_dict


def test_composite_index():
    db.tables[0].create_index(["zhenya1", "zhenya2"])
    ideal_dict = SortedDict({(1, 'le'): [1361], (5000, 'b'): [422]})
    assert ideal_dict == db.tables[0].indexes[1].data_dict
