import engine.db_structure as db_py
import os

filename = "io.vdb"
if os.path.isfile(filename):
    os.remove(filename)
db = db_py.Database(False, filename)


def test_create_io():
    db.create_table("vadik_table", {"zhenya1": "int", "zhenya2": "str"})
    assert db.get_io_count() == 31


def test_insert_io():
    db.tables[0].insert(["zhenya1", "zhenya2"], [5000, "b"])
    assert db.get_io_count() == 101


def test_update_io():
    db.tables[0].update(["zhenya2"], [["lovetsov"]], [db.tables[0].get_row_by_id(0)])
    assert db.get_io_count() == 318


def test_delete_io():
    db.tables[0].delete()
    assert db.get_io_count() == 453


def test_cache_io():
    db.tables[0].insert(["zhenya1", "zhenya2"], [5000, "b"])
    io_count_first = db.get_io_count()
    db.tables[0].get_row_by_id(0)
    io_count_second = db.get_io_count()
    db.tables[0].get_row_by_id(0)
    io_count_third = db.get_io_count()
    assert (io_count_second - io_count_first) > (io_count_third - io_count_second)