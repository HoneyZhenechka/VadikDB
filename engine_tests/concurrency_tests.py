import engine.db_structure as db_py
import threading
import os

filename = "concurrency.vdb"
if os.path.isfile(filename):
    os.remove(filename)
db = db_py.Database(False, filename)
db.create_table("vadik_table", {"zhenya1": "int", "zhenya2": "str"})
lock = threading.Lock()


def test_multithreading_insert():
    def insert_func():
        for i in range(10):
            db.tables[0].insert(["zhenya1", "zhenya2"], [99, "test_string_123"])

    thread1 = threading.Thread(target=insert_func)
    thread2 = threading.Thread(target=insert_func)
    thread1.start()
    thread1.join()
    thread2.start()
    thread2.join()
    assert db.tables[0].count_rows() == 20


def test_repeatable_read():
    def func():
        id = db.tables[0].start_transaction()
        db.tables[0].update(["zhenya2"], [["TEST"]], [db.tables[0].get_row_by_id(1)], id)
        db.tables[0].end_transaction(id)

    id = db.tables[0].start_transaction()
    selected_rows_one = db.tables[0].select(["zhenya2"], db.tables[0].get_row_by_id(1), id)
    selected_value_one = selected_rows_one[0].fields_values_dict["zhenya2"]
    thread = threading.Thread(target=func)
    thread.start()
    thread.join()
    selected_rows_two = db.tables[0].select(["zhenya2"], db.tables[0].get_row_by_id(1), id)
    selected_value_two = selected_rows_two[0].fields_values_dict["zhenya2"]
    db.tables[0].end_transaction(id)
    assert selected_value_one == selected_value_two


def test_multithreading_update():
    def update_func_n(n):
        def update_func():
            db.tables[0].update(["zhenya1"], [[n]], [db.tables[0].get_row_by_id(4)])
        return update_func

    threads = []
    for i in range(200):
        func = update_func_n(i)
        threads.append(threading.Thread(target=func))
    for thread in threads:
        thread.start()
        thread.join()
    assert db.tables[0].get_row_by_id(4).fields_values_dict["zhenya1"] == 199