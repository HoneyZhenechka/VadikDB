import engine.db_structure as db_py
import threading

filename = "test.vdb"
db = db_py.Database(False, filename)
db.create_table("vadik_table", 0, {"zhenya1": "int", "zhenya2": "str"})


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


def test_multithreading_insert():
    def insert_func():
        for i in range(10):
            db.tables[0].insert(["zhenya1", "zhenya2"], [99, "test_string_123"])

    thread1 = threading.Thread(target=insert_func)
    thread2 = threading.Thread(target=insert_func)
    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()
    assert db.tables[0].count_rows() == 20


def test_multithreading_update():
    rows_list = get_all_rows_list()

    def update_func_n(n):
        def update_func():
            db.tables[0].update(["zhenya1"], [[n]], [rows_list[0][1]])
        return update_func

    threads = []
    for i in range(200):
        func = update_func_n(i)
        threads.append(threading.Thread(target=func))
        threads[-1].start()
    for thread in threads:
        thread.join()
    rows_list = get_all_rows_list()
    assert rows_list[0][1].fields_values_dict["zhenya1"] == 199


def test_repeatable_read():
    rows_list = get_all_rows_list()

    def func():
        id = db.tables[0].start_transaction()
        db.tables[0].update(["zhenya2"], [["TEST"]], [rows_list[0][1]], id)
        db.tables[0].end_transaction(id)

    id = db.tables[0].start_transaction()
    selected_rows_one = db.tables[0].select(["zhenya2"], rows_list[0], id)
    selected_value_one = selected_rows_one[1].fields_values_dict["zhenya2"]
    thread = threading.Thread(target=func)
    thread.start()
    thread.join()
    selected_rows_two = db.tables[0].select(["zhenya2"], rows_list[0], id)
    selected_value_two = selected_rows_two[1].fields_values_dict["zhenya2"]
    db.tables[0].end_transaction(id)
    assert selected_value_one == selected_value_two
