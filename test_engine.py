import threading


def test_transaction():
    id = db.tables[0].start_transaction()
    rows_list = get_all_rows_list()
    db.tables[0].update(["zhenya2"], [["lovetsov"]], [rows_list[0][0]], id)
    db.tables[0].update(["zhenya1"], [[98]], [rows_list[0][0]], id)
    db.tables[0].insert(["zhenya1", "zhenya2"], [99, "test_string_123"], transaction_id=id)
    db.tables[0].insert(["zhenya1", "zhenya2"], [992, "test_string_321"], transaction_id=id)
    db.tables[0].delete([rows_list[0][-1].index_in_file], id)
    db.tables[0].end_transaction(id)
    rows_list = get_all_rows_list()
    assert rows_list[0][0].fields_values_dict["zhenya2"] == "lovetsov"
    assert rows_list[0][0].fields_values_dict["zhenya1"] == 98
    assert len(rows_list[0]) == 3


def test_read_commited():
    id = db.tables[0].start_transaction()
    rows_list = get_all_rows_list()
    db.tables[0].update(["zhenya2"], [["anime"]], [rows_list[0][0]], id)
    result_rows = db.tables[0].select(["zhenya2"], rows_list[0], id)
    result_value = result_rows[0].fields_values_dict["zhenya2"]
    db.tables[0].end_transaction(id)
    assert result_value == "lovetsov"


def test_rollback():
    id = db.tables[0].start_transaction()
    db.tables[0].insert(["zhenya1", "zhenya2"], [992, "test_string_321"], transaction_id=id)
    db.tables[0].insert(["zhenya1", "zhenya2"], [992, "tesssst_string_321"], transaction_id=id)
    db.tables[0].insert(["zhenya1", "zhenya2"], [992, "tesssttttt_string_321"], transaction_id=id)
    db.tables[0].end_transaction(id, True)
    db.tables[0].get_rows()
    assert len(db.tables[0].rows) == 6
    db.tables[0].rollback_transaction(id)
    db.tables[0].get_rows()
    assert len(db.tables[0].rows) == 3


def test_wide_rollback():
    id = db.tables[0].start_transaction()
    db.tables[0].update(["zhenya2"], [["xxx"]], [db.tables[0].rows[0]], id)
    db.tables[0].end_transaction(id, True)
    db.close_db()
    db.connect_to_db("zhavoronkov.vdb")
    db.tables[0].get_rows()
    assert db.tables[0].rows[0].fields_values_dict["zhenya2"] == "anime"
    assert len(db.tables[0].rows) == 3


def test_durability():
    db.tables[0].insert(["zhenya1", "zhenya2"], [992, "tesssst_string_321"], test_rollback=True)
    db.close_db()
    db.connect_to_db("zhavoronkov.vdb")
    db.tables[0].get_rows()
    assert len(db.tables[0].rows) == 3


def thread_function():
    id = db.tables[0].start_transaction()
    db.tables[0].update(["zhenya2"], [["mmmm"]], [db.tables[0].rows[2]], id)
    db.tables[0].end_transaction(id)


def test_repeatable_read():
    id = db.tables[0].start_transaction()
    db.tables[0].insert(["zhenya1", "zhenya2"], [228, "greta_sobaka"], transaction_id=id)
    result_rows_one = db.tables[0].select(["zhenya2"], db.tables[0].rows, id)
    result_value_one = result_rows_one[2].fields_values_dict["zhenya2"]
    new_thread = threading.Thread(target=thread_function)
    new_thread.start()
    result_rows_two = db.tables[0].select(["zhenya2"], db.tables[0].rows, id)
    result_value_two = result_rows_two[2].fields_values_dict["zhenya2"]
    db.tables[0].end_transaction(id)
    assert result_value_one == result_value_two
