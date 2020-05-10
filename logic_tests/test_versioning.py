import logic
import os
from datetime import datetime
import time

fileList = [f for f in os.listdir() if f.endswith(".db_meta")]
for f in fileList:
    os.remove(f)

log = logic.Logic("Testing.vdb")

time_list = []


def test_transaction_users_end_not_error():
    excepted_result = "\n| id | name | \n| 1 | admin | \n"

    log.query("CREATE TABLE FIRST (id int, name str);", 1)
    log.query('INSERT INTO FIRST VALUES (1, "admin");', 1)

    time_list.append(datetime.now())
    time_list.append(datetime.now())

    request = ("SELECT * FROM FIRST FOR SYSTEM TIME FROM " +
               time_list[0].isoformat(' ') + " TO " + time_list[1].isoformat(' ') + ";")
    result = log.query(request, 1)

    assert excepted_result == result.str_for_print


def test_transaction_users_rollback_not_error():
    time.sleep(2)
    log.query("DELETE FROM FIRST;")

    time_list.append(datetime.now())
    time_list.append(datetime.now())

    request_empty = ("SELECT * FROM FIRST FOR SYSTEM TIME FROM " +
               time_list[2].isoformat(' ') + " TO " + time_list[3].isoformat(' ') + ";")
    request_full = ("SELECT * FROM FIRST FOR SYSTEM TIME FROM " +
               time_list[1].isoformat(' ') + " TO " + time_list[2].isoformat(' ') + ";")

    empty_result = log.query(request_empty, 1)
    full_result = log.query(request_full, 1)

    assert empty_result.str_for_print == "\n| id | name | \n"
    assert full_result.str_for_print == "\n| id | name | \n| 1 | admin | \n"

