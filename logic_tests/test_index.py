import logic
import os
from datetime import datetime
import time

fileList = [f for f in os.listdir() if f.endswith(".db_meta")]
for f in fileList:
    os.remove(f)

log = logic.Logic("Testing.vdb")

time_list = []


def test_index():
    log.query("CREATE TABLE FIRST (id int, name str);", 1)
    log.query('INSERT INTO FIRST VALUES (10, "first");', 1)
    log.query('INSERT INTO FIRST VALUES (20, "second");', 1)
    log.query('INSERT INTO FIRST VALUES (30, "third");', 1)
    log.query('INSERT INTO FIRST VALUES (40, "first");', 1)
    log.query('INSERT INTO FIRST VALUES (50, "second");', 1)
    log.query('INSERT INTO FIRST VALUES (60, "third");', 1)
    log.query('INSERT INTO FIRST VALUES (70, "first");', 1)
    log.query('INSERT INTO FIRST VALUES (80, "second");', 1)
    log.query('INSERT INTO FIRST VALUES (90, "third");', 1)

    begin_first_time = datetime.now()
    temp1 = log.query("SELECT id FROM FIRST;", 1)
    end_first_time = datetime.now()
    first_time = end_first_time - begin_first_time

    log.query("CREATE INDEX ind1 ON FIRST (id);", 1)

    begin_second_time = datetime.now()
    temp2 = log.query("SELECT id FROM FIRST;", 1)
    end_second_time = datetime.now()
    second_time = end_second_time - begin_second_time

    assert temp1.str_for_print == temp2.str_for_print
    assert first_time > second_time


def test_show_index():
    excepted_result = "\n| index name | \n| ind1 | \n"

    result = log.query("SHOW INDEX FIRST;", 1)

    assert excepted_result == result.str_for_print


def test_drop_index():
    excepted_result = "\n| index name | \n"

    log.query("DROP INDEX ind1 ON FIRST;", 1)
    result = log.query("SHOW INDEX FIRST;", 1)

    assert excepted_result == result.str_for_print



