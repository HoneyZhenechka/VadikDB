import logic
import os

fileList = [f for f in os.listdir() if f.endswith(".db_meta")]
for f in fileList:
    os.remove(f)

log = logic.Logic("Testing.vdb")


def test_transaction_users_end_not_error():
    excepted_result = "\n| id | name | \n| 4 | admin | \n| 3 | notadmin | \n"

    log.query("CREATE TABLE FIRST (id int, name str);", 1)
    log.query('INSERT INTO FIRST VALUES (1, "admin");', 1)

    log.query('BEGIN TRANSACTION;', 1)
    log.query("UPDATE FIRST SET id = 2 where id = 1;", 1)
    log.query('INSERT INTO FIRST VALUES (2, "notadmin");', 3)
    log.query("UPDATE FIRST SET id = 3 where id = 2;", 2)
    log.query("UPDATE FIRST SET id = 4 where id = 2;", 1)
    log.query('END TRANSACTION;', 1)

    result = log.query("SELECT * FROM FIRST;", 1)
    assert excepted_result == result.str_for_print


def test_transaction_users_rollback_not_error():
    excepted_result = "\n| id | \n| 1 | \n| 2 | \n"

    log.query("CREATE TABLE SECOND (id int);", 1)
    log.query('INSERT INTO SECOND VALUES (1);', 1)

    log.query('BEGIN TRANSACTION;', 1)
    log.query('DELETE FROM FIRST;', 1)
    log.query('INSERT INTO SECOND VALUES (2);', 2)
    log.query('DELETE FROM FIRST;', 1)
    log.query('ROLLBACK;', 1)

    result = log.query("SELECT * FROM SECOND;", 1)
    assert excepted_result == result.str_for_print
