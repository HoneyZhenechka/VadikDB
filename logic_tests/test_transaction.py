import logic
import os

fileList = [f for f in os.listdir() if f.endswith(".db_meta")]
for f in fileList:
    os.remove(f)

log = logic.Logic("Testing.vdb")


def test_transaction_end_delete_not_error():
    excepted_result = "\n| id | name | \n"

    log.query("CREATE TABLE FIRST (id int, name str);", 1)
    log.query('INSERT INTO FIRST VALUES (1, "admin");', 1)
    log.query('INSERT INTO FIRST VALUES (2, "notadmin");', 1)

    log.query('BEGIN TRANSACTION;', 1)
    log.query('DELETE FROM FIRST;', 1)
    log.query('END TRANSACTION;', 1)

    result = log.query("SELECT * FROM FIRST;", 1)
    assert excepted_result == result.str_for_print


def test_transaction_rollback_delete_not_error():
    excepted_result = "\n| id | name | \n| 1 | admin | \n| 2 | notadmin | \n"

    log.query('INSERT INTO FIRST VALUES (1, "admin");', 1)
    log.query('INSERT INTO FIRST VALUES (2, "notadmin");', 1)

    log.query('BEGIN TRANSACTION;', 1)
    log.query('DELETE FROM FIRST;', 1)
    log.query('ROLLBACK;', 1)

    result = log.query("SELECT * FROM FIRST;", 1)
    assert excepted_result == result.str_for_print


def test_transaction_end_update_not_error():
    excepted_result = "\n| id | name | \n| 2 | notadmin | \n| 101 | admin | \n"

    log.query('BEGIN TRANSACTION;', 1)
    log.query("UPDATE FIRST SET id = 101 where id = 1;", 1)
    log.query('END TRANSACTION;', 1)

    result = log.query("SELECT * FROM FIRST;", 1)
    assert excepted_result == result.str_for_print


def test_transaction_rollback_update_not_error():
    excepted_result = "\n| id | name | \n| 2 | notadmin | \n| 101 | admin | \n"

    log.query('BEGIN TRANSACTION;', 1)
    log.query("UPDATE FIRST SET id = 1 where id = 101;", 1)
    log.query('ROLLBACK;', 1)

    result = log.query("SELECT * FROM FIRST;", 1)
    assert excepted_result == result.str_for_print


def test_transaction_end_insert_not_error():
    excepted_result = "\n| id | name | \n| 2 | notadmin | \n| 101 | admin | \n| 1 | admin | \n"

    log.query('BEGIN TRANSACTION;', 1)
    log.query('INSERT INTO FIRST VALUES (1, "admin");', 1)
    log.query('END TRANSACTION;', 1)

    result = log.query("SELECT * FROM FIRST;", 1)
    assert excepted_result == result.str_for_print


def test_transaction_rollback_insert_not_error():
    excepted_result = "\n| id | name | \n| 2 | notadmin | \n| 101 | admin | \n| 1 | admin | \n"

    log.query('BEGIN TRANSACTION;', 1)
    log.query('INSERT INTO FIRST VALUES (222, "aaadmin");', 1)
    log.query('ROLLBACK;', 1)

    result = log.query("SELECT * FROM FIRST;", 1)
    assert excepted_result == result.str_for_print


def test_transaction_select_not_error():
    excepted_result = "\n| id | name | \n| 2 | notadmin | \n| 101 | admin | \n| 1 | admin | \n"

    log.query('BEGIN TRANSACTION;', 1)
    log.query('INSERT INTO FIRST VALUES (202, "admin");', 1)
    result = log.query("SELECT * FROM FIRST;", 1)
    log.query('END TRANSACTION;', 1)

    assert excepted_result == result.str_for_print


def test_transaction_error_end_not_defined():
    excepted_result = "Error code: " + "15" + " -- User(user_index: " + "101" + ") not defined transaction"
    result = log.query('END TRANSACTION;', 101)
    assert excepted_result == result[0].str_for_print


def test_transaction_error_rollback_not_defined():
    excepted_result = "Error code: " + "15" + " -- User(user_index: " + "101" + ") not defined transaction"
    result = log.query('ROLLBACK;', 101)
    assert excepted_result == result[0].str_for_print
