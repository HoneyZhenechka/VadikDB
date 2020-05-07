import logic
import os

fileList = [f for f in os.listdir() if f.endswith(".db_meta")]
for f in fileList:
    os.remove(f)

log = logic.Logic("Testing.vdb")


def test_transaction_not_error():
    excepted_result = "\n| id | name | \n"

    log.query("CREATE TABLE FIRST (id int, name str);")
    log.query('INSERT INTO FIRST VALUES (1, "admin");')
    log.query('INSERT INTO FIRST VALUES (2, "notadmin");')
    log.query('CREATE TABLE SECOND (id int, name str);')
    log.query('INSERT INTO SECOND VALUES (1, "admin");')
    log.query('INSERT INTO SECOND VALUES (3, "vadic");')

    log.query('BEGIN TRANSACTION;')
    log.query('DELETE FROM FIRST;')
    log.query('END TRANSACTION;')

    result = log.query("SELECT * FROM FIRST;")
    assert excepted_result == result.str_for_print


def test_transaction_rollback_not_error():
    excepted_result = "\n| id | name | \n"

    log.query('BEGIN TRANSACTION;')
    log.query('INSERT INTO FIRST VALUES (1, "admin");')
    log.query('INSERT INTO FIRST VALUES (2, "notadmin");')
    log.query('ROLLBACK;')

    result = log.query("SELECT * FROM FIRST;")
    assert excepted_result == result.str_for_print
