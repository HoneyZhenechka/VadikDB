import logic
import exception
import os

fileList = [f for f in os.listdir() if f.endswith(".db_meta")]
for f in fileList:
    os.remove(f)
log = logic.Logic("Testing.vdb")


def test_intersect_not_error():
    excepted_result = "\n| id | name | \n| 1 | admin | \n"
    log.query("CREATE TABLE FIRST (id int, name str);")
    log.query('INSERT INTO FIRST VALUES (1, "admin");')
    log.query('INSERT INTO FIRST VALUES (2, "notadmin");')
    log.query('CREATE TABLE SECOND (id int, name str);')
    log.query('INSERT INTO SECOND VALUES (1, "admin");')
    log.query('INSERT INTO SECOND VALUES (3, "vadic");')
    result = log.query("SELECT * FROM FIRST INTERSECT SELECT * FROM SECOND;")
    assert excepted_result == result.str_for_print


def test_union_not_error():
    excepted_result = "\n| id | name | \n| 1 | admin | \n| 2 | notadmin | \n| 3 | vadic | \n"
    result = log.query("SELECT * FROM FIRST UNION SELECT * FROM SECOND;")
    assert excepted_result == result.str_for_print


def test_join_on_not_error():
    excepted_result = "\n| id | name | id | name | \n| 1 | admin | 1 | admin | \n"
    result = log.query("SELECT * FROM FIRST JOIN SECOND ON FIRST.id = SECOND.id;")
    assert excepted_result == result.str_for_print


def test_join_not_error():
    excepted_result = "\n| id | name | id | name | \n| 1 | admin | 1 | admin | \n| 1 | admin | 3 | vadic | \n| 2 | notadmin | 1 | admin | \n| 2 | notadmin | 3 | vadic | \n"
    result = log.query("SELECT * FROM FIRST JOIN SECOND;")
    assert excepted_result == result.str_for_print


def test_join_error_table_not_exist():
    excepted_result = "Error code: " + "04" + " -- Table " + "NOTEXIST" + " not exists!"
    result = log.query("SELECT * FROM FIRST JOIN NOTEXIST;")
    assert excepted_result == result.str_for_print


def test_join_error_field_not_exist():
    excepted_result = "Error code: " + "05" + " -- Field " + "NOTEXIST" + " not exists!"
    result = log.query("SELECT FIRST.NOTEXIST FROM FIRST JOIN SECOND ON FIRST.id = SECOND.id;")
    assert excepted_result == result.str_for_print
