import logic
import exception
import os

fileList = [f for f in os.listdir() if f.endswith(".db_meta")]
for f in fileList:
    os.remove(f)

log = logic.Logic("Testing.vdb")


def test_create_not_error():
    excepted_result = False
    result = log.query("CREATE TABLE VADIC (id int, name str);")
    assert excepted_result == result.is_exception


def test_create_error_table_already_exists():
    excepted_result = "Error code: " + "03" + " -- Table " + "VADIC" + " already exists!"
    result = log.query("CREATE TABLE VADIC (id int, name str);")
    assert excepted_result == result.str_for_print


def test_create_error_duplicate_fields():
    excepted_result = "Error code: " + "08" + " -- Duplicate field: " + "id int"
    result = log.query("CREATE TABLE VADIC (id int, id int);")
    assert excepted_result == result.str_for_print


def test_show_create_table_not_error():
    excepted_result = False
    result = log.query("SHOW CREATE TABLE VADIC;")
    assert excepted_result == result.is_exception


def test_show_create_table_error_table_not_exists():
    excepted_result = "Error code: " + "04" + " -- Table " + "CREATETABLE" + " not exists!"
    result = log.query("SHOW CREATE TABLE CREATETABLE;")
    assert excepted_result == result.str_for_print


def test_select_not_error():
    excepted_result = "\n| id | name | \n"
    result = log.query("SELECT * FROM VADIC;")
    assert excepted_result == result.str_for_print


def test_select_error_table_not_exists():
    excepted_result = "Error code: " + "04" + " -- Table " + "CREATETABLE" + " not exists!"
    result = log.query("SELECT * FROM CREATETABLE;")
    assert excepted_result == result.str_for_print


def test_select_error_field_not_exists():
    excepted_result = "Error code: " + "05" + " -- Field " + "NOTEXISTS" + " not exists!"
    result = log.query("SELECT NOTEXISTS FROM VADIC;")
    assert excepted_result == result.str_for_print


def test_insert_not_error():
    excepted_result = "\n| id | name | \n| 1 | admin | \n"
    temp = log.query('INSERT INTO VADIC VALUES(1, "admin");')
    result = log.query("SELECT * FROM VADIC;")
    assert excepted_result == result.str_for_print


def test_insert_error_table_not_exists():
    excepted_result = "Error code: " + "04" + " -- Table " + "NOTEXISTS" + " not exists!"
    result = log.query('INSERT INTO NOTEXISTS VALUES(1, "admin");')
    assert excepted_result == result.str_for_print


def test_insert_error_field_not_exists():
    excepted_result = "Error code: " + "05" + " -- Field " + "NOTEXISTS" + " not exists!"
    result = log.query('INSERT INTO VADIC (NOTEXISTS) VALUES(1);')
    assert excepted_result == result.str_for_print


def test_update_not_error():
    excepted_result = "\n| id | name | \n| 2 | admin | \n"
    temp = log.query("UPDATE VADIC SET id = 2;")
    result = log.query("SELECT * FROM VADIC;")
    assert excepted_result == result.str_for_print


def test_update_error_table_not_exists():
    excepted_result = "Error code: " + "04" + " -- Table " + "NOTEXISTS" + " not exists!"
    result = log.query("UPDATE NOTEXISTS SET id = 1;")
    assert excepted_result == result.str_for_print


def test_update_error_field_not_exists():
    excepted_result = "Error code: " + "05" + " -- Field " + "NOTEXISTS" + " not exists!"
    result = log.query("UPDATE VADIC SET NOTEXISTS = 1;")
    assert excepted_result == result.str_for_print


def test_delete_not_error():
    excepted_result = "\n| id | name | \n"
    temp = log.query("DELETE FROM VADIC;")
    result = log.query("SELECT * FROM VADIC;")
    assert excepted_result == result.str_for_print


def test_delete_error_table_not_exists():
    excepted_result = "Error code: " + "04" + " -- Table " + "NOTEXISTS" + " not exists!"
    result = log.query("DELETE FROM NOTEXISTS;")
    assert excepted_result == result.str_for_print


def test_select_condition():
    excepted_result = "\n| id | name | \n| 1 | admin | \n"
    log.query('INSERT INTO VADIC VALUES(1, "admin");')
    log.query('INSERT INTO VADIC VALUES(2, "notadmin");')
    result = log.query("SELECT * FROM VADIC WHERE id < 1 + 1;")
    assert excepted_result == result.str_for_print


def test_update_condition():
    excepted_result = "\n| id | name | \n| 2 | notadmin | \n| 2 | admin | \n"
    log.query("UPDATE VADIC SET id = 2 where id < 1 + 1;")
    result = log.query("SELECT * FROM VADIC;")
    assert excepted_result == result.str_for_print


def test_delete_condition():
    excepted_result = "\n| id | name | \n| 2 | notadmin | \n| 2 | admin | \n"
    log.query('INSERT INTO VADIC VALUES(1, "admin");')
    log.query("DELETE FROM VADIC where id < 1 + 1;")
    result = log.query("SELECT * FROM VADIC;")
    assert excepted_result == result.str_for_print


