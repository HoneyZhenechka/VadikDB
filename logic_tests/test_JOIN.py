import logic
import exception
import os

filename = "Testing.vdb"
if os.path.isfile(filename):
    os.remove(filename)
log = logic.Logic(filename)


def test_intersect_not_error():
    excepted_result = "\n| id | name | \n| 1 | admin | \n"
    log.query("CREATE TABLE FIRST (id int, name str);")
    log.query("INSERT INTO FIRST VALUES (1, admin);")
    log.query("INSERT INTO FIRST VALUES (2, notadmin);")
    log.query("CREATE TABLE SECOND (id int, name str);")
    log.query("INSERT INTO SECOND VALUES (1, admin);")
    log.query("INSERT INTO SECOND VALUES (3, vadic);")
    result = log.query("SELECT * FROM FIRST INTERSECT SELECT * FROM FIRST;")
    assert excepted_result == result.str_for_print


def test_union_not_error():
    excepted_result = "\n| id | name | \n| 1 | admin | \n| 2 | notadmin | \n| 1 | admin | \n| 3 | vadic | \n"
    result = log.query("SELECT * FROM FIRST UNION SELECT * FROM SECOND;")
    assert excepted_result == result.str_for_print


def test_join_on_not_error():
    excepted_result = ""
    result = log.query("SELECT * FROM FIRST JOIN SECOND ON FIRST.id = SECOND.id;")
    assert excepted_result == result.str_for_print
