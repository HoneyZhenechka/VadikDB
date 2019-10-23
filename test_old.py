import pytest
import logic

tester = logic.Logic()


def test_create_1():
    test = tester.query("create table students ( I INT , J str );")
    assert test == "NOT ERROR"
    test = tester.query("create table students ( I INT , J str );")
    assert test == "ERROR"
    test = tester.query("create table students2 ();")
    assert test == "ERROR"
    test = tester.query("create table students2 ( I IN, J);")
    assert test == "ERROR"


def test_show_2():
    test = tester.query("create table VADIC ( I INT , J str );")
    assert test == "NOT ERROR"
    test = tester.query(("show create table VADIC;"))
    assert test == "NOT ERROR"
    test = tester.query(("showcreate table VADIC;"))
    assert test == "ERROR"
    test = tester.query(("show create VADIC;"))
    assert test == "ERROR"


def test_drop_1():
    test = tester.query("create table VADIC1 ( I INT , J STR );")
    assert test == "NOT ERROR"
    test = tester.query("drop table VADIC1;")
    assert test == "NOT ERROR"


def test_create_2():
    test = tester.query("create table ( I INT, J CHAR(10) );")
    assert test == "ERROR"
    test = tester.query("create table ( I IN, J ST );")
    assert test == "ERROR"
    test = tester.query("eate ble ( I IN, J CHA(10) );")
    assert test == "ERROR"
    test = tester.query("create table ( I IN, J STR );")
    assert test == "ERROR"
    test = tester.query("create table  I INT, J STR ")
    assert test == "ERROR"
    test = tester.query("create table ( I INT, J STR )")
    assert test == "ERROR"


def test_show_2():
    test = tester.query("show table ( I INT, J CHAR(10) )")
    assert test == "ERROR"
    test = tester.query(("show create table students;"))
    assert test == "NOT ERROR"
    test = tester.query(("show create table students"))
    assert test == "ERROR"
    test = tester.query(("show create table student;"))
    assert test == "ERROR"
    test = tester.query(("show__create table students;"))
    assert test == "ERROR"
    test = tester.query(("showcreate table students;"))
    assert test == "ERROR"
    test = tester.query(("show create students;"))
    assert test == "ERROR"
    test = tester.query(("show create students table;"))
    assert test == "ERROR"


def test_show_3():
    test = tester.query(("show create show create table students;"))
    assert test == "ERROR"
    test = tester.query("show create table;")
    assert test == "ERROR"
    test = tester.query("show create table students")
    assert test == "ERROR"


def test_query():
    test = tester.query("куевфывсмсы;")
    assert test == "ERROR"
    test = tester.query("")
    assert test == "ERROR"
    test = tester.query("empty")
    assert test == "ERROR"
    test = tester.query("create table 123")
    assert test == "ERROR"


def test_drop_2():
    test = tester.query("drop table")
    assert test == "ERROR"
    test = tester.query("drop table not_existing_table;")
    assert test == "ERROR"
    test = tester.query("drop table create table;")
    assert test == "ERROR"
    test = tester.query("drop table drop table;")
    assert test == "ERROR"
    test = tester.query("droptable VADIC")
    assert test == "ERROR"


def test_syntax_1():
    test = tester.query("show create table 123")
    assert test == "ERROR"
    test = tester.query("show create table ")
    assert test == "ERROR"
    test = tester.query("show create table """)
    assert test == "ERROR"
    test = tester.query("show create table show create table")
    assert test == "ERROR"
    test = tester.query("show create table table create")
    assert test == "ERROR"
    test = tester.query("show")
    assert test == "ERROR"
    test = tester.query("create show")
    assert test == "ERROR"
    test = tester.query("ow create table 444")
    assert test == "ERROR"


def test_recursive():
    test = tester.query("create table create table create table TEST_TABLE ( I INT , J CHAR(10) )")
    assert test == "ERROR"
    test = tester.query("create table show create table 123 ( I INT , J CHAR(10) )")
    assert test == "ERROR"
    test = tester.query("create table create table create table TEST_TABLE")
    assert test == "ERROR"


def test_insert_1():
    test = tester.query("INSERT INTO students VALUES (1, 'admin');")
    assert test == "NOT ERROR"
    test = tester.query("INSERT INTO students VALUES (1, 'admin');")
    assert test == "NOT ERROR"
    test = tester.query("INSERT INTO students VALUES (2, 'admin', '123');")
    assert test == "ERROR"
    test = tester.query("INSERT INTO usersr VALUES (2, 'admin', '123');")
    assert test == "ERROR"
    test = tester.query("INSERT INTO students VALUES (1, 'admin');")
    assert test == "NOT ERROR"


def test_insert_2():
    test = tester.query("INSERTINTO students VALUES (2, 'admin');")
    assert test == "ERROR"
    test = tester.query("INSERT INTO students VALUES (2, admin);")
    assert test == "NOT ERROR"
    test = tester.query("INSERT INTO students VALUES ('hh', 'admin');")
    assert test == "ERROR"
    test = tester.query("INSERT INTO students VALUES VALUES (2, admin);")
    assert test == "ERROR"
    test = tester.query("INSERT INTO INSERT INTO students VALUES (2, admin);")
    assert test == "ERROR"


def test_update_1():
    test = tester.query("UPDATE students SET J = 'vasya';")
    assert test == "NOT ERROR"
    test = tester.query("UPDATE students SET J = 'petya';")
    assert test == "NOT ERROR"
    test = tester.query("UPDATE students SET J = 123;")
    assert test == "NOT ERROR"
    test = tester.query("UPDATE students SET I = 123;")
    assert test == "NOT ERROR"
    test = tester.query("UPDATE students SET I = 'vasya';")
    assert test == "ERROR"
    test = tester.query("UPDATE users SET I = 1;")
    assert test == "ERROR"
    test = tester.query("UPDATE students SET I=1, J='vasya';")
    assert test == "NOT ERROR"
    test = tester.query("UPDATE students SET K=1, J='vasya';")
    assert test == "ERROR"


def test_update_2():
    test = tester.query("UPDATEE students SET I=1, J='vasya';")
    assert test == "ERROR"
    test = tester.query("UPDATE students  I=1, J='vasya';")
    assert test == "ERROR"
    test = tester.query("UPDATE SET students I=1, J='vasya';")
    assert test == "ERROR"
    test = tester.query("UPDATEstudentsSET I=1, J='vasya';")
    assert test == "ERROR"
    test = tester.query("UPDATE UPDATE students SET I=1, J='vasya';")
    assert test == "ERROR"
    test = tester.query("UPDATE students SET SET I=1, J='vasya';")
    assert test == "ERROR"
    test = tester.query("UPDATE '' SET;")
    assert test == "ERROR"
    test = tester.query("UPDATE students;")
    assert test == "ERROR"


def test_update_3():
    test = tester.query("UPDATE students SET I=1 WHERE I = 1;")
    assert test == "NOT ERROR"
    test = tester.query("UPDATE students SET I=2 WHERE I = 1;")
    assert test == "NOT ERROR"
    test = tester.query("UPDATE students SET I=1 WHERE I = 2;")
    assert test == "NOT ERROR"
    test = tester.query("UPDATE students SET J='st' WHERE J = 'vasya';")
    assert test == "NOT ERROR"
    test = tester.query("UPDATE students SET J='int' WHERE J = 'vasya';")
    assert test == "ERROR"
    test = tester.query("UPDATE students SET I=1 WHERE J = 'vasya';")
    assert test == "NOT ERROR"


def test_delete_1():
    test = tester.query("DELETE FROM students WHERE I=1;")
    assert test == "NOT ERROR"
    test = tester.query("DELETE FROM students WHERE I=1;")
    assert test == "ERROR"
    test = tester.query("DELETE FROM users WHERE I=1;")
    assert test == "ERROR"
    test = tester.query("DELETE FROM students WHERE K=1;")
    assert test == "ERROR"
    test = tester.query("DELETE FROM students;")
    assert test == "ERROR"


def test_delete_2():
    test = tester.query("DELETEFROM students;")
    assert test == "ERROR"
    test = tester.query("DELETE FROM students x;")
    assert test == "ERROR"
    test = tester.query("DELETE FROM WHERE;")
    assert test == "ERROR"
    test = tester.query("DELETE students;")
    assert test == "ERROR"
    test = tester.query("DELETE")
    assert test == "ERROR"


def test_select_1():
    test = tester.query("SELECT * FROM students;")
    assert test == "NOT ERROR"
    test = tester.query("SELECT * FROM users;")
    assert test == "ERROR"
    test = tester.query("INSERT INTO students VALUES (1, 'admin');")
    assert test == "NOT ERROR"
    test = tester.query("SELECT * FROM students WHERE I = 1;")
    assert test == "NOT ERROR"
    test = tester.query("SELECT * FROM students WHERE I = *;")
    assert test == "ERROR"
    test = tester.query("SELECT *, I FROM students WHERE I = 1;")
    assert test == "NOT ERROR"
    test = tester.query("SELECT I, * FROM students WHERE I = 1;")
    assert test == "ERROR"
    test = tester.query("SELECT * FROM students WHERE;")
    assert test == "ERROR"

def test_select_2():
    test = tester.query("SELECT  FROM students WHERE I = 1;")
    assert test == "ERROR"
    test = tester.query("SELECT students WHERE I = 1;")
    assert test == "ERROR"
    test = tester.query("* FROM students WHERE I = 1;")
    assert test == "ERROR"
    test = tester.query("SELECT * FROM students WHERE I = 1;")
    assert test == "NOT ERROR"
    test = tester.query("SELECT * SELECT * FROM students WHERE I = 1;")
    assert test == "ERROR"
    test = tester.query("SELECT * FROM students FROM students WHERE I = 1;")
    assert test == "ERROR"
    test = tester.query("SELECTT * FROMM students WHERE I = 1;")
    assert test == "ERROR"
    test = tester.query("SELECT *I FROM students WHERE I = 1;")
    assert test == "ERROR"
    test = tester.query("            SELECT * FROM students WHERE;")
    assert test == "ERROR"

    # temp = (pars.build_tree("DELETE FROM tv_series WHERE id = 4;"))


