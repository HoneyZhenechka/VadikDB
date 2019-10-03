import pytest
import logic

tester = logic.Logic()


def test_1():
    test = tester.query("create table students ( I INT , J CHAR(10) )")
    assert test == "Not Error"
    test1 = tester.query("create table VADIC ( I INT , J CHAR(10) )")
    assert test1 == "Not Error"
    test2 = tester.query("show create table VADIC")
    assert test2 == "Not Error"
    test4 = tester.query("drop table VADIC")
    assert test4 == "Not Error"


def test_2():
    test1 = tester.query("create table VADIC ( I INT , J CHAR(10) )")
    assert test1 == "Not Error"
    test2 = tester.query("show create table VADIC")
    assert test2 == "Not Error"
    test4 = tester.query("drop table VADIC")
    assert test4 == "Not Error"


def test_3():
    test = tester.query("create table VADIC ( I INT , J CHAR(10) )")
    test == "Not Error"
    test2 = tester.query("create table VADIC ( I INT , J CHAR(10) )")
    assert test2 == "Error"


def test_4():
    test = tester.query("create table ( I INT, J CHAR(10) )")
    assert test == "Error"
    test = tester.query("create table ( I IN, J CHA(10) )")
    assert test == "Error"
    test = tester.query("eate ble ( I IN, J CHA(10) )")
    assert test == "Error"


def test_5():
    test = tester.query("show table ( I INT, J CHAR(10) )")
    assert test == "Error"
    test = tester.query("create table ( I INT, J CHAR(10) )")
    assert test == "Error"
    test = tester.query("create table ( I INT, J CHAR(10) )")
    assert test == "Error"


def test_6():
    test = tester.query("create table VADIC ( I INT , J CHAR(10) )")
    assert test == 'Error'   #change

def test_7():
    test = tester.query("куевфывсмсы")
    assert test == "Error"
    test = tester.query("create table")
    assert test == "Error"
    test = tester.query("")
    assert test == "Error"

def test_8():
    test = tester.query("create table VADIC ( I INT , J CHAR(10) )")
    assert test == "Error"  #change

def test_9():
    test = tester.query("drop table")
    assert test == "Error"
    test = tester.query("drop table 123")
    assert test == "Error"
    test = tester.query("drop table create table")
    assert test == "Error"
    test = tester.query("drop table drop table")
    assert test == "Error"
    test = tester.query("droptable VADIC")
    assert test == "Error"


def test_10():
    test = tester.query("show create table 123")
    assert test == "Error"
    test = tester.query("show create table ")
    assert test == "Error"
    test = tester.query("show create table """)
    assert test == "Error"











