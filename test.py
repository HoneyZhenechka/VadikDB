class TestFramework:
    def __init__(self):
        pass

    queries_list_CREATE = [
        "",
        "CREATE TABLE students",
        "CREATE TABLE students",
        "CREATE TABLE table",
        "CREATE TABLE create",
        "CREATE TABLE drop",
        "CREATE TABLE CREATE TABLE CREATE",
        "CREAT TABLE test1",
        "REATE TABLE test2",
        "CREATE ABLE test3",
        "CREATE TABL test4"
        "TABLE CREATE test5",
        "TABLE",
        "CREATE",
        "CREATE CREATE",
        "TABLE TABLE",
        "NULL",
        "TABLECREATE test7",
        "CREATETABLE test6",
    ]

    __queries_list_DROP = [
        "DROP TABLE drop1",
        "DROP TABLE  ",
        "DROP DROP",
        "TABLE DROP",
        "TABLE DROP drop2",
        ""
    ]

    __queries_list_SHOW_CREATE = [

    ]

    def check(self):
        for query in self.__queries_list_CREATE:
            print(query)
            # execute query here compare results


a = TestFramework()
a.check()