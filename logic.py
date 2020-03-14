import SQL_parser.SQL_parser as pars
import preprocessor as pre
import Result


class Logic:

    def __init__(self, db_filename):
        self.pr = pre.Preprocessor(db_filename)

    def query(self, sql_request):
        request = pars.build_tree(sql_request)
        if type(request) is Result.Result:
            return request
        try:
            if request.type.lower() == "create":
                return self.pr.create_table(request.name, request.fields)
            elif request.type.lower() == "show":
                return self.pr.show_create_table(request.name)
            elif request.type.lower() == "drop":
                return self.pr.drop_table(request.name)
            elif request.type.lower() == "tree selects":
                return self.pr.tree_selects(request.tree)
            elif request.type.lower() == "insert":
                return self.pr.insert(request.insert.name, request.insert.fields, request.insert.values)
            elif request.type.lower() == "update":
                return self.pr.update(request.name, request.fields, request.values, request.condition)
            elif request.type.lower() == "delete":
                return self.pr.delete(request.name, request.condition)
        except Exception as ex:
            print(ex)

#temp.query("CREATE TABLE VADICS (id int, name str);")
#temp.query("SHOW CREATE TABLE VADICS;")
#temp.query("SELECT * FROM VADICS;")
#print(pars.build_tree("INSERT INTO VADICS VALUES (1.1, 1123);").insert.values)
#temp.query("SELECT * FROM VADICS;")
#temp.query("INSERT INTO VADICS VALUES (2, admin);")
#temp.query("SELECT * FROM VADICS;")
#temp.query("UPDATE VADICS SET name = notadmin;")
#temp.query("SELECT * FROM VADICS;")
#temp.query("DELETE FROM VADICS WHERE id = 1;")
#temp.query("SELECT * FROM VADICS;")
#print(temp.query("DELETE FROM VADIC WHERE id = 1*2 + 3/4 - 5*(6+7);"))
#print(pars.build_tree("DELETE FROM VADIC WHERE id = ((2 + 2) + 4 + (6 + (32 + 2882 + id)));").condition)
#print(temp.query("select * from vadic where id > 1;").str_for_print)
#sql_request1 = "select * from vadic join vadic1 ON vadic.id = vadic1.id where id > 1 UNION select * from vadic join vadic1 ON vadic.id = vadic1.id where id > 1;"
#temp = pars.build_tree(sql_request1)
