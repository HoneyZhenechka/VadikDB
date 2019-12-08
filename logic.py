import SQL_parser.SQL_parser as pars
import preprocessor as pre


class Logic:

    def __init__(self, db_filename):
        self.pr = pre.Preprocessor(db_filename)

    def query(self, sql_request):
        tree = pars.build_tree(sql_request)
        try:
            if tree.type.lower() == "create":
                return self.pr.create_table(tree.name, tree.fields)
            elif tree.type.lower() == "show":
                return self.pr.show_create_table(tree.name)
            elif tree.type.lower() == "drop":
                return self.pr.drop_table(tree.name)
            elif tree.type.lower() == "select":
                return self.pr.select(tree.select.name, tree.select.fields, tree.select.isStar, tree.condition)
            elif tree.type.lower() == "insert":
                return self.pr.insert(tree.insert.name, tree.insert.fields, tree.insert.values)
            elif tree.type.lower() == "update":
                return self.pr.update(tree.name, tree.fields, tree.values, tree.condition)
            elif tree.type.lower() == "delete":
                return self.pr.delete(tree.name, tree.condition)
        except:
            pass

#temp = Logic()
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