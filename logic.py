import SQL_parser.SQL_parser as pars
import engine


class Logic:
    db = engine.DatabaseManager("db")

    def __init__(self):
        pass

    def query(self, sql_request):
        tree = pars.build_tree(sql_request)
        try:
            if tree.type.lower() == "create":
                type = self.db.create_table(tree.name, tree.values)
            elif tree.type.lower() == "show":
                type = self.db.show_create_table(tree.name)
            elif tree.type.lower() == "drop":
                type = self.db.drop_table(tree.name)
            elif tree.type.lower() == "select":
                type = self.db.select(tree.select.name, tree.select.fields, True, tree.select.isStar, tree.condition[0], tree.condition[1])
            elif tree.type.lower() == "insert":
                type = self.db.insert(tree.insert.name, tree.insert.fields, tree.insert.values)
            elif tree.type.lower() == "update":
                type = self.db.update(tree.name, tree.fields, tree.values, tree.condition[0], tree.condition[1])
            elif tree.type.lower() == "delete":
                type = self.db.delete(tree.name, tree.condition[0], tree.condition[1])
        except:
            type = "ERROR"
        return type

#request = ""
#obj = Logic()
#while request != "exit":
    #request = input()
    #if (request != "exit"):
        #obj.query(request)

#print(check.query("SHOW TABLE VADIC;"))
#temp = (pars.build_tree("CREATE TABLE VADIC(id INT, name str);"))
#print(temp.type, temp.values)
#temp = (pars.build_tree("SHOW CREATE TABLE VADIC;"))
#print(temp.type, temp.name)
#temp = (pars.build_tree("DROP TABLE VADIC;"))
#print(temp.type, temp.name)
#temp = (pars.build_tree("SELECT * FROM users;"))
#print(temp.type, temp.select.name, temp.select.fields, temp.condition, temp.select.isStar)
#temp = (pars.build_tree("SELECT *, agb FROM users;"))
#print(temp.type, temp.select.name, temp.select.fields, temp.condition, temp.select.isStar)
#temp = (pars.build_tree("SELECT * FROM users WHERE admin = lah;"))
#print(temp.type, temp.select.name, temp.select.fields, temp.condition, temp.select.isStar)
#temp = (pars.build_tree("SELECT id, name FROM users WHERE admin = lax;"))
#print(temp.type, temp.select.name, temp.select.fields, temp.condition, temp.select.isStar)
#temp = (pars.build_tree("INSERT INTO users VALUES (1, 'admin', '123');"))
#print(temp.type, temp.insert.name, temp.insert.values)
#temp = (pars.build_tree("INSERT INTO users (id, name, status) VALUES (1, 'admin', '123');"))
#print(temp.type, temp.insert.name, temp.insert.fields, temp.insert.values)
#temp = (pars.build_tree("UPDATE tv_series SET genre = 'drama';"))
#print(temp.type, temp.name, temp.fields, temp.values, temp.condition)
#temp = (pars.build_tree("UPDATE tv_series SET genre = 'drama' WHERE name = 'GameofThrones';"))
#print(temp.type, temp.name, temp.fields, temp.values, temp.condition)
#temp = (pars.build_tree("DELETE FROM tv_series;"))
#print(temp.type, temp.name, temp.condition)
#temp = (pars.build_tree("DELETE FROM tv_series WHERE id = 4;"))
#print(temp.type, temp.name, temp.condition)