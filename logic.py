import SQL_parser
import engine


class Logic:
    #db = engine.DBManager("db")

    def __init__(self):
        pass

    def query(self, sql_request):
        tree = SQL_parser.build_tree(sql_request)
        #if tree["type"] == "create":
            #self.db.create_table(tree["name"], tree["fields"])
        #if tree["type"] == "show":
            #self.db.show_create_table(tree["name"])
        #if tree["type"] == "drop":
            #self.db.drop_table(tree["name"])


temp = (SQL_parser.build_tree("CREATE TABLE VADIC(id INT, name str);"))
print(temp.type, temp.values)
#temp = (SQL_parser.build_tree("SHOW CREATE TABLE VADIC;"))
#print(temp.type, temp.name)
#temp = (SQL_parser.build_tree("DROP TABLE VADIC;"))
#print(temp.type, temp.name)
#temp = (SQL_parser.build_tree("SELECT id, name FROM users;"))
#print(temp.type, temp.select.name, temp.select.fields, temp.select.condition)
#temp = (SQL_parser.build_tree("SELECT id, name FROM users WHERE admin = lax;"))
#print(temp.type, temp.select.name, temp.select.fields, temp.select.condition)
#temp = (SQL_parser.build_tree("INSERT INTO users VALUES (1, 'admin', '123');"))
#print(temp.type, temp.insert.name, temp.insert.values)
#temp = (SQL_parser.build_tree("INSERT INTO users (id, name, status) VALUES (1, 'admin', '123');"))
#print(temp.type, temp.insert.name, temp.insert.fields, temp.insert.values)
#temp = (SQL_parser.build_tree("UPDATE tv_series SET genre = 'drama';"))
#print(temp.type, temp.name, temp.set, temp.condition)
#temp = (SQL_parser.build_tree("UPDATE tv_series SET genre = 'drama' WHERE name = 'GameofThrones';"))
#print(temp.type, temp.name, temp.set, temp.condition)
#temp = (SQL_parser.build_tree("DELETE FROM tv_series;"))
#print(temp.type, temp.name, temp.condition)
#temp = (SQL_parser.build_tree("DELETE FROM tv_series WHERE id = 4;"))
#print(temp.type, temp.name, temp.condition)