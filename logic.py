import SQL_parser
import engine

def query(str):
    tree = SQL_parser.init_parser(str)
    if (tree.type == "create"):
        db.create_table(tree["name"], tree.fields)
    if (tree.type == "show"):
        db.show_create_table(tree["name"])
    if (tree.type == "drop"):
        db.drop_table(tree["name"])


db = engine.DBManager()
db.create_db("vadikcDB")

request = ""

while (request != "exit"):
    request = input()
    if (request != "exit"):
        query(request)
