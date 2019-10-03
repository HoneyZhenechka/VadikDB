import SQL_parser
import engine


class Logic:
    db = engine.DBManager()

    def __init__(self):
        self.db.create_db("testDB")

    def query(self, sql_request):
        tree = SQL_parser.init_parser(sql_request)
        if tree["type"] == "create":
            self.db.create_table(tree["name"], tree["fields"])
        if tree["type"] == "show":
            self.db.show_create_table(tree["name"])
        if tree["type"] == "drop":
            self.db.drop_table(tree["name"])
        return tree["type"]