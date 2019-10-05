import SQL_parser
import engine


class Logic:
    db = engine.DatabaseManager("db")

    def __init__(self):
        pass

    def query(self, sql_request):
        tree = SQL_parser.init_parser(sql_request)
        type = "NOT ERROR"
        if tree["type"] == "create":
            type = self.db.create_table(tree["name"], tree["fields"])
        if tree["type"] == "show":
            type = self.db.show_create_table(tree["name"])
        if tree["type"] == "drop":
            type = self.db.drop_table(tree["name"])
        if (tree["type"] == "Error" or type == "ERROR"):
            return "Error"
        return "Not Error"