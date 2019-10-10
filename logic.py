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
                isError = self.db.create_table(tree.name, tree.values)
            elif tree.type.lower() == "show":
                isError = self.db.show_create_table(tree.name)
            elif tree.type.lower() == "drop":
                isError = self.db.drop_table(tree.name)
            elif tree.type.lower() == "select":
                isError = self.db.select(tree.select.name, tree.select.fields, True, tree.select.isStar, tree.condition[0], tree.condition[1])
            elif tree.type.lower() == "insert":
                isError = self.db.insert(tree.insert.name, tree.insert.fields, tree.insert.values)
            elif tree.type.lower() == "update":
                isError = self.db.update(tree.name, tree.fields, tree.values, tree.condition[0], tree.condition[1])
            elif tree.type.lower() == "delete":
                isError = self.db.delete(tree.name, tree.condition[0], tree.condition[1])
        except:
            isError = "ERROR"
        if isError != "ERROR":
            isError = "NOT ERROR"
        return isError
