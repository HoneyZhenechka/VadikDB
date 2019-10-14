import SQL_parser.SQL_parser as pars
import preprocessor


class Logic:
    db = engine.DatabaseManager("db")

    def __init__(self):
        pass

    def query(self, sql_request):
        tree = pars.build_tree(sql_request)
        is_error = "NOT ERROR"
        try:
            if tree.type.lower() == "create":
                is_error = self.db.create_table(tree.name, tree.fields)
            elif tree.type.lower() == "show":
                is_error = self.db.show_create_table(tree.name)
            elif tree.type.lower() == "drop":
                is_error = self.db.drop_table(tree.name)
            elif tree.type.lower() == "select":
                is_error = self.db.select(tree.select.name, tree.select.fields, tree.select.isStar, tree.condition)
            elif tree.type.lower() == "insert":
                is_error = self.db.insert(tree.insert.name, tree.insert.fields, tree.insert.values)
            elif tree.type.lower() == "update":
                is_error = self.db.update(tree.name, tree.fields, tree.values, tree.condition)
            elif tree.type.lower() == "delete":
                is_error = self.db.delete(tree.name, tree.condition)
        except:
            is_error = "ERROR"
        return is_error
