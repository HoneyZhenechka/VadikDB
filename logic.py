import SQL_parser.SQL_parser as pars
import preprocessor as pre


class Logic:


    def __init__(self):
        self.pr = pre.preprocessor()

    def query(self, sql_request):
        tree = pars.build_tree(sql_request)
        try:
            if tree.type.lower() == "create":
                self.pr.create_table(tree.name, tree.fields)
            elif tree.type.lower() == "show":
                self.pr.show_create_table(tree.name)
            elif tree.type.lower() == "drop":
                self.pr.drop_table(tree.name)
            elif tree.type.lower() == "select":
                self.pr.select(tree.select.name, tree.select.fields, tree.select.isStar, tree.condition)
            elif tree.type.lower() == "insert":
                self.pr.insert(tree.insert.name, tree.insert.fields, tree.insert.values)
            elif tree.type.lower() == "update":
                self.pr.update(tree.name, tree.fields, tree.values, tree.condition)
            elif tree.type.lower() == "delete":
                self.pr.delete(tree.name, tree.condition)
        except:
            pass
