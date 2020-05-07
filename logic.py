import SQL_parser.SQL_parser as pars
import preprocessor as pre
import Result


class Logic:

    def __init__(self, db_filename):
        self.pr = pre.Preprocessor(db_filename)
        self.is_transaction = False
        self.transactions = {}

    def begin_transaction(self):
        self.is_transaction = True

    def begin_table_transaction(self, tree):
        table_name = ""
        if tree.type.lower() == "insert":
            if self.pr.is_table_exists(tree.insert.name):
                table_name = tree.insert.name
        elif tree.type.lower() == "delete" or tree.type.lower() == "update":
            if self.pr.is_table_exists(tree.name):
                table_name = tree.name
        if not table_name:
            return
        table_index = self.pr.get_table_index(table_name)
        if table_index in self.transactions:
            return
        transaction_index = self.pr.db.tables[table_index].start_transaction()
        self.transactions[table_index] = transaction_index

    def rollback_transaction(self):
        for table_index in self.transactions:
            self.pr.db.tables[table_index].rollback_transaction(self.transactions[table_index])
        self.transactions = {}
        self.is_transaction = False

    def end_transaction(self):
        for table_index in self.transactions:
            self.pr.db.tables[table_index].end_transaction(self.transactions[table_index])
        self.transactions = {}
        self.is_transaction = False

    def query(self, sql_request):
        request = pars.build_tree(sql_request)
        if type(request) is Result.Result:
            return request
        if self.is_transaction:
            self.begin_table_transaction(request)
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
            elif request.type.lower() == "begin":
                self.begin_transaction()
            elif request.type.lower() == "rollback":
                self.rollback_transaction()
            elif request.type.lower() == "end":
                self.end_transaction()
        except Exception as ex:
            print(ex)
