import SQL_parser.SQL_parser as pars
import preprocessor as pre
import Result
import exception_for_client


class User:

    def __init__(self, user_index):
        self.user_index = user_index
        self.is_transaction = False
        self.transactions = {}


class Logic:

    def __init__(self, db_filename):
        self.pr = pre.Preprocessor(db_filename)

    def begin_transaction(self, user_index):
        user = self.pr.get_user(user_index)
        user.is_transaction = True
        return Result.Result(False, "")

    def rollback_transaction(self, user_index):
        user = self.pr.get_user(user_index)
        if not user.is_transaction:
            return Result.Result(True, exception_for_client.DBExceptionForClient().TransactionNotDefined(user_index))
        for table_index in user.transactions:
            self.pr.db.tables[table_index].rollback_transaction(user.transactions[table_index])
            self.pr.db.tables[table_index].is_locked = False
        user.transactions = {}
        user.is_transaction = False
        return Result.Result(False, "")

    def end_transaction(self, user_index):
        user = self.pr.get_user(user_index)
        if not user.is_transaction:
            return Result.Result(True, exception_for_client.DBExceptionForClient().TransactionNotDefined(user_index))
        for table_index in user.transactions:
            self.pr.db.tables[table_index].end_transaction(user.transactions[table_index])
            self.pr.db.tables[table_index].is_locked = False
        user.transactions = {}
        user.is_transaction = False
        return Result.Result(False, "")

    def check_user(self, user_index):
        for user in self.pr.users:
            if user.user_index == user_index:
                return False
        return True

    def add_user(self, user_index):
        self.pr.users.append(User(user_index))

    def query(self, sql_request, user_index=0):

        if self.check_user(user_index):
            self.add_user(user_index)
        self.pr.current_user_index = user_index

        request = pars.build_tree(sql_request)
        if type(request) is Result.Result:
            return request
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
                return self.begin_transaction(user_index)
            elif request.type.lower() == "rollback":
                return self.rollback_transaction(user_index)
            elif request.type.lower() == "end":
                return self.end_transaction(user_index)
        except Exception as ex:
            print(ex)
