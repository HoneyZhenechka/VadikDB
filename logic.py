import SQL_parser.SQL_parser as pars
import preprocessor as pre
import Result


class Logic:

    def __init__(self, db_filename):
        self.pr = pre.Preprocessor(db_filename)

    def begin_transaction(self):
        self.pr.is_transaction = True

    def rollback_transaction(self):
        for table_index in self.pr.transactions:
            self.pr.db.tables[table_index].rollback_transaction(self.pr.transactions[table_index])
        self.pr.transactions = {}
        self.pr.is_transaction = False

    def end_transaction(self):
        for table_index in self.pr.transactions:
            self.pr.db.tables[table_index].end_transaction(self.pr.transactions[table_index])
        self.pr.transactions = {}
        self.pr.is_transaction = False

    def query(self, sql_request):
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
                self.begin_transaction()
            elif request.type.lower() == "rollback":
                self.rollback_transaction()
            elif request.type.lower() == "end":
                self.end_transaction()
        except Exception as ex:
            print(ex)

#request = ("SELECT * FROM FIRST;")
#request = ("SELECT * FROM FIRST JOIN SELECT * FROM SECOND;")
#request = ("(SELECT * FROM FIRST JOIN (SELECT * FROM SECOND1 JOIN THIRD) AS T2) UNION (SELECT * FROM FIRST1 JOIN SECOND2);")
#request = ("SELECT * FROM FIRST JOIN SECOND ON FIRST.id=SECOND.id;")
#request = ("SELECT * FROM FIRST JOIN SELECT * FROM SECOND AS S ON FIRST.id=SECOND.id;")
#request = ("SELECT * FROM FIRST JOIN (SELECT * FROM SECOND JOIN THIRD) AS S JOIN THIRD;")
#request = ("SELECT * FROM FIRST JOIN (SELECT * FROM SECOND JOIN THIRD) AS T2 ON FIRST.id=SECOND.id;")
#temp = pars.build_tree(request)
#temp1 = 1