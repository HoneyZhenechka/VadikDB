import exception
import engine.db_structure as eng
import typing
import Result
import exception_for_client


class Row:

    def __init__(self, fields_values_dict):
        self.fields_values_dict = fields_values_dict


class Table:

    def __init__(self, fields, rows):
        self.type = "table"
        self.fields = fields
        self.rows = rows


class Preprocessor:

    def __init__(self, db_filename: str):
        self.first_table_name = ""
        self.second_table_name = ""
        self.table_count = 0
        self.users = []
        self.current_user_index = 0
        self.locked_tables = {}
        if db_filename == "":
            self.db = eng.Database()
        else:
            self.db = eng.Database(False, db_filename)

    def get_user(self, user_index):
        for user in self.users:
            if user.user_index == user_index:
                return user

    def begin_table_transaction(self, table_index):
        user = self.get_user(self.current_user_index)
        if table_index in user.transactions:
            return
        transaction_index = self.db.tables[table_index].start_transaction()
        self.db.tables[table_index].is_locked = True
        self.locked_tables[table_index] = self.current_user_index
        user.transactions[table_index] = transaction_index

    @staticmethod
    def get_correct_fields(fields=()) -> dict or list:
        correct_fields = {}
        for field in fields:
            if field[0] in correct_fields:
                return str(field[0] + " " + field[1])
            else:
                correct_fields[field[0]] = field[1]
        return correct_fields

    def is_fields_exist(self, name: str, fields=()) -> str or bool:
        for field in fields:
            if not field.name in self.db.tables[self.get_table_index(name)].fields:
                return field.name
        return True

    def is_table_exists(self, name: str) -> bool:
        for table in self.db.tables:
            if table.name == name:
                return True
        return False

    def get_table_index(self, name: str) -> int:
        for i in range(len(self.db.tables)):
            if name == self.db.tables[i].name:
                return i

    def solve_expression(self, root, row) -> int or Result.Result:
        if root.getRootVal() == '+':
            value = self.solve_expression(root.getLeftChild(), row) + self.solve_expression(root.getRightChild(), row)
        elif root.getRootVal() == '-':
            value = self.solve_expression(root.getLeftChild(), row) - self.solve_expression(root.getRightChild(), row)
        elif root.getRootVal() == '*':
            value = self.solve_expression(root.getLeftChild(), row) * self.solve_expression(root.getRightChild(), row)
        elif root.getRootVal() == '/':
            value = self.solve_expression(root.getLeftChild(), row) / self.solve_expression(root.getRightChild(), row)
        else:
            try:
                value = float(root.getRootVal().name)
            except:
                if root.getRootVal().is_str:
                    value = row.fields_values_dict[root.getRootVal().name]
                else:
                    if root.getRootVal().name in row.fields_values_dict:
                        value = row.fields_values_dict[root.getRootVal().name]
                    else:
                        return Result.Result(True, exception_for_client.DBExceptionForClient().FieldNotExists(root.getRootVal().name))
        return value

    def solve_comparison(self, root, row) -> bool or Result.Result:
        if type(root) == Result.Result:
            return root
        elif root.getRootVal() == '>':
            return self.solve_expression(root.getLeftChild(), row) > self.solve_expression(root.getRightChild(), row)
        elif root.getRootVal() == '<':
            return self.solve_expression(root.getLeftChild(), row) < self.solve_expression(root.getRightChild(), row)
        elif root.getRootVal() == '>=':
            return self.solve_expression(root.getLeftChild(), row) >= self.solve_expression(root.getRightChild(), row)
        elif root.getRootVal() == '<=':
            return self.solve_expression(root.getLeftChild(), row) <= self.solve_expression(root.getRightChild(), row)
        elif root.getRootVal() == '!=':
            return self.solve_expression(root.getLeftChild(), row) != self.solve_expression(root.getRightChild(), row)
        elif root.getRootVal() == '=':
            return self.solve_expression(root.getLeftChild(), row) == self.solve_expression(root.getRightChild(), row)

    def solve_condition(self, root, row) -> bool or Result.Result:
        if root == True:
            return True
        if type(root) == Result.Result:
            return root
        elif root.getRootVal().upper() == 'AND':
            return self.solve_comparison(root.getLeftChild(), row) and self.solve_comparison(root.getRightChild(), row)
        elif root.getRootVal().upper() == 'OR':
            return self.solve_comparison(root.getLeftChild(), row) or self.solve_comparison(root.getRightChild(), row)
        else:
            return self.solve_comparison(root, row)

    def build_fields(self, fields: list, is_star: bool, table_index: int) -> list:
        result = []
        if is_star:
            for field in self.db.tables[table_index].fields:
                result.append(field)
        for field in fields:
            result.append(field.name)
        return result

    def get_types(self, name: str, values: list, fields=()) -> list:
        table_index = self.get_table_index(name)
        types = []

        if len(fields) == 0:
            types = self.db.tables[table_index].types
            if len(values) != len(types):
                return []
        else:
            for field in fields:
                for index_of_field in range(len(self.db.tables[table_index].fields)):
                    if field.name == self.db.tables[table_index].fields[index_of_field]:
                        types.append(self.db.tables[table_index].types[index_of_field])
        return types

    def get_values_with_expression(self, name: str, values: list, row, fields=()) -> list or Result.Result:
        types = self.get_types(name, values, fields)
        if len(types) == 0:
            return []
        new_values = []

        for i in range(len(values)):
            if types[i].name == "float":
                try:
                    new_values.append(float(self.solve_expression(values[i], row)))
                except:
                    return []
            elif types[i].name == "int":
                try:
                    new_values.append(int(self.solve_expression(values[i], row)))
                except:
                    return []
            elif types[i].name == "bool":
                if self.solve_expression(values[i], row) == "False":
                    new_values.append(False)
                elif self.solve_expression(values[i], row) == "True":
                    new_values.append(True)
                else:
                    return []
            elif types[i].name == "str":
                if values[i].type == "field":
                    if values[i].is_str:
                        new_values.append(values[i].name)
                    else:
                        return Result.Result(True,
                                             exception_for_client.DBExceptionForClient().WrongFieldType(values[i].name))
                else:
                    new_values.append(values[i].name)
            else:
                return []
        return new_values

    def get_values(self, name: str, values: list, fields=()) -> list or Result.Result:
        types = self.get_types(name, values, fields)
        if len(types) == 0:
            return []

        for i in range(len(values)):
            if types[i].name == 'float':
                try:
                    values[i] = float(values[i].name)
                except:
                    return []
            elif types[i].name == "int":
                try:
                    values[i] = int(values[i].name)
                except:
                    return []
            elif types[i].name == "bool":
                if values[i].name == "False":
                    values[i] = False
                elif values[i].name == "True":
                    values[i] = True
                else:
                    return []
            elif types[i].name == "str":
                if values[i].is_str:
                    values[i] = values[i].name
                else:
                    return Result.Result(True,
                                         exception_for_client.DBExceptionForClient().WrongFieldType(values[i].name))
            else:
                return []
        return values

    def create_table(self, name: str, fields: list = ()) -> Result:
        correct_fields = self.get_correct_fields(fields)
        if not (type(correct_fields) is dict):
            return Result.Result(True, exception_for_client.DBExceptionForClient().DuplicateFields(correct_fields))
        elif self.is_table_exists(name):
            return Result.Result(True, exception_for_client.DBExceptionForClient().TableAlreadyExists(name))
        else:
            self.db.create_table(name, correct_fields)
            return Result.Result(False)

    def show_create_table(self, name: str) -> Result:
        if not self.is_table_exists(name):
            return Result.Result(True, exception_for_client.DBExceptionForClient().TableNotExists(name))
        else:
            table_index = self.get_table_index(name)
            return Result.Result(False, self.db.tables[table_index].show_create())

    def drop_table(self, name: str) -> Result:
        if not self.is_table_exists(name):
            return Result.Result(True, exception_for_client.DBExceptionForClient().TableNotExists(name))
        else:
            return Result.Result(False)

    @staticmethod
    def are_rows_equal(first_row, second_row) -> bool:
        if len(first_row[0]) != len(second_row[0]):
            return False
        for i in range(len(first_row[0])):
            if first_row[0][i] != second_row[0][i]:
                return False
        for i in range(len(first_row[0])):
            if first_row[1].fields_values_dict[i][1] != second_row[1].fields_values_dict[i][1]:
                return False
        return True

    def union(self, first_table: Table, second_table: Table, is_all=False) -> Table or Result.Result:
        if len(first_table.fields) != len(second_table.fields):
            return Result.Result(True, exception_for_client.DBExceptionForClient().DifferentNumberOfColumns())
        table = Table(first_table.fields, [])
        if is_all:
            for row in first_table.rows:
                table.rows.append(row)
            for row in second_table.rows:
                table.rows.append(row)
        else:
            for first_row in first_table.rows:
                is_unique = True
                for row in table.rows:
                    if self.are_rows_equal([table.fields, first_row], [table.fields, row]):
                        is_unique = False
                if is_unique:
                    table.rows.append(first_row)
            for second_row in second_table.rows:
                is_unique = True
                for row in table.rows:
                    if self.are_rows_equal([table.fields, second_row], [table.fields, row]):
                        is_unique = False
                if is_unique:
                    table.rows.append(second_row)
        return table

    def intersect(self, first_table: Table, second_table: Table) -> Table:
        table = Table(first_table.fields, [])
        for first_row in first_table.rows:
            for second_row in second_table.rows:
                if self.are_rows_equal([first_table.fields, first_row], [second_table.fields, second_row]):
                    table.rows.append(first_row)
                    break
        return table

    def solve_join(self, root) -> Table or Result.Result:
        join_condition = (root.getRootVal()).join_condition
        left_token = root.getLeftChild()
        right_token = root.getRightChild()

        if left_token.getRootVal().type != "select":
            left_table = self.solve_tree_selects(left_token)
            right_table = self.solve_tree_selects(right_token)
            return self.join(left_table, right_table, join_condition)
        else:
            if right_token.getRootVal().type == "named tree":
                right_table = right_token.getRootVal()
                right_table_name = right_table.name
                right_table = self.solve_tree_selects(right_table.tree.tree)
            else:
                if right_token.getRootVal().type == "name table":
                    right_table_name = right_token.getRootVal().name
                else:
                    right_table_name = right_token.getRootVal().select.name
                right_table = self.solve_tree_selects(right_token)
            left_table = left_token.getRootVal()

            if left_table.select.isStar:
                left_table = self.solve_tree_selects(left_token)
                return self.join(left_table, right_table, join_condition)

            fields = left_table.select.fields
            left_table.select.fields = []
            left_table.select.isStar = True
            left_table_name = left_table.select.name
            left_token.setRootVal(left_table)
            left_table = self.solve_tree_selects(left_token)
            return self.join(left_table, right_table, join_condition, fields, left_table_name, right_table_name)

    @staticmethod
    def check_required_fields(left_table: Table, right_table: Table,
                              fields: list, left_table_name:str, right_table_name:str) -> Result.Result:
        for field in fields:
            if field.type == "field":
                return Result.Result(True, exception_for_client.DBExceptionForClient().NoTableSpecified(field.name))
            if not (field.name_table in [left_table_name, right_table_name]):
                return Result.Result(True, exception_for_client.DBExceptionForClient().FieldNotExists(field.name))
            if not (field.name in left_table.fields) or not (field.name in right_table.fields):
                return Result.Result(True, exception_for_client.DBExceptionForClient().FieldNotExists(field.name))

    def join_rows(self, fields, first_row, second_row, first_table_name, second_table_name):
        fields_values = []
        for field in fields:
            if type(field) is dict:
                if field["name_table"] == first_table_name:
                    fields_values.append([field["name"], self.get_value_from_row(first_row, field["name"])])
                if field["name_table"] == second_table_name:
                    fields_values.append([field["name"], self.get_value_from_row(second_row, field["name"])])
            else:
                if field.name_table == first_table_name:
                    fields_values.append([field.name, self.get_value_from_row(first_row, field["name"])])
                if field.name_table == second_table_name:
                    fields_values.append([field.name, self.get_value_from_row(second_row, field["name"])])
        new_row = Row(fields_values)
        return new_row

    @staticmethod
    def join_fields(left_table: Table, right_table: Table, join_condition) -> list:
        fields = []
        for field in left_table.fields:
            fields.append({
                "name": field,
                "name_table": "left"
            })
        for field in right_table.fields:
            if join_condition.type == "using":
                is_exist = False
                for f in join_condition.fields:
                    if f.name == field:
                        is_exist = True
                if not (is_exist):
                    fields.append(
                        {
                            "name": field,
                            "name_table": "right"
                        }
                    )
            else:
                fields.append({
                            "name": field,
                            "name_table": "right"
                        })
        return fields

    @staticmethod
    def get_value_from_row(row, name):
        for field in row.fields_values_dict:
            if field[0] == name:
                return field[1]

    def join(self, left_table: Table, right_table: Table, join_condition,
             required_fields: list = [], left_table_name: str = "left", right_table_name: str = "right") -> Table or Result.Result:

        if type(left_table) is Result.Result:
            return left_table
        if type(right_table) is Result.Result:
            return right_table

        if len(required_fields) == 0:
            fields = self.join_fields(left_table, right_table, join_condition)
            table = Table(fields, [])
        else:
            table = Table(required_fields, [])
            temp = self.check_required_fields(left_table, right_table,
                                              table.fields, left_table_name, right_table_name)
            if type(temp) is Result.Result:
                return temp

        for first_row in left_table.rows:
            for second_row in right_table.rows:
                if join_condition.type == "undefined":
                    table.rows.append(self.join_rows(table.fields, first_row,
                                      second_row, left_table_name, right_table_name))
                elif join_condition.type == "on":
                    if (self.get_value_from_row(first_row, join_condition.first_field.name)
                            == self.get_value_from_row(second_row, join_condition.second_field.name)):
                        table.rows.append(self.join_rows(table.fields, first_row,
                                          second_row, left_table_name, right_table_name))
                elif join_condition.type == "using":
                    is_right_row = True
                    for field in join_condition.fields:
                        if self.get_value_from_row(first_row, field.name) == self.get_value_from_row(second_row, field.name):
                            is_right_row = False
                    if not is_right_row:
                        table.rows.append(self.join_rows(table.fields, first_row,
                                          second_row, left_table_name, right_table_name))
        for i in range(len(table.fields)):
            if type(table.fields[i]) is dict:
                table.fields[i] = table.fields[i]["name"]
            else:
                table.fields[i] = table.fields[i].name
        return table

    def solve_tree_selects(self, root):
        el = root.getRootVal()
        if type(root) is Result.Result:
            return root
        elif el.type == "tree selects":
            return self.solve_tree_selects(el.tree)
        elif el.type == "named tree":
            el.tree = self.solve_tree_selects(el.tree)
            return el
        elif el.type == "union":
            return self.union(self.solve_tree_selects(root.getLeftChild()),
                              self.solve_tree_selects(root.getRightChild()), el.is_all)
        elif el.type == "intersect":
            return self.intersect(self.solve_tree_selects(root.getLeftChild()),
                                  self.solve_tree_selects(root.getRightChild()))
        elif el.type == "join":
            return self.solve_join(root)
        elif el.type == "name table":
            return self.select(el.name, [], True, True)
        elif el.type == "select":
            return self.select(el.select.name, el.select.fields, el.select.isStar, el.condition)

    def tree_selects(self, tree):
        table = self.solve_tree_selects(tree)
        if type(table) is Result.Result:
            return table
        result = "\n| "
        for field in table.fields:
            result += str(field) + " | "
        result += "\n"
        for i in range(len(table.rows)):
            result += "| "
            for j in range(len(table.rows[i].fields_values_dict)):
                result += str(table.rows[i].fields_values_dict[j][1]) + " | "
            result += "\n"
        return Result.Result(False, result)

    @staticmethod
    def dict_to_list(dictionary: dict) -> list:
        result = []
        for field in dictionary:
            result.append([field, dictionary[field]])
        return result

    def select(self, name: str, fields: list, is_star: bool, condition):
        if not self.is_table_exists(name):
            return Result.Result(True, exception_for_client.DBExceptionForClient().TableNotExists(name))
        elif type(self.is_fields_exist(name, fields)) is str:
            return Result.Result(True, exception_for_client.DBExceptionForClient().FieldNotExists(self.is_fields_exist(name, fields)))
        else:
            table_index = self.get_table_index(name)
            rows = []
            for block in self.db.tables[table_index].iter_blocks():
                for row in block.iter_rows():
                    if row.status == 1:
                        if self.solve_condition(condition, row):
                            rows.append(row)
            fields = self.build_fields(fields, is_star, table_index)
            transaction_index = 0
            user = self.get_user(self.current_user_index)
            if user.is_transaction:
                self.begin_table_transaction(table_index)
                transaction_index = user.transactions[table_index]
            rows = self.db.tables[table_index].select(fields, rows, transaction_index)
            for i in range(len(rows)):
                rows[i] = Row(self.dict_to_list(rows[i].fields_values_dict))
            return Table(fields, rows)

    def insert(self, name: str, fields: list, values: list):
        if not self.is_table_exists(name):
            return Result.Result(True, exception_for_client.DBExceptionForClient().TableNotExists(name))
        elif type(self.is_fields_exist(name, fields)) is str:
            return Result.Result(True, exception_for_client.DBExceptionForClient().FieldNotExists(self.is_fields_exist(name, fields)))
        else:
            new_values = self.get_values(name, values, fields)
            if type(new_values) is Result.Result:
                return new_values
            if not len(new_values):
                return Result.Result(True, exception_for_client.DBExceptionForClient().InvalidDataType())
            table_index = self.get_table_index(name)
            if self.db.tables[table_index].is_locked:
                if not self.current_user_index == self.locked_tables[table_index]:
                    return Result.Result(True, "Table is locked")
            if len(fields) == 0:
                for i in range(len(values)):
                    fields.append(self.db.tables[table_index].fields[i])
            if len(new_values) != 0:
                transaction_index = 0
                user = self.get_user(self.current_user_index)
                if user.is_transaction:
                    self.begin_table_transaction(table_index)
                    transaction_index = user.transactions[table_index]
                self.db.tables[table_index].insert(fields, new_values, -1, transaction_index)
            return Result.Result(False)

    def update(self, name: str, fields: list, values: list, condition):
        if not self.is_table_exists(name):
            return Result.Result(True, exception_for_client.DBExceptionForClient().TableNotExists(name))
        elif type(self.is_fields_exist(name, fields)) is str:
            return Result.Result(True, exception_for_client.DBExceptionForClient().FieldNotExists(self.is_fields_exist(name, fields)))
        else:
            table_index = self.get_table_index(name)
            if self.db.tables[table_index].is_locked:
                if not self.current_user_index == self.locked_tables[table_index]:
                    return Result.Result(True, "Table is locked")
            rows = []
            new_values = []
            for block in self.db.tables[table_index].iter_blocks():
                for row in block.iter_rows():
                    if row.status == 1:
                        if self.solve_condition(condition, row):
                            temp = self.get_values_with_expression(name, values, row, fields)
                            if type(temp) == Result.Result:
                                return temp
                            if len(temp) == 0:
                                return Result.Result(True, exception_for_client.DBExceptionForClient().InvalidDataType())
                            new_values.append(temp)
                            rows.append(row)
            for i in range(len(fields)):
                fields[i] = fields[i].name
            sorted(rows, key=lambda row: row.transaction_end)
            transaction_index = 0
            user = self.get_user(self.current_user_index)
            if user.is_transaction:
                self.begin_table_transaction(table_index)
                transaction_index = user.transactions[table_index]
            self.db.tables[table_index].update(fields, new_values, rows, transaction_index)
            return Result.Result(False)

    def delete(self, name: str, condition):
        if not self.is_table_exists(name):
            return Result.Result(True, exception_for_client.DBExceptionForClient().TableNotExists(name))
        else:
            table_index = self.get_table_index(name)
            if self.db.tables[table_index].is_locked:
                if not self.current_user_index == self.locked_tables[table_index]:
                    return Result.Result(True, "Table is locked")
            rows_indices = []
            for block in self.db.tables[table_index].iter_blocks():
                for row in block.iter_rows():
                    if row.status == 1:
                        if self.solve_condition(condition, row):
                            rows_indices.append(row.index_in_file)
            transaction_index = 0
            user = self.get_user(self.current_user_index)
            if user.is_transaction:
                self.begin_table_transaction(table_index)
                transaction_index = user.transactions[table_index]
            self.db.tables[table_index].delete(rows_indices, transaction_index)
            return Result.Result(False)
