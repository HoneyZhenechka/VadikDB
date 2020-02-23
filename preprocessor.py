import exception
import engine.db_structure as eng
import typing
import Result
import exception_for_client


class Preprocessor:

    def __init__(self, db_filename: str):
        self.table_count = 0
        if db_filename == "":
            self.db = eng.Database()
        else:
            self.db = eng.Database(False, db_filename)

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

    def solve_expression(self, root, row) -> int:
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
                if not (root.getRootVal().name in row.fields_values_dict):
                    value = root.getRootVal().name
                else:
                    value = row.fields_values_dict[root.getRootVal().name]
        return value

    def solve_comparison(self, root, row) -> bool:
        if root.getRootVal() == '>':
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

    def solve_condition(self, root, row) -> bool:
        if root == True:
            return True
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
            result.append(field)
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

    @staticmethod
    def are_rows_equal(first_row, second_row) -> bool:
        if len(first_row[0]) != len(second_row[0]):
            return False
        for i in range(len(first_row[0])):
            if first_row[0][i] != second_row[0][i]:
                return False
        for field in first_row[0]:
            if first_row[1].fields_values_dict[field] != second_row[1].fields_values_dict[field]:
                return False
        return True

    def union(self, first_table, second_table, is_all=False):
        if len(first_table[0]) != len(second_table[0]):
            return Result.Result(True, exception_for_client.DBExceptionForClient().DifferentNumberOfColumns())
        rows = [first_table[0], []]
        for first_row in first_table[1]:
            for second_row in second_table[1]:
                if self.are_rows_equal([first_table[0], first_row], [second_table[0], second_row]):
                    rows[1].append(first_row)
                    if is_all:
                        rows[1].append(second_row)
                else:
                    rows[1].append(first_row)
                    rows[1].append(second_row)
        return rows

    def intersect(self, first_table, second_table):
        rows = [first_table[0], []]
        for first_row in first_table[1]:
            for second_row in second_table[1]:
                if self.are_rows_equal([first_table[0], first_row], [second_table[0], second_row]):
                    rows[1].append(first_row)
        return rows

    def join(self, first_table, second_table):
        pass

    def left_outer_join(self, first_table, second_table):
        pass

    def right_outer_join(self, first_table, second_table):
        pass

    def get_values_with_expression(self, name: str, values: list, row, fields=()) -> list:
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
                new_values.append(values[i].name)
            else:
                return []
        return new_values

    def get_values(self, name: str, values: list, fields=()) -> list:
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
                values[i] = values[i].name
            else:
                return []
        return values

    def create_table(self, name: str, fields: list = ()) -> Result:
        correct_fields = self.get_correct_fields(fields)
        if not(type(correct_fields) is dict):
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

    def solve_tree_selects(self, root):
        el = root.getRootVal()
        if type(root) is Result.Result:
            return root
        elif el.type == "union":
            return self.union(self.solve_tree_selects(root.getLeftChild()),
                              self.solve_tree_selects(root.getRightChild()), el.is_all)
        elif el.type == "intersect":
            return self.intersect(self.solve_tree_selects(root.getLeftChild()),
                                  self.solve_tree_selects(root.getRightChild()))
        elif el.type == "join":
            if el.form == "":
                return self.join(self.solve_tree_selects(root.getLeftChild()),
                                 self.solve_tree_selects(root.getRightChild()))
            elif el.form.upper() == "LEFT OUTER":
                return self.left_outer_join(self.solve_tree_selects(root.getLeftChild()),
                                            self.solve_tree_selects(root.getRightChild()))
            elif el.form.upper() == "RIGHT OUTER":
                return self.right_outer_join(self.solve_tree_selects(root.getLeftChild()),
                                             self.solve_tree_selects(root.getRightChild()))
        elif el.type == "select":
            return self.select(el.select.name, el.select.fields, el.select.isStar, el.condition)

    def tree_selects(self, tree):
        fields_and_rows = self.solve_tree_selects(tree)
        if type(fields_and_rows) is Result.Result:
            return fields_and_rows
        result = "\n| "
        for field in fields_and_rows[0]:
            result += str(field) + " | "
        result += "\n"
        for row in fields_and_rows[1]:
            result += "| "
            for field in row.fields_values_dict:
                result += str(row.fields_values_dict[field]) + " | "
            result += "\n"
        return Result.Result(False, result)

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
            rows = self.db.tables[table_index].select(fields, rows)
            return fields, rows

    def insert(self, name: str, fields: list, values: list):
        if not self.is_table_exists(name):
            return Result.Result(True, exception_for_client.DBExceptionForClient().TableNotExists(name))
        elif type(self.is_fields_exist(name, fields)) is str:
            return Result.Result(True, exception_for_client.DBExceptionForClient().FieldNotExists(self.is_fields_exist(name, fields)))
        else:
            new_values = self.get_values(name, values, fields)
            if not len(new_values):
                return Result.Result(True, exception_for_client.DBExceptionForClient().InvalidDataType())
            table_index = self.get_table_index(name)
            if len(fields) == 0:
                for i in range(len(values)):
                    fields.append(self.db.tables[table_index].fields[i])
            if len(new_values) != 0:
                self.db.tables[table_index].insert(fields, new_values)
            return Result.Result(False)

    def update(self, name: str, fields: list, values: list, condition):
        if not self.is_table_exists(name):
            return Result.Result(True, exception_for_client.DBExceptionForClient().TableNotExists(name))
        elif type(self.is_fields_exist(name, fields)) is str:
            return Result.Result(True, exception_for_client.DBExceptionForClient().FieldNotExists(self.is_fields_exist(name, fields)))
        else:
            table_index = self.get_table_index(name)
            rows = []
            new_values = []
            for block in self.db.tables[table_index].iter_blocks():
                for row in block.iter_rows():
                    if row.status == 1:
                        if self.solve_condition(condition, row):
                            temp = self.get_values_with_expression(name, values, row, fields)
                            if len(temp) == 0:
                                return Result.Result(True, exception_for_client.DBExceptionForClient().InvalidDataType())
                            new_values.append(temp)
                            rows.append(row)
            for i in range(len(fields)):
                fields[i] = fields[i].name
            sorted(rows, key=lambda row: row.transaction_end)
            self.db.tables[table_index].update(fields, new_values, rows)
            return Result.Result(False)

    def delete(self, name: str, condition):
        if not self.is_table_exists(name):
            return Result.Result(True, exception_for_client.DBExceptionForClient().TableNotExists(name))
        else:
            table_index = self.get_table_index(name)
            rows_indices = []
            for block in self.db.tables[table_index].iter_blocks():
                for row in block.iter_rows():
                    if row.status == 1:
                        if self.solve_condition(condition, row):
                            rows_indices.append(row.index_in_file)
            self.db.tables[table_index].delete(rows_indices)
            return Result.Result(False)
