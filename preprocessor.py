import exception
import engine.db_structure as eng
import typing
import Result
import exception_for_client


class Row:

    def __init__(self, fields_values_dict):
        self.fields_values_dict = fields_values_dict


class Preprocessor:

    def __init__(self, db_filename: str):
        self.first_table_name = ""
        self.second_table_name = ""
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
                        return Result.Result(True, exception_for_client.DBExceptionForClient.FieldNotExists(root.getRootVal().name))
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
        for i in range(len(first_row[0])):
            if first_row[1].fields_values_dict[i][1] != second_row[1].fields_values_dict[i][1]:
                return False
        return True

    def union(self, first_table, second_table, is_all=False):
        if len(first_table[0]) != len(second_table[0]):
            return Result.Result(True, exception_for_client.DBExceptionForClient().DifferentNumberOfColumns())
        rows = [first_table[0], []]
        if is_all:
            for row in first_table[1]:
                rows[1].append(row)
            for row in second_table[1]:
                rows[1].append(row)
        else:
            for first_row in first_table[1]:
                is_unique = True
                for row in rows[1]:
                    if self.are_rows_equal([rows[0], first_row], [rows[0], row]):
                        is_unique = False
                if is_unique:
                    rows[1].append(first_row)
            for second_row in second_table[1]:
                is_unique = True
                for row in rows[1]:
                    if self.are_rows_equal([rows[0], second_row], [rows[0], row]):
                        is_unique = False
                if is_unique:
                    rows[1].append(second_row)
        return rows

    def intersect(self, first_table, second_table):
        rows = [first_table[0], []]
        for first_row in first_table[1]:
            for second_row in second_table[1]:
                if self.are_rows_equal([first_table[0], first_row], [second_table[0], second_row]):
                    rows[1].append(first_row)
        return rows

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
                    return Result.Result(True, exception_for_client.DBExceptionForClient().WrongFieldType(values[i].name))
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
        elif el.type == "select":
            return self.select(el.select.name, el.select.fields, el.select.isStar, el.condition,
                               el.join, el.right_table)

    def tree_selects(self, tree):
        fields_and_rows = self.solve_tree_selects(tree)
        if type(fields_and_rows) is Result.Result:
            return fields_and_rows
        result = "\n| "
        for field in fields_and_rows[0]:
            result += str(field) + " | "
        result += "\n"
        for i in range(len(fields_and_rows[1])):
            result += "| "
            for j in range(len(fields_and_rows[1][i].fields_values_dict)):
                result += str(fields_and_rows[1][i].fields_values_dict[j][1]) + " | "
            result += "\n"
        return Result.Result(False, result)

    @staticmethod
    def dict_to_list(dictionary: dict) -> list:
        result = []
        for field in dictionary:
            result.append([field, dictionary[field]])
        return result

    @staticmethod
    def join_rows(fields, first_dict, second_dict, first_table_index, second_table_index):
        fields_values = []
        for field in fields:
            if field[0] == first_table_index:
              fields_values.append([field[1], first_dict[field[1]]])
            if field[0] == second_table_index:
              fields_values.append([field[1], second_dict[field[1]]])
        new_row = Row(fields_values)
        return new_row

    def build_fields_for_join(self, fields: list, is_star: bool,
                              first_table_index: int, second_table_index: int,
                              second_table) -> list or Result.Result:
        result = []
        if is_star:
            for table_index in [first_table_index, second_table_index]:
                for field in self.db.tables[table_index].fields:
                    if second_table.type == "on":
                        result.append([table_index, field])
                    if second_table.type == "using":
                        if second_table.field.name != field.name:
                            result.append([table_index, field])
        for field in fields:
            if field.type == "field":
                return Result.Result(True, exception_for_client.DBExceptionForClient().NoTableSpecified(field.name))
            table_index = self.get_table_index(field.name_table)
            if table_index in [first_table_index, second_table_index]:
                if field.name in self.db.tables[table_index].fields:
                    if second_table.type == "on":
                        result.append([table_index, field])
                    if second_table.type == "using":
                        if second_table.field.name != field.name:
                            result.append([table_index, field])
            else:
                return Result.Result(True, exception_for_client.DBExceptionForClient().FieldNotExists(field.name))
        return result

    def select(self, name: str, fields: list, is_star: bool, condition, join, second_table):
        if not self.is_table_exists(name):
            return Result.Result(True, exception_for_client.DBExceptionForClient().TableNotExists(name))
        elif join != False and not self.is_table_exists(second_table.name):
            return Result.Result(True, exception_for_client.DBExceptionForClient().TableNotExists(second_table.name))
        elif type(self.is_fields_exist(name, fields)) is str:
            return Result.Result(True, exception_for_client.DBExceptionForClient().FieldNotExists(self.is_fields_exist(name, fields)))
        else:
            if join == False:
                table_index = self.get_table_index(name)
                rows = []
                for block in self.db.tables[table_index].iter_blocks():
                    for row in block.iter_rows():
                        if row.status == 1:
                            if self.solve_condition(condition, row):
                                rows.append(row)
                fields = self.build_fields(fields, is_star, table_index)
                rows = self.db.tables[table_index].select(fields, rows)
                for i in range(len(rows)):
                    rows[i] = Row(self.dict_to_list(rows[i].fields_values_dict))
                return fields, rows
            else:
                first_table_index = self.get_table_index(name)
                second_table_index = self.get_table_index(second_table.name)
                rows = []
                fields = self.build_fields_for_join(fields, is_star,
                                                    first_table_index, second_table_index, second_table)
                if type(fields) is Result.Result:
                    return fields
                for first_block in self.db.tables[first_table_index].iter_blocks():
                    for second_block in self.db.tables[second_table_index].iter_blocks():
                        for first_row in first_block.iter_rows():
                            for second_row in second_block.iter_rows():
                                if join.form == "":
                                    if second_table.type == "on":
                                        if (first_row.fields_values_dict[second_table.first_field.name]
                                                == second_row.fields_values_dict[second_table.second_field.name]):
                                            rows.append(self.join_rows(fields, first_row.fields_values_dict,
                                                        second_row.fields_values_dict,
                                                        first_table_index, second_table_index))
                fields_for_print = []
                for field in fields:
                    fields_for_print.append(field[1])
                return fields_for_print, rows

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
                            if type(temp) == Result.Result:
                                return temp
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
