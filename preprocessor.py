import exception
import engine.db_structure as eng
import typing

class preprocessor:

    def __init__(self, db_filename):
        self.table_count = 0
        if db_filename == "":
            self.db = eng.Database()
        else:
            self.db = eng.Database(False, db_filename)

    def is_correct_fields(self, fields=()) -> typing.Tuple[bool, dict]:
        temp = {}
        for field in fields:
            if field[0] in temp:
                try:
                    raise exception.DuplicateFields(str(field[0] + " " + field[1]))
                except Exception as ex:
                    print(ex)
                    return False, temp
            else:
                temp[field[0]] = field[1]
        return True, temp

    def is_fields_exist(self, name: str, fields=()) -> typing.Tuple[bool, str]:
        for field in fields:
            if not field in self.db.tables[self.get_table_index(name)].fields:
                return False, field
        return True, ""

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
        value = 0
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
                value = int(root.getRootVal())
            except:
                if not (root.getRootVal() in row.fields_values_dict):
                    value = root.getRootVal()
                else:
                    value = row.fields_values_dict[root.getRootVal()]
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

    def get_correct_values(self, name: str, values: list, row, fields=()) -> list:
        table_index = self.get_table_index(name)
        types = []
        new_values = []
        is_exception = False

        if len(fields) == 0:
            types = self.db.tables[table_index].types
            if len(values) != len(types):
                is_exception = True
        else:
            for field in fields:
                for index_of_field in range(len(self.db.tables[table_index].fields)):
                    if field == self.db.tables[table_index].fields[index_of_field]:
                        types.append(self.db.tables[table_index].types[index_of_field])

        for i in range(len(values)):
            if types[i].name == "int":
                try:
                    new_values.append(int(self.solve_expression(values[i], row)))
                except:
                    is_exception = True
            elif types[i].name == "bool":
                if self.solve_expression(values[i], row) == "False":
                    new_values.append(False)
                elif self.solve_expression(values[i], row) == "True":
                    new_values.append(True)
                else:
                    is_exception = True
            else:
                new_values.append(str(values[i].getRootVal()))
        if is_exception:
            try:
                raise exception.InvalidDataType()
            except Exception as ex:
                print(ex)
                return []
        return new_values

    def get_values(self, name:str, values:list, fields=()):
        table_index = self.get_table_index(name)
        types = []

        if len(fields) == 0:
            types = self.db.tables[table_index].types
            if len(values) != len(types):
                return False, values
        else:
            for field in fields:
                for index_of_field in range(len(self.db.tables[table_index].fields)):
                    if field == self.db.tables[table_index].fields[index_of_field]:
                        types.append(self.db.tables[table_index].types[index_of_field])

        for i in range(len(values)):
            if types[i].name == "int":
                try:
                    values[i] = int(values[i])
                except:
                    return False, values
            if types[i].name == "bool":
                if values[i] == "False":
                    values[i] = False
                elif values[i] == "True":
                    values[i] = True
                else:
                    return False, values
        return True, values

    def create_table(self, name: str, fields: list = ()):
        temp = self.is_correct_fields(fields)
        if self.is_table_exists(name):
            try:
                raise exception.TableAlreadyExists(name)
            except Exception as ex:
                print(ex)
        elif temp[0]:
            self.db.create_table(name, self.table_count, temp[1])
            self.table_count += 1

    def show_create_table(self, name: str):
        if not self.is_table_exists(name):
            try:
                raise exception.TableNotExists(name)
            except Exception as ex:
                print(ex)
        else:
            for i in self.db.tables:
                if i.name == name:
                    print(i.show_create())

    def drop_table(self, name: str):
        if not self.is_table_exists(name):
            try:
                raise exception.TableNotExists(name)
            except Exception as ex:
                print(ex)
        else:
            pass

    def select(self, name: str, fields: list, is_star: bool, condition):
        if not self.is_table_exists(name):
            try:
                raise exception.TableNotExists(name)
            except Exception as ex:
                print(ex)
        elif not self.is_fields_exist(name, fields)[0]:
            try:
                raise exception.FieldNotExists(self.is_fields_exist(name, fields)[1])
            except Exception as ex:
                print(ex)
        else:
            table_index = self.get_table_index(name)
            self.db.tables[table_index].get_rows()
            rows = []
            for row in self.db.tables[table_index].rows:
                if self.solve_condition(condition, row):
                    rows.append(row)
            fields = self.build_fields(fields, is_star, table_index)
            rows = self.db.tables[table_index].select(fields, rows)
            result = "\n| "
            for field in fields:
                result += field + " | "
            result += "\n"
            for row in rows:
                result += "| "
                for field in row.fields_values_dict:
                    result += str(row.fields_values_dict[field]) + " | "
                result += "\n"
            print(result)



    def insert(self, name: str, fields: list, values: list):
        if not (self.is_table_exists(name)):
            try:
                raise exception.TableNotExists(name)
            except Exception as ex:
                print(ex)
        elif not self.is_fields_exist(name, fields)[0]:
            try:
                raise exception.FieldNotExists(self.is_fields_exist(name, fields)[1])
            except Exception as ex:
                print(ex)
        else:
            new_values = self.get_values(name, values)
            if not new_values[0]:
                try:
                    raise exception.InvalidDataType()
                except Exception as ex:
                    print(ex)
            else:
                table_index = self.get_table_index(name)
                if len(fields) == 0:
                    for i in range(len(values)):
                        fields.append(self.db.tables[table_index].fields[i])
                self.db.tables[table_index].insert(fields, new_values[1])

    def update(self, name: str, fields: list, values: list, condition):
        if not self.is_table_exists(name):
            try:
                raise exception.TableNotExists(name)
            except Exception as ex:
                print(ex)
        elif not self.is_fields_exist(name, fields)[0]:
            try:
                raise exception.FieldNotExists(self.is_fields_exist(name, fields)[1])
            except Exception as ex:
                print(ex)
        else:
            table_index = self.get_table_index(name)
            self.db.tables[table_index].get_rows()
            rows = []
            new_values = []
            for row in self.db.tables[table_index].rows:
                if self.solve_condition(condition, row):
                    new_values.append(self.get_correct_values(name, values, row, fields))
                    rows.append(row)
            self.db.tables[table_index].update(fields, new_values, rows)

    def delete(self, name: str, condition):
        if not self.is_table_exists(name):
            try:
                raise exception.TableNotExists(name)
            except Exception as ex:
                print(ex)
        else:
            table_index = self.get_table_index(name)
            self.db.tables[table_index].get_rows()
            rows_indices = []
            for index_row in range(len(self.db.tables[table_index].rows)):
                if self.solve_condition(condition, self.db.tables[table_index].rows[index_row]):
                    rows_indices.append(self.db.tables[table_index].rows[index_row].index_in_file)
            self.db.tables[table_index].delete(rows_indices)
