import exception
import engine.db_structure as eng


class preprocessor:

    def __init__(self, db_filename):
        self.table_count = 0
        if db_filename == "":
            self.db = eng.Database()
        else:
            self.db = eng.Database(False, db_filename)

    def is_correct_fields(self, fields):
        temp = {}
        for field in fields:
            if field[0] in temp:
                try:
                    raise exception.DuplicateFields(str(field[0] + " " + field[1]))
                except Exception as ex:
                    print(ex)
                    return [False, temp]
            else:
                temp[field[0]] = field[1]
        return [True, temp]

    def is_fields_exists(self, name, fields):
        for field in fields:
            if not field in self.db.tables[self.get_table_index(name)].fields:
                return [False, field]
        return [True, -1]

    def is_table_exists(self, name):
        result = False
        for table in self.db.tables:
            if table.name == name:
                result = True
                break
        return result

    def get_table_index(self, name):
        for i in range(len(self.db.tables)):
            if name == self.db.tables[i].name:
                return i

    def solve_polynomial(self, root):
        if root.getRootVal() == '+':
            value = self.solve_polynomial(root.getLeftChild()) + self.solve_polynomial(root.getRightChild())
        elif root.getRootVal() == '-':
            value = self.solve_polynomial(root.getLeftChild()) - self.solve_polynomial(root.getRightChild())
        elif root.getRootVal() == '*':
            value = self.solve_polynomial(root.getLeftChild()) * self.solve_polynomial(root.getRightChild())
        elif root.getRootVal() == '/':
            value = self.solve_polynomial(root.getLeftChild()) / self.solve_polynomial(root.getRightChild())
        else:
            try:
                value = int(root.getRootVal())
            except:
                pass
        return value

    def build_condition(self, condition):
        return [condition[0], self.solve_polynomial(condition[1])]

    def is_correct_condition(self, name, condition):
        if condition[0] == "":
            return True
        if condition[0] in self.db.tables[self.get_table_index(name)].fields:
            return True
        return False

    def build_fields(self, fields, is_star, table_index):
        result = []
        if is_star:
            for field in self.db.tables[table_index].fields:
                result.append(field)
        for field in fields:
            result.append(field)
        return result

    def is_correct_values(self, name, values, fields=[]):
        table_index = self.get_table_index(name)
        types = []

        if len(fields) == 0:
            types = self.db.tables[table_index].types
            if len(values) != len(types):
                return [False, values]
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
                    return [False, values]
            if types[i].name == "bool":
                if values[i] == "False":
                    values[i] = False
                elif values[i] == "True":
                    values[i] = True
                else:
                    return [False, values]
        return [True, values]

    def create_table(self, name, fields):
        temp = self.is_correct_fields(fields)
        if self.is_table_exists(name):
            try:
                raise exception.TableAlreadyExists(name)
            except Exception as ex:
                print(ex)
        elif temp[0]:
            self.db.create_table(name, self.table_count, temp[1])
            self.table_count += 1

    def show_create_table(self, name):
        if not self.is_table_exists(name):
            try:
                raise exception.TableNotExists(name)
            except Exception as ex:
                print(ex)
        else:
            for i in self.db.tables:
                if i.name == name:
                    print(i.show_create())

    def drop_table(self, name):
        if not self.is_table_exists(name):
            try:
                raise exception.TableNotExists(name)
            except Exception as ex:
                print(ex)
        else:
            pass

    def select(self, name, fields, is_star, condition):
        if not self.is_table_exists(name):
            try:
                raise exception.TableNotExists(name)
            except Exception as ex:
                print(ex)
        elif not self.is_fields_exists(name, fields)[0]:
            try:
                raise exception.FieldNotExists(self.is_fields_exists(name, fields)[1])
            except Exception as ex:
                print(ex)
        elif not self.is_correct_condition(name, condition):
            try:
                raise exception.FieldNotExists(condition[0])
            except Exception as ex:
                print(ex)
        else:
            table_index = self.get_table_index(name)
            self.db.tables[table_index].get_rows()
            if condition[0] == "":
                rows = self.db.tables[table_index].rows
            else:
                rows = []
                for i in self.db.tables[table_index].rows:
                    if i.fields_values_dict[condition[0]] == condition[1]:
                        rows.append(i)
            fields = self.build_fields(fields, is_star, table_index)
            rows = self.db.tables[table_index].select(fields, rows)
            result = "| "
            for field in self.db.tables[table_index].fields:
                result += field + " | "
            result += "\n"
            for row in rows:
                result += "| "
                for field in row.fields_values_dict:
                    result += str(row.fields_values_dict[field]) + " | "
                result += "\n"
            print(result)



    def insert(self, name, fields, values):
        temp_correct_values = self.is_correct_values(name, values)
        if not self.is_table_exists(name):
            try:
                raise exception.TableNotExists(name)
            except Exception as ex:
                print(ex)
        elif not self.is_fields_exists(name, fields)[0]:
            try:
                raise exception.FieldNotExists(self.is_fields_exists(name, fields)[1])
            except Exception as ex:
                print(ex)
        elif not temp_correct_values[0]:
            try:
                raise exception.InvalidDataType()
            except Exception as ex:
                print(ex)
        else:
            table_index = self.get_table_index(name)
            if len(fields) == 0:
                for i in range(len(values)):
                    fields.append(self.db.tables[table_index].fields[i])
            self.db.tables[table_index].insert(fields, temp_correct_values[1])

    def update(self, name, fields, values, condition):
        temp_correct_values = self.is_correct_values(name, values, fields)
        if not self.is_table_exists(name):
            try:
                raise exception.TableNotExists(name)
            except Exception as ex:
                print(ex)
        elif not self.is_fields_exists(name, fields)[0]:
            try:
                raise exception.FieldNotExists(self.is_fields_exists(name, fields)[1])
            except Exception as ex:
                print(ex)
        elif not temp_correct_values[0]:
            try:
                raise exception.InvalidDataType()
            except Exception as ex:
                print(ex)
        elif not self.is_correct_condition(name, condition):
            try:
                raise exception.FieldNotExists(condition[0])
            except Exception as ex:
                print(ex)
        else:
            table_index = self.get_table_index(name)
            self.db.tables[table_index].get_rows()
            rows = []
            if condition[0] != "":
                for row in self.db.tables[table_index].rows:
                    if row.fields_values_dict[condition[0]] == condition[1]:
                        rows.append(row)
            else:
                for row in self.db.tables[table_index].rows:
                    rows.append(row)
            self.db.tables[table_index].update(fields, temp_correct_values[1], rows)

    def delete(self, name, condition):
        condition = self.build_condition(condition)
        if not self.is_table_exists(name):
            try:
                raise exception.TableNotExists(name)
            except Exception as ex:
                print(ex)
        elif not self.is_correct_condition(name, condition):
            try:
                raise exception.FieldNotExists(condition[0])
            except Exception as ex:
                print(ex)
        else:
            table_index = self.get_table_index(name)
            self.db.tables[table_index].get_rows()
            rows_indices = []
            for index_row in range(len(self.db.tables[table_index].rows)):
                if self.db.tables[table_index].rows[index_row].fields_values_dict[condition[0]] == condition[1]:
                    rows_indices.append(self.db.tables[table_index].rows[index_row].index_in_file)
            self.db.tables[table_index].delete(rows_indices)
