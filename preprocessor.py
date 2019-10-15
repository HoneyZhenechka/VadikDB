import exception
import engine.db_structure as eng


class preprocessor:

    def __init__(self):
        self.db = eng.Database()

    def is_correct_fields(self, fields):
        result = [True, -1]
        for i in range(len(fields)):
            result = self.is_fields_exists(fields[i][0], fields)
            if result[1] != -1 and result[1] != i:
                result = [False, i]
                break
        return result

    def is_fields_exists(self, name, fields):
        result = [False, -1]
        for i in range(len(fields)):
            if name == fields[i][0]:
                result = [True, i]
                break
        return result

    def is_table_exists(self, name):
        result = False
        for i in self.db.tables:
            if self.db.tables.name == name:
                result = True
        return result

    def is_correct_condition(self, name, condition):
        pass

    def is_correct_values(self, name, values):
        pass

    def create_table(self, name, fields):
        if self.is_table_exists(name):
            try:
                raise exception.TableAlreadyExists(name)
            except Exception as ex:
                print(ex)
        elif not self.is_correct_fields(fields)[0]:
            try:
                temp_res = self.is_correct_fields(fields)
                raise exception.DuplicateFields(str(fields[temp_res[1]][0]) + " " + str(fields[temp_res[1]][1]))
            except Exception as ex:
                print(ex)
        else:
            self.db.tables.append(eng.Table(self.db.file))
            temp_index = len(self.db.tables) - 1
            self.db.tables[temp_index].index = temp_index
            self.db.tables[temp_index].name = name
            for i in range(len(fields)):
                self.db.tables[temp_index].fields.append(fields[i][0])
                self.db.tables[temp_index].types.append(fields[i][1])

    def show_create_table(self, name):
        if not self.is_table_exists(name):
            try:
                raise exception.TableNotExists(name)
            except Exception as ex:
                print(ex)
        else:
            pass  # show_table in ENGINE

    def drop_table(self, name):
        if not self.is_table_exists(name):
            try:
                raise exception.TableNotExists(name)
            except Exception as ex:
                print(ex)
        else:
            pass  # drop_table in ENGINE

    def select(self, name, fields, is_star, condition):
        if not self.is_table_exists(name):
            try:
                raise exception.TableNotExists(name)
            except Exception as ex:
                print(ex)
        elif not self.is_fields_exists(name, fields):
            try:
                raise exception.FieldNotExists(is_fields_exists(name, fields))
            except Exception as ex:
                print(ex)
        elif self.is_correct_condition(name, condition):
            try:
                raise exception.FieldNotExists(is_fields_exists(name, fields))
            except Exception as ex:
                print(ex)
        else:
            pass  # select in ENGINE

    def insert(self, name, fields, values):
        if not self.is_table_exists(name):
            try:
                raise exception.TableNotExists(name)
            except Exception as ex:
                print(ex)
        elif not self.is_fields_exists(name, fields):
            try:
                raise exception.FieldNotExists(is_fields_exists(name, fields))
            except Exception as ex:
                print(ex)
        elif not self.is_correct_values(name, values):
            try:
                raise exception.InvalidDataType()
            except Exception as ex:
                print(ex)
        else:
            pass  # insert in ENGINE

    def update(self, name, fields, values, condition):
        if not self.is_table_exists(name):
            try:
                raise exception.TableNotExists(name)
            except Exception as ex:
                print(ex)
        elif not self.is_fields_exists(name, fields):
            try:
                raise exception.FieldNotExists(is_fields_exists(name, fields))
            except Exception as ex:
                print(ex)
        elif not self.is_correct_values(name, values):
            try:
                raise exception.InvalidDataType()
            except Exception as ex:
                print(ex)
        elif self.is_correct_condition(name, condition):
            try:
                raise exception.FieldNotExists(is_fields_exists(name, fields))
            except Exception as ex:
                print(ex)
        else:
            pass  # update in ENGINE

    def delete(self, name, condition):
        if not self.is_table_exists(name):
            try:
                raise exception.TableNotExists(name)
            except Exception as ex:
                print(ex)
        elif self.is_correct_condition(name, condition):
            try:
                raise exception.FieldNotExists(is_fields_exists(name, fields))
            except Exception as ex:
                print(ex)
        else:
            pass  # delete in ENGINE
