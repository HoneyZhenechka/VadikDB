import exception
import engine.db_structure as eng


class preprocessor:

    def __init__(self):
        self.table_count = 0
        self.db = eng.Database()
        sign_len = self.db.file.read_integer(0, 1)
        sign_str = self.db.file.read_str(1, sign_len)
        if sign_str != self.db.signature:
            pass

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
        if name in fields:
            return [True, name]
        return [False, -1]

    def is_table_exists(self, name):
        result = False
        for i in self.db.tables:
            if i.name == name:
                result = True
        return result

    def is_correct_condition(self, name, condition):
        pass

    def is_correct_values(self, name, values):
        pass

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

