import exception


class preprocessor_datebase:

    def __init__(self):
        pass

    def create_datebase(self):
        pass


class preprocessor_table:

    def __init__(self):
        pass

    def is_correct_fields(self, fields):
        pass

    def is_fields_exists(self, name, fields):
        pass

    def is_table_exists(self, name):
        pass

    def is_correct_condition(self, name, condition):
        pass

    def is_correct_values(self, name, values):
        pass

    def create_table(self, name, fields):
        if is_table_exists(name):
            try:
                raise exception.TableAlreadyExists(name)
            except Exception as ex:
                print(ex)
        elif is_correct_fields(fields):
            pass  # create_table in ENGINE

    def show_create_table(self, name):
        if not is_table_exists(name):
            try:
                raise exception.TableNotExists(name)
            except Exception as ex:
                print(ex)
        else:
            pass  # show_table in ENGINE

    def drop_table(self, name):
        if not is_table_exists(name):
            try:
                raise exception.TableNotExists(name)
            except Exception as ex:
                print(ex)
        else:
            pass  # drop_table in ENGINE

    def select(self, name, fields, is_star, condition):
        if not is_table_exists(name):
            try:
                raise exception.TableNotExists(name)
            except Exception as ex:
                print(ex)
        elif not is_fields_exists(name, fields):
            try:
                raise exception.FieldNotExists(is_fields_exists(name, fields))
            except Exception as ex:
                print(ex)
        elif is_correct_condition(name, condition):
            try:
                raise exception.FieldNotExists(is_fields_exists(name, fields))
            except Exception as ex:
                print(ex)
        else:
            pass  # select in ENGINE

    def insert(self, name, fields, values):
        if not is_table_exists(name):
            try:
                raise exception.TableNotExists(name)
            except Exception as ex:
                print(ex)
        elif not is_fields_exists(name, fields):
            try:
                raise exception.FieldNotExists(is_fields_exists(name, fields))
            except Exception as ex:
                print(ex)
        elif not is_correct_values(name, values):
            try:
                raise exception.InvalidDataType()
            except Exception as ex:
                print(ex)
        else:
            pass  # insert in ENGINE

    def update(self, name, fields, values, condition):
        if not is_table_exists(name):
            try:
                raise exception.TableNotExists(name)
            except Exception as ex:
                print(ex)
        elif not is_fields_exists(name, fields):
            try:
                raise exception.FieldNotExists(is_fields_exists(name, fields))
            except Exception as ex:
                print(ex)
        elif not is_correct_values(name, values):
            try:
                raise exception.InvalidDataType()
            except Exception as ex:
                print(ex)
        elif is_correct_condition(name, condition):
            try:
                raise exception.FieldNotExists(is_fields_exists(name, fields))
            except Exception as ex:
                print(ex)
        else:
            pass  # update in ENGINE

    def delete(self, name, condition):
        if not is_table_exists(name):
            try:
                raise exception.TableNotExists(name)
            except Exception as ex:
                print(ex)
        elif is_correct_condition(name, condition):
            try:
                raise exception.FieldNotExists(is_fields_exists(name, fields))
            except Exception as ex:
                print(ex)
        else:
            pass  # update in ENGINE
