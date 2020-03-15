import sys
import logic


def call_exception(func, fields=()):
    try:
        if type(fields) is str:
            raise func(fields)
        elif len(fields) == 0:
            raise func()
        elif len(fields) == 2:
            raise func(fields[0], fields[1])
    except Exception as ex:
        pass


print("Enter db name:")
filename = input()
request = ""
db_logic = logic.Logic(filename)
while True:
    print("Enter sql query:")
    request = input()
    if request != "exit":
        result = db_logic.query(request)
        try:
            if result.is_exception:
                call_exception(result.exception_func, result.fields_for_func)
            elif result.str_for_print != "":
                print(result.str_for_print)
        except:
            print("В КЛИЕНТ ПРИШЛО NONE")
    else:
        sys.exit()
