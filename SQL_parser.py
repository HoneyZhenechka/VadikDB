from SQL_lexer import tokens
import ply.yacc as yacc


class Struct:

    def __init__(self, **dictionary):
        self.__dict__.update(dictionary)

    def __getitem__(self, name):
        return self.__dict__[name]

    def __setitem__(self, name, value):
        self.__dict__[name] = value

    def __iter__(self):
        for i in self.__dict__.keys():
            yield i


class PCreate(Struct):

    def __init__(self, name="", values=[]):
        self.name = name
        self.type = "create"
        self.values = values


class PShow(Struct):

    def __init__(self, name=""):
        self.name = name
        self.type = "show"


class PDrop(Struct):

    def __init__(self, name=""):
        self.name = name
        self.type = "drop"


class PSelect(Struct):

    def __init__(self, select_body):
        self.type = "select"
        self.select = select_body


class PSelectBody(Struct):

    def __init__(self, name="", fields=[], condition=[]):
        self.name = name
        self.fields = fields
        self.condition = condition


class PInsert(Struct):

    def __init__(self, insert_body):
        self.type = "insert"
        self.insert = insert_body


class PInsertBody(Struct):

    def __init__(self, name="", fields=[], values=[]):
        self.name = name
        self.fields = fields
        self.values = values


class PUpdate(Struct):

    def __init__(self, name="", set=[], condition=[]):
        self.name = name
        self.type = "update"
        self.set = set
        self.condition = condition


class PDelete(Struct):

    def __init__(self, name="", condition=[]):
        self.name = name
        self.type = "delete"
        self.condition = condition


def p_start(p):
    '''start : create
             | show
             | drop
             | select
             | insert
             | update
             | delete'''

    p[0] = p[1]


def p_create(p):
    '''create : CREATE create_body ENDREQUEST'''

    p[0] = p[2]


def p_create_body(p):
    '''create_body : TABLE NAME LBRACKET values RBRACKET'''

    p[0] = PCreate(p[2], p[4])


def p_values(p):
    '''values : NAME type
              | values COMMA NAME type'''

    if len(p) == 3:
        p[0] = []
        p[0].append([p[1], p[2]])
    else:
        p[0] = p[1]
        p[0].append([p[3], p[4]])


def p_show(p):
    '''show : SHOW CREATE TABLE NAME ENDREQUEST'''

    p[0] = PShow(p[4])


def p_drop(p):
    '''drop : DROP TABLE NAME ENDREQUEST'''

    p[0] = PDrop(p[3])


def p_select(p):
    '''select : SELECT select_body ENDREQUEST'''

    p[0] = PSelect(p[2])


def p_select_body(p):
    '''select_body : fields FROM NAME
                   | fields FROM NAME condition'''

    if len(p) != 4:
        p[0] = PSelectBody(p[3], p[1], p[4])
    else:
        p[0] = PSelectBody(p[3], p[1], [])


def p_insert(p):
    '''insert : INSERT insert_body ENDREQUEST'''

    p[0] = PInsert(p[2])


def p_insert_body(p):
    '''insert_body : INTO NAME VALUES LBRACKET fields RBRACKET
                   | INTO NAME LBRACKET fields RBRACKET VALUES LBRACKET fields RBRACKET'''

    if len(p) == 7:
        p[0] = PInsertBody(p[2], [], p[5])
    else:
        p[0] = PInsertBody(p[2], p[4], p[8])


def p_update(p):
    '''update : UPDATE update_body ENDREQUEST'''

    p[0] = p[2]


def p_update_body(p):
    '''update_body : NAME SET expression
                   | NAME SET expression condition'''

    if len(p) == 4:
        p[0] = PUpdate(p[1], p[3], [])
    else:
        p[0] = PUpdate(p[1], p[3], p[4])


def p_expression(p):
    '''expression : field operator field
                  | expression COMMA field operator field'''

    if len(p) == 4:
        p[0] = []
        p[0].append([p[1], p[3]])
    else:
        p[0] = p[1]
        p[0].append([p[3], p[5]])


def p_delete(p):
    '''delete : DELETE FROM NAME ENDREQUEST
              | DELETE FROM NAME condition ENDREQUEST'''

    if len(p) == 5:
        p[0] = PDelete(p[3], [])
    else:
        p[0] = PDelete(p[3], p[4])


def p_fields(p):
    '''fields : NAME
              | fields COMMA NAME'''

    if len(p) == 2:
        p[0] = []
        p[0].append(p[1])
    else:
        p[0] = p[1]
        p[0].append(p[3])


def p_field(p):
    '''field : NAME'''

    p[0] = p[1]


def p_operator(p):
    '''operator : EQUAL'''

    p[0] = p[1]

def p_condition(p):
    '''condition : WHERE field operator field'''

    p[0] = [p[2], p[4]]


def p_type(p):
    '''type : int
            | str
            | bol
            | bool'''

    p[0] = p[1]


parser = yacc.yacc()


def build_tree(code):

    result = parser.parse(code)

    return result