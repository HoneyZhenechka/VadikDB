from SQL_parser.SQL_lexer import tokens
import exception
import ply.yacc as yacc
from pythonds.basic.stack import Stack
from pythonds.trees.binaryTree import BinaryTree
import Result
import exception_for_client
from datetime import datetime


class Struct:

    def __getitem__(self, name):
        return self.__dict__[name]

    def __setitem__(self, name, value):
        self.__dict__[name] = value

    def __iter__(self):
        for i in self.__dict__.keys():
            yield i


def build_tree_condition(elements):
    if len(elements) == 1:
        return elements[0]
    tree = BinaryTree('')
    for i in range(len(elements)):
        if type(elements[i]) is BinaryTree:
            if tree.getRootVal() == '':
                tree.insertLeft(elements[i])
            else:
                tree.insertRight(elements[i])
        elif tree.getRootVal() in ['AND', 'OR', 'or', 'and']:
            new_tree = BinaryTree('')
            new_tree.setRootVal(elements[i])
            new_tree.insertLeft(tree)
            tree = new_tree
        else:
            tree.setRootVal(elements[i])
    return tree


def build_tree_expression(elements):
    stack = Stack()
    tree = BinaryTree('')
    if len(elements) == 1:
        tree.setRootVal(elements[0])
        return tree
    stack.push(tree)
    for i in range(len(elements)):
        if (elements[i] == '(') or (i == 0):
            tree.insertLeft('')
            if elements[i] == '(':
                stack.push("(")
            stack.push(tree)
            tree = tree.getLeftChild()
            if elements[i] != '(':
                tree.setRootVal(elements[i])
                parent = stack.pop()
                tree = parent
        elif elements[i] not in ['+', '-', '*', '/', ')']:
            tree.setRootVal(elements[i])
            parent = stack.pop()
            tree = parent
        elif elements[i] in ['+', '-', '*', '/']:
            sign = elements[i]
            if tree.getRootVal() in ['+', '-', '*', '/']:
                if sign in ['+', '-'] or elements[i - 1] == ')':
                    temp = BinaryTree('')
                    parent = stack.pop()
                    if stack.size() == 0:
                        temp.insertLeft(parent)
                        parent = temp
                        tree = parent
                        stack.push(parent)
                    elif parent == "(":
                        temp.insertLeft(tree)
                        parent = stack.pop()
                        parent.insertRight(temp)
                        stack.push(parent)
                        stack.push("(")
                        tree = parent.getRightChild()
                    else:
                        temp.insertLeft(tree)
                        parent.insertRight(temp)
                        stack.push(parent)
                        tree = parent.getRightChild()
                elif sign in ['*', '/']:
                    rChild = tree.getRightChild()
                    rChild.insertLeft(rChild.getRootVal())
                    tree = rChild
            tree.setRootVal(elements[i])
            tree.insertRight('')
            stack.push(tree)
            tree = tree.getRightChild()
        elif elements[i] == ')':
            parent = ""
            while parent != "(":
                parent = stack.pop()
                if parent == "(":
                    tree = stack.pop()
            stack.push(tree)
        if i + 1 == len(elements):
            for j in range(stack.size()):
                parent = stack.pop()
                tree = parent
    return tree


class PCreate(Struct):

    def __init__(self, name="", fields=(), is_versioning=False):
        self.name = name
        self.type = "create"
        self.fields = fields
        self.is_versioning = is_versioning


class PShow(Struct):

    def __init__(self, name=""):
        self.name = name
        self.type = "show"


class PTreeSelects(Struct):

    def __init__(self, tree):
        self.type = "tree selects"
        self.tree = tree


class PDrop(Struct):

    def __init__(self, name=""):
        self.name = name
        self.type = "drop"


class PSelect(Struct):

    def __init__(self, select_body, condition=True, is_versioning=False, from_date=None, to_date=None):
        self.type = "select"
        self.select = select_body
        self.condition = condition
        self.is_versioning = is_versioning
        self.from_date = from_date
        self.to_date = to_date


class PSelectBody(Struct):

    def __init__(self, name="", fields=(), isStar=False):
        self.name = name
        self.fields = fields
        self.isStar = isStar


class PInsert(Struct):

    def __init__(self, insert_body):
        self.type = "insert"
        self.insert = insert_body


class PInsertBody(Struct):

    def __init__(self, name="", fields=(), values=()):
        self.name = name
        self.fields = fields
        self.values = values


class PUpdate(Struct):

    def __init__(self, name="", fields_values=((), ()), condition=True):
        self.name = name
        self.type = "update"
        self.fields = fields_values[0]
        self.values = fields_values[1]
        self.condition = condition


class PDelete(Struct):

    def __init__(self, name="", condition=True):
        self.name = name
        self.type = "delete"
        self.condition = condition


class PUndefined(Struct):

    def __init__(self):
        self.type = "undefined"


class PJoin(Struct):

    def __init__(self, join_condition=PUndefined(), form=""):
        self.form = form
        self.type = "join"
        self.join_condition = join_condition


class POn(Struct):

    def __init__(self, first_field, second_field):
        self.type = "on"
        self.first_field = first_field
        self.second_field = second_field


class PUsing(Struct):

    def __init__(self, fields):
        self.type = "using"
        self.fields = fields


class PUnion(Struct):

    def __init__(self, is_all=False):
        self.type = "union"
        self.is_all = is_all


class PIntersect(Struct):

    def __init__(self):
        self.type = "intersect"


class PNameTable(Struct):

    def __init__(self, name):
        self.type = "name table"
        self.name = name


class PField(Struct):

    def __init__(self, name, is_str=False):
        self.type = "field"
        self.name = name
        self.is_str = is_str


class PFieldOfTable(Struct):

    def __init__(self, name_table, name):
        self.type = "field of table"
        self.name_table = name_table
        self.name = name


class PTransaction(Struct):

    def __init__(self, name):
        self.type = name


class PNamedTree(Struct):

    def __init__(self, tree, name):
        self.type = "named tree"
        self.tree = tree
        self.name = name


class PDate(Struct):

    def __init__(self, year, month, day, hour=0, minute=0, second=0, millisecond=0):
        self.year = int(year)
        self.month = int(month)
        self.day = int(day)
        self.hour = int(hour)
        self.minute = int(minute)
        self.second = int(second)
        self.millisecond = int(millisecond)


class PCreateIndex(Struct):

    def __init__(self, index_name, table_name, fields):
        self.type = "create index"
        self.index_name = index_name
        self.table_name = table_name
        self.fields = fields


class PShowIndex(Struct):

    def __init__(self, table_name):
        self.type = "show index"
        self.table_name = table_name


class PDropIndex(Struct):

    def __init__(self, index_name, table_name):
        self.type = "drop index"
        self.index_name = index_name
        self.table_name = table_name


def p_start(p):
    '''start : create ENDREQUEST
             | show ENDREQUEST
             | drop ENDREQUEST
             | tree_selects ENDREQUEST
             | insert ENDREQUEST
             | update ENDREQUEST
             | delete ENDREQUEST
             | transaction ENDREQUEST'''

    p[0] = p[1]


def p_transaction(p):
    '''transaction : BEGIN TRANSACTION
                   | END TRANSACTION
                   | ROLLBACK'''

    if p[1].lower() == "begin":
        p[0] = PTransaction("begin")
    elif p[1].lower() == "end":
        p[0] = PTransaction("end")
    else:
        p[0] = PTransaction("rollback")


def p_create(p):
    '''create :   CREATE INDEX NAME ON NAME LBRACKET fields RBRACKET
                | CREATE create_body'''

    if len(p) == 3:
        p[0] = p[2]
    else:
        p[0] = PCreateIndex(p[3], p[5], p[7])


def p_create_body(p):
    '''create_body :  TABLE NAME LBRACKET variables RBRACKET WITH LBRACKET VERSIONING EQUAL ON RBRACKET
                    | TABLE NAME LBRACKET variables RBRACKET'''

    if len(p) > 6:
        p[0] = PCreate(p[2], p[4], True)
    else:
        p[0] = PCreate(p[2], p[4])


def p_variables(p):
    '''variables : NAME type
              | variables COMMA NAME type'''

    if len(p) == 3:
        p[0] = []
        p[0].append([p[1], p[2]])
    else:
        p[0] = p[1]
        p[0].append([p[3], p[4]])


def p_show(p):
    '''show :  SHOW INDEX NAME
             | SHOW CREATE TABLE NAME'''

    if len(p) == 5:
        p[0] = PShow(p[4])
    else:
        p[0] = PShowIndex(p[3])


def p_drop(p):
    '''drop : DROP INDEX NAME ON NAME
            | DROP TABLE NAME'''

    if len(p) == 4:
        p[0] = PDrop(p[3])
    else:
        p[0] = PDropIndex(p[3], p[5])


def p_join(p):
    '''join : JOIN
            | LEFT OUTER JOIN
            | RIGHT OUTER JOIN'''

    if len(p) == 2:
        p[0] = PJoin()
    else:
        p[0] = PJoin(p[1] + p[2])


def p_union(p):
    '''union : UNION
            | UNION ALL'''

    if len(p) == 2:
        p[0] = PUnion()
    else:
        p[0] = PUnion(True)


def p_intersect(p):
    '''intersect : INTERSECT'''

    p[0] = PIntersect()


def p_tree_selects(p):
    '''tree_selects :   LBRACKET tree_selects RBRACKET union LBRACKET tree_selects RBRACKET
                    |   LBRACKET tree_selects RBRACKET intersect LBRACKET tree_selects RBRACKET
                    |   LBRACKET tree_selects RBRACKET join LBRACKET tree_selects RBRACKET join_condition
                    |   LBRACKET tree_selects RBRACKET join LBRACKET tree_selects RBRACKET
                    |   LBRACKET tree_selects RBRACKET union select
                    |   LBRACKET tree_selects RBRACKET intersect select
                    |   LBRACKET tree_selects RBRACKET join select join_condition
                    |   LBRACKET tree_selects RBRACKET join select
                    |   LBRACKET tree_selects RBRACKET join name_table join_condition
                    |   LBRACKET tree_selects RBRACKET join name_table
                    |   select union LBRACKET tree_selects RBRACKET
                    |   select intersect LBRACKET tree_selects RBRACKET
                    |   select join LBRACKET tree_selects RBRACKET AS name_table join_condition
                    |   select join LBRACKET tree_selects RBRACKET AS name_table
                    |   select union select
                    |   select intersect select
                    |   select join select join_condition
                    |   select join select
                    |   select join name_table join_condition
                    |   select join name_table
                    |   select'''

    tree = BinaryTree('')

    if len(p) == 2:
        tree.setRootVal(p[1])
        p[0] = tree
    elif len(p) == 4:
        tree.insertLeft(p[1])
        tree.setRootVal(p[2])
        tree.insertRight(p[3])
    elif len(p) == 5:
        tree.insertLeft(p[1])
        temp = p[2]
        temp.join_condition = p[4]
        tree.setRootVal(temp)
        tree.insertRight(p[3])
    elif len(p) == 6:
        if p[1] == "(":
            tree.insertLeft(p[2])
            tree.setRootVal(p[4])
            tree.insertRight(p[5])
        else:
            tree.insertLeft(p[1])
            tree.setRootVal(p[2])
            tree.insertRight(p[4])
    elif len(p) == 7:
        tree.insertLeft(p[2])
        temp = p[4]
        temp.join_condition = p[6]
        tree.setRootVal(temp)
        tree.insertRight(p[5])
    elif len(p) == 8:
        if p[5] == "(":
            tree.insertLeft(p[2])
            tree.setRootVal(p[4])
            tree.insertRight(p[6])
        else:
            tree.insertLeft(p[1])
            tree.setRootVal(p[2])
            tree.insertRight(PNamedTree(p[4], p[7]))
    elif len(p) == 9:
        if p[5] == "(":
            tree.insertLeft(p[2])
            temp = p[4]
            temp.join_condition = p[8]
            tree.setRootVal(temp)
            tree.insertRight(p[6])
        else:
            tree.insertLeft(p[1])
            temp = p[2]
            temp.join_condition = p[8]
            tree.setRootVal(temp)
            tree.insertRight(PNamedTree(p[4], p[7]))
    p[0] = PTreeSelects(tree)


def p_join_right_table(p):
    '''join_condition : ON field EQUAL field
            | USING LBRACKET fields RBRACKET'''
    if p[1].lower() == "on":
        p[0] = POn(p[2], p[4])
    elif p[1].lower() == "using":
        p[0] = PUsing(p[3])


def p_name_table(p):
    '''name_table : NAME'''

    p[0] = PNameTable(p[1])


def p_select(p):
    '''select : SELECT select_body condition FOR SYSTEM TIME FROM date TO date
              | SELECT select_body FOR SYSTEM TIME FROM date TO date
              | SELECT select_body condition
              | SELECT select_body'''

    if len(p) == 3:
        p[0] = PSelect(p[2])
    elif len(p) == 4:
        p[0] = PSelect(p[2], p[3])
    elif len(p) == 10:
        p[0] = PSelect(p[2], True, True, p[7], p[9])
    elif len(p) == 11:
        p[0] = PSelect(p[2], p[3], True, p[7], p[9])


def p_date(p):
    '''date : NAME HYPHEN NAME HYPHEN NAME NAME COLON NAME COLON NAME
            | NAME HYPHEN NAME HYPHEN NAME NAME COLON NAME
            | NAME HYPHEN NAME HYPHEN NAME NAME
            | NAME HYPHEN NAME HYPHEN NAME
            | NAME HYPHEN NAME'''

    if len(p) == 11:
        millisecond = p[10][3:9]
        second = p[10][0:2]
        if not millisecond:
            millisecond = 0
        p[0] = PDate(p[1], p[3], p[5], p[6], p[8], second, millisecond)
    elif len(p) == 9:
        p[0] = PDate(p[1], p[3], p[5], p[6], p[8])
    elif len(p) == 7:
        p[0] = PDate(p[1], p[3], p[5], p[6])
    elif len(p) == 6:
        p[0] = PDate(p[1], p[3], p[5])
    elif len(p) == 4:
        p[0] = PDate(datetime.now().year, p[3], p[5])


def p_select_body(p):
    '''select_body : fields FROM NAME
                   | STAR COMMA fields FROM NAME
                   | STAR FROM NAME'''

    if (len(p) == 4) and (p[1] != '*'):
        p[0] = PSelectBody(p[3], p[1])
    elif (len(p) == 4) and (p[1] == '*'):
        p[0] = PSelectBody(p[3], [], True)
    elif len(p) == 6:
        p[0] = PSelectBody(p[5], p[3], True)


def p_insert(p):
    '''insert : INSERT insert_body'''

    p[0] = PInsert(p[2])


def p_insert_body(p):
    '''insert_body : INTO NAME VALUES LBRACKET values RBRACKET
                   | INTO NAME LBRACKET fields RBRACKET VALUES LBRACKET values RBRACKET'''

    if len(p) == 7:
        p[0] = PInsertBody(p[2], [], p[5])
    else:
        p[0] = PInsertBody(p[2], p[4], p[8])


def p_update(p):
    '''update : UPDATE update_body'''

    p[0] = p[2]


def p_update_body(p):
    '''update_body : NAME SET expression
                   | NAME SET expression condition'''

    if len(p) == 4:
        p[0] = PUpdate(p[1], p[3])
    else:
        p[0] = PUpdate(p[1], p[3], p[4])


def p_expression(p):
    '''expression : field EQUAL tree_expression
                  | expression COMMA field EQUAL tree_expression'''

    if len(p) == 4:
        p[0] = [[], []]
        p[0][0].append(p[1])
        p[0][1].append(build_tree_expression(p[3]))
    else:
        p[0] = p[1]
        p[0][0].append(p[3])
        p[0][1].append(build_tree_expression(p[5]))


def p_delete(p):
    '''delete : DELETE FROM NAME
              | DELETE FROM NAME condition'''
    if len(p) == 4:
        p[0] = PDelete(p[3])
    else:
        p[0] = PDelete(p[3], p[4])


def p_values(p):
    '''values : value
              | values COMMA value'''

    if len(p) == 2:
        p[0] = []
        p[0].append(p[1])
    else:
        p[0] = p[1]
        p[0].append(p[3])


def p_value(p):
    '''value : NAME
             | QUOTE NAME QUOTE'''

    if len(p) == 2:
        p[0] = PField(p[1])
    else:
        p[0] = PField(p[2], True)


def p_fields(p):
    '''fields : field
              | fields COMMA field'''

    if len(p) == 2:
        p[0] = []
        p[0].append(p[1])
    else:
        p[0] = p[1]
        p[0].append(p[3])


def p_field(p):
    '''field : NAME
            | NAME DOT NAME'''

    if len(p) == 2:
        p[0] = PField(p[1])
    elif len(p) == 4:
        p[0] = PFieldOfTable(p[1], p[3])


def p_condition(p):
    '''condition : WHERE tree_condition'''

    p[0] = build_tree_condition(p[2])


def p_tree_condition(p):
    '''tree_condition : tree_comparison operator_condition tree_condition
                        | tree_comparison'''
    p[0] = []
    for i in range(len(p) - 1):
        try:
            if type(p[i + 1]) is str:
                p[0].append(p[i + 1])
            elif type(p[i + 1]) in [PField, PFieldOfTable]:
                p[0].append(p[i + 1])
            else:
                for el in p[i + 1]:
                    p[0].append(el)
        except:
            p[0].append(p[i + 1])


def p_tree_comparison(p):
    '''tree_comparison :  tree_expression operator_comparison tree_expression'''

    p[0] = BinaryTree('')
    p[0].insertLeft(build_tree_expression(p[1]))
    p[0].setRootVal(p[2])
    p[0].insertRight(build_tree_expression(p[3]))


def p_tree_expression(p):
    '''tree_expression : value
            | value operator_expression tree_expression
            | operator_expression tree_expression
            | LBRACKET tree_expression RBRACKET
            | tree_expression operator_expression tree_expression'''

    p[0] = []
    for i in range(len(p) - 1):
        if type(p[i+1]) is str:
            p[0].append(p[i+1])
        elif type(p[i+1]) in [PField, PFieldOfTable]:
            p[0].append(p[i+1])
        else:
            for el in p[i+1]:
                p[0].append(el)


def p_operator_condition(p):
    '''operator_condition : AND
                            | OR '''

    p[0] = p[1]


def p_operator_comparison(p):
    '''operator_comparison :  GREATER_THAN_OR_EQUAL
                            | LESS_THAN_OR_EQUAL
                            | NOT_EQUAL
                            | EQUAL
                            | GREATER_THAN
                            | LESS_THAN'''

    p[0] = p[1]


def p_operator_expression(p):
    '''operator_expression : PLUS
                | MINUS
                | STAR
                | DIVISION'''

    p[0] = p[1]


def p_type(p):
    '''type : int
            | str
            | bol
            | bool
            | float'''

    p[0] = p[1]


def p_error(p):
    if p == None:
        pos = "Last symbol"
        val = ""
    else:
        pos = p.lexpos
        val = p.value
    global result
    result = Result.Result(True, exception_for_client.DBExceptionForClient().IncorrectSyntax(pos, val))

result = None
parser = yacc.yacc()


def build_tree(code):

    tree = parser.parse(code)
    global result
    if type(result) is Result.Result:
        return result
    else:
        return tree
