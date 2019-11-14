from SQL_parser.SQL_lexer import tokens
import exception
import ply.yacc as yacc
from pythonds.basic.stack import Stack
from pythonds.trees.binaryTree import BinaryTree


def build_parse_tree(list):
    stack = Stack()
    tree = BinaryTree('')
    if len(list) == 1:
        tree.setRootVal(list[0])
        return tree
    stack.push(tree)
    for i in range(len(list)):
        if (list[i] == '(') or (i == 0):
            tree.insertLeft('')
            if list[i] == '(':
                stack.push("(")
            stack.push(tree)
            tree = tree.getLeftChild()
            if list[i] != '(':
                tree.setRootVal(list[i])
                parent = stack.pop()
                tree = parent
        elif list[i] not in ['+', '-', '*', '/', ')']:
            tree.setRootVal(list[i])
            parent = stack.pop()
            tree = parent
        elif list[i] in ['+', '-', '*', '/']:
            sign = list[i]
            if tree.getRootVal() in ['+', '-', '*', '/']:
                if sign in ['+', '-'] or list[i - 1] == ')':
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
            tree.setRootVal(list[i])
            tree.insertRight('')
            stack.push(tree)
            tree = tree.getRightChild()
        elif list[i] == ')':
            parent = ""
            while parent != "(":
                parent = stack.pop()
                if parent == "(":
                    tree = stack.pop()
            stack.push(tree)
        if i + 1 == len(list):
            for j in range(stack.size()):
                parent = stack.pop()
                tree = parent
    return tree

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

    def __init__(self, name="", fields=[]):
        self.name = name
        self.type = "create"
        self.fields = fields


class PShow(Struct):

    def __init__(self, name=""):
        self.name = name
        self.type = "show"


class PDrop(Struct):

    def __init__(self, name=""):
        self.name = name
        self.type = "drop"


class PSelect(Struct):

    def __init__(self, select_body, condition=["", ""]):
        self.type = "select"
        self.select = select_body
        self.condition = condition


class PSelectBody(Struct):

    def __init__(self, name="", fields=[], isStar=False):
        self.name = name
        self.fields = fields
        self.isStar = isStar


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

    def __init__(self, name="", set=[[], []], condition=["", ""]):
        self.name = name
        self.type = "update"
        self.fields = set[0]
        self.values = set[1]
        self.condition = condition


class PDelete(Struct):

    def __init__(self, name="", condition=["", ""]):
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
    '''select : SELECT select_body ENDREQUEST
              | SELECT select_body condition ENDREQUEST'''
    if (len(p) == 4):
        p[0] = PSelect(p[2])
    else:
        p[0] = PSelect(p[2], p[3])


def p_select_body(p):
    '''select_body : fields FROM NAME
                   | STAR COMMA fields FROM NAME
                   | STAR FROM NAME'''

    if (len(p) == 4) and (p[1] != '*'):
        p[0] = PSelectBody(p[3], p[1])
    elif (len(p) == 4) and (p[1] == '*'):
        p[0] = PSelectBody(p[3], [], True)
    elif (len(p) == 6):
        p[0] = PSelectBody(p[5], p[3], True)


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
        p[0] = PUpdate(p[1], p[3])
    else:
        p[0] = PUpdate(p[1], p[3], p[4])


def p_expression(p):
    '''expression : field EQUAL field
                  | expression COMMA field EQUAL field'''

    if len(p) == 4:
        p[0] = [[],[]]
        p[0][0].append(p[1])
        p[0][1].append(p[3])
    else:
        p[0] = p[1]
        p[0][0].append(p[3])
        p[0][1].append(p[5])


def p_delete(p):
    '''delete : DELETE FROM NAME ENDREQUEST
              | DELETE FROM NAME condition ENDREQUEST'''
    if len(p) == 5:
        p[0] = PDelete(p[3])
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
    '''operator : PLUS
                | MINUS
                | STAR
                | DIVISION'''

    p[0] = p[1]


def p_condition(p):
    '''condition : WHERE field EQUAL tree'''

    p[0] = [p[2], build_parse_tree(p[4])]


def p_tree(p):
    '''tree : field
            | field operator tree
            | operator tree
            | LBRACKET tree RBRACKET
            | tree operator tree'''

    p[0] = []
    for i in range(len(p) - 1):
        if type(p[i+1]) is str:
            p[0].append(p[i+1])
        else:
            for el in p[i+1]:
                p[0].append(el)


def p_type(p):
    '''type : int
            | str
            | bol
            | bool
            | float'''

    p[0] = p[1]


def p_error(p):
    try:
        raise exception.IncorrectSyntax(p.lexpos, p.value)
    except Exception as ex:
        print(ex)


parser = yacc.yacc()


def build_tree(code):

    result = parser.parse(code)

    return result