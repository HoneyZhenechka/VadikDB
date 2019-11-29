import ply.lex as lex
from ply.lex import TOKEN
import re


tokens = (
    'CREATE', 'SHOW', 'DROP', 'SELECT', 'INSERT', 'UPDATE', 'DELETE',
    'FROM', 'INTO', 'TABLE', 'WHERE', 'SET',
    'NAME', 'VALUES',
    'int', 'str', 'bol', 'bool', 'float',
    'EQUAL', 'RBRACKET',
    'LBRACKET', 'COMMA', 'ENDREQUEST', 'PLUS', 'MINUS', 'DIVISION',
    'STAR', 'NOT_EQUAL', 'GREATER_THAN',
    'LESS_THAN', 'GREATER_THAN_OR_EQUAL',
    'LESS_THAN_OR_EQUAL', 'OR',
    'NOT', 'AND'
)

ident = r'\d+\.\d+|\w+'

t_CREATE = r'CREATE'
t_SHOW = r'SHOW'
t_DROP = r'DROP'
t_SELECT = r'SELECT'
t_INSERT = r'INSERT'
t_UPDATE = r'UPDATE'
t_DELETE = r'DELETE'

t_TABLE = r'TABLE'
t_FROM = r'FROM'
t_INTO = r'INTO'
t_WHERE = r'WHERE'
t_SET = r'SET'

t_VALUES = r'VALUES'

t_RBRACKET = r'\)'
t_LBRACKET = r'\('
t_COMMA = r','
t_ENDREQUEST = r'\;'
t_PLUS = r'\+'
t_MINUS = r'\-'
t_STAR = r'\*'
t_DIVISION = r'\/'

t_EQUAL = r'\='
t_NOT_EQUAL = r'!='
t_GREATER_THAN = r'\>'
t_LESS_THAN = r'\<'
t_GREATER_THAN_OR_EQUAL = r'>='
t_LESS_THAN_OR_EQUAL = r'<='
t_OR = r'OR'
t_NOT = r'NOT'
t_AND = r'AND'


@TOKEN(ident)
def t_NAME(t):
    if (t.value.upper() == 'CREATE'):
        t.type = 'CREATE'

    elif (t.value.upper() == 'SHOW'):
        t.type = 'SHOW'

    elif (t.value.upper() == 'DROP'):
        t.type = 'DROP'

    elif (t.value.upper() == 'SELECT'):
        t.type = 'SELECT'

    elif (t.value.upper() == 'INSERT'):
        t.type = 'INSERT'

    elif (t.value.upper() == 'UPDATE'):
        t.type = 'UPDATE'

    elif (t.value.upper() == 'DELETE'):
        t.type = 'DELETE'

    elif (t.value.upper() == 'TABLE'):
        t.type = 'TABLE'

    elif (t.value.upper() == 'FROM'):
        t.type = 'FROM'

    elif (t.value.upper() == 'INTO'):
        t.type = 'INTO'

    elif (t.value.upper() == 'WHERE'):
        t.type = 'WHERE'

    elif (t.value.upper() == 'SET'):
        t.type = 'SET'

    elif (t.value.upper() == 'VALUES'):
        t.type = 'VALUES'

    elif (t.value == '+'):
        t.type = 'PLUS'

    elif (t.value == '-'):
        t.type = 'MINUS'

    elif (t.value == '*'):
        t.type = 'STAR'

    elif (t.value == '/'):
        t.type = 'DIVISION'

    elif (t.value.upper() == '='):
        t.type = 'EQUAL'

    elif (t.value.upper() == '!='):
        t.type = 'NOT_EQUAL'

    elif (t.value.upper() == '>'):
        t.type = 'GREATER_THAN'

    elif (t.value.upper() == '<'):
        t.type = 'LESS_THAN'

    elif (t.value.upper() == '>='):
        t.type = 'GREATER_THAN_OR_EQUAL'

    elif (t.value.upper() == '<='):
        t.type = 'LESS_THAN_OR_EQUAL'

    elif (t.value.upper() == 'OR'):
        t.type = 'OR'

    elif (t.value.upper() == 'NOT'):
        t.type = 'NOT'

    elif (t.value.upper() == 'AND'):
        t.type = 'AND'

    elif (t.value == '('):
        t.type = 'LBRACKET'

    elif (t.value == ')'):
        t.type = 'RBRACKET'

    elif (t.value == ';'):
        t.type = 'ENDREQUEST'

    elif (
            t.value.lower() == 'int' or
            t.value.lower() == 'integer'
    ):
        t.type = 'int'

    elif (
            t.value.lower() == 'bol' or
            t.value.lower() == 'bool'
    ):
        t.type = 'bol'

    elif (
            t.value.lower() == 'str' or
            t.value.lower() == 'string'
    ):
        t.type = 'str'
    elif (
        t .value.lower() == 'float'
    ):
        t.type = 'float'

    return t


def t_error(t):
    print("Illegal character '%s'" % t.value[0])
    t.lexer.skip(1)


t_ignore = ''' ' " '''

lexer = lex.lex(reflags=re.UNICODE | re.DOTALL)
