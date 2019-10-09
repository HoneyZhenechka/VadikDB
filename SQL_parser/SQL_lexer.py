import ply.lex as lex
from ply.lex import TOKEN
import re


tokens = (

    'CREATE', 'SHOW', 'DROP', 'SELECT', 'INSERT', 'UPDATE', 'DELETE',
    'FROM', 'INTO', 'TABLE', 'WHERE', 'SET',
    'NAME', 'VALUES',
    'int', 'str', 'bol', 'bool',
    'EQUAL', 'RBRACKET',
    'LBRACKET', 'COMMA', 'ENDREQUEST',
    'STAR'
)

ident = r'[a-z a-zA-Z0-9_ \- \+]\w*'

t_CREATE = r'CREATE'
t_SHOW = r'SHOW'
t_DROP = r'DROP'
t_SELECT = r'SELECT'
t_INSERT = r'INSERT'
t_UPDATE = r'UPDATE'
p_DELETE = r'DELETE'

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
t_STAR = r'\*'

t_EQUAL = r'\=\=|\=|"IS"'


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

    return t


def t_error(t):
    print("Illegal character '%s'" % t.value[0])
    t.lexer.skip(1)


t_ignore = ''' ' " '''

lexer = lex.lex(reflags=re.UNICODE | re.DOTALL)
