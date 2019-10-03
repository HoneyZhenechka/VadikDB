import exception
from pyparsing import Word, alphas, ZeroOrMore, Optional, nums


def check_request(str):

    types = {101: "create", 102: "show", 103: "drop", 1001: "Error"}

    state = 0

    word = Word(alphas + "," + "(" + ")" + nums).ignore('"').ignore("'")
    request = Optional("create") + Optional("show") + Optional("create") + Optional("drop") + "table" + word + Optional("(") + ZeroOrMore(word) + Optional(")")
    list_ = request.parseString(str)

    stateMatrix = [
        {"create": 1, "show": 7, "drop": 10},
        {"table": 2},
        {"name": 3},
        {"(": 4},
        {"name": 5},
        {"INT": 6, "CHAR(10)": 6},
        {",": 4, ")": 101},
        {"create": 8},
        {"table": 9},
        {"name": 102},
        {"table": 11},
        {"name": 103},
    ]

    for i in range(len(list_)):
        if state < 100:
            if (list_[i] in stateMatrix[state]):
                state = stateMatrix[state][list_[i]]
            else:
                if (state in [2, 4, 9, 11]):
                    state = stateMatrix[state]["name"]
        else:
            state = 1001

    result = types[state]

    return result


#print(check_request("drop table VADIC"))
#print(check_request("create table VADIC ( i INT , j CHAR(10) )"))
#print(check_request("show create table VADIC"))

def init_parser(str):
    tree = {}

    type = check_request(str)

    if type == "create":
        tree = create_table(str)
    if type == "show":
        tree = show_create_table(str)
    if type == "drop":
        tree = drop_table(str)
    if type == "Error":
        tree = {type: "Error"}
        exception.IncorrectSyntax()

    return tree


def create_table(str):

    result = {
        "type": "create",
        "name": "",
        "fields": []
    }

    langCommands = ["create"]
    langWords = ["table"]
    bracketIsOpen = False

    word = Word(alphas + "," + "(" + ")" + nums).ignore('"').ignore("'")
    request = Optional("create") + Optional("show") + Optional("create") + Optional("drop") + "table" + word + Optional(
        "(") + ZeroOrMore(word) + Optional(")")
    list_ = request.parseString(str)

    values = []

    i = 0
    while i < len(list_):

        if (list_[i] == "("):
            bracketIsOpen = True
            i = i + 1
            continue

        if (list_[i] == ")"):
            bracketIsOpen = False
            i = i + 1
            continue

        if ((not (list_[i] in langWords) and not (list_[i] in langCommands) and not (bracketIsOpen))):
            result["name"] = list_[i]

        if ((not (list_[i] in langWords) and not (list_[i] in langCommands) and (bracketIsOpen)) and not(list_[i] == ",")):
            temp = [list_[i], list_[i+1]]
            values.append(temp)
            i = i + 1
        i = i + 1

    result["fields"] = values

    return result


def show_create_table(str):

    result = {
        "type": "show",
        "name": ""
    }

    langCommands = ["show"]
    langWords = ["table"]

    word = ZeroOrMore(Word(alphas).ignore(",").ignore('"').ignore("'"))

    request = word

    list_ = request.parseString(str)
    for i in range(len(list_)):
        if not (list_[i] in langWords) and not (list_[i] in langCommands):
                result["name"] = list_[i]

    return result


def drop_table(str):

    langCommands = ["drop"]
    langWords = ["table"]

    word = ZeroOrMore(Word(alphas).ignore(",").ignore('"').ignore("'"))

    result = {
        "type": "drop",
        "name": ""
    }

    request = word

    list_ = request.parseString(str)
    for i in range(len(list_)):
        if ((not (list_[i] in langWords) and not (list_[i] in langCommands))):
                result["name"] = list_[i]

    return result
