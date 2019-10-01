from pyparsing import Word, alphas, ZeroOrMore

def create(str):

    result = {
        "table": {
            "name": "",
            "fields": []
        },
    }

    langCommands = ["create"]
    langWords = ["table"]
    bracketIsOpen = False

    word = ZeroOrMore(Word(alphas + '(' + ')').ignore(",").ignore('"').ignore("'"))

    request = word

    list_ = request.parseString(str)
    values = []

    for i in range(len(list_)):

        if (list_[i] == "("):
            bracketIsOpen = True
            continue

        if (list_[i] == ")"):
            bracketIsOpen = False
            continue

        if ((not (list_[i] in langWords) and not (list_[i] in langCommands) and not (bracketIsOpen))):
            nameTable = list_[i]
            if nameTable != " ":
                result["table"]["name"] = nameTable

        if ((not (list_[i] in langWords) and not (list_[i] in langCommands) and (bracketIsOpen))):
            values.append(list_[i])

    result["table"]["fields"] = values

    return result

def show_create(str):

    result = {
        "show": {
            "name": ""
        },
    }

    langCommands = ["show"]
    langWords = ["table"]

    word = ZeroOrMore(Word(alphas).ignore(",").ignore('"').ignore("'"))

    request = word

    list_ = request.parseString(str)
    for i in range(len(list_)):
        if ((not (list_[i] in langWords) and not (list_[i] in langCommands))):
            nameTable = list_[i]
            if nameTable != " ":
                result["show"]["name"] = nameTable

    return result

def drop(str):

    langCommands = ["drop"]
    langWords = ["table"]

    word = ZeroOrMore(Word(alphas).ignore(",").ignore('"').ignore("'"))

    result = {
        "drop": {
            "name": ""
        },
    }

    request = word

    list_ = request.parseString(str)
    for i in range(len(list_)):
        if ((not (list_[i] in langWords) and not (list_[i] in langCommands))):
            nameTable = list_[i]
            if nameTable != " ":
                result["drop"]["name"] = nameTable

    return result