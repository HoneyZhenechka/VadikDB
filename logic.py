import parser

def query(str):
    if (str.find("create") != -1):
        if (str.find("show") != -1):
            tree = parser.show_create(str)
        else:
            tree = parser.create(str)
    if (str.find("drop") != -1):
        tree = parser.drop(str)


request = ""

while (request != "exit"):
    request = input()
    if (request != "exit"):
        query(request)