import logic

print("Enter file name:")
filename = input()
request = ""
db_logic = logic.Logic(filename)
while request != "exit":
    print("Enter sql query:")
    request = input()
    if request != "exit":
        db_logic.query(request)