import logic

print("Enter file name:")
filename = input()
request = ""
obj = logic.Logic(filename)
while request != "exit":
    print("Enter sql query:")
    request = input()
    obj.query(request)