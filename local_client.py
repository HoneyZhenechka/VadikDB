import sys
import logic

print("Enter file name:")
filename = input()
request = ""
db_logic = logic.Logic(filename)
while True:
    print("Enter sql query:")
    request = input()
    if request != "exit":
        db_logic.query(request)
    else:
        sys.exit()
