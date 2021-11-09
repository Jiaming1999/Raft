import sys
import time
while True:
    if not sys.stdin.isatty():
        line = sys.stdin.readline()
        print(line)
        break
    else:
        time.sleep(10)
        print("Hello World")
