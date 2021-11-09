import threading
import time
import sys
print(time.time())


def test1():
    line = sys.stdin.readline()
    print("thread:"+str(time.time()))


x = threading.Thread(target=test1, args=())
x.start()
x.join(timeout=1)
print(time.time())
x.join(timeout=0.01)
print(time.time())

print("Hello world")
