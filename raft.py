import time
import sys
# raft node script file.


pid = int(sys.argv[1])
n = int(sys.argv[2])
last = None
print(f"Starting raft node {pid}", file=sys.stderr)

while True:
    print(f"SEND {(pid+1)%n} PING {pid}", flush=True)
    line = sys.stdin.readline()
    if line is None:
        break
    print(f"Got {line.strip()}", file=sys.stderr)
    time.sleep(2)

print(f"raft node {pid} done", file=sys.stderr)
