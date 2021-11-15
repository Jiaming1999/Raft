import time
import sys

pid = int(sys.argv[1])
n = int(sys.argv[2])
last = None
print(f"Starting pinger {pid}", file=sys.stderr)

while True:
    print(f"SEND {(pid+1)%n} PING {pid}", flush=True)
    line = sys.stdin.readline()
    if line is None:
        break
    print(f"Got {line.strip()}", file=sys.stderr)
    time.sleep(2)

print(f"Pinger {pid} done", file=sys.stderr)

