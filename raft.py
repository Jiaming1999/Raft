import time
import sys
from state import State
from collections import defaultdict
# raft node script file.
CANDIDATE = 'CANDIDATE'
LEADER = 'LEADER'
FOLLOWER = 'FOLLOWER'

# Message type
RequestRPC = "RequestVotes"
RequestRPCResponse = "RequestVoteResponse"
HeartBeat = "Heartbeat"

last_heard = time.time()
pid = int(sys.argv[1])
n = int(sys.argv[2])  # num of nodes in total
majority = n // 2 + 1  # majority of the cluster
pstate = State()
hasVoted = defaultdict(bool, False)


print(f"Starting raft node {pid}", file=sys.stderr)


def IamFollower():
    # receive hb from leader and rpc from leader
    global last_heard
    line = sys.stdin.readline()
    if RequestRPC in line:
        # send agree if not vote before, else refuse
        line = line.strip("\n")
        content = line.split(" ")
        heardFrom = int(content[1])
        term = int(content[3])
        if term <= pstate.term:
            # refuse
            return
        if hasVoted[term] == True:
            # refuse
            return
        # agree
        return
    elif HeartBeat in line:
        # reset　timeout
        last_heard = time.time()
        return
    if time.time()*1000 - last_heard*1000 > 4:
        pstate.state = CANDIDATE
        return


def IamLeader():
    # send hearbeat to other nodes
    sendHB()
    time.sleep(2)


def IamCandidate():
    line = sys.stdin.readline()
    if RequestRPC in line:
        # send refuse
        pass
    elif RequestRPCResponse in line:
        # collect votes
        pass
    # send RPC Request Vote, start the election
    election()


def sendHB():
    pass


def election():
    pass


while True:
    # print(f"SEND {(pid+1)%n} PING {pid}", flush=True)
    # line = sys.stdin.readline()
    # if line is None:
    #     break
    # print(f"Got {line.strip()}", file=sys.stderr)
    # time.sleep(2)
    if pstate.state == CANDIDATE:
        IamCandidate()
    elif pstate.state == FOLLOWER:
        IamFollower()
    elif pstate.state == LEADER:
        IamLeader()
    else:
        break
