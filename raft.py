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
hasElected = defaultdict(bool, False)


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
            #TODO: refuse
            return
        #TODO: agree
        pstate.term = term
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
        # TODO: send refuse if term <= myterm, else agree
        line = line.strip("\n")
        content = line.split(" ")
        heardFrom = int(content[1])
        term = int(content[3])
        if term <= pstate.term:
            # TODO: send refuse to heardFrom
            pass
        else:
            # TODO: agree to heardFrom
            pstate.term = term
            pstate.state = FOLLOWER
            return
    elif RequestRPCResponse in line:
        line = line.strip('\n')
        content = line.split(" ")
        result = content[-1]
        term = int(content[3])
        if result == 'true' and pstate.term == term:
            pstate.votes += 1
        if pstate.votes >= majority:
            # TODO: announce leader
            pstate.state = LEADER
            pstate.votes = 0
        return
    # send RPC Request Vote, start the election
    if hasElected[pstate.term] == False:
        election()
        hasElected[pstate.term] = True


def sendHB():
  # TODO: send heartbeat to all non leader nodes
    pass


def election():
  # TODO: start an election
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
