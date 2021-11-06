import time
import sys
import threading
from state import State
from collections import defaultdict
import random
# raft node script file.
CANDIDATE = 'CANDIDATE'
LEADER = 'LEADER'
FOLLOWER = 'FOLLOWER'

# Message type
SEND = "SEND"
RECEIVE = "RECEIVE"
RequestRPC = "RequestVotes"
RequestRPCResponse = "RequestVoteResponse"
HeartBeat = "Heartbeat"

STATE_term = "STATE term="
STATE_state = "STATE state="
STATE_leader = "STATE leader="

pid = int(sys.argv[1])
n = int(sys.argv[2])  # num of nodes in total
majority = n // 2 + 1  # majority of the cluster
pstate = State()
hasElected = defaultdict(bool, False)
hasHB = False
shouldElect = False

# Every process have different timeout between 4-8 sec
TIMEOUT = 4 + 4 * random.uniform(0, 1)
HB_TIMEOUT = 2


print(f"Starting raft node {pid}", file=sys.stderr)


def IamFollower():
    # receive hb from leader and rpc from leader
    # If didn't receive HB after timeout, follower become candidate
    global hasHB
    monitorStdin = threading.Thread(target=followerOnReceive, args=())
    monitorStdin.start()
    monitorStdin.join(timeout=TIMEOUT)
    monitorStdin.join(timeout=0.001)
    if hasHB == False:
        pstate.state = CANDIDATE
    else:
        hasHB = False


def followerOnReceive():
    global pstate
    global hasHB
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
        hasHB = True
        return


def IamLeader():
    # send hearbeat to other nodes
    sendHB()
    time.sleep(HB_TIMEOUT)


def IamCandidate():
    '''
    If i am candidate:
    1. start election if i haven't started electon.
    2. if i've started election, I wait for response. 
        if timeout, start new election
        if get major votes, i'm leader
        if get voteRequest from higher term, i become follower
    '''

    # DEBUG: the program stuck at the following line

    monitorCandidate = threading.Thread(target=candidateReceive, args=())
    monitorCandidate.start()
    monitorCandidate.join(timeout=TIMEOUT)
    monitorCandidate.join(timeout=0.001)
    # send RPC Request Vote, start the election
    if hasElected[pstate.term] == False:
        startElection()
        hasElected[pstate.term] = True


def candidateReceive():
    global pstate
    global shouldElect
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


def sendHB():
    '''
    '''
    # TODO: send heartbeat to all non leader nodes
    for node in range(n):
        if node != pid:
            # e.g.: SEND 2 Heartbeat 0
            print(f"{SEND} {node} {HeartBeat} {pid}")


def startElection():
    '''
    Every time starting a election, I do the following:
    set me to candidate
    output STATE leader=null -Campuswire#607
    increase term
    vote for myself
    send requestVote RPCs to all other nodes
    '''
    # TODO: start an election

    # set my state to CANDIDATE
    pstate.state = CANDIDATE
    print(f"{STATE_state}{pstate.state}")

    # set leader to None
    pstate.leader = None
    print(f"{STATE_leader}{pstate.leader}")

    # increase my term
    pstate.term += 1
    print(f"{STATE_term}{pstate.term}")

    # vote for myself
    pstate.votes = 1

    # send requestVote RPC
    for node in range(n):
        if node != pid:
            print(f"{SEND} {node} {RequestRPC} {pstate.term}")


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
