import time
import sys
import threading
from state import State
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
true = "true"

STATE_term = "STATE term="
STATE_state = "STATE state="
STATE_leader = "STATE leader="

pid = int(sys.argv[1])
n = int(sys.argv[2])  # num of nodes in total
majority = n // 2 + 1  # majority of the cluster
pstate = State()
hasTimeout = set([])
follower_read = None
candidate_read = None


# Every process have different timeout between 4-8 sec
ELECTION_TIMEOUT = (pid + 1) * 0.5
TIMEOUT = 2
HB_TIMEOUT = 1


print(f"Starting raft node {pid}", file=sys.stderr)
print(f"timeoutis {TIMEOUT}", file=sys.stderr)


def print_term():
    print(f"{STATE_term}{pstate.term}", flush=True)


def print_state():
    print(f"{STATE_state}\"{pstate.state}\"", flush=True)


def print_leader():
    print(f"{STATE_leader}\"{pstate.leader}\"", flush=True)


def IamFollower():
    # receive hb from leader and rpc from leader
    # If didn't receive HB after timeout, follower become candidate
    print(f"I am follower {pid}", file=sys.stderr)
    monitorStdin = threading.Thread(target=followerOnReceive, args=())
    monitorStdin.start()
    monitorStdin.join(timeout=TIMEOUT)
    if follower_read is None:
        pstate.state = CANDIDATE
        hasTimeout.add(pstate.term)
        return


def followerOnReceive():
    global pstate
    global follower_read
    follower_read = sys.stdin.readline()
    if pstate.term in hasTimeout:
        return
    if RequestRPC in follower_read:
        # i response to candidate
        # send agree if not vote before, else refuse(No refuse, only timeout)
        line = follower_read.strip("\n")
        content = line.split(" ")
        heardFrom = int(content[1])
        term = int(content[3])

        # I agree if received term is higher than mine, else i do nothing
        if term > pstate.term:
            #TODO: agree
            # send my agree to sender
            print(
                f"{SEND} {heardFrom} {RequestRPCResponse} {term} {true}", flush=True)
            # update my term to the latest term
            pstate.term = term
            print_term()
        return
    elif HeartBeat in follower_read:
        # I response to leader
        line = follower_read.strip("\n")
        content = line.split(" ")
        heardFrom = int(content[1])
        term = int(content[3])

        # reset　timeout

        # update term and print term
        pstate.term = term
        print_term()
        # update state and print state
        pstate.state = FOLLOWER
        print_state()
        # update leader
        pstate.leader = heardFrom
        print_leader()
        # update lastheard time

        return


def IamLeader():
    # send hearbeat to other nodes
    print(f"I am leader {pid}", file=sys.stderr)
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
    global pstate
    startElection()
    line = sys.stdin.readline()
    print(f"reading {line} as candidate", file=sys.stderr)
    if RequestRPC in line:
        # TODO: send refuse(no refuse, no thing) if term <= myterm, else agree
        line = line.strip("\n")
        content = line.split(" ")
        heardFrom = int(content[1])
        term = int(content[3])
        if pstate.term < term:
            # TODO: agree to heardFrom
            pstate.term = term
            pstate.state = FOLLOWER
            # send agree
            print(
                f"{SEND} {heardFrom} {RequestRPCResponse} {term} {true}", flush=True)
            # print term state
            print(f"{STATE_term}{pstate.term}", flush=True)
            # # print follower state
            print(f"{STATE_state}\"{pstate.state}\"", flush=True)
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
            pstate.leader = pid
            print_state()
            print_leader()
        return
    elif HeartBeat in line:
        pstate.state = FOLLOWER
        return


def candidateReceive():
    global pstate
    line = sys.stdin.readline()
    print(f"reading {line} as candidate", file=sys.stderr)
    if RequestRPC in line:
        # TODO: send refuse(no refuse, no thing) if term <= myterm, else agree
        line = line.strip("\n")
        content = line.split(" ")
        heardFrom = int(content[1])
        term = int(content[3])
        if pstate.term < term:
            # TODO: agree to heardFrom
            pstate.term = term
            pstate.state = FOLLOWER
            # send agree
            print(
                f"{SEND} {heardFrom} {RequestRPCResponse} {term} {true}", flush=True)
            # print term state
            # print(f"{STATE_term}{pstate.term}", flush=True)
            # # print follower state
            # print(f"{STATE_state}{pstate.state}", flush=True)
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
            pstate.leader = pid
            print_leader()
            print_state()
        return
    elif HeartBeat in line:
        pstate.state = FOLLOWER
        return


def sendHB():
    '''
    '''
    # TODO: send heartbeat to all non leader nodes
    for node in range(n):
        if node != pid:
            # e.g.: SEND 2 Heartbeat 7
            print(f"{SEND} {node} {HeartBeat} {pstate.term}", flush=True)


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
    print(f"start election", file=sys.stderr)
    # set my state to CANDIDATE
    # print(f"{STATE_state}{pstate.state}", flush=True)

    # set leader to None
    pstate.leader = None
    # print(f"{STATE_leader}{pstate.leader}", flush=True)

    # increase my term
    pstate.term += 1
    # print(f"{STATE_term}{pstate.term}", flush=True)

    # vote for myself
    pstate.votes = 1

    # send requestVote RPC
    for node in range(n):
        if node != pid:
            print(f"SEND {node} {RequestRPC} {pstate.term}", flush=True)


while True:
    if pstate.state == CANDIDATE:
        IamCandidate()
    elif pstate.state == FOLLOWER:
        IamFollower()
    elif pstate.state == LEADER:
        IamLeader()
    else:
        break
