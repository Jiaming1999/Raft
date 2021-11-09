import time
import sys
import threading
from typing import Collection, DefaultDict
from state import State
import random
from collections import defaultdict
from newState import newState
# raft node script file.
CANDIDATE = 'CANDIDATE'
LEADER = 'LEADER'
FOLLOWER = 'FOLLOWER'

# Message type
SEND = "SEND"
RECEIVE = "RECEIVE"
RequestRPC = "RequestVotes"
RequestRPCResponse = "RequestVoteResponse"
HeartBeat = "AppendEntries"
HeartBeatReply = "AppendEntryResponse"
true = "true"
false = "false"


pid = int(sys.argv[1])
n = int(sys.argv[2])  # num of nodes in total
majority = n // 2 + 1  # majority of the cluster
pstate = newState()
pstate.pid = pid
hasElected = set([])
shouldElect = False
# last_heard = time.time()
received_line_handled = True
received_line = None
mutex = threading.Lock()
# Every process have different timeout between 4-8 sec

# TIMEOUT = 0.15 + 0.15 * random.uniform(0, 1)
weight = 1
HB_TIMEOUT = 0.3 * weight


# print(f"Starting raft node {pid}", file=sys.stderr)
# print(f"timeout is randome", file=sys.stderr)


def random_timeout():
    # return (0.15 + 0.15 * random.uniform(0, 1)) * weight
    return pid + 1


def print_term(term):
    print(f'STATE term={term}', flush=True)
    return


def print_state(state):
    print(f'STATE state="{state}"', flush=True)
    return


def print_leader(leader):
    if leader == None:
        print(f'STATE leader=null', flush=True)
        return
    print(f'STATE leader="{leader}"', flush=True)
    return


def res_become_follower(term, leader):
    global pstate
    if pstate.term != term:
        pstate.leader = None
        print_leader(pstate.leader)

        pstate.term = term
        print_term(pstate.term)

    if pstate.state != FOLLOWER:
        pstate.state = FOLLOWER
        print_state(pstate.state)

    pstate.voteFor = -1
    pstate.votes = 0
    pstate.election_reset_time = time.time()


def become_follower(term, leader):
    global pstate
    if pstate.term != term:
        pstate.leader = None
        print_leader(pstate.leader)

        pstate.term = term
        print_term(pstate.term)

    if pstate.state != FOLLOWER:
        pstate.state = FOLLOWER
        print_state(pstate.state)

    # print(f"{pid}: parameter leader={leader} myoldleader={pstate.leader}",
    #       file=sys.stderr, flush=True)
    if pstate.leader != leader:
        pstate.leader = leader
        # print(f"{pid} has new leader={leader} term={term}",
        #       file=sys.stderr, flush=True)
        print_leader(pstate.leader)

    pstate.voteFor = -1
    pstate.votes = 0
    pstate.election_reset_time = time.time()


def become_leader(term):
    global pstate
    if pstate.term != term:
        pstate.leader = None
        print_leader(pstate.leader)
        pstate.term = term
        print_term(pstate.term)

    if pstate.state != LEADER:
        pstate.state = LEADER
        print_state(pstate.state)

    if pstate.leader != pid:
        pstate.leader = pid
        print_leader(pstate.leader)

    pstate.voteFor = -1
    pstate.votes = 0
    pstate.election_reset_time = time.time()


def response_RequestVote(line):
    # response to candidate
    # send agree if not vote before, else refuse(No refuse, only timeout)
    global pstate
    line = line.strip("\n")
    content = line.split(" ")
    heardFrom = int(content[1])
    term = int(content[3])

    # update my term if received higher term
    if term > pstate.term:
        res_become_follower(term, pstate.leader)

    if term == pstate.term and (pstate.voteFor == -1 or pstate.voteFor == heardFrom):
        # TODO: agree
        pstate.voteFor = heardFrom
        pstate.election_reset_time = time.time()
        # send my agree to sender
        print(
            f"{SEND} {heardFrom} {RequestRPCResponse} {pstate.term} {true}", flush=True)
        # print(f"{pid} voteFor {heardFrom}, {pid}'s current state:",
        #       file=sys.stderr, flush=True)
        # pstate.print_newState()
    else:
        # refuse
        print(
            f"{SEND} {heardFrom} {RequestRPCResponse} {pstate.term} {false}", flush=True)
        # print(f"{pid} refuseVote {heardFrom}, {pid}'s current state:",
        #       file=sys.stderr, flush=True)
        # pstate.print_newState()


def response_heartbeat(line):
    # I response to leader
    line = line.strip("\n")
    content = line.split(" ")
    heardFrom = int(content[1])
    term = int(content[3])

    # reset　timeout
    # last_heard = time.time()

    if term > pstate.term:
        become_follower(term, heardFrom)

    if term == pstate.term:
        if pstate.state != FOLLOWER:
            become_follower(term, heardFrom)
        if pstate.leader != heardFrom:
            pstate.leader = heardFrom
            print_leader(pstate.leader)

        pstate.election_reset_time = time.time()
        print(
            f"{SEND} {heardFrom} {HeartBeatReply} {pstate.term} {true}", flush=True)
        # print(f"{pid}'s current leader={pstate.leader}",
        #       file=sys.stderr, flush=True)
        # print(f"{pid} agreeHB from {heardFrom}, {pid}'s current state:",
        #       file=sys.stderr, flush=True)
        pstate.print_newState()
    else:
        print(f"{SEND} {heardFrom} {HeartBeatReply} {pstate.term} {false}", flush=True)
        # print(f"{pid} refuseHB from {heardFrom}, {pid}'s current state:",
        #       file=sys.stderr, flush=True)
        # pstate.print_newState()


def start_election():
    global pstate
    # set my state to CANDIDATE
    if pstate.state != CANDIDATE:
        pstate.state = CANDIDATE
        print_state(pstate.state)

    # set leader to None
    if pstate.leader != None:
        pstate.leader = None
        print_leader(pstate.leader)

    # increase my term
    pstate.election_reset_time = time.time()
    pstate.term += 1
    print_term(pstate.term)

    # vote for myself
    pstate.votes = 1
    pstate.voteFor = pid

    print(f"{pid} start election at term={pstate.term}",
          file=sys.stderr, flush=True)

    # send requestVote RPC
    for node in range(n):
        if node != pid:
            # print(f"{pid} request votes", file=sys.stderr, flush=True)
            print(f"{SEND} {node} {RequestRPC} {pstate.term}", flush=True)


def IamFollower():
    # receive hb from leader and rpc from leader
    # If didn't receive HB after timeout, follower become candidate
    global received_line
    global received_line_handled

    if not received_line_handled:
        '''
        handle received message
        '''
        mutex.acquire()
        line = received_line
        received_line = None
        received_line_handled = True
        mutex.release()

        # print(f"follower {pid} reading {line}", file=sys.stderr, flush=True)
        followerOnReceive(line)

    if time.time() - pstate.election_reset_time > random_timeout():
        '''
        handle timeout: become candidate and start election
        '''
        start_election()


def followerOnReceive(line):
    global pstate
    if RequestRPC in line:
        response_RequestVote(line)
    elif HeartBeat in line:
        response_heartbeat(line)


def IamLeader():
    # send hearbeat to other nodes
    global pstate
    global received_line_handled
    global received_line
    # print(f"I am leader {pid}", file=sys.stderr, flush=True)
    sendHB()
    time.sleep(HB_TIMEOUT)
    if not received_line_handled:
        mutex.acquire()
        line = received_line
        received_line = None
        received_line_handled = True
        mutex.release()

        if HeartBeatReply in line:
            line = line.strip('\n')
            content = line.split(" ")
            heardFrom = content[1]
            term = int(content[3])
            result = content[-1]

            if result == 'false':
                res_become_follower(term, None)
                return
        elif RequestRPC in line:
            response_RequestVote(line)
        elif HeartBeat in line:
            response_heartbeat(line)


def IamCandidate():
    '''
    If i am candidate:
    1. start election if i haven't started electon. (handled in IamFollower)
    2. if i've started election, I wait for response.
        if timeout, start new election
        if get major votes, i'm leader
        if get voteRequest from higher term, i become follower
    '''
    global pstate
    global received_line_handled
    global received_line

    # handle restart election
    if time.time() - pstate.election_reset_time > random_timeout():
        # print(f"{pid} as candidate handle={received_line_handled} recv={received_line}",
        #       file=sys.stderr, flush=True)
        print(f"{pid} restart election at term {pstate.term}",
              file=sys.stderr, flush=True)
        start_election()

    # handle received messages
    if not received_line_handled:
        mutex.acquire()
        line = received_line
        received_line = None
        received_line_handled = True
        mutex.release()

        # print(f"candidate {pid} reading {line}", file=sys.stderr, flush=True)
        if RequestRPC in line:
            response_RequestVote(line)
        elif RequestRPCResponse in line:
            line = line.strip('\n')
            content = line.split(" ")
            result = content[-1]
            term = int(content[3])

            if term > pstate.term:
                # my term is outdated
                become_follower(term, None)
                return
            elif term == pstate.term:
                if result == true:
                    pstate.votes += 1
                    print(f"{pid} has {pstate.votes} votes",
                          file=sys.stderr, flush=True)
                    if pstate.votes >= majority:
                        become_leader(pstate.term)
                        print(f"{pid} become leader at term={pstate.term}",
                              file=sys.stderr, flush=True)

        elif HeartBeat in line:
            response_heartbeat(line)


def sendHB():
    '''
    '''
    # TODO: send heartbeat to all non leader nodes
    for node in range(n):
        if node != pid:
            # e.g.: SEND 2 Heartbeat 7
            print(f"{SEND} {node} {HeartBeat} {pstate.term}", flush=True)


def receive_stdin():
    '''
    keep track of stdin
    '''
    global received_line
    global received_line_handled
    count = 0
    while True:
        # if pstate.state == CANDIDATE:

        #     print(f"{pid} as candidate handled={received_line_handled}, in while",
        #           file=sys.stderr, flush=True)
        mutex.acquire()
        # if pstate.state == CANDIDATE:
        #     print(f"{pid} as candidate handled={received_line_handled}, in mutex",
        #           file=sys.stderr, flush=True)
        if received_line_handled:
            # print(f"{pid} read stdin {count} time",
            #       file=sys.stderr, flush=True)
            received_line = sys.stdin.readline()
            # print(f"{pid} thread read:{received_line}",
            #       flush=True, file=sys.stderr)
            received_line_handled = False
            count += 1
        mutex.release()


monitorPipeline = threading.Thread(target=receive_stdin, args=(), daemon=True)
monitorPipeline.start()

while True:
    try:

        if pstate.state == CANDIDATE:
            IamCandidate()
        elif pstate.state == FOLLOWER:
            IamFollower()
        elif pstate.state == LEADER:
            IamLeader()
        else:
            break
    except KeyboardInterrupt:
        sys.exit()
