import time
import sys
import threading
from typing import Collection, DefaultDict
from state import State
import random
from collections import defaultdict
from newState import newState
import re
# raft node script file.
CANDIDATE = 'CANDIDATE'
LEADER = 'LEADER'
FOLLOWER = 'FOLLOWER'

# Message type
SEND = "SEND"
RECEIVE = "RECEIVE"
LOG = "LOG"
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
pstate.node_count = n
received_line_handled = True
received_line = None
mutex = threading.Lock()

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


def print_log(log_idx, term, entry):
    # our log start from index 0, log is supposed to start from index 1
    print(f'STATE log[{log_idx+1}]=[{term},"{entry}"]', flush=True)


def print_commitIndex(commitIndex):
    # print(f'{pid} STATE commitIndex={commitIndex}',
    #       flush=True, file=sys.stderr)
    print(f'STATE commitIndex={commitIndex+1}', flush=True)
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

    # handling log table
    pstate.init_leader_params()


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
    global pstate

    line = line.strip("\n")
    content = line.split(" ")
    heardFrom = int(content[1])
    term = int(content[3])
    last_log_index = int(content[4])
    last_log_term = int(content[5])
    commitIndex = int(content[-2])
    nextIndex = content[-1]
    list1 = line.split("[")[1]
    list2 = list1.split("]")[0]
    list3 = list(re.split(",| |\(|\)|'", list2))
    rest_entries = []
    pair = []
    for string in list3:
        if len(string) > 0:
            if len(string) < 4:
                pair.append(int(string))
            else:
                pair.append(string)
                rest_entries.append(tuple(pair))
                pair = []

    if term > pstate.term:
        become_follower(term, heardFrom)

    agreeLeader = "false"

    if term == pstate.term:
        if pstate.state != FOLLOWER:
            become_follower(term, heardFrom)
        if pstate.leader != heardFrom:
            pstate.leader = heardFrom
            print_leader(pstate.leader)

        pstate.election_reset_time = time.time()

        if last_log_index < 0 or (last_log_index < len(pstate.log) and last_log_term == pstate.log[last_log_index][0]):
            agreeLeader = "true"
            log_append_index = last_log_index + 1
            local_entry_index = 0
            while True:
                if len(pstate.log) <= log_append_index or len(rest_entries) <= local_entry_index or pstate.log[log_append_index][0] != rest_entries[local_entry_index][0]:
                    break
                log_append_index += 1
                local_entry_index += 1

            if len(rest_entries) > local_entry_index:
                pstate.log = pstate.log[:log_append_index] + \
                    rest_entries[local_entry_index:]

                # print(f"{pid} log {pstate.log}", file=sys.stderr)

                for cur_idx in range(log_append_index, len(pstate.log)):
                    print_log(cur_idx, pstate.term, pstate.log[cur_idx][1])

            if commitIndex > pstate.commitIndex:
                # print(
                #     f"{pid} receive receive leader commitidx{commitIndex} myidx{pstate.commitIndex}", file=sys.stderr)
                pstate.commitIndex = min(len(pstate.log)-1, commitIndex)
                print_commitIndex(pstate.commitIndex)
    print(f"{SEND} {heardFrom} {HeartBeatReply} {pstate.term} {nextIndex} {len(rest_entries)} {commitIndex} {agreeLeader}", flush=True)


def response_log(line):
    global pstate
    line = line.strip('\n')
    content = line.split(" ")
    entry = content[1]
    pstate.log.append((pstate.term, entry))
    print_log(len(pstate.log)-1, pstate.term, entry)


def handle_heartbeatReply(line):
    global pstate
    line = line.strip('\n')
    content = line.split(" ")
    heardFrom = int(content[1])
    term = int(content[3])
    result = content[-1]

    nextIndex = int(content[4])
    len_entries = int(content[5])
    commitIndex = int(content[6])

    if term > pstate.term:
        res_become_follower(term, None)
        return

    if term == pstate.term:
        if result == 'false':
            pstate.nextIndex[heardFrom] = nextIndex - 1
            return

        else:
            pstate.nextIndex[heardFrom] = nextIndex + len_entries
            pstate.matchIndex[heardFrom] = pstate.nextIndex[heardFrom] - 1
            old_commitIndex = pstate.commitIndex
            for i in range(old_commitIndex+1, len(pstate.log)):
                if pstate.log[i][0] == pstate.term:
                    count = 1
                    for node in range(n):
                        if pstate.matchIndex[node] >= i:
                            count += 1
                    if count >= majority:
                        pstate.commitIndex = i
            if pstate.commitIndex != old_commitIndex:
                print_commitIndex(pstate.commitIndex)


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
    start_time = time.time()
    cur_time = time.time()
    while cur_time - start_time < HB_TIMEOUT and pstate.state == LEADER:
        cur_time = time.time()
        if not received_line_handled:
            mutex.acquire()
            line = received_line
            received_line = None
            received_line_handled = True
            mutex.release()

            if HeartBeatReply in line:
                handle_heartbeatReply(line)
            elif RequestRPC in line:
                response_RequestVote(line)
            elif HeartBeat in line:
                response_heartbeat(line)
            elif LOG in line:
                response_log(line)


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
    global pstate
    for node in range(n):
        if node != pid:
            last_log_idx = pstate.nextIndex[node] - 1
            last_log_term = -1
            if last_log_idx != -1:
                last_log_term = pstate.log[last_log_idx][0]
            rest_entries = pstate.log[pstate.nextIndex[node]:]

            # e.g.: SEND 2 Heartbeat 7
            print(
                f"{SEND} {node} {HeartBeat} {pstate.term} {last_log_idx} {last_log_term} {rest_entries} {pstate.commitIndex} {pstate.nextIndex[node]}", flush=True)


def receive_stdin():
    '''
    keep track of stdin
    '''
    global received_line
    global received_line_handled
    while True:
        mutex.acquire()
        if received_line_handled:
            received_line = sys.stdin.readline()
            received_line_handled = False
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
