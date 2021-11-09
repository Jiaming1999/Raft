import time
import sys


class newState:

    # default constructor
    def __init__(self, pid=-1, term=0, leader=None, state='FOLLOWER', voteFor=-1):
        self.pid = pid
        self.term = term
        self.leader = leader
        self.state = state
        self.votes = 0
        self.voteFor = voteFor
        self.election_reset_time = time.time()

    def print_newState(self):
        print(f"{self.pid} {self.state} leader={self.leader} term={self.term} votes={self.votes} voteFor={self.voteFor} rst_time={self.election_reset_time}",
              file=sys.stderr, flush=True)
