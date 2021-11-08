import time


class newState:

    # default constructor
    def __init__(self, term=0, leader=None, state='FOLLOWER', voteFor=-1):
        self.term = term
        self.leader = leader
        self.state = state
        self.votes = 0
        self.voteFor = voteFor
        self.election_reset_time = time.time()
