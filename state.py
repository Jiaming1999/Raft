
class State:

    # default constructor
    def __init__(self, term=0, leader=None, state='FOLLOWER'):
        self.term = term
        self.leader = leader
        self.state = state
        self.votes = 0
