import framework 
import alog
import asyncio
from logging import INFO, ERROR, DEBUG
from collections import defaultdict
from collections.abc import Sequence
import sys

class RaftGroup:
    def __init__(self, n):
        # map from terms to leaders of term 
        self.leaders = {}
        # map from term to set of processes that
        # reached normal operation in term, i.e., became
        # a leader or a follower        
        self.normal_op = defaultdict(set)
        self.normal_op_event = None
        self.normal_op_threshold = None
        self.normal_op_term = None
        self.predicate = None
        self.predicate_event = None
        self.n = n
        self.error = asyncio.Event()
        self.network = framework.Network()
        self.logs = { str(i): {} for i in range(self.n) }
        self.commitIndex = { str(i): -1 for i in range(self.n) }

    async def start(self, command=["./raft"]):
        await alog.log(INFO, f"# Starting {self.n} processes")
        processes = await asyncio.gather(*[RaftProcess.create(str(pid), self.network, 
            *command, str(pid), str(self.n), group=self) for pid in range(self.n)])
        self.processes = { p.pid : p for p in processes }
        self.tasks = [ p.reader_task for p in processes ] + [ p.writer_task for p in processes ]
    
    async def wait_predicate(self, predicate, timeout=30):
        # first check if predicate alreaddy true!
        if predicate(self):
            return

        self.predicate = predicate
        self.predicate_event = asyncio.Event()

        error_task = asyncio.create_task(self.error.wait())
        predicate_task = asyncio.create_task(self.predicate_event.wait())

        try:
            done, _ = await asyncio.wait(self.tasks + [ error_task, predicate_task ],
                    timeout = timeout, return_when=asyncio.FIRST_COMPLETED) 
            if not done:
                raise RuntimeError("Timeout occurred in wait_predicate")
            if error_task in done:
                raise RuntimeError("Error occurred during test")
            else: 
                error_task.cancel()

            # propagate exceptions
            for t in done:
                if t != predicate_task:
                    await t
        finally:
            self.predicate = None
            self.predicate_event = None

    def stop_all(self):
        for p in self.processes.values():
            p.stop()
        self.processes = {}
        self.tasks = []

    def stop(self, pid):
        p = self.processes.pop(pid)
        self.tasks.remove(p.reader_task)
        self.tasks.remove(p.writer_task)
        p.stop()

    def check_predicate(self):
        if self.predicate is not None:
            if self.predicate(self):
                self.predicate_event.set()

    
class RaftProcess(framework.Process):
    def __init__(self, *args, group, **kwargs):
        self.term = None
        self.group = group
        super().__init__(*args, **kwargs)

    def update_state(self, var, value, index=None):
        alog.log_no_wait(DEBUG, f"update_state [{self.pid}@{self.term}]: {var}[{index}]={value}")
        if var == "term":
            self.term = int(value)

        if self.term is None:
            return

        if var == "leader" and value is not None:
            leader = str(value)
            if self.term in self.group.leaders:
                if leader != self.group.leaders[self.term]:
                    alog.log_no_wait(ERROR, f"### Error! Inconsistent leaders for term {self.term}")
                    self.group.error.set()
                    return
            else:
                self.group.leaders[self.term] = leader
            self.group.normal_op[self.term].add(self.pid)
            alog.log_no_wait(DEBUG, f"Leaders: {self.group.leaders} normal_op: {self.group.normal_op}")

        elif var == "log":
            if index is None or not index.isnumeric():
                alog.log_no_wait(ERROR, f"### Error! Log index must be numeric, not {repr(index)}")
                self.group.error.set()
                return
            index = int(index)
            if isinstance(value, str) or not isinstance(value, Sequence) or \
                len(value) != 2:
                alog.log_no_wait(ERROR, f"### Log entry must be [term,logEntry], not {repr(value)}")
                self.group.error.set()
                return
            term, log_entry = value
            if isinstance(term, str):
                if not term.isnumeric():
                    alog.log_no_wait(ERROR, f"### Log term must be numeric, not {repr(term)}")
                    self.group.error.set()
                    return
                term = int(term)
            if not isinstance(term, int) or term < 0:
                alog.log_no_wait(ERROR, f"### Log term must be numeric and positive, not {term}")
                self.group.error.set()
                return
            if not isinstance(log_entry, str):
                alog.log_no_wait(ERROR, f"### Log entry must be a string, not {repr(log_entry)}")
                self.group.error.set()
                return
            self.group.logs[self.pid][index] = value

            # check log matching property
            for other in map(str,range(self.group.n)):
                if other == self.pid:
                    continue
                if index in self.group.logs[other] and self.group.logs[other][index][0] == term:
                    for ix in range(1, index+1):
                        if ix not in self.group.logs[self.pid] or ix not in self.group.logs[other] or \
                            self.group.logs[other][ix] != self.group.logs[self.pid][ix]:
                            alog.log_no_wait(ERROR, f"### Log matching property failed for {self.pid} and {other}")
                            alog.log_no_wait(ERROR, f"### Logs: {self.group.logs}")
                            self.group.error.set()
                            return
        elif var == "commitIndex":
            try:
                new_commit_index = int(value)
            except:
                alog.log_no_wait(ERROR, f"###Error!  commitIndex must be numeric, not {repr(value)}")
                self.group.error.set()
                return
            if new_commit_index < 0:
                alog.log_no_wait(ERROR, f"###Error!  commitIndex must be positive, not {new_commit_index}")
                self.group.error.set()
                return
            if new_commit_index > len(self.group.logs[self.pid]):
                alog.log_no_wait(ERROR, f"### Error! commitIndex cannot be larger than len(log)")
                alog.log_no_wait(ERROR, f"commitIndex[{self.pid}] = {new_commit_index}, logs[{self.pid}] = {self.group.logs[self.pid]}")
                self.group.error.set()
                return
            if new_commit_index < self.group.commitIndex[self.pid]:
                alog.log_no_wait(ERROR, f"### Error! commitIndex cannot decrease! old: {self.group.commitIndex[self.pid]}, new: {new_commit_index}")
                self.group.error.set()
                return

            if new_commit_index > 0 and not sum(1 for pid in range(self.group.n) if 
                self.group.logs[str(pid)].get(new_commit_index,None) == 
                    self.group.logs[self.pid][new_commit_index]) > self.group.n/2:
                alog.log_no_wait(ERROR, f"### Error! entry committed at {new_commit_index} is replicated to less than a majority of servers")
                alog.log_no_wait(ERROR, f"### Logs: {self.group.logs}")
                self.group.error_set()
                return
            
            for other in map(str,range(self.group.n)):
                if other != self.pid:
                    for ix in range(1, min(new_commit_index,self.group.commitIndex[other])+1):
                        if self.group.logs[self.pid][ix] != self.group.logs[other][ix]:
                            alog.log_no_wait(ERROR, f"### Mismatch among committed entries!")
                            alog.log_no_wait(ERROR, f"### {self.pid}'s log[{ix}] = {self.group.logs[self.pid][ix]}")
                            alog.log_no_wait(ERROR, f"### {other}'s log[{ix}] = {self.group.logs[other][ix]}")
                            self.group.error_set()
                            return

            self.group.commitIndex[self.pid] = new_commit_index

            
        self.group.check_predicate()

async def run_test(test_function):
    log_level = INFO
    args = sys.argv[1:]
    try:
        if args[0] == "-d" or args[0] == "--debug":
            log_level = DEBUG
            args = args[1:]
        n = int(args[0])
    except:
        print(f"Usage: {sys.argv[0]} [-d] n_nodes [command]")
        sys.exit(1)

    if len(args) > 1:
        command = args[1:]
    else:
        command = ["./raft"]
    
    await alog.init(log_level)

    group = RaftGroup(n)
    try: 
        await group.start(command)

        await test_function(n, group)
        
        await alog.log(INFO, f"# Sent {group.network.message_count} messages, {group.network.byte_count} bytes")
    finally:
        group.stop_all()
        await asyncio.sleep(1)

    


    
