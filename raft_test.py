import framework
import alog
import asyncio
from logging import INFO, ERROR, DEBUG
from collections import defaultdict


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
        self.n = n
        self.error = asyncio.Event()
        self.network = framework.Network()

    async def start(self, command=["./raft"]):
        await alog.log(INFO, f"# Starting {self.n} processes")
        processes = await asyncio.gather(*[RaftProcess.create(str(pid), self.network,
                                                              *command, str(pid), str(self.n)) for pid in range(self.n)])
        self.processes = {p.pid: p for p in processes}
        for p in processes:
            p.group = self

        self.tasks = [p.reader_task for p in processes] + \
            [p.writer_task for p in processes]

    async def wait_for_normal_op(self, threshold, term, timeout=30):
        self.normal_op_threshold = threshold
        self.normal_op_term = term
        self.normal_op_event = asyncio.Event()

        error_task = asyncio.create_task(self.error.wait())
        normal_op_task = asyncio.create_task(self.normal_op_event.wait())

        try:
            done, pending = await asyncio.wait(self.tasks + [error_task, normal_op_task],
                                               timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
            if error_task in done:
                raise RuntimeError("Error occurred during test")

            # propagate exceptions
            for t in done:
                if t != normal_op_task:
                    await t

        except asyncio.TimeoutError:
            await alog.log(ERROR, "### Error! Normal operation not reached in {timeout} seconds")
            raise

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


class RaftProcess(framework.Process):
    def __init__(self, *args, **kwargs):
        self.term = None
        super().__init__(*args, **kwargs)

    def update_state(self, var, value, index=None):
        alog.log_no_wait(
            DEBUG, f"update_state [{self.pid}@{self.term}]: {var}[{index}]={value}")
        if var == "term":
            self.term = int(value)
            return

        if self.term is None:
            return

        if var == "leader" and value is not None:
            leader = str(value)
            if self.term in self.group.leaders:
                if leader != self.group.leaders[self.term]:
                    alog.log_no_wait(
                        ERROR, f"### Error! Inconsistent leaders for term {self.term}")
                    self.group.error.set()
            else:
                self.group.leaders[self.term] = leader
            self.group.normal_op[self.term].add(self.pid)
            alog.log_no_wait(
                DEBUG, f"Leaders: {self.group.leaders} normal_op: {self.group.normal_op}")
            if self.group.normal_op_threshold and self.term > self.group.normal_op_term and \
                    len(self.group.normal_op[self.term]) >= self.group.normal_op_threshold:
                self.group.normal_op_event.set()
