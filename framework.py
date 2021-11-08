import asyncio
import re
from collections import defaultdict
import json
from sys import stderr
DEBUG = True
try:
    import aioconsole
except ImportError:
    DEBUG = False


async def log(message):
    if DEBUG:
        await aioconsole.aprint(asyncio.get_event_loop().time(), message)


def alog(message):
    if DEBUG:
        asyncio.create_task(log(message))


class Process:
    def __init__(self, pid, network, subproc):
        self.network = network
        self.pid = pid
        self.network.register(self, pid)
        self.subproc = subproc
        self.state = defaultdict(dict)

        self.message_queue = asyncio.Queue()
        self.reader_task = asyncio.create_task(self.reader())
        self.writer_task = asyncio.create_task(self.writer())

    @classmethod
    async def create(cls, pid, network, command, *args):
        subproc = await asyncio.create_subprocess_exec(command, *args, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE)

        self = cls(pid, network, subproc)
        return self

    async def reader(self):
        """
        read messages, process them, and send them to the network
        """
        await log(f"Starting reader {self.pid}")
        while True:
            line = await self.subproc.stdout.readline()
            await log(f"{self.pid}>{line.decode().strip()}")
            if not line:
                break
            if line.startswith(b"SEND"):
                # print(f"frameword readfrom {self.pid}:{line}", file=stderr)
                _, dst, msg = line.strip().split(b' ', 2)
                dst = dst.decode()
                self.network.send(self.pid, dst, msg)
            elif line.startswith(b"STATE"):
                _, stateline = line.strip().split(b' ', 1)
                var, value = stateline.split(b'=', 1)
                decoded_value = json.loads(value)
                if m := re.match(rb'(.*)\[(.*)\]', var):
                    dict_name = m.group(1)
                    index = m.group(2)

                    self.state[dict_name.decode()][index.decode()
                                                   ] = decoded_value
                else:
                    # print(f"decoded_value={decoded_value} {type(decoded_value)}",
                    #       file=stderr, flush=True)
                    self.state[var.decode()] = decoded_value
                self.update_state()

    async def writer(self):
        while True:
            (src, msg) = await self.message_queue.get()
            line = f"RECEIVE {src} ".encode() + msg + b"\n"
            # print(f"framework output stdin:{line}", file=stderr)
            await log(f"{self.pid}<{line.decode().strip()}")
            self.subproc.stdin.write(line)
            await self.subproc.stdin.drain()

    def send_message(self, src, msg):
        self.message_queue.put_nowait((src, msg))

    def stop(self):
        """ stops the running process """
        self.network.deregister(self.pid)
        self.reader_task.cancel()
        self.writer_task.cancel()
        self.subproc.terminate()

    def update_state(self):
        pass  # to be overridden in derived classes


class Network:
    def __init__(self):
        self.processes = {}
        self.message_count = 0
        self.byte_count = 0
        self.partition = None

    def register(self, process, pid):
        pid = str(pid)
        if pid in self.processes:
            raise ValueError(f"{pid} already registered withthe network")

        self.processes[pid] = process

    def deregister(self, pid):
        pid = str(pid)
        self.processes.pop(pid)

    def send(self, src, dst, msg):
        self.message_count += 1
        self.byte_count += len(msg)
        if dst not in self.processes:
            alog(
                f"sending a message from {src} to {dst} but {dst} is not registered")
            return

        if self.partition and self.partition[src] != self.partition[dst]:
            alog("dropping message from {src} to {dst} due to partition")
            return

        self.processes[dst].send_message(src, msg)

    def set_partition(self, *partitions):
        """
        Arguments are a list of processes in each partition. E.g.: `set_partition([0,1],[2,3,4])`
        creates 2 partitions with 0 & 1 in the first and 2,3,4 in the second. Messages between
        servers in different partitions will be dropped.
        """
        self.partition = {}
        # partition maps from server to partition number
        for i, part in enumerate(partitions):
            for server in part:
                self.partition[server] = i
        # yeah this could've been a comprehension

    def repair_partition(self):
        """ resets to no partition """
        self.partition = None


async def main():
    import sys

    network = Network()
    for pid in range(int(sys.argv[1])):
        await Process.create(str(pid), network, *sys.argv[2:], str(pid), sys.argv[1])

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(main())
    loop.run_forever()
