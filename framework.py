import asyncio
import re
from collections import defaultdict
import json
import sys
import alog
from logging import INFO, ERROR, WARN, DEBUG

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
    async def create(cls, pid, network, command, *args, **kwargs):
        subproc = await asyncio.create_subprocess_exec(command, *args, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE)

        self = cls(pid, network, subproc, **kwargs)
        return self

    async def reader(self):
        """
        read messages, process them, and send them to the network
        """
        try: 
            while True:
                line = await self.subproc.stdout.readline()
                await alog.log(DEBUG, f"{self.pid}>{line.decode().strip()}")
                if not line:
                    break
                if line.startswith(b"SEND"):
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
                        self.update_state(dict_name.decode(), decoded_value, index.decode())
                    else:
                        self.update_state(var.decode(), decoded_value)
        except Exception as e:
            await alog.log(ERROR, f"Got exception {type(e)} {e} while processing line {line} on {self.pid}")
            await asyncio.sleep(10)
            sys.exit(1)

    async def writer(self):
        while True:
            (src, msg) = await self.message_queue.get()
            if src is not None:
                line = f"RECEIVE {src} ".encode() + msg + b"\n"
            else:
                line = f"LOG {msg}\n".encode()
            await alog.log(DEBUG, f"{self.pid}<{line.decode().strip()}")
            self.subproc.stdin.write(line)
            await self.subproc.stdin.drain()

    def log_entry(self, entry):
        self.message_queue.put_nowait((None,entry))

    def send_message(self, src, msg):
        self.message_queue.put_nowait((src, msg))

    def stop(self):
        """ stops the running process """
        self.network.deregister(self.pid)
        self.reader_task.cancel()
        self.writer_task.cancel()
        self.subproc.terminate()


    def update_state(self, var, value, index=None):
        pass # to be overridden in derived classes

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
            alog.log_no_wait(WARN, f"sending a message from {src} to {dst} but {dst} is not registered")
            return

        if self.partition and self.partition[src] != self.partition[dst]:
            alog.log_no_wait(DEBUG, f"dropping message from {src} to {dst} due to partition")
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
        for i,part in enumerate(partitions):
            for server in part: 
                self.partition[server] = i
        # yeah this could've been a comprehension

    def repair_partition(self):
        """ resets to no partition """
        self.partition = None

async def main():
    await alog.init()
    network = Network()
    tasks = []
    for pid in range(int(sys.argv[1])):
        p = await Process.create(str(pid), network, *sys.argv[2:], str(pid), sys.argv[1])
        tasks += [p.reader_task, p.writer_task]

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
