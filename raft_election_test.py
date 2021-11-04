import framework
import asyncio
import sys
from collections import defaultdict

LEADERS = {}
NORMAL_OP = defaultdict(set)
NORMAL_OP_EVENT = None
NORMAL_OP_THRESHOLD = None
NORMAL_OP_TERM = None

class RaftProcess(framework.Process):
    def update_state(self):
        # some checks
        if "term" not in self.state:
            return 
        term = self.state["term"]
        if "leader" in self.state and self.state["leader"] is not None:
            leader = self.state["leader"]
            if term in LEADERS:
                if leader != LEADERS[term]:
                    print(f"Inconsistent leaders for term {term}")
            else:
                LEADERS[term] = leader
            NORMAL_OP[term].add(self.pid)
            if NORMAL_OP_THRESHOLD and term > NORMAL_OP_TERM and \
                len(NORMAL_OP[term]) >= NORMAL_OP_THRESHOLD:
                NORMAL_OP_EVENT.set()

async def monitor_exceptions(tasks):
    try:
        asyncio.gather(*tasks)
    except Exception as e:
        print(e)

async def main():
    n = int(sys.argv[1])
    network = framework.Network()
    global NORMAL_OP_EVENT
    global NORMAL_OP_THRESHOLD
    global NORMAL_OP_TERM 

    NORMAL_OP_EVENT = asyncio.Event()
    NORMAL_OP_THRESHOLD = n
    NORMAL_OP_TERM = 0

    print("# Starting processes, waiting for election")
    processes = await asyncio.gather(*[RaftProcess.create(str(pid), network, "./raft",
        str(pid), str(n)) for pid in range(n)])
    process_dict = { p.pid : p  for p in processes }

    tasks = [ p.reader_task for p in processes ] + \
        [ p.writer_task for p in processes ]
    asyncio.create_task(monitor_exceptions(tasks))
    
    try:
        await asyncio.wait_for(asyncio.create_task(NORMAL_OP_EVENT.wait()),
            timeout=30)

        if len(LEADERS) > 1:
            print("### Error!  more than 1 term with a leader despite no failures!")
            return

        term, leader = LEADERS.popitem()
        print(f"# Successfully elected {leader} for term {term}")
        print(f"### Simple Election Test Passed!")
        NORMAL_OP_TERM = term 
        NORMAL_OP_THRESHOLD = n-1
        NORMAL_OP_EVENT = asyncio.Event()

        print(f"# Stopping leader {leader}, waiting for next one to be elected")
        process_dict[leader].stop()

        # allow 30 seconds for election
        await asyncio.wait_for(asyncio.create_task(NORMAL_OP_EVENT.wait()),
            timeout=30)

        if len(LEADERS) > 2:
            print("### Error!  more than 2 terms with a leader!")
            return

        term2, leader2 = max(LEADERS.items())

        print(f"# Successfully elected {leader2} for term {term2}")
 #       print(f"Leaders are: {LEADERS}, nodes in normal operational mode: {NORMAL_OP}")
        print("### Election after failure test passed!")

        for p in processes:
            if p.pid != leader:
                p.stop()

        print(f"# Sent {network.message_count} messages, {network.byte_count} bytes")

    except asyncio.TimeoutError:
        print("## Error! Election did not terminate in 30 seconds!")
    return

if __name__ == "__main__":
#    framework.DEBUG = True
    asyncio.get_event_loop().run_until_complete(main())


    
    
        
