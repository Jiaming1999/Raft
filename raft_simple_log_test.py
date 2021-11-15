import framework
import asyncio
import sys
from collections import defaultdict
import alog
from logging import INFO, CRITICAL, DEBUG, ERROR
import raft_test
import secrets
import raft_election_test

async def main(n, group):
    term, leader = await raft_election_test.elect_leader(n, group)

    entry = secrets.token_urlsafe()
    await alog.log(INFO, f"# Logging {entry}, waiting for it to be logged and committed")
    group.processes[leader].log_entry(entry)

    def all_committed(group):
        return min(group.commitIndex.values()) == 1

    await group.wait_predicate(all_committed)

    for p in map(str,range(n)):
        if len(group.logs[p]) != 1 or 1 not in group.logs[p] or \
            group.logs[p][1] != [term,entry]:
            await alog.log(ERROR, f"### Incorrect log for {p}: {group.logs[p]}")
            await alog.log(ERROR, f"### Expected {{ 1 : {repr([term,entry])} }}")
            return

    await alog.log(INFO, f"### Simple log test passed")


if __name__ == "__main__":
    asyncio.run(raft_test.run_test(main))
    
    
        
