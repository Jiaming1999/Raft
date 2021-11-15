import framework
import asyncio
import sys
from collections import defaultdict
import alog
from logging import INFO, CRITICAL, DEBUG, ERROR
import raft_test

async def elect_leader(n, group):
    await alog.log(INFO, f"Waiting for a leader to be elected")
    def reached_normal_op(group):
        if not group.normal_op: # no leaders yet
            return False
        _, nodes = max(group.normal_op.items())
        if len(nodes) == n:
            return True
        else:
            return False
    await group.wait_predicate(reached_normal_op)

    if len(group.leaders) > 1:
        await alog.log(ERROR, "### Error!  more than 1 term with a leader despite no failures!")
        await alog.log(ERROR, f"Leaders: {group.leaders}")
        raise RuntimeError("More than 1 leader term")

    term, leader = max(group.leaders.items())
    await alog.log(INFO, f"# Successfully elected {leader} for term {term}")

    return (term, leader)


async def main(n, group):
    term, leader = await elect_leader(n, group)
    await alog.log(INFO, f"### Election test passed")

if __name__ == "__main__":
    asyncio.run(raft_test.run_test(main))
    
    
        
