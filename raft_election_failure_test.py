import asyncio
import sys
import raft_test
import alog
from logging import INFO, ERROR
import raft_election_test


async def elect_new_leader(n, group, term, leader):
    await alog.log(INFO, f"# Stopping leader {leader}, waiting for next one to be elected")

    group.stop(leader)

    def next_term(group):
        term2, nodes = max(group.normal_op.items())
        return term2 > term and len(nodes) == n-1

    await group.wait_predicate(next_term)

    if len(group.leaders) > 2:
        await alog.log(ERROR, "### Error!  more than 2 terms with a leader!")
        await alog.log(ERROR, f"leaders: {group.leaders}")
        raise RuntimeError("Leaader election error")

    term2, leader2 = max(group.leaders.items())

    if leader2 == leader:
        await alog.log(ERROR, "### Error! new leader cannot be stopped process!")
        raise RuntimeError("Leaader election error")

    await alog.log(INFO, f"# Successfully elected {leader2} for term {term2}")

    return term2, leader2

async def main(n, group):
    term, leader = await raft_election_test.elect_leader(n, group)

    await elect_new_leader(n, group, term, leader)

    await alog.log(INFO, "### Election after failure test passed!")

if __name__ == "__main__":
    asyncio.run(raft_test.run_test(main))


    
    
        
