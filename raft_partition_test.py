import framework
import asyncio
import sys
from collections import defaultdict
import alog
from logging import INFO, CRITICAL, DEBUG, ERROR
import raft_test
import raft_election_test

async def partition_and_reelect(n, group, term, leader):
    await alog.log(INFO, f"# Partitioning off leader {leader}, waiting for next one to be elected")
    group.network.set_partition([leader], [str(p) for p in range(n) if str(p) != leader])

    def next_term(group):
        term2, nodes = max(group.normal_op.items())
        return term2 > term and len(nodes) == n-1

    await group.wait_predicate(next_term)

    if len(group.leaders) > 2:
        await alog.log(ERROR, "### Error!  more than 2 terms with a leader!")
        raise RuntimeError("Error in second leader election")

    term2, leader2 = max(group.leaders.items())

    if leader2 == leader:
        await alog.log(ERROR, "### Error! Partitioned leader elected!")
        raise RuntimeError("Error in second leader election")

    await alog.log(INFO, f"# Successfully elected {leader2} for term {term2}")
    return term2, leader2


async def main(n, group):
    term, leader = await raft_election_test.elect_leader(n, group)

    await partition_and_reelect(n, group, term, leader)

    await alog.log(INFO, f"# Repairing partition, waiting for old leader ({leader}) to catch up")

    group.network.repair_partition()

    def full_group(group):
        _, nodes = max(group.normal_op.items())
        return len(nodes) == n

    await group.wait_predicate(full_group)

    if len(group.leaders) > 2:
        await alog.log(ERROR, "### Error! Repairing partition should not result in a new term")
        return 

    await alog.log(INFO, "### Partition test passed!")

if __name__ == "__main__":
    asyncio.run(raft_test.run_test(main))
    
    
        
