import asyncio
import alog
from logging import INFO, ERROR
import raft_test
import secrets
import raft_election_test
import raft_partition_test
import raft_log_partition_test
import raft_election_failure_test

NENTRIES = 5


async def main(n, group):
    term, leader = await raft_election_test.elect_leader(n, group)

    entries, first_three = await raft_log_partition_test.log_three(n, group, term, leader)

    term2, leader2 = await raft_election_failure_test.elect_new_leader(n, group, term, leader)

    await raft_log_partition_test.log_five_more(n, group, term, leader, entries, first_three, term2, leader2)

    await alog.log(INFO, "### Leader failure log test passed!")


if __name__ == "__main__":
    asyncio.run(raft_test.run_test(main))
    
    
        
