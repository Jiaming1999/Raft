import asyncio
import alog
from logging import INFO, ERROR, DEBUG
import raft_test
import secrets
import raft_election_test
import raft_log_partition_test
import raft_election_failure_test
import random 
import raft_log5_test
from math import ceil

NENTRIES = 5


async def main(n: int, group: raft_test.RaftGroup):
    term, leader = await raft_election_test.elect_leader(n, group)

    entries = [ secrets.token_urlsafe() for _ in range(NENTRIES) ]
    await alog.log(INFO, f"Committing {entries}")

    for entry in entries:
        group.processes[leader].log_entry(entry)
        asyncio.sleep(0.5)

    def leader_committed(group):
        return group.commitIndex[leader] == NENTRIES

    await alog.log(INFO, f"Waiting for leader to commit all {NENTRIES}")
    await group.wait_predicate(leader_committed)

    await raft_log5_test.check_entries(group, term, leader, entries)

    # kill followers but keep a majority
    followers_to_kill = random.sample([pid for pid in map(str,range(n)) if pid != leader], ceil(n/2)-1)

    await alog.log(INFO, f"Stopping {followers_to_kill}")
    for pid in followers_to_kill:
        group.stop(pid)

    entries2 = [ secrets.token_urlsafe() for _ in range(NENTRIES) ]
    await alog.log(INFO, f"Committing {entries2}")

    for entry in entries2:
        group.processes[leader].log_entry(entry)
        asyncio.sleep(0.5)

    def all_committed(group):
        return all(group.commitIndex[pid] == NENTRIES*2 for pid in map(str,range(n)) if pid not in followers_to_kill)

    await alog.log(INFO, f"Waiting for all reachable processes to commit all {NENTRIES}")
    await group.wait_predicate(all_committed)

    await raft_log5_test.check_entries(group, term, leader, entries2, len(entries))


    one_more_to_kill = random.choice([pid for pid in map(str,range(n)) if pid != leader and pid not in followers_to_kill])

    await alog.log(INFO, f"Stopping {one_more_to_kill} to leave no majority")
    group.stop(one_more_to_kill)

    saved_commit_index = group.commitIndex.copy()

    entry3 = secrets.token_urlsafe()
    await alog.log(INFO, f"Trying to log {entry3}")
    group.processes[leader].log_entry(entry3)

    await alog.log(INFO, f"Testing to see if anyone commits it")

    await asyncio.sleep(10)

    if group.commitIndex != saved_commit_index:
        await alog.log(ERROR, "Commit happened without majority of followers!")
        await alog.log(ERROR, f"Saved commitIndex: {saved_commit_index}")
        await alog.log(ERROR, f"Current commitIndex: {group.commitIndex}")
    else:
        await alog.log(INFO, "### Follower failure test passed!")

if __name__ == "__main__":
    asyncio.run(raft_test.run_test(main))
    
    
        
