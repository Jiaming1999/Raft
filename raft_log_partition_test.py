import asyncio
import alog
from logging import INFO, ERROR
import raft_test
import secrets
import raft_election_test
import raft_partition_test

NENTRIES = 5

async def log_three(n, group, term, leader):
    entries = [ secrets.token_urlsafe() for _ in range(NENTRIES) ]
    await alog.log(INFO, f"# Logging {entries}, waiting for 3 to be committed by leader")
    async def log_task():
        try:     
            for entry in entries:
                group.processes[leader].log_entry(entry)
                await asyncio.sleep(0.5)
        except:
            pass  # ignore exceptions if we run task after the end
    # start logging in the background
    asyncio.create_task(log_task())

    def leader_reached_3(group):
        return group.commitIndex[leader] >= 3

    await group.wait_predicate(leader_reached_3)

    # we only need to check leader logs b/c followers logs will be checked for
    # consistency during commitIndex checks

    log_good = False
    if len(group.logs[leader]) < 3 or not all(i in group.logs[leader] for i in range(1,4)):
        await alog.log(ERROR, "### Expected leader log to have at least 3 entries")
    elif not all(group.logs[leader][i][0] == term for i in range(1,4)):
        await alog.log(ERROR, f"### Expected leader log to have all entries from term {term}")
    else:
        first_three = [ group.logs[leader][i][1] for i in range(1,4) ] 
        if len(set(first_three)) == 3 and set(first_three) < set(entries):
            log_good = True
        else:
            await alog.log(ERROR, f"### Leader log contains incorrect entries")

    if not log_good:
        await alog.log(ERROR, f"### Leader log: {group.logs[leader]}")
        raise RuntimeError("Leader log incorrect")
    
    return entries, first_three

async def log_five_more(n, group, term, leader, entries, first_three, term2, leader2):
# check the new leader's log
    log_good = False
    if not 3 <= len(group.logs[leader2]) <= 5:
        await alog.log(INFO, "New leader's log wrong size")
    elif not all(group.logs[leader2][i+1] == [term,first_three[i]] for i in range(3)):
        await alog.log(INFO, "New leader does not have previously committed entries")
    else:
        new_entries = first_three
        log_good = True
        for i in range(3, len(group.logs[leader2])):
            t, e = group.logs[leader2][i+1]
            if t != term or e not in set(entries) - set(new_entries):
                log_good = False
                break
            else:
                new_entries.append(e)
    if not log_good:
        await alog.log(INFO, f"New leader {leader2} log: {group.logs[leader2]}")
        raise RuntimeError("new leader log is in error")

    entries2 = [ secrets.token_urlsafe() for _ in range(5) ]
    await alog.log(INFO, f"# Logging new entries: {entries2}")

    for entry in entries2:
        group.processes[leader2].log_entry(entry)
        await asyncio.sleep(0.5)

    # wait for all to commit
    def all_committed(group):
        return min(group.commitIndex[p] for p in map(str,range(n)) if p != leader) == len(new_entries) + 5

    await alog.log(INFO, "# Waiting for new entries to be committed to all reachable nodes")
    await group.wait_predicate(all_committed)

    # check leader's log
    log_good = False
    if len(group.logs[leader2]) != len(new_entries) + 5:
        await alog.log(ERROR, "New leader's log has the wrong size")
    elif { tuple(group.logs[leader2][i+len(new_entries)+1]) for i in range(5) } != { (term2,entry) for entry in entries2 }:
        await alog.log(ERROR, "New leader's log has the wrong contents")
    else:
        log_good = True
    if not log_good:
        await alog.log(ERROR, f"New leader {leader2}'s log: {group.logs[leader2]}")
        raise RuntimeError("New leader's log in error")

async def main(n, group):
    term, leader = await raft_election_test.elect_leader(n, group)

    entries, first_three = await log_three(n, group, term, leader)

    term2, leader2 = await raft_partition_test.partition_and_reelect(n, group, term, leader)

    await log_five_more(n, group, term, leader, entries, first_three, term2, leader2)

    await alog.log(INFO, f"Repairing partition, waiting for old leader {leader} to catch up")
    group.network.repair_partition()

    def caught_up(group):
        return group.commitIndex[leader] == group.commitIndex[leader2]

    # commitIndex consistency checks will ensure new leader's log is correct

    await group.wait_predicate(caught_up)

    await alog.log(INFO, "### Partition log test passed!")


if __name__ == "__main__":
    asyncio.run(raft_test.run_test(main))
    
    
        
