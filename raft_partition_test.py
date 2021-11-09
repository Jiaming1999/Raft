import framework
import asyncio
import sys
from collections import defaultdict
import alog
from logging import INFO, CRITICAL, DEBUG, ERROR
import raft_test


async def main():
    n = int(sys.argv[1])
    if len(sys.argv) > 2:
        command = sys.argv[2:]
    else:
        command = ["./raft"]
    group = raft_test.RaftGroup(n)
    await alog.init(DEBUG)

    await group.start(command)

    await alog.log(INFO, f"Waiting for a leader to be elected")
    await group.wait_for_normal_op(n, 0)

    if len(group.leaders) > 1:
        await alog.log(ERROR, "### Error!  more than 1 term with a leader despite no failures!")
        await asyncio.sleep(5)
        return

    term, leader = max(group.leaders.items())

    await alog.log(INFO, f"# Successfully elected {leader} for term {term}")

    await alog.log(INFO, f"# Partitioning off leader {leader}, waiting for next one to be elected")
    group.network.set_partition([leader], [str(p)
                                for p in range(n) if str(p) != leader])

    await group.wait_for_normal_op(n-1, term)

    if len(group.leaders) > 2:
        await alog.log(ERROR, "### Error!  more than 2 terms with a leader!")
        await asyncio.sleep(5)

        return

    term2, leader2 = max(group.leaders.items())

    if leader2 == leader:
        await alog.log(ERROR, "### Error! Partitioned leader elected!")
        await asyncio.sleep(5)
        return

    await alog.log(INFO, f"# Successfully elected {leader2} for term {term2}")
    await alog.log(INFO, f"# Repairing partition, waiting for old leader ({leader}) to catch up")

    group.network.repair_partition()

    await group.wait_for_normal_op(n, term)

    if len(group.leaders) > 2:
        await alog.log(ERROR, "### Error! Repairing partition should not result in a new term")
        await asyncio.sleep(5)
        return

    await alog.log(INFO, "### Partition test passed!")

    group.stop_all()

    await alog.log(INFO, f"# Sent {group.network.message_count} messages, {group.network.byte_count} bytes")

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
