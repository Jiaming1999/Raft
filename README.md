# Raft Testing Framework

This repository contains a framework for simulating communication between raft processes and a collection of test harnesses. Please refer to the MP description for more details.

## Getting started

You can run the testing framework using `python3.10 framework.py`. (Note: the framework is tested using python3.9 and python3.10; it may not work on Python versions older than 3.7). The arguments to the tester are the number of processes and the command to run each individual process, e.g.:

```
python3.10 framework.py 5 ./raft
```
or
```
python3.10 framework.py 2 python3.10 pinger.py
```

It will create n processes using the command you specify: the pid of the process (0, ..., n-1) and the total number of processes. So the first command will run:
```
./raft 0 5
./raft 1 5
./raft 2 5
./raft 3 5
./raft 4 5
``` 
as separate processes and connect them up to the communication framework. Likewise, the second command will run:
```
python3.10 pinger.py 0 2
python3.10 pinger.py 1 2
```

The `pinger.py` script is a very simple user of the framework that simply sends a `PING` message to the next PID in a ring.

## Running tests

The tests take a single argument, _n_, which is the number of Raft processes to use. E.g.: `python3.10 raft_partiton_test.py 5`. Currently enabled tests:

* `raft_election_test`: tests election of a Raft leader. First it waits for processes to elect a leader. If this is successful, it then terminates the leader and waits for the remaining processes to elect a leader.
* `raft_partition_test`: waits for a leader to be elected, then partitions that leader off from the rest of the group. Once the remaining group elects a new leader, it repairs the partition and waits for the previous leader to catch up

