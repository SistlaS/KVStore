# CS 739 MadKV Project 3

**Group members**: Name `email`, Name `email`

## Design Walkthrough

*FIXME: add your design walkthrough text*

## Self-provided Testcases

You will run the four described testcase scenarios during demo time.

### Explanations

*FIXME: add your explanations of each testcase*

## Fuzz Testing

<u>Parsed the following fuzz testing results:</u>

server_rf | crashing | outcome
:-: | :-: | :-:
5 | no | PASSED
5 | yes | PASSED

You may be asked to run a crashing fuzz test during demo time.

### Comments

*FIXME: add your comments on fuzz testing*

## YCSB Benchmarking

<u>10 clients throughput/latency across workloads & replication factors:</u>

![ten-clients](plots-p3/ycsb-ten-clients.png)

<u>Agg. throughput trend vs. number of clients with different replication factors:</u>

![tput-trend](plots-p3/ycsb-tput-trend.png)

### Comments

The plots currently reflect the benchmark runs we successfully completed for the `1 rf` and `3 rf` configurations. The `5 rf` runs are not shown because we were not able to collect stable complete logs for them, so the report generator skipped those cases instead of fabricating data.

Across the 10-client workload sweep, workload `c` has by far the highest throughput. This is expected because `c` is read-only, so requests avoid the replicated write path and do not pay the full cost of log replication, commit, and durable update. In contrast, workloads such as `a`, `b`, `d`, `e`, and especially `f` include writes or read-modify-write behavior, so they incur consensus overhead and therefore achieve much lower throughput.

The latency plots are consistent with the throughput plot. Workload `c` has the lowest average and tail latency because it is dominated by reads. Workloads `a` and `f` have the highest average and p99 latencies because they are write-heavy and repeatedly exercise the leader, replication, and commit path. Workload `f` in particular is expensive because each operation depends on both reading existing state and then issuing an update.

Comparing `1 rf` and `3 rf`, the general trend is that replication tends to increase the cost of update-heavy workloads. That is the expected tradeoff: `3 rf` improves fault tolerance, but every committed mutation now has to reach a majority of replicas before the leader can respond. For mostly read-oriented workloads, the gap is much smaller because once leadership is stable, reads are served directly by the leader and do not replicate new log entries.

The throughput trend for YCSB-A scales upward from 1 client to 10 clients in the data we collected. This suggests the implementation is able to use additional concurrency effectively at low to moderate load, rather than saturating immediately with a single client. We only have partial scaling data for this plot, so the graph should be interpreted as an observed trend for the completed runs rather than a full saturation study.

Overall, the benchmark results match the design of the system: read-only workloads perform best, write-heavy workloads pay for replication and durability, and increasing replication improves availability at the cost of higher write latency and lower update throughput.

## Additional Discussion

*OPTIONAL: add extra discussions if applicable*
