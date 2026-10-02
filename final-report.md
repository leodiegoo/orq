# 231: stalled worker wakes the coordinator

`next_without_user` returns the first stuck / not started / stopped worker; `wake_stopped` escalates and records `acorda.escalada`. Deviation: the reason carries no "N min" (it is the count/blocker key and would reset every tick).

## Conformance

1. red→green `test_ticket231_stuck_not_started_and_stopped_worker_is_the_next_step_without_the_user` (test_orq.py:16596); red: `next_without_user` had no such branch and returned None
2. `test_ticket231_asking_hibernated_integration_service_limit_and_declared_wait_do_not_enter`
3. `test_ticket231_dispatch_with_open_steer_inside_the_read_window_does_not_enter`
4. `test_ticket231_wake_repeats_with_escalation_and_the_reason_key_does_not_change`
5. `test_ticket231_worker_that_runs_again_resets_the_count_and_leaves_the_reason`
6. `test_ticket231_away_stop_blocks_at_most_away_blockers_times_for_the_same_stuck_worker`
7. README.md (automatic list) and docs/design.md ("A stalled worker wakes the coordinator", ticket 231)

Full suite (env -u ORQ_HOOK_TIMEOUT): 1226/1226; `python3 test_noite_replay.py` 3/3 three runs in a row. The stalled-worker reason now comes after the unpushed-commits one (`test_ticket231_unpushed_commits_come_before_a_stalled_worker`): before, it masked the ticket 180 notice and broke night-replay invariant (a). The event contract table (EVENTOS_LIDOS) lists the steer events as read by wake_stopped too.
