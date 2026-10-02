# Ticket 225: review correction loop with recorded decisions

`orq review --decidir` records `achado_decisao`; the next review of the branch appends the decisions and the commits since the last reviewed head to the intent; `orq send-back --achado` appends the invariant rule. `--decidir` does not run the review. Full suite: 1164/1169; the 5 failures (`finding_8` x2, `guard_fails_open`, `heartbeat_orca_down`, `review2_b4_alarm`) fail identically on origin/main. `test_precompact.py` ok.

## Conformance

1. red→green `test_ticket225_review_intent_is_pure_and_unchanged_without_decisions_or_commits`, `test_ticket225_ignored_finding_shows_in_next_review_of_the_branch_and_not_in_another_branch`, `test_ticket225_commits_after_the_last_reviewed_head_get_their_section_and_otherwise_the_intent_is_unchanged`, `test_ticket225_send_back_with_achado_carries_the_invariant_rule_and_without_it_does_not` (test_orq.py, `python3 test_orq.py ticket225` -> 4/4)
2. README.md (`orq review` paragraph, `--decidir`, both sections, event) and docs/design.md (section "The correction loop keeps its memory (ticket 225)")
