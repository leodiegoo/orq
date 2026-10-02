# 227: orq pr open audits commits and branch before the push

## Conformance
1. red→green `test_ticket227_pr_open_audit_refuses_wrong_author_and_trailer_but_not_the_product_name`, `test_ticket227_pr_open_guard_refuses_local_only_main_commit_and_the_environment_inside_the_feature`, `test_ticket227_pr_open_clean_branch_passes_and_lists_the_commits` (test_orq.py, temp origin + clone): `python3 test_orq.py ticket227` → `3/3 testes passaram`
2. README.md (`orq pr open` bullet) and docs/design.md ("Audit before the push (ticket 227)") updated

Note: full suite 1162-1163/1168; the failing ones are alarm/timing tests (finding_8, guard_fails_open, heartbeat_orca_down, review2_b4, ticket134_run_that_really_stopped) and also fail on the live main.
