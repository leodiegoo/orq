# 223: PR with no registered check is not ready

`_gh_ci` flags an empty rollup (`sem_check`), `_pr_reading` shows `? no check registered for N min` and `pronto=False`; project key `sem_ci: true` opts out.

## Conformance
1. red→green `test_ticket223_empty_rollup_is_not_ready_and_says_how_long_without_check` (red against origin/main orqlib), `test_ticket223_empty_rollup_is_ready_when_the_project_declares_sem_ci`, `test_ticket223_checks_that_show_up_in_a_sem_ci_project_still_count`, `test_ticket223_one_green_check_is_still_ready` (test_orq.py)
2. README.md item 9 of the project file steps; docs/design.md last paragraph (ticket 223)
