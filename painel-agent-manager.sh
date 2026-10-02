#!/bin/sh
# Agent manager panel: runs in its terminal (no LLM) and refreshes every 10 s, up to 30 s if the previous round was slow (orq manager interval). With `orq manager bind` done in the coordinator, the Run
# stays bound to this terminal: each round walks the manager's Runs, confirms the heartbeats and notifies the coordinator of the rest, one line per
# Run. The `manager-alive` stamp is written before calling orq: the coordinator knows the panel stopped even with a broken orq.py.
# With `orq manager serve` alive (ticket 128) absorb exits with 3 and does nothing: the panel only shows the serve log, and goes back to absorbing if it dies.
h="${ORQ_HOME:-$(cd "$(dirname "$0")" && pwd -P)}"  # the state lives in the clone this panel belongs to (ticket 124)
n=0
while true; do
  touch "$h/manager-alive"
  line=$(orq manager absorb 2>&1)
  rc=$?
  [ $rc -eq 0 ] && touch "$h/manager-ok"  # progress stamp (ticket 229): only a round that exited 0 counts, even when orq.py does not import
  if [ $rc -eq 3 ]; then
    clear
    printf 'agent manager (orq) — %s\n%s\n\n' "$(date '+%H:%M:%S')" "$line"
    tail -n 20 "$h/logs/gerente.log" 2>/dev/null
    sleep 10
    continue
  fi
  clear
  printf 'agent manager (orq) — %s\n%s\n\n' "$(date '+%H:%M:%S')" "$line"
  orq agents 2>&1
  printf '\n'
  orq status 2>&1 | head -6
  n=$((n+1)); [ $((n % 6)) -eq 1 ] && orq digest >/dev/null 2>&1  # the 8765 panel's atual.json, every ~60 s
  sleep "$(orq manager interval 2>/dev/null || echo 10)"
done
