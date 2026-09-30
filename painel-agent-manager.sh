#!/bin/sh
# Painel do agent manager: roda no terminal dele (sem LLM) e atualiza a cada 10 s. Com `orq gerente ligar` feito no coordenador, o Run
# fica ligado a este terminal: cada volta percorre os Runs do gerente, confirma os heartbeats e avisa o coordenador do resto, uma linha por
# Run. O carimbo `gerente-vivo` sai antes de chamar o orq: o coordenador sabe que o painel parou mesmo com o orq.py quebrado.
n=0
while true; do
  touch "${ORQ_HOME:-$HOME/.claude/orq}/gerente-vivo"
  linha=$(orq gerente absorver 2>&1)
  clear
  printf 'agent manager (orq) — %s\n%s\n\n' "$(date '+%H:%M:%S')" "$linha"
  orq agentes 2>&1
  printf '\n'
  orq status 2>&1 | head -6
  n=$((n+1)); [ $((n % 6)) -eq 1 ] && orq digest >/dev/null 2>&1  # atual.json do painel da 8765, a cada ~60 s
  sleep 10
done
