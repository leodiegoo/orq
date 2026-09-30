#!/bin/sh
# Painel do agent manager: roda no terminal dele (sem LLM) e atualiza a cada 10 s. Com `orq gerente ligar` feito no coordenador, o Run
# fica ligado a este terminal: cada volta percorre os Runs do gerente, confirma os heartbeats e avisa o coordenador do resto, uma linha por
# Run.
while true; do
  linha=$(orq gerente absorver 2>&1)
  clear
  printf 'agent manager (orq) — %s\n%s\n\n' "$(date '+%H:%M:%S')" "$linha"
  orq agentes 2>&1
  printf '\n'
  orq status 2>&1 | head -6
  sleep 10
done
