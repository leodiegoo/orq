#!/bin/sh
# Painel do agent manager: roda no terminal dele (sem LLM) e atualiza a cada 10 s, até 30 s se a volta anterior foi lenta (orq gerente intervalo). Com `orq gerente ligar` feito no coordenador, o Run
# fica ligado a este terminal: cada volta percorre os Runs do gerente, confirma os heartbeats e avisa o coordenador do resto, uma linha por
# Run. O carimbo `manager-alive` sai antes de chamar o orq: o coordenador sabe que o painel parou mesmo com o orq.py quebrado.
# Com o `orq gerente serve` vivo (ticket 128) o absorver sai com 3 e não faz nada: o painel só mostra o log do serve, e volta a absorver se ele cair.
h="${ORQ_HOME:-$HOME/.claude/orq}"
n=0
while true; do
  touch "$h/manager-alive"
  linha=$(orq manager absorb 2>&1)
  if [ $? -eq 3 ]; then
    clear
    printf 'agent manager (orq) — %s\n%s\n\n' "$(date '+%H:%M:%S')" "$linha"
    tail -n 20 "$h/logs/gerente.log" 2>/dev/null
    sleep 10
    continue
  fi
  clear
  printf 'agent manager (orq) — %s\n%s\n\n' "$(date '+%H:%M:%S')" "$linha"
  orq agents 2>&1
  printf '\n'
  orq status 2>&1 | head -6
  n=$((n+1)); [ $((n % 6)) -eq 1 ] && orq digest >/dev/null 2>&1  # atual.json do painel da 8765, a cada ~60 s
  sleep "$(orq manager interval 2>/dev/null || echo 10)"
done
