#!/bin/sh
# Statusline do usuário: repassa o stdin ao launcher do HUD do OMC e, com o modo ausente ligado, acrescenta
# "away desde HH:MM" em amarelo à primeira linha. Caminho quente sem Python: o marcador estado/away é escrito
# por `orq ausente ligar` e apagado por `orq ausente desligar`. Desligado (ou marcador ilegível) a saída é a do HUD.
HUD=${ORQ_HUD:-${CLAUDE_CONFIG_DIR:-$HOME/.claude}/hud/omc-hud-cache.sh}
case "$0" in */*) AQUI=${0%/*} ;; *) AQUI=. ;; esac
MARCA=${ORQ_HOME:-$AQUI}/estado/away
desde=
[ -r "$MARCA" ] && desde=$(cat "$MARCA" 2>/dev/null)
if [ -z "$desde" ]; then
  exec sh "$HUD" "$@"
fi
seg=$(printf ' \033[33maway desde %s\033[0m' "$desde")
out=$(sh "$HUD" "$@")
nl='
'
case $out in
  "") printf '%s\n' "${seg# }" ;;
  *"$nl"*) printf '%s%s\n%s\n' "${out%%"$nl"*}" "$seg" "${out#*"$nl"}" ;;
  *) printf '%s%s\n' "$out" "$seg" ;;
esac
exit 0
