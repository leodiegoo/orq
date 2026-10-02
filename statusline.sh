#!/bin/sh
# The user's statusline: passes stdin to the OMC HUD launcher and, with away mode on, appends
# "away since HH:MM" in yellow to the first line. Hot path with no Python: the estado/away marker is written
# by `orq away on` and deleted by `orq away off`. Off (or an unreadable marker) the output is the HUD's.
HUD=${ORQ_HUD:-${CLAUDE_CONFIG_DIR:-$HOME/.claude}/hud/omc-hud-cache.sh}
case "$0" in */*) HERE=${0%/*} ;; *) HERE=. ;; esac
MARK=${ORQ_HOME:-$HERE}/estado/away
since=
[ -r "$MARK" ] && since=$(cat "$MARK" 2>/dev/null)
if [ -z "$since" ]; then
  exec sh "$HUD" "$@"
fi
seg=$(printf ' \033[33maway since %s\033[0m' "$since")
out=$(sh "$HUD" "$@")
nl='
'
case $out in
  "") printf '%s\n' "${seg# }" ;;
  *"$nl"*) printf '%s%s\n%s\n' "${out%%"$nl"*}" "$seg" "${out#*"$nl"}" ;;
  *) printf '%s%s\n' "$out" "$seg" ;;
esac
exit 0
