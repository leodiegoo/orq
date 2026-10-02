"""One reading of a Bash command for every orq hook that matches patterns on it (ticket 250). stdlib only.

`rtk git push`, `rtk proxy gh pr merge`, `env X=1 git push`, `for b in a b; do rtk git push; done`: the verdict has to be the same as for the bare command, so
no hook runs a regex on the raw `tool_input.command`. They ask for `segments(cmd)` and match with `^` on each one.
ponytail: regex, no shell parser. `bash -c "git push"` and `$(git push)` inside quotes pass; a real parser if that ever bites.
"""
import re

_HEREDOC = re.compile(r"<<-?\s*([\'\"]?)(\w+)\1([^\n]*)\n.*?\n[ \t]*\2[ \t]*(?=\n|$)", re.S)
_QUOTED = re.compile(r'"(?:[^"\\]|\\.)*"|\'[^\']*\'')
_REDIR_AMP = re.compile(r"(?<!&)&>|>&|>\|")  # `2>&1` becomes `2>1`, `&>log`, `>&log` and `>|log` become `>log`: still a redirection, no longer a separator
_CMD_PATH = re.compile(r"^\\?(?:[^\s/=<>]*/)*")  # `/usr/bin/git`, `\git`: bash runs git all the same
_SEPARATOR = re.compile(r"[;&|(){}\n]+")
_KEYWORD = re.compile(r"(?:!|do|then|else|elif|if|while|until)\s+")  # a loop or an `if` runs the command that follows
_PREFIX = re.compile(r"^(?:(?:\d*(?:>>?|<)\s*\S+|\w+=\S*|rtk(?:\s+proxy)?|env(?:\s+(?:-[CPSu]\s*\S+|--chdir[=\s]\S+|-\S+))*|command|time(?:\s+-p)?|sudo|nohup|exec|nice(?:\s+-n\s*\S+|\s+-\S+)*|stdbuf(?:\s+-\S+)*|timeout(?:\s+(?:-[sk]\s*\S+|-\S+))*\s+\d\S*)\s+)+")
_ENV_CHDIR = re.compile(r"\benv\b.*?\s(?:-C\s*|--chdir[=\s])(\S+)")  # `env -C dir cmd` runs cmd in dir: it becomes a `cd dir` segment


def no_text(cmd):
    """The command without heredoc bodies or quoted text: what is in there (a commit message, an echo) is not a command. The `&` of a redirection (`2>&1`,
    `&>log`, `>&log`) is dropped too, so the split at `&` does not cut the arguments that follow it off the command."""
    return _REDIR_AMP.sub(">", _QUOTED.sub('""', _HEREDOC.sub(r"\3", cmd)))


def segments(cmd):
    """Each simple command of `cmd`, in command position, without `rtk`, `rtk proxy`, `env`, `VAR=x`, `command`, `time`, `sudo` or a leading `do`/`then`.
    Quotes and heredocs are blanked first. `a && rtk proxy git push` gives ["a", "git push"]."""
    out = []
    for seg in _SEPARATOR.split(no_text(cmd)):
        seg = seg.strip()
        while m := _KEYWORD.match(seg):
            seg = seg[m.end():]
        seg = _CMD_PATH.sub("", seg)
        if m := _PREFIX.match(seg):
            c = _ENV_CHDIR.search(m.group(0))
            out += [f"cd {c.group(1)}"] if c else []
            seg = _CMD_PATH.sub("", seg[m.end():].strip())
        if seg:
            out.append(seg)
    return out
