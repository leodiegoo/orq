#!/usr/bin/env python3
"""Testes do precompact.py: Orca, orq, gh e engram falsos, tudo em diretório temporário. Rodam com `python3 test_precompact.py`."""
import json
import os
import subprocess
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(HERE, "precompact.py")

T = tempfile.mkdtemp()
def w(item_name, txt, exe=False):
    p = os.path.join(T, item_name)
    open(p, "w").write(txt)
    if exe:
        os.chmod(p, 0o755)
    return p

ORCA = w("orca", '#!/usr/bin/env python3\nimport json,os\nprint(json.dumps({"ok":True,"result":{"run":json.load(open(os.environ["FAKE_RUN"]))}}))\n', True)
CLI = w("cli", '''#!/usr/bin/env python3
import json,sys
a=sys.argv[1:]
if a[0]=="status": print("Aberto: backlog 1, rodando 1.")
elif a[0]=="agents": print(json.dumps([
 {"task":"task_a","run":"run_1","titulo":"vivo","estado":"rodando","modelo":"m","terminal":"term_a","fase":"x"},
 {"task":"task_b","run":"run_2","titulo":"perguntando","estado":"perguntando","modelo":"m","terminal":"term_b"},
 {"task":"task_c","run":"run_3","titulo":"acabou","estado":"liberado","modelo":"m","terminal":"term_c"}]))
elif a[:2]==["ticket","list"]: print("12 orq: snapshot (ready-for-agent)")
''', True)
GH = w("gh", '#!/bin/sh\necho \'[{"number":7,"title":"fix x","url":"https://github.com/o/r/pull/7"}]\'\n', True)
ENG = w("engram", '#!/bin/sh\nprintf "%s\\n" "$@" > "$FAKE_ENG"\n', True)
w("pend.json", json.dumps({"itens": [{"id": "freio", "tipo": "decisao", "titulo": "Decidir freio", "desde": "2026-09-28", "espera": "time de dados"}]}))
HOME = os.path.join(T, "home"); os.makedirs(HOME)
open(os.path.join(HOME, "events.jsonl"), "w").write("\n".join(json.dumps(e) for e in [
    {"tipo": "entrada", "origem": "usuario", "id": "e1", "ts": "t1", "texto": "faça x"},
    {"tipo": "intake", "entrada": "e1", "efeito": "tarefa", "ref": "task_z"},
    {"tipo": "entrada", "origem": "usuario", "id": "e2", "ts": "t2", "texto": "e y?"},
]) + "\n")
REPO = os.path.join(T, "repo"); os.makedirs(REPO); subprocess.run(["git", "init", "-q", REPO], check=True)

ENV = {**os.environ, "ORQ_HOME": HOME, "ORQ_ORCA": ORCA, "ORQ_CLI": CLI, "ORQ_GH": GH, "ORQ_ENGRAM": ENG,
       "ORQ_PENDENCIAS": os.path.join(T, "pend.json"), "ORQ_LOG": os.path.join(T, "orq.log"), "ORQ_DESENHO": "/x/desenho.md",
       "FAKE_RUN": os.path.join(T, "run.json"), "FAKE_ENG": os.path.join(T, "eng.txt"), "ORCA_TERMINAL_HANDLE": "term_x"}

def run_it(arg, ev, **env):
    p = subprocess.run([sys.executable, SCRIPT, *arg], input=json.dumps(ev), capture_output=True, text=True, env={**ENV, **env})
    assert p.returncode == 0, p.stderr
    return p.stdout

last_item = os.path.join(HOME, "handoff", "ultimo.md")

# sem Run ligado: não grava nada, não salva no engram
open(os.path.join(T, "run.json"), "w").write("null")
run_it([], {"session_id": "s1", "cwd": REPO})
assert not os.path.exists(last_item) and not os.path.exists(ENV["FAKE_ENG"])
assert run_it(["retomar"], {"session_id": "s1", "source": "compact"}) == ""

# coordenador: todas as seções, arquivo com data, link fixo e engram pela CLI
open(os.path.join(T, "run.json"), "w").write('{"id": "run_1"}')
run_it([], {"session_id": "s1", "cwd": REPO, "trigger": "manual"})
md = open(last_item).read()
for snippet in ["run_1", "task_a", "task_b", "python3 " + os.path.expanduser("~/.claude/scripts/orca-wait-runs.py") + " run_1 run_2",
               "12 orq: snapshot", "freio", "time de dados", "https://github.com/o/r/pull/7", "e1", "-> tarefa task_z", "-> no effect", "/x/desenho.md"]:
    assert snippet in md, snippet
assert "task_c" not in md  # agente liberado não entra
assert "orq status" not in md and "Aberto: backlog" not in md  # B29: o status vem do hook session, não daqui
assert os.path.islink(last_item) and os.readlink(last_item).endswith(".md") and len(os.listdir(os.path.dirname(last_item))) == 2
eng = open(ENV["FAKE_ENG"]).read().splitlines()
assert eng[0] == "save" and eng[1].startswith("Handoff ") and "--project" in eng and "repo" in eng
assert eng[eng.index("--topic") + 1] == "sessao/handoff" and eng[eng.index("--type") + 1] == "decision"

# fora de repo git: seção de PRs avisa em vez de falhar
run_it([], {"session_id": "s1", "cwd": T})
assert "outside a git repo" in open(last_item).read()

# retomada: só com source compact, no máximo 40 linhas, JSON de SessionStart
assert run_it(["retomar"], {"session_id": "s1", "source": "startup"}) == ""
out = json.loads(run_it(["retomar"], {"session_id": "s1", "source": "compact"}))["hookSpecificOutput"]
assert out["hookEventName"] == "SessionStart" and "Handoff" in out["additionalContext"] and len(out["additionalContext"].splitlines()) <= 41
# retomada em worker (sem handle): calada
assert run_it(["retomar"], {"session_id": "s1", "source": "compact"}, ORCA_TERMINAL_HANDLE="") == ""

# fail-open: entrada inválida, Orca quebrado
p = subprocess.run([sys.executable, SCRIPT], input="não é json", capture_output=True, text=True, env=ENV)
assert p.returncode == 0 and p.stdout == ""
run_it([], {"session_id": "s1"}, ORQ_ORCA="/nao/existe")

# ---- review-6 ----
import time

def test_review6_m12_resume_shows_newest_entry_and_the_map_within_the_cap():
    item_list = [{"id": f"p{i}", "tipo": "decisao", "titulo": f"Pendência {i}", "desde": "d", "espera": "x"} for i in range(12)]
    open(os.path.join(T, "pend.json"), "w").write(json.dumps({"itens": item_list}))
    evs = []
    for i in range(1, 13):
        evs += [{"tipo": "entrada", "origem": "usuario", "id": f"e{i}", "ts": f"t{i}", "texto": f"pedido {i}"}]
    open(os.path.join(HOME, "events.jsonl"), "w").write("\n".join(json.dumps(e) for e in evs) + "\n")
    run_it([], {"session_id": "s1", "cwd": REPO})
    out = json.loads(run_it(["retomar"], {"session_id": "s1", "source": "compact"}))["hookSpecificOutput"]["additionalContext"]
    assert "pedido 12" in out and "/x/desenho.md" in out and "pedido 3" in out and "pedido 2'" not in out, out
    assert out.index("pedido 12") < out.index("/x/desenho.md") < out.index("## Agents") < out.index("## User pending items"), "entradas e mapa vêm antes das seções que cortam (B40)"
    assert "+7 (orq pend)" in out and "lines cut" not in out and len(out.splitlines()) <= 61, len(out.splitlines())


def test_review6_b32_delivered_without_release_appear_with_the_command():
    cli = open(CLI).read().replace('{"task":"task_c"', '{"dispatch":"ctx_e","task":"task_e","run":"run_5","titulo":"pronto","estado":"entregue","modelo":"m","terminal":"term_e"},\n {"task":"task_r","run":"r","titulo":"retido","estado":"entregue","retido":"user_takeover","modelo":"m","terminal":"t"},\n {"task":"task_c"')
    open(CLI, "w").write(cli)
    run_it([], {"session_id": "s1", "cwd": REPO})
    md = open(last_item).read()
    assert "orq release ctx_e" in md and "task_r" not in md and "task_c" not in md, md


def test_review6_b33_last_old_is_notified_and_the_cap_skips_what_does_not_fit():
    ago = time.time() - 3600
    os.utime(os.path.realpath(last_item), (ago, ago))
    out = json.loads(run_it(["retomar"], {"session_id": "s1", "source": "compact"}))["hookSpecificOutput"]["additionalContext"]
    assert out.startswith("STALE handoff") and "did not write" in out, out.splitlines()[0]
    run_it([], {"session_id": "s1", "cwd": REPO})
    assert json.loads(run_it(["retomar"], {"session_id": "s1", "source": "compact"}))["hookSpecificOutput"]["additionalContext"].startswith("Handoff saved")
    sys.path.insert(0, HERE)
    os.environ.update({k: ENV[k] for k in ("ORQ_HOME", "ORQ_ORCA", "ORQ_LOG")})
    import precompact
    precompact.T0 = time.monotonic() - precompact.CAP_S - 1
    assert precompact.run_it(["echo", "x"]) == "", "sem orçamento, a seção fica vazia em vez de estourar o hook"


def test_review6_b29_snapshot_does_not_repeat_orq_status():
    run_it([], {"session_id": "s1", "cwd": REPO})
    assert "orq status" not in open(last_item).read() and "Aberto: backlog" not in open(last_item).read()


test_review6_m12_resume_shows_newest_entry_and_the_map_within_the_cap()
test_review6_b29_snapshot_does_not_repeat_orq_status()
test_review6_b32_delivered_without_release_appear_with_the_command()
test_review6_b33_last_old_is_notified_and_the_cap_skips_what_does_not_fit()
print("ok")
