#!/usr/bin/env python3
"""Night replay (ticket 216): the night of 01/10 played back against the real Stop and the real manager, with a simulated clock.

The unit tests build the events by hand, and the failures of that night were in what a producer really writes versus what a consumer reads (ticket 180:
the tests of 174 wrote the `cycle` event the integrator never writes). Here the events come from the real log (fixtures/noite-2026-10-01), fed in order,
and at each step the real commands run in a subprocess with the fake Orca of test_orq: `orq hook stop` at each moment the coordinator really ended a turn,
`orq gerente absorver` every 5 min while it really sat idle, `orq ingest` at each delivery. Invariants, any one broken fails the replay:

  (a) away on, coordinator idle and work that does not need the user: a notice is typed into the coordinator within 10 min;
  (b) no Stop blocks for a service dispatch, a sent-back one, or a queue give-up whose ticket was dispatched later;
  (c) the branch a delivery puts on the integrator queue is the branch of the ticket's worktree, never a file name from the worker's text.

What the log does not hold (unpushed commits, when the coordinator sat idle, the delivery branches) is in verdade.json, from the tickets' reports.
The fixture is the real log through scripts/anonimizar-noite.py (ticket 349): versioned, with nothing the public repository cannot hold; a missing one fails here, loudly.
`python3 test_noite_replay.py` runs the night and the self-checks; `scripts/integrar.py` runs it before the fast-forward."""
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import test_orq as t  # noqa: E402 - Env, the fake Orca and the isolated ORQ_HOME

orqlib = t.orqlib
FIX = os.path.join(HERE, "fixtures", "noite-2026-10-01")
FIXTURE_FILES = ("events.jsonl", "cursor.json", "verdade.json", "ciclos.log")
GENERATE = "python3 scripts/anonimizar-noite.py --in <folder with the real events.jsonl and cursor.json> --out fixtures/noite-2026-10-01"


def _require_fixture(folder):
    """A replay with no night to play must not pass: it exits 1 naming what is missing and the command that generates it."""
    if missing := [f for f in FIXTURE_FILES if not os.path.isfile(os.path.join(folder, f))]:
        sys.exit(f"night replay: the fixture is incomplete, {', '.join(missing)} missing in {folder}\nthe real log stays out of git, so generate the fixture from it:\n  {GENERATE}")


_require_fixture(FIX)
TRUTH = json.load(open(os.path.join(FIX, "verdade.json")))
RUN = TRUTH["run"]
LOCAL = timezone(timedelta(hours=-3))  # ciclos.log is in the integrator's local time
STOP_OUT = {"resposta_coordenador"}  # what `orq hook stop` writes on each turn: its timestamps are the Stop moments, the replay's Stop writes it again
MANAGER_STEP = timedelta(minutes=5)
NOTICE_MAX = timedelta(minutes=10)


def _iso(d):
    return d.strftime("%Y-%m-%dT%H:%M:%SZ")


def _at(series, now_at):
    """The value of a [[ts, value, ...], ...] series at `now_at` (the last point not after it)."""
    return next((p[1] for p in reversed(series) if orqlib._dt(p[0]) <= now_at), 0)


def _night():
    """(raw lines, the same events with pt keys) of the fixture, in log order."""
    lines = [x for x in open(os.path.join(FIX, "events.jsonl")) if x.strip()]
    return lines, [orqlib.to_pt(json.loads(x)) for x in lines]


def _cycles():
    """[(UTC time, line)] of the fixture's ciclos.log."""
    out = []
    for x in open(os.path.join(FIX, "ciclos.log"), encoding="utf-8"):
        if m := re.match(r"(\d{4}-\d\d-\d\d \d\d:\d\d) ", x):
            out.append((datetime.strptime(m.group(1), "%Y-%m-%d %H:%M").replace(tzinfo=LOCAL).astimezone(timezone.utc), x))
    return out


def _ticks(events, since):
    """[(time, action)] in order: `stop` at each real end of turn, `gerente` every 5 min in the idle windows, `ingest` at each delivery."""
    stops, last = [], None
    for e in events:
        if e.get("tipo") in STOP_OUT and (d := orqlib._dt(e["ts"])) >= since and (last is None or d - last >= timedelta(minutes=2)):
            stops.append((d, "stop"))
            last = d
    manager = []
    for start, end, _ in TRUTH["coordenador_parado"]:
        d = orqlib._dt(start)
        while d <= orqlib._dt(end):
            if d >= since:
                manager.append((d, "gerente"))
            d += MANAGER_STEP
    deliveries = [(orqlib._dt(x["ts"]), "ingest") for x in TRUTH["entregas"] if orqlib._dt(x["ts"]) >= since]
    return sorted(stops + manager + deliveries)


def _setup(env):
    """The Env with the manager on the night's Run, the cursor of that moment, the tickets and worktrees of the deliveries."""
    a = t.Env(run=RUN, **env)
    tmp = a.tmp.name
    repo = t._repo_with_branch(tmp)
    a.env["ORQ_REPOS"] = repo
    for x in TRUTH["entregas"]:
        subprocess.run(["git", "-C", repo, "worktree", "add", "-q", "-b", x["branch"], os.path.join(a.env["ORQ_WT_ROOT"], x["ticket"])], check=True)
    os.makedirs(a.env["ORQ_ISSUES"])
    os.makedirs(a.home)
    t._write_state(os.path.join(a.home, "gerente.json"), {"coordenador": "term_coord", "gerente": "term_ger", "runs": [RUN]})
    with open(os.path.join(a.home, "cursor.json"), "w") as f:
        f.write(open(os.path.join(FIX, "cursor.json")).read())
    a.set("terminals.json", ["term_coord", "term_ger"])
    return a


def _queue_file(a, fed):
    """integrate-queue.json as `orq integrate queue add|rm` left it after the fed events (that command writes the event and the file; the replay feeds
    the event). # ponytail: rebuilds the file at each step, fine for a few hundred events."""
    items = {}
    for e in fed:
        if e.get("tipo") == "integrar_fila" and e.get("ticket"):
            n = str(e["ticket"]).zfill(2)
            if e.get("op") == "add":
                items[n] = {"branch": e.get("branch"), "ticket": n, "ts": e["ts"]}
            elif e.get("op") == "rm":
                items.pop(n, None)
    path = os.path.join(a.home, orqlib.INTEGRATE_QUEUE_FILE)
    current = orqlib.to_pt(json.load(open(path))).get("itens") or [] if os.path.exists(path) else []
    replayed = {i["ticket"]: i for i in current if i.get("ticket") in {x["ticket"].zfill(2) for x in TRUTH["entregas"]}}  # what the replay's ingest added
    json.dump(orqlib.to_en({"itens": [*{**items, **replayed}.values()]}), open(path, "w"))


def _tickets_files(a, fed):
    """The plan's tickets as `orq ticket novo|fechar` left them: claimed after `novo`, resolved after `fechar` (the Stop does not cite a closed ticket's delivery)."""
    tickets = {}
    for e in fed:
        if e.get("tipo") == "ticket" and e.get("ticket") and e.get("op") in ("novo", "fechar"):
            n = str(e["ticket"])
            tickets[n] = {**tickets.get(n, {}), "task": e.get("task") or tickets.get(n, {}).get("task"), "status": "claimed" if e["op"] == "novo" else orqlib.STATUS_CLOSED}
    for x in TRUTH["entregas"]:
        tickets.setdefault(x["ticket"], {"task": x["task"], "status": "claimed"})
    for n, tk in tickets.items():
        with open(os.path.join(a.env["ORQ_ISSUES"], f"{n}-noite.md"), "w") as f:
            f.write(f"# {n}: orq: noite\n\nStatus: {tk['status']}\nBlocked by: (nenhum)\nRun: {RUN}\nTask: {tk['task']}\n")


def _agents_file(a, fed):
    """open.json `agentes` as the refresh writes them from the worker-list: each dispatch of the night still on it (no `dispatch_end` nor `liberar`, retained
    included, which `agents()` hides), `entregue` after its worker_done. reassess (in the Stop) turns them into servico or devolvida from the log, which is what the replay checks."""
    rows, done = {}, {e.get("dispatch") for e in fed if e.get("tipo") == "worker_done"}
    gone = {e.get("dispatch") for e in fed if e.get("tipo") in ("fim_dispatch", "liberar")}
    for e in fed:
        if e.get("tipo") == "despacho" and e.get("dispatch") and e["dispatch"] not in gone:
            rows[e["dispatch"]] = {"dispatch": e["dispatch"], "task": e.get("task"), "titulo": e.get("titulo"), "run": e.get("run"), "desde": e["ts"],
                                   "estado": "entregue" if e["dispatch"] in done else "rodando", "agente": e.get("agente") or "claude"}
    json.dump(orqlib.to_en({"agentes": list(rows.values())}), open(os.path.join(a.home, "open.json"), "w"))


def _stop_violation(reason, fed):
    """Why the Stop's block (its `reason`, with the events `fed` so far) breaks invariant (b), or None."""
    m = re.search(r"worker (ctx_\w+) delivered", reason)
    if m:
        d = m.group(1)
        if any(e.get("dispatch") == d and (e.get("tipo") == "despacho" and e.get("servico") or e.get("tipo") == "servico_marcado") for e in fed):
            return f"blocked for the service dispatch {d}"
        if d in orqlib._sent_back(fed):
            return f"blocked for {d}, sent back to the worker"
    m = re.search(r"gave up on (.+?) \(", reason)
    if m:
        quoted = m.group(1).rstrip("…")
        gave = [e for e in fed if e.get("tipo") == "despacho_fila" and e.get("op") == "desistiu" and str(e.get("titulo") or "").startswith(quoted)]
        for g in gave:
            later = [e for e in fed if e.get("tipo") == "despacho" and e["ts"] > g["ts"] and (e.get("titulo") == g.get("titulo") or g.get("ticket") and e.get("ticket") == g["ticket"])]
            if later:
                return f"blocked for the give-up {g.get('id')} ({quoted}), dispatched again at {later[0]['ts']}"
    return None


def replay(since=None, **env):
    """Plays the night from `since` (default: the whole fixture) and returns (violations, report). `env` goes to every command (e.g. ORQ_ACORDA_PARADO_MIN)."""
    started = time.time()
    lines, events = _night()
    since = orqlib._dt(since) if since else orqlib._dt(events[0]["ts"])
    a = _setup(env)
    cycles, ticks = _cycles(), _ticks(events, since)
    log_path, cycles_path = os.path.join(a.home, "events.jsonl"), a.env["ORQ_CICLOS_LOG"]
    fed, i, violations, notices, counts = [], 0, [], [], {"stop": 0, "gerente": 0, "ingest": 0}
    for now_at, action in ticks:
        with open(log_path, "a") as f:
            while i < len(events) and orqlib._dt(events[i]["ts"]) <= now_at:
                if events[i].get("tipo") not in STOP_OUT:
                    f.write(lines[i])
                    fed.append(events[i])
                i += 1
        with open(cycles_path, "w", encoding="utf-8") as f:
            f.writelines(x for d, x in cycles if d <= now_at)
        _queue_file(a, fed)
        _agents_file(a, fed)
        _tickets_files(a, fed)
        clock = {"ORQ_AGORA": _iso(now_at), "ORQ_SEM_PUSH": str(_at(TRUTH["sem_push"], now_at))}
        counts[action] += 1
        if action == "stop":
            r = a.orq("hook", "stop", stdin=json.dumps({"session_id": "6d715968aaaa"}), **clock)
            out = json.loads(r.stdout) if r.stdout.strip() else {}
            if out.get("decision") == "block" and (v := _stop_violation(out.get("reason") or "", fed)):
                violations.append(f"(b) {_iso(now_at)}: {v}")
        elif action == "gerente":
            before = len(t._log(a, "send.log"))
            a.orq("gerente", "absorver", ORCA_TERMINAL_HANDLE="term_ger", **clock)
            sent = t._log(a, "send.log")[before:]
            if any(c[0] == "send" and c[c.index("--terminal") + 1] == "term_coord" and "--text" in c and c[c.index("--text") + 1].startswith("orq: coordinator stopped") for c in sent):
                notices.append(now_at)
        else:
            x = next(x for x in TRUTH["entregas"] if orqlib._dt(x["ts"]) == now_at)
            a.set("inbox.json", {"result": {"messages": [{"id": x["msg"], "run_id": RUN, "type": "worker_done", "subject": x["corpo"].split(";")[0], "body": x["corpo"],
                                                          "sequence": 1 + TRUTH["entregas"].index(x), "read": 0, "created_at": x["ts"],
                                                          "payload": json.dumps({"taskId": x["task"], "dispatchId": x["dispatch"], "outcome": "succeeded"})}]}})
            a.orq("ingest", **clock)
            queued = {q["ticket"]: q["branch"] for q in orqlib.to_pt(json.load(open(os.path.join(a.home, orqlib.INTEGRATE_QUEUE_FILE)))).get("itens") or []}
            if queued.get(x["ticket"]) != x["branch"]:
                violations.append(f"(c) {_iso(now_at)}: ticket {x['ticket']} entered the integrator queue as {queued.get(x['ticket'])!r}, its worktree is on {x['branch']}")
    violations += _wake_violations(notices, since)
    secs = time.time() - started
    report = f"night replay: {len(fed)} events, {counts['stop']} Stop(s), {counts['gerente']} manager round(s), {counts['ingest']} ingest(s), {len(notices)} notice(s) typed, {secs:.1f} s"
    return violations, report


def _wake_violations(notices, since):
    """(a): each stretch where the coordinator sat idle with unpushed commits must have a notice typed within NOTICE_MAX of its start."""
    out = []
    for start, end, _ in TRUTH["coordenador_parado"]:
        s, e = orqlib._dt(start), orqlib._dt(end)
        waiting = next((orqlib._dt(p[0]) for p in TRUTH["sem_push"] if s <= orqlib._dt(p[0]) <= e and p[1]), s if _at(TRUTH["sem_push"], s) else None)
        if waiting is None or waiting < since or e - waiting <= NOTICE_MAX:
            continue
        first = next((n for n in notices if waiting <= n <= e), None)
        if not first or first - waiting > NOTICE_MAX:
            out.append(f"(a) {_iso(waiting)}: coordinator idle with unpushed commits and no notice typed for over {NOTICE_MAX.seconds // 60} min"
                       + (f" (first at {_iso(first)})" if first else " (none until it woke at " + end + ")"))
    return out


def test_ticket216_the_night_of_10_01_keeps_the_three_invariants():
    violations, report = replay()
    print(report)
    assert not violations, "\n".join(violations)


def test_ticket216_the_replay_fails_with_the_wake_up_turned_off():
    violations, _ = replay(since="2026-10-02T09:00:00Z", ORQ_ACORDA_PARADO_MIN="100000")
    assert any(v.startswith("(a) 2026-10-02T09:22:34Z") for v in violations), violations


def test_ticket216_a_stop_that_blocks_for_a_service_dispatch_is_a_violation():
    fed = [{"tipo": "despacho", "dispatch": "ctx_i", "servico": True, "ts": "2026-10-02T00:00:00Z"}]
    assert "service" in _stop_violation("away is on ...: worker ctx_i delivered ticket 83 and the delivery was not integrated", fed)
    fed = [{"tipo": "despacho_fila", "op": "desistiu", "id": "fd1", "titulo": "orq: ligar o backlog", "ts": "2026-10-02T02:03:58Z"},
           {"tipo": "despacho", "dispatch": "ctx_b", "titulo": "orq: ligar o backlog", "ts": "2026-10-02T02:04:46Z"}]
    assert "dispatched again" in _stop_violation("the dispatch queue gave up on orq: ligar o backlog (New worktrees require --name)", fed)
    assert _stop_violation("worker ctx_x delivered ticket 9 and the delivery was not integrated", fed) is None


def _copy_code(dst, fixture=True):
    """The orq code (top-level *.py) and, with `fixture`, the night fixture, in `dst`: a tree the test may break without touching this one."""
    for f in os.listdir(HERE):
        if f.endswith(".py"):
            shutil.copy(os.path.join(HERE, f), dst)
    if fixture:
        shutil.copytree(FIX, os.path.join(dst, "fixtures", "noite-2026-10-01"))


def _run_replay(folder):
    """`python3 test_noite_replay.py the_night_of_10_01` in `folder` (the invariants test alone): (exit code, output)."""
    r = subprocess.run([sys.executable, "test_noite_replay.py", "the_night_of_10_01"], cwd=folder, capture_output=True, text=True, timeout=300)
    return r.returncode, r.stdout + r.stderr


# What each fix of the night of 10/01 undoes, as (the fix's ticket, text before which the edit applies, the fix as it is, how it reads without it, what the red replay says)
REVERTS = [
    (180, "def _no_push():", 'return int(os.environ["ORQ_SEM_PUSH"])', "return 0", "(a) "),  # unpushed commits no longer read
    (169, "def _integrator_dispatch(", 'branch = (_payload_branch(p.get("branch")) or _orq_wt_branch(t["num"]) or (wt and _orq_branch((_git(wt, "branch", "--show-current") or "").strip()))\n              or _text_branch(text_value))',
     "branch = next(iter(BRANCH_RE.findall(text_value)), None)", "(c) "),  # the branch read from the worker's text, whatever it cites
    (135, "def reassess(", "_dispatch_ticket(events), _services(events)", "_dispatch_ticket(events), {}", "blocked for the service dispatch"),  # a legacy integrator is a delivery again
    (175, "def away_abandoned(", 'and not (e.get("titulo") and v.get("titulo") == e["titulo"])}', "}", "blocked for the give-up"),  # an old give-up no longer matches by title
]


def _revert(fix, folder):
    """Undoes fix `fix` in the orqlib.py of `folder`: the first `old` at or after `after`. ValueError if the code no longer reads that way: update REVERTS, do not skip."""
    _, after, old, new, _ = next(x for x in REVERTS if x[0] == fix)
    path = os.path.join(folder, "orqlib.py")
    text = open(path, encoding="utf-8").read()
    where = text.index(old, text.index(after))
    with open(path, "w", encoding="utf-8") as f:
        f.write(text[:where] + new + text[where + len(old):])


def test_ticket349_the_fixture_is_not_gitignored():
    """The root cause of 349: `events.jsonl` and `cursor.json` are runtime state everywhere else, so git dropped the fixture. `git check-ignore -q` exits 1 for a path that is not ignored."""
    for f in ("events.jsonl", "cursor.json"):
        r = subprocess.run(["git", "-C", HERE, "check-ignore", "-q", os.path.join("fixtures", "noite-2026-10-01", f)])
        assert r.returncode == 1, f"fixtures/noite-2026-10-01/{f} is ignored by git (.gitignore needs `!fixtures/**/{f}`)"


def test_ticket349_a_missing_fixture_fails_loud_with_the_command_to_generate_it():
    with tempfile.TemporaryDirectory() as tmp:
        _copy_code(tmp, fixture=False)
        code, out = _run_replay(tmp)
    assert code != 0 and "scripts/anonimizar-noite.py" in out and "events.jsonl" in out, (code, out[-500:])


def _sample(folder):
    """A small made-up 'real' night: what the anonymizer must swap, none of it from the real log."""
    os.makedirs(folder)
    lines = [
        {"ts": "2026-10-01T10:00:00Z", "type": "entry", "path": "/Users/alice/Developer/acme/widgets/.scratch/a.md", "project": "widgets", "task_closed": 1},
        {"ts": "2026-10-01T10:01:00Z", "type": "pr", "url": "https://github.com/AcmeCorp/widgets/pull/42", "number": 42, "head": "docs/cache-prisma-77", "terminal": "term_deadbeef",
         "run": "run_0123456789ab", "task": "task_0123456789ab", "title": "widgets: fix for prisma 77, owner bob@acme.example"},
        {"ts": "2026-10-01T10:02:00Z", "type": "entry", "path": "/Users/alice/.claude/orq/x.md", "group": "billing", "terminal": "term_cafebabe"},
        {"ts": "2026-10-01T10:03:00Z", "type": "entry", "path": "/Users/alice/orca/workspaces/gadgets/wt1", "project": "gadgets", "terminal": "term_deadbeef", "run": "run_0123456789ab"},
    ]
    with open(os.path.join(folder, "events.jsonl"), "w") as f:
        f.writelines(json.dumps(x, ensure_ascii=False) + "\n" for x in lines)
    with open(os.path.join(folder, "cursor.json"), "w") as f:
        json.dump({"ingest": {"runs": ["run_0123456789ab"]}}, f)


def _anonymize(src, out, **env):
    return subprocess.run([sys.executable, os.path.join(HERE, "scripts", "anonimizar-noite.py"), "--in", src, "--out", out], capture_output=True, text=True,
                          env={**os.environ, "ORQ_TERMOS": os.path.join(src, "none.txt"), **env})


def test_ticket349_the_anonymizer_is_deterministic_and_swaps_every_private_thing():
    with tempfile.TemporaryDirectory() as tmp:
        _sample(os.path.join(tmp, "real"))
        for n in ("one", "two"):
            r = _anonymize(os.path.join(tmp, "real"), os.path.join(tmp, n))
            assert r.returncode == 0, r.stderr
        files = {f: open(os.path.join(tmp, "one", f)).read() for f in ("events.jsonl", "cursor.json")}
        assert files == {f: open(os.path.join(tmp, "two", f)).read() for f in files}, "two runs gave different bytes"
    out = "\n".join(files.values())
    for private in ("alice", "acme", "AcmeCorp", "widgets", "gadgets", "bob@", "billing", "deadbeef", "0123456789ab", "/Users/"):
        assert private not in out, f"{private!r} survived:\n{out}"
    rows = [json.loads(x) for x in files["events.jsonl"].splitlines()]
    assert rows[0]["path"] == "/home/dev/app-a/.scratch/a.md" and rows[0]["project"] == "app-a" and rows[0]["task_closed"] == 1  # a JSON key that looks like an id stays
    assert rows[1]["url"] == "https://github.com/example-org/app/pull/42" and rows[1]["number"] == 42  # same PR number
    assert rows[1]["terminal"] == rows[3]["terminal"] == "term_001" and rows[2]["terminal"] == "term_002"  # sequential, one per handle
    assert rows[1]["run"] == rows[3]["run"] == "run_001" and rows[1]["task"] == "task_001"
    assert json.loads(files["cursor.json"])["ingest"]["runs"] == ["run_001"], "the cursor must use the events' Run"
    assert rows[2]["path"] == "/home/dev/.claude/orq/x.md" and rows[3]["path"] == "/home/dev/app-d/wt1" and rows[3]["project"] == "app-d"  # one fake per project, in order of appearance
    assert rows[2]["group"].startswith("app-") and rows[1]["head"] == "docs/cache-app-b" and "app-b" in rows[1]["title"]  # the group and the client (`prisma 77`) get fakes too


def test_ticket349_the_anonymizer_writes_nothing_when_the_forbidden_list_still_matches():
    with tempfile.TemporaryDirectory() as tmp:
        _sample(os.path.join(tmp, "real"))
        with open(os.path.join(tmp, "terms.txt"), "w") as f:
            f.write("# private\nre:pull/\\d+\n")  # `pull/42` survives the swap (the number stays on purpose), so the list must stop it
        r = _anonymize(os.path.join(tmp, "real"), os.path.join(tmp, "out"), ORQ_TERMOS=os.path.join(tmp, "terms.txt"))
        assert r.returncode == 1 and "events.jsonl:2: forbidden term" in r.stderr, (r.returncode, r.stderr)
        assert not os.path.exists(os.path.join(tmp, "out")), "wrote the fixture although the check failed"


def test_ticket349_the_fixture_passes_the_publication_audit():
    """The fixture goes through `orq audit-publication` as a commit of its own. The private forbidden list is used when this machine has it; the generic leaks (a user's
    path, an e-mail, a GitHub URL outside example-org) are checked either way, so a clean clone without the list is not a silent pass."""
    generic = ["re:/Users/", r"re:/home/(?!dev/)", r"re:[\w.+-]+@[\w-]+\.\w", r"re:github\.com/(?!example-org/)"]
    private = os.environ.get("ORQ_TERMOS") or os.path.join(orqlib.PLAN, "termos-proibidos.txt")
    with tempfile.TemporaryDirectory() as tmp:
        terms = os.path.join(tmp, "terms.txt")
        with open(terms, "w") as f:
            f.write("\n".join(generic + ([open(private).read()] if os.path.exists(private) else [])) + "\n")
        repo = os.path.join(tmp, "repo")
        shutil.copytree(FIX, os.path.join(repo, "fixtures", "noite-2026-10-01"))
        git = lambda *x: subprocess.run(["git", "-C", repo, *x], check=True, capture_output=True, text=True)  # noqa: E731
        git("init", "-q")
        git("config", "user.email", "dev@example.com"), git("config", "user.name", "dev")
        git("add", "-A"), git("commit", "-q", "-m", "test: add the night fixture")
        old = {k: os.environ.get(k) for k in ("ORQ_TERMOS", "ORQ_AUTOR")}
        os.environ["ORQ_TERMOS"], os.environ["ORQ_AUTOR"] = terms, "dev@example.com"
        try:
            assert orqlib.audit_publication(["HEAD"], repo=repo) == []
            with open(os.path.join(repo, "leak.txt"), "w") as f:
                f.write("/Users/alice/x\n")
            git("add", "-A"), git("commit", "-q", "-m", "test: a leak")
            assert orqlib.audit_publication(["HEAD~1..HEAD"], repo=repo), "the audit did not catch a /Users path: the check above proves nothing"
        finally:
            for k, v in old.items():
                os.environ.pop(k, None) if v is None else os.environ.__setitem__(k, v)


def test_ticket349_reverting_each_fix_of_the_night_keeps_the_replay_red():
    """The proof ticket 216 made with the real log, now with the fixture: each fix undone alone (in a copy of the code) turns the invariants test red with its own message."""
    def red(fix):
        with tempfile.TemporaryDirectory() as tmp:
            _copy_code(tmp)
            _revert(fix, tmp)
            return fix, *_run_replay(tmp)
    with ThreadPoolExecutor(len(REVERTS)) as pool:
        results = list(pool.map(red, [x[0] for x in REVERTS]))
    for fix, code, out in results:
        want = next(x[4] for x in REVERTS if x[0] == fix)
        assert code == 1 and want in out, f"without fix {fix} the replay should fail with {want!r}; exit {code}\n{out[-1500:]}"


if __name__ == "__main__":
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f) and (sys.argv[1] if len(sys.argv) > 1 else "") in n]
    failures = []
    for name, fn in tests:
        try:
            fn()
            print(f"ok      {name}")
        except Exception as e:  # noqa: BLE001 - the report shows all failures at once
            failures.append(name)
            print(f"FALHOU  {name}: {type(e).__name__}: {str(e)[-2000:]}")
    print(f"{len(tests) - len(failures)}/{len(tests)} testes passaram")
    sys.exit(1 if failures else 0)
