#!/usr/bin/env python3
"""orq `deploy_check` for projects on ZCloud: asks the quave-one whether the environment's deploy already covers the commit.

    quave-deploy-check.py --env <environment> --sha <commit> --ids development=<id>,staging=<id>,main=<id>[+<id>]

`--ids` links the git flow environment to its quave-one appEnvId(s) (`+` joins more than one: production has web and jobs). orq calls this as the `deploy_check`
of `projects/<name>.json` (`{base}` and `{sha}` of the merged PR). The quave-one speaks MCP over HTTP (https://mcp.quave.cloud/), so the call is a JSON-RPC
`tools/call get-app-env-status`, with no Claude involved. The token comes from QUAVE_MCP_TOKEN or from the `Authorization` of the `quave-one` server in ~/.claude.json.

A deploy covers the commit when `latestDeployment.gitCommitId` is the commit or a descendant of it (local git, in the project's folder).
Exit 0: every id covers it and is DEPLOYED (first stdout line: the proof, `v<version> <commit>`); 2: not covered yet or still building, or the quave-one did
not answer now; 1: the deploy that covers the commit failed; 3: it could not ask (token, environment id, unexpected answer).
"""
import argparse
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request

URL = os.environ.get("QUAVE_MCP_URL") or "https://mcp.quave.cloud/"
HTTP_S, FETCH_S = 15, 20  # orq cuts the `deploy_check` at 60 s: two environments and a fetch fit


class Unable(Exception):
    """It could not ask: the exit code goes in `code`."""

    def __init__(self, msg, code=3):
        super().__init__(msg)
        self.code = code


def token():
    """QUAVE_MCP_TOKEN, otherwise the Bearer of the quave-one server in ~/.claude.json (the user's or any project's)."""
    if os.environ.get("QUAVE_MCP_TOKEN"):
        return os.environ["QUAVE_MCP_TOKEN"]
    try:
        d = json.load(open(os.path.expanduser("~/.claude.json")))
    except (OSError, ValueError):
        d = {}
    for servers in [d.get("mcpServers"), *[p.get("mcpServers") for p in (d.get("projects") or {}).values() if isinstance(p, dict)]]:
        auth = ((servers or {}).get("quave-one") or {}).get("headers", {}).get("Authorization")
        if auth:
            return auth.removeprefix("Bearer ").strip()
    raise Unable("no quave-one token: set QUAVE_MCP_TOKEN or the quave-one server in ~/.claude.json")


def status(app_env_id):
    """The environment's `get-app-env-status` as a dict. Network down: Unable(code=2); HTTP error or odd answer: Unable(code=3)."""
    body = {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "get-app-env-status", "arguments": {"appEnvId": app_env_id}}}
    req = urllib.request.Request(URL, json.dumps(body).encode(), {"Authorization": f"Bearer {token()}", "Content-Type": "application/json",
                                                                   "Accept": "application/json, text/event-stream"})
    try:
        text = urllib.request.urlopen(req, timeout=HTTP_S).read().decode()
    except urllib.error.HTTPError as e:
        raise Unable(f"quave-one answered HTTP {e.code} for {app_env_id}") from e
    except (urllib.error.URLError, OSError, TimeoutError) as e:
        raise Unable(f"quave-one is down right now ({type(e).__name__})", 2) from e
    lines = [l[5:].strip() for l in text.splitlines() if l.startswith("data:")]  # the server answers in SSE; plain JSON works too
    try:
        res = json.loads(lines[-1] if lines else text)["result"]
        if res.get("isError"):
            raise Unable(f"quave-one refused {app_env_id}: {res['content'][0]['text'][:200]}")
        return json.loads(res["content"][0]["text"])
    except (ValueError, KeyError, IndexError, TypeError) as e:
        raise Unable(f"unexpected answer from the quave-one for {app_env_id}: {text[:200]!r}") from e


def covers(sha, deployed):
    """True if the deployed commit is `sha` or descends from it. An object the local repository lacks: one `git fetch` and another try."""
    if deployed == sha:
        return True
    for attempt in (0, 1):
        r = subprocess.run(["git", "merge-base", "--is-ancestor", sha, deployed], capture_output=True, text=True)
        if r.returncode in (0, 1):
            return r.returncode == 0
        if attempt == 0:
            try:
                subprocess.run(["git", "fetch", "--quiet", "origin"], capture_output=True, timeout=FETCH_S)
            except (OSError, subprocess.TimeoutExpired):
                return False
    return False


def verify(app_env_id, sha):
    """(code, line) of one appEnvId: 0 deployed and covers the commit, 1 failed covering it, 2 not yet."""
    d = (status(app_env_id).get("latestDeployment") or {})
    deployed = d.get("gitCommitId") or ""
    if not deployed or not covers(sha, deployed):
        return 2, f"{app_env_id}: the newest deploy ({deployed[:7] or 'none'}) does not cover {sha[:7]} yet"
    proof = f"v{d.get('version')} {deployed[:7]}"
    if d.get("isFailed"):
        return 1, f"{app_env_id}: deploy {proof} failed ({d.get('statusLabel') or d.get('status')})"
    if d.get("isSuccess"):
        return 0, proof
    return 2, f"{app_env_id}: deploy {proof} in progress ({d.get('statusLabel') or d.get('status')})"


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--env", required=True)
    p.add_argument("--sha", required=True)
    p.add_argument("--ids", required=True, help="environment=id[+id],…")
    a = p.parse_args(argv)
    ids = {k: v.split("+") for k, _, v in (x.partition("=") for x in a.ids.split(",") if x)}
    if a.env not in ids or not a.sha.strip():
        print(f"no quave-one id for {a.env!r} or no commit (ids: {', '.join(ids) or 'none'})", file=sys.stderr)
        return 3
    try:
        res = [verify(i, a.sha.strip()) for i in ids[a.env]]
    except Unable as e:
        print(e, file=sys.stderr)
        return e.code
    worst = 1 if any(c == 1 for c, _ in res) else 2 if any(c == 2 for c, _ in res) else 0
    if worst == 0:
        print(" + ".join(l for _, l in res))
    else:
        print("\n".join(l for c, l in res if c == worst), file=sys.stderr)
    return worst


if __name__ == "__main__":
    sys.exit(main())
