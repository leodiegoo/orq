#!/usr/bin/env python3
"""Marks the current directory as trusted in Claude Code (~/.claude.json)."""
import json, os, sys

path = os.path.realpath(sys.argv[1] if len(sys.argv) > 1 else os.getcwd())
cfg = os.path.expanduser('~/.claude.json')
with open(cfg) as f:
    data = json.load(f)
project = data.setdefault('projects', {}).setdefault(path, {})
if project.get('hasTrustDialogAccepted'):
    sys.exit(0)
project['hasTrustDialogAccepted'] = True
tmp = f'{cfg}.{os.getpid()}.tmp'
with open(tmp, 'w') as f:
    json.dump(data, f, indent=2)
os.replace(tmp, cfg)
print(f'claude: pasta confiável {path}')
