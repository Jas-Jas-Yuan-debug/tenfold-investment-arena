# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Jas-Jas-Yuan-debug

"""Install the localhost arena as a user LaunchAgent after the old server stops."""
from __future__ import annotations

import os
import plistlib
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LABEL = 'local.tenfold.investment-arena'


def main():
    agents = Path.home() / 'Library' / 'LaunchAgents'
    agents.mkdir(parents=True, exist_ok=True)
    logs = ROOT / 'data' / 'logs'
    logs.mkdir(parents=True, exist_ok=True)
    path = agents / (LABEL + '.plist')
    config = {
        'Label': LABEL,
        'ProgramArguments': [str(ROOT / '.venv' / 'bin' / 'python'), '-m', 'uvicorn',
                             'app:app', '--host', '127.0.0.1', '--port', '8787', '--no-access-log',
                             '--timeout-graceful-shutdown', '15'],
        'WorkingDirectory': str(ROOT),
        'EnvironmentVariables': {'PATH': '/usr/bin:/bin:/usr/sbin:/sbin',
                                 'PYTHONUNBUFFERED': '1', 'ARENA_PROCESS_MANAGER': 'launchd'},
        'RunAtLoad': True,
        'KeepAlive': True,
        'ThrottleInterval': 10,
        'StandardOutPath': str(logs / 'server.log'),
        'StandardErrorPath': str(logs / 'server-error.log'),
        'ProcessType': 'Background',
    }
    if path.exists():
        previous = plistlib.loads(path.read_bytes())
        if previous.get('WorkingDirectory') != str(ROOT):
            raise RuntimeError('An unrelated service already uses this label')
    path.write_bytes(plistlib.dumps(config))
    path.chmod(0o600)
    subprocess.run(['/usr/bin/plutil', '-lint', str(path)], check=True)
    domain = f'gui/{os.getuid()}'
    probe = subprocess.run(['/bin/launchctl', 'print', domain + '/' + LABEL], capture_output=True)
    if probe.returncode == 0:
        print('Background service is already registered: ' + LABEL)
    else:
        subprocess.run(['/bin/launchctl', 'bootstrap', domain, str(path)], check=True)
        print('Background service registered: ' + LABEL)
    print('Dashboard: http://127.0.0.1:8787')


if __name__ == '__main__':
    main()
