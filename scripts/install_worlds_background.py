# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Jas-Jas-Yuan-debug

"""Add the historical companion without stopping the live main LaunchAgent."""
from __future__ import annotations

import os
from pathlib import Path
import plistlib
import subprocess

ROOT = Path(__file__).resolve().parents[1]
LABEL = 'local.tenfold.historical-worlds'


def main():
    directory = Path.home() / 'Library' / 'LaunchAgents'
    directory.mkdir(parents=True, exist_ok=True)
    logs = ROOT / 'data' / 'logs'
    logs.mkdir(parents=True, exist_ok=True)
    path = directory / (LABEL + '.plist')
    config = {
        'Label': LABEL,
        'ProgramArguments': [str(ROOT / '.venv' / 'bin' / 'python'), '-m', 'uvicorn',
                             'app:app', '--host', '127.0.0.1', '--port', '8790', '--no-access-log',
                             '--timeout-graceful-shutdown', '15'],
        'WorkingDirectory': str(ROOT),
        'EnvironmentVariables': {'PATH': '/usr/bin:/bin:/usr/sbin:/sbin', 'PYTHONUNBUFFERED': '1',
                                 'ARENA_PROCESS_MANAGER': 'launchd', 'ARENA_COMPANION': '1',
                                 'ARENA_ENABLE_WORLDS': '1', 'MAIN_RUNTIME_URL': 'http://127.0.0.1:8787'},
        'RunAtLoad': True, 'KeepAlive': True, 'ThrottleInterval': 10,
        'StandardOutPath': str(logs / 'worlds.log'),
        'StandardErrorPath': str(logs / 'worlds-error.log'), 'ProcessType': 'Background',
    }
    if path.exists() and plistlib.loads(path.read_bytes()).get('WorkingDirectory') != str(ROOT):
        raise RuntimeError('An unrelated service already uses the companion label')
    path.write_bytes(plistlib.dumps(config))
    path.chmod(0o600)
    subprocess.run(['/usr/bin/plutil', '-lint', str(path)], check=True)
    domain = f'gui/{os.getuid()}'
    probe = subprocess.run(['/bin/launchctl', 'print', domain + '/' + LABEL], capture_output=True)
    if probe.returncode:
        subprocess.run(['/bin/launchctl', 'bootstrap', domain, str(path)], check=True)
    print('Historical companion registered: ' + LABEL)
    print('Dashboard: http://127.0.0.1:8790')


if __name__ == '__main__':
    main()
