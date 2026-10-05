# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Jas-Jas-Yuan-debug

"""Checkpoint only after the running legacy batch finishes naturally.

This deliberately never pauses an active batch and never resets the ledger.
The caller then stops the old web process and installs the new LaunchAgent.
"""
from __future__ import annotations

import json
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main():
    url = 'http://127.0.0.1:8787'
    with httpx.Client(trust_env=False, timeout=15) as client:
        while True:
            response = client.get(url + '/api/health')
            response.raise_for_status()
            status = response.json()['runtime']
            if status.get('inProgress') or status.get('pausing'):
                time.sleep(2)
                continue
            next_round = status.get('nextRound')
            if status.get('running'):
                if not next_round:
                    time.sleep(1)
                    continue
                remaining = (datetime.fromisoformat(next_round.replace('Z', '+00:00')) - datetime.now(timezone.utc)).total_seconds()
                if remaining < 15:
                    time.sleep(2)
                    continue
            # The legacy scheduler is idle with at least 15 seconds to its next batch.
            result = client.post(url + '/api/simulation/pause', json={})
            result.raise_for_status()
            stopped = result.json()
            if stopped.get('inProgress') or stopped.get('pausing'):
                raise RuntimeError('The old scheduler did not reach its idle boundary')
            from arena.store import Store
            ledger = Store(ROOT / 'data' / 'arena.db')
            backup_dir = ROOT / 'data' / 'backups'
            backup_dir.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')
            backup = backup_dir / ('before-continuous-' + stamp + '.db')
            with sqlite3.connect(backup) as target:
                ledger.db.backup(target)
            snapshot = {**status, 'mode': 'continuous', 'inProgress': False,
                        'nextRound': None, 'pausing': False}
            ledger.set_setting('runtimeSnapshot', snapshot)
            checkpoint = {'runtime': snapshot, 'cash': sum(a['cash'] for a in ledger.list_agents()),
                          'tradeCount': ledger.count_trades(), 'eventId': ledger.events(1)[0]['id'],
                          'backup': str(backup), 'timestamp': datetime.now(timezone.utc).isoformat()}
            (ROOT / 'data' / 'upgrade-checkpoint.json').write_text(json.dumps(checkpoint, ensure_ascii=False, indent=2))
            ledger.close()
            print(json.dumps({'safeBoundary': True, 'completedRound': status.get('round'),
                              'cash': checkpoint['cash'], 'tradeCount': checkpoint['tradeCount'],
                              'backup': str(backup)}, ensure_ascii=False), flush=True)
            return


if __name__ == '__main__':
    main()
