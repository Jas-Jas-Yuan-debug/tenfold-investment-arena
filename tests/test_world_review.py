# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Jas-Jas-Yuan-debug

"""Independent world-ledger regressions; fixtures never control live investors."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import threading

from arena.store import Store


def test_contended_writer_cannot_commit_merge_after_original_deadline(tmp_path):
    path = tmp_path / 'isolated-main.db'
    writer, merger = Store(path), Store(path)
    started = threading.Event()
    clock = [datetime(2026, 10, 4, 12, tzinfo=timezone.utc)]
    deadline = clock[0] + timedelta(seconds=300)

    def traced(statement):
        if statement.strip().upper() == 'BEGIN IMMEDIATE':
            started.set()

    merger.db.set_trace_callback(traced)
    writer.db.execute('BEGIN IMMEDIATE')
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(merger.apply_world_delta, 'astra-1', 100,
                'historical-2023-contended', 'approved-choice',
                baseline_equity=800000, final_equity=800100,
                deadline_at=deadline.isoformat(), now=lambda: clock[0])
            try:
                assert started.wait(timeout=3), 'Merge did not attempt the writer lock'
                assert not future.done(), 'Fixture must hold the ledger writer lock'
                # Approval arrived in time, but another writer held the database
                # until after the immutable window. The transaction must recheck.
                clock[0] = deadline + timedelta(microseconds=1)
            finally:
                writer.db.execute('COMMIT')
            receipt = future.result(timeout=3)
        assert receipt['status'] == 'failed'
        assert 'expired' in receipt['error']
        assert receipt['cashBefore'] == receipt['cashAfter'] == 800000
        assert merger.get_agent('astra-1')['cash'] == 800000
        assert merger.positions() == []
        assert merger.trades() == []
        # Receipt recovery after the deadline is idempotent and does not retry a
        # consumed choice or turn a previous failure into a profit transfer.
        retry = merger.apply_world_delta('astra-1', 100,
            'historical-2023-contended', 'approved-choice',
            baseline_equity=800000, final_equity=800100,
            deadline_at=deadline.isoformat(), now=lambda: clock[0])
        assert retry == receipt
        assert len(merger.world_transfers()) == 1
    finally:
        if writer.db.in_transaction:
            writer.db.execute('ROLLBACK')
        writer.close()
        merger.close()


def test_successful_receipt_recovery_after_deadline_does_not_credit_twice(tmp_path):
    ledger = Store(tmp_path / 'isolated-main.db')
    now = datetime(2026, 10, 4, 12, tzinfo=timezone.utc)
    deadline = now + timedelta(seconds=300)
    try:
        first = ledger.apply_world_delta('sol-1', 123,
            'historical-2023-recovered', 'approved-once',
            baseline_equity=800000, final_equity=800123,
            deadline_at=deadline.isoformat(), now=lambda: now)
        retry = ledger.apply_world_delta('sol-1', 123,
            'historical-2023-recovered', 'approved-once',
            baseline_equity=800000, final_equity=800123,
            deadline_at=deadline.isoformat(), now=lambda: deadline + timedelta(days=1))
        assert first == retry
        assert retry['status'] == 'applied'
        assert ledger.get_agent('sol-1')['cash'] == 800123
        assert len(ledger.world_transfers()) == 1
    finally:
        ledger.close()
