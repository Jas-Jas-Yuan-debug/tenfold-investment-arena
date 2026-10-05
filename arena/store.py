# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Jas-Jas-Yuan-debug

"""Persistent paper-trading ledger.

Cash and total position cost are integer micro-VC (one millionth of a CNY).
Quantities are exact decimal strings. Average cost is the weighted aggregate
cost, not FIFO; purchase lots separately enforce A-share T+1 availability.
No live broker connection exists. All balance changes use one SQLite write
transaction, including the execution key, trade and lot records.
"""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path

MICROS = 1_000_000
INITIAL_CASH = 800_000 * MICROS
MAX_QUANTITY = Decimal('1000000000')


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def decimal_value(value, label='value') -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f'{label} must be a finite number')
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError(f'{label} must be a finite number') from None
    if not result.is_finite():
        raise ValueError(f'{label} must be finite')
    if result != 0 and (result.adjusted() > 15 or result.adjusted() < -12):
        raise ValueError(f'{label} is outside supported precision')
    return result


def micros(value) -> int:
    return int((decimal_value(value) * MICROS).to_integral_value(rounding=ROUND_HALF_UP))


def money(value: int) -> float:
    return value / MICROS


AGENTS = [
    ('astra-1', 'Atlas', 'gpt-6-astra', 'Quality compounders; assess competitive moats, earnings and valuation. Prefer patient diversified holdings.', '#8b9bff'),
    ('astra-2', 'Aurora', 'gpt-6-astra', 'Macro investor; connect rates, currencies, policy and sector rotation. Treat macro news as uncertain evidence.', '#b69aff'),
    ('astra-3', 'Axiom', 'gpt-6-astra', 'Deep value investor; seek resilient balance sheets and measurable discounts. Avoid obvious value traps.', '#789fff'),
    ('astra-4', 'Artemis', 'gpt-6-astra', 'Research growth and innovation; verify company fundamentals and compare credible growth catalysts.', '#dc94ee'),
    ('astra-5', 'Anchor', 'gpt-6-astra', 'Capital preservation first; prefer modest position sizes, liquidity and evidence. Holding cash is a valid choice.', '#b5bbff'),
    ('sol-1', 'Solstice', 'gpt-6.1-sol', 'Trend and momentum investor; evaluate price evidence and recent news, with clear exits and controlled exposure.', '#f6bd60'),
    ('sol-2', 'Sage', 'gpt-6.1-sol', 'Contrarian investor; test consensus with primary sources, avoid acting on rumors and seek asymmetrical value.', '#80d5b6'),
    ('sol-3', 'Spark', 'gpt-6.1-sol', 'Digital asset specialist; study BTC/ETH network, liquidity and regulatory evidence. Limit speculative concentration.', '#f6a57d'),
    ('sol-4', 'Scout', 'gpt-6.1-sol', 'Event-driven researcher; examine confirmed earnings, policy and announcements while avoiding unsupported predictions.', '#6cd1db'),
    ('sol-5', 'Summit', 'gpt-6.1-sol', 'Balanced allocator; compare A-shares, US equities and crypto. Rebalance with explicit risk reasoning.', '#b3d674'),
]


class Store:
    def __init__(self, db_path):
        self.path = Path(db_path) if str(db_path) != ':memory:' else None
        if self.path:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.db = sqlite3.connect(str(db_path), check_same_thread=False, isolation_level=None, timeout=15)
        self.db.row_factory = sqlite3.Row
        self.db.execute('PRAGMA foreign_keys = ON')
        self.db.execute('PRAGMA journal_mode = WAL')
        self.db.execute('PRAGMA busy_timeout = 15000')
        self.db.executescript('''
          CREATE TABLE IF NOT EXISTS agents (
            id TEXT PRIMARY KEY, name TEXT NOT NULL, model TEXT NOT NULL,
            effort TEXT NOT NULL, strategy TEXT NOT NULL, color TEXT NOT NULL,
            cash_micros INTEGER NOT NULL CHECK(cash_micros >= 0),
            status TEXT NOT NULL DEFAULT 'idle', error TEXT);
          CREATE TABLE IF NOT EXISTS positions (
            agent_id TEXT NOT NULL REFERENCES agents(id), instrument_id TEXT NOT NULL,
            quantity TEXT NOT NULL, cost_micros INTEGER NOT NULL CHECK(cost_micros >= 0),
            currency TEXT NOT NULL, PRIMARY KEY(agent_id, instrument_id));
          CREATE TABLE IF NOT EXISTS lots (
            id INTEGER PRIMARY KEY AUTOINCREMENT, agent_id TEXT NOT NULL REFERENCES agents(id),
            instrument_id TEXT NOT NULL, quantity TEXT NOT NULL, acquired_date TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT, agent_id TEXT NOT NULL REFERENCES agents(id),
            action TEXT NOT NULL, instrument_id TEXT NOT NULL, quantity TEXT NOT NULL,
            price TEXT NOT NULL, currency TEXT NOT NULL, fx TEXT NOT NULL,
            amount_micros INTEGER NOT NULL, realized_micros INTEGER NOT NULL,
            reason TEXT NOT NULL, timestamp TEXT NOT NULL, quote_timestamp TEXT NOT NULL,
            source TEXT NOT NULL, execution_key TEXT UNIQUE, round_id TEXT);
          CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT, sender TEXT NOT NULL, recipient TEXT NOT NULL,
            kind TEXT NOT NULL, content TEXT NOT NULL, timestamp TEXT NOT NULL,
            scope TEXT NOT NULL DEFAULT 'public', room_id TEXT,
            archived INTEGER NOT NULL DEFAULT 0, world_id TEXT NOT NULL DEFAULT 'main');
          CREATE TABLE IF NOT EXISTS speaking_sessions (
            session_id TEXT NOT NULL, world_id TEXT NOT NULL,
            message_id INTEGER NOT NULL REFERENCES messages(id) ON DELETE CASCADE,
            timestamp TEXT NOT NULL, PRIMARY KEY(session_id,world_id));
          CREATE TABLE IF NOT EXISTS world_transfers (
            id INTEGER PRIMARY KEY AUTOINCREMENT, agent_id TEXT NOT NULL REFERENCES agents(id),
            world_id TEXT NOT NULL, decision_id TEXT NOT NULL,
            delta_micros INTEGER NOT NULL, cash_before_micros INTEGER NOT NULL,
            cash_after_micros INTEGER NOT NULL, status TEXT NOT NULL, error TEXT,
            baseline_micros INTEGER, final_micros INTEGER, details TEXT,
            timestamp TEXT NOT NULL, UNIQUE(agent_id,world_id));
          CREATE TABLE IF NOT EXISTS chat_groups (
            id TEXT PRIMARY KEY, name TEXT NOT NULL, creator TEXT NOT NULL, timestamp TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS chat_group_members (
            group_id TEXT NOT NULL REFERENCES chat_groups(id) ON DELETE CASCADE,
            agent_id TEXT NOT NULL REFERENCES agents(id), PRIMARY KEY(group_id, agent_id));
          CREATE TABLE IF NOT EXISTS chat_rooms (
            id TEXT PRIMARY KEY, round_id TEXT NOT NULL UNIQUE, number INTEGER NOT NULL,
            name TEXT NOT NULL, status TEXT NOT NULL, timestamp TEXT NOT NULL, closed_at TEXT,
            agent_id TEXT REFERENCES agents(id));
          CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, agent_id TEXT,
            content TEXT NOT NULL, details TEXT, timestamp TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS equity_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT, agent_id TEXT NOT NULL REFERENCES agents(id),
            equity REAL NOT NULL, timestamp TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
          CREATE INDEX IF NOT EXISTS lots_agent ON lots(agent_id, instrument_id);
          CREATE INDEX IF NOT EXISTS equity_agent ON equity_history(agent_id, timestamp);
          CREATE INDEX IF NOT EXISTS trades_agent ON trades(agent_id, id);
          CREATE INDEX IF NOT EXISTS events_agent_kind ON events(agent_id, kind, id);
          CREATE INDEX IF NOT EXISTS group_member_agent ON chat_group_members(agent_id, group_id);
          CREATE INDEX IF NOT EXISTS world_transfers_agent ON world_transfers(agent_id,id);
          CREATE INDEX IF NOT EXISTS world_transfers_decision ON world_transfers(world_id,decision_id);
        ''')
        self._migrate_chat()
        self._seed()

    def _migrate_chat(self):
        """Add chat metadata in place without resetting an existing ledger."""
        with self._lock:
            self.db.execute('BEGIN IMMEDIATE')
            try:
                columns = {row['name'] for row in self.db.execute('PRAGMA table_info(messages)')}
                if 'scope' not in columns:
                    self.db.execute("ALTER TABLE messages ADD COLUMN scope TEXT NOT NULL DEFAULT 'public'")
                    self.db.execute("UPDATE messages SET scope=CASE WHEN kind='public_speech' THEN 'speech' WHEN recipient='all' THEN 'public' WHEN recipient LIKE 'group:%' THEN 'group' ELSE 'direct' END")
                if 'room_id' not in columns:
                    self.db.execute('ALTER TABLE messages ADD COLUMN room_id TEXT')
                if 'archived' not in columns:
                    self.db.execute('ALTER TABLE messages ADD COLUMN archived INTEGER NOT NULL DEFAULT 0')
                if 'world_id' not in columns:
                    self.db.execute("ALTER TABLE messages ADD COLUMN world_id TEXT NOT NULL DEFAULT 'main'")
                # The user removed speaking invitations. The database gate also
                # applies to the existing server without restarting its workers.
                self.db.execute("DELETE FROM messages WHERE sender='arena' AND kind='invitation'")
                self.db.execute('DROP TRIGGER IF EXISTS one_visible_speaking_invitation')
                self.db.execute("""
                  CREATE TRIGGER IF NOT EXISTS speaking_invitations_disabled
                  BEFORE INSERT ON messages
                  WHEN NEW.sender='arena' AND NEW.kind='invitation'
                  BEGIN
                    SELECT RAISE(IGNORE);
                  END
                """)
                room_columns = {row['name'] for row in self.db.execute('PRAGMA table_info(chat_rooms)')}
                if 'agent_id' not in room_columns:
                    self.db.execute('ALTER TABLE chat_rooms ADD COLUMN agent_id TEXT REFERENCES agents(id)')
                self.db.execute('DROP INDEX IF EXISTS active_chat_room')
                self.db.execute("CREATE UNIQUE INDEX IF NOT EXISTS active_chat_room_owner ON chat_rooms(COALESCE(agent_id,'')) WHERE status='active'")
                self.db.execute('CREATE INDEX IF NOT EXISTS messages_recipient_id ON messages(recipient,id)')
                self.db.execute('CREATE INDEX IF NOT EXISTS messages_sender_id ON messages(sender,id)')
                self.db.execute('COMMIT')
            except BaseException:
                self.db.execute('ROLLBACK')
                raise

    def _seed(self):
        with self._lock:
            for agent in AGENTS:
                result = self.db.execute('INSERT OR IGNORE INTO agents(id,name,model,effort,strategy,color,cash_micros) VALUES(?,?,?,\'max\',?,?,?)', (*agent, INITIAL_CASH))
                if result.rowcount:
                    self.db.execute('INSERT INTO equity_history(agent_id,equity,timestamp) VALUES(?,?,?)', (agent[0], 800_000, utcnow()))

    def close(self):
        with self._lock:
            self.db.close()

    def _agent(self, row):
        if row is None:
            return None
        result = dict(row)
        result['cash'] = money(result.pop('cash_micros'))
        baselines = self.get_setting('branchInitialCapital', {})
        result['initialCash'] = baselines.get(result['id'], 800_000) if isinstance(baselines, dict) else 800_000
        aggressive = {'astra-4', 'sol-1', 'sol-3'}
        cautious = {'astra-3', 'astra-5', 'sol-5'}
        style, sizing, reserve = ('aggressive', 40, 10) if result['id'] in aggressive else (('cautious', 10, 50) if result['id'] in cautious else ('balanced', 25, 25))
        result['riskProfile'] = {
            'style': style, 'positionSizingPct': sizing, 'cashReservePct': reserve,
            'description': f'{style.title()} approach: target each position at or below {sizing}% of equity and keep at least {reserve}% in cash. These are investor guidelines, not enforced trading caps.'}
        return result

    def list_agents(self):
        with self._lock:
            return [self._agent(row) for row in self.db.execute('SELECT * FROM agents ORDER BY id')]

    def get_agent(self, agent_id):
        with self._lock:
            return self._agent(self.db.execute('SELECT * FROM agents WHERE id=?', (agent_id,)).fetchone())

    def positions(self, agent_id=None):
        sql, args = 'SELECT * FROM positions', ()
        if agent_id is not None:
            sql, args = sql + ' WHERE agent_id=?', (agent_id,)
        with self._lock:
            return [{'agentId': row['agent_id'], 'instrumentId': row['instrument_id'],
                     'quantity': float(Decimal(row['quantity'])),
                     'avgCostCny': float(Decimal(row['cost_micros']) / MICROS / Decimal(row['quantity'])),
                     'costBasisCny': money(row['cost_micros']), 'currency': row['currency']}
                    for row in self.db.execute(sql + ' ORDER BY agent_id,instrument_id', args)]

    @staticmethod
    def _trade(row):
        return {'id': row['id'], 'agentId': row['agent_id'], 'action': row['action'],
                'instrumentId': row['instrument_id'], 'quantity': float(Decimal(row['quantity'])),
                'price': float(Decimal(row['price'])), 'currency': row['currency'],
                'fxRate': float(Decimal(row['fx'])), 'amountCny': money(row['amount_micros']),
                'realizedPnl': money(row['realized_micros']), 'reason': row['reason'],
                'timestamp': row['timestamp'], 'quoteTimestamp': row['quote_timestamp'],
                'source': row['source'], 'roundId': row['round_id']}

    def trades(self, limit=100, agent_id=None):
        sql, args = 'SELECT * FROM trades', []
        if agent_id is not None:
            sql += ' WHERE agent_id=?'
            args.append(agent_id)
        args.append(max(0, min(int(limit), 10000)))
        with self._lock:
            return [self._trade(row) for row in self.db.execute(sql + ' ORDER BY id DESC LIMIT ?', args)]

    def count_trades(self):
        with self._lock:
            return self.db.execute('SELECT COUNT(*) FROM trades').fetchone()[0]

    def messages(self, limit=100, agent_id=None, include_archived=False):
        sql, args = 'SELECT * FROM messages', []
        filters = []
        if not include_archived:
            filters.append('archived=0')
        if agent_id is not None:
            if self.get_agent(agent_id) is None:
                raise ValueError('Unknown investor')
            filters.append("(sender=? OR recipient=? OR scope IN ('public','speech') OR (scope='group' AND recipient IN (SELECT group_id FROM chat_group_members WHERE agent_id=?)))")
            args.extend([agent_id, agent_id, agent_id])
        if filters:
            sql += ' WHERE ' + ' AND '.join(filters)
        args.append(max(0, min(int(limit), 10000)))
        with self._lock:
            return [self._message(row) for row in self.db.execute(sql + ' ORDER BY id DESC LIMIT ?', args)]

    @staticmethod
    def _message(row):
        result = dict(row)
        result['roomId'] = result.get('room_id')
        result['worldId'] = result.pop('world_id', 'main')
        result['archived'] = bool(result.get('archived', False))
        return result

    @staticmethod
    def _event(row):
        event = dict(row)
        event['agentId'] = event.pop('agent_id')
        event['details'] = json.loads(event['details']) if event['details'] else None
        return event

    def events(self, limit=100):
        with self._lock:
            return [self._event(row) for row in self.db.execute('SELECT * FROM events ORDER BY id DESC LIMIT ?', (max(0, min(int(limit), 10000)),))]

    def latest_agent_events(self, agent_id, kinds=('decision', 'response_completed', 'knowledge_updated', 'research')):
        """Retrieve each investor's latest metadata without a shared history cap."""
        if self.get_agent(agent_id) is None:
            raise ValueError('Unknown investor')
        if not isinstance(kinds, (list, tuple)) or len(kinds) > 4 or not all(isinstance(kind, str) for kind in kinds):
            raise ValueError('Use up to four event kinds')
        with self._lock:
            result = {}
            for kind in dict.fromkeys(kinds):
                row = self.db.execute('SELECT * FROM events WHERE agent_id=? AND kind=? ORDER BY id DESC LIMIT 1', (agent_id, kind)).fetchone()
                if row:
                    result[kind] = self._event(row)
            return result

    def add_event(self, kind, agent_id, content, details=None):
        event = {'kind': str(kind)[:100], 'agentId': agent_id, 'content': str(content)[:12000], 'details': details, 'timestamp': utcnow()}
        payload = json.dumps(details, ensure_ascii=False, allow_nan=False) if details is not None else None
        with self._lock:
            event['id'] = self.db.execute('INSERT INTO events(kind,agent_id,content,details,timestamp) VALUES(?,?,?,?,?)', (event['kind'], agent_id, event['content'], payload, event['timestamp'])).lastrowid
        return event

    def add_message(self, sender, recipient, kind, content, room_id=None):
        if sender not in {'human', 'arena'} and self.get_agent(sender) is None:
            raise ValueError('Unknown message sender')
        if kind not in {'tip', 'distraction', 'message', 'public_speech', 'invitation'}:
            raise ValueError('Unsupported message kind')
        if (sender == 'arena') != (kind == 'invitation'):
            raise ValueError('Only the arena may send a round invitation')
        if kind in {'public_speech', 'invitation'} and recipient != 'all':
            raise ValueError('Public speeches and invitations must address all investors')
        if not isinstance(recipient, str):
            raise ValueError('Unknown message recipient')
        content = str(content).strip()
        if not content or len(content) > 2000:
            raise ValueError('Message must contain 1 to 2000 characters')
        with self._lock:
            if recipient.startswith('group:'):
                if self.db.execute('SELECT 1 FROM chat_groups WHERE id=?', (recipient,)).fetchone() is None:
                    raise ValueError('Unknown chat group')
                if sender != 'human' and self.db.execute('SELECT 1 FROM chat_group_members WHERE group_id=? AND agent_id=?', (recipient, sender)).fetchone() is None:
                    raise ValueError('Only group members may send messages to this group')
                scope = 'group'
            elif recipient == 'all':
                scope = 'speech' if kind == 'public_speech' else 'public'
            elif self.get_agent(recipient) is not None:
                scope = 'direct'
            else:
                raise ValueError('Unknown message recipient')
            if room_id is None:
                active = self.db.execute("SELECT id FROM chat_rooms WHERE status='active' AND agent_id=? ORDER BY rowid DESC LIMIT 1", (sender,)).fetchone()
                if active is None:
                    active = self.db.execute("SELECT id FROM chat_rooms WHERE status='active' ORDER BY rowid DESC LIMIT 1").fetchone()
                room_id = active['id'] if active else None
            elif self.db.execute('SELECT 1 FROM chat_rooms WHERE id=?', (room_id,)).fetchone() is None:
                raise ValueError('Unknown chat room')
            if kind == 'invitation':
                if content != 'Anyone want to speak?':
                    raise ValueError('The arena invitation must use the standard round prompt')
                return None
            timestamp = utcnow()
            world_id = self.get_setting('branchWorldId', 'main')
            if kind == 'invitation':
                existing = self.db.execute("SELECT * FROM messages WHERE sender='arena' AND kind='invitation' AND archived=0 AND world_id=? ORDER BY id LIMIT 1", (world_id,)).fetchone()
                if existing:
                    return self._message(existing)
            message_id = self.db.execute('INSERT INTO messages(sender,recipient,kind,content,timestamp,scope,room_id,world_id) VALUES(?,?,?,?,?,?,?,?)', (sender, recipient, kind, content, timestamp, scope, room_id, world_id)).lastrowid
            return {'id': message_id, 'sender': sender, 'recipient': recipient, 'kind': kind,
                    'content': content, 'timestamp': timestamp, 'scope': scope,
                    'roomId': room_id, 'room_id': room_id, 'worldId': world_id, 'archived': False}

    @staticmethod
    def _identifier(value, label):
        if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:-]{0,199}', value):
            raise ValueError(f'{label} must be a genuine identifier of 1 to 200 characters')
        return value

    def announce_speaking_session(self, session_id, world_id='main', room_id=None):
        """Compatibility hook: speaking invitations have been disabled."""
        session_id = self._identifier(session_id, 'Session ID')
        world_id = self._identifier(world_id, 'World ID')
        actual_world = self.get_setting('branchWorldId', 'main')
        if world_id != actual_world:
            raise ValueError('Speaking session world does not match this ledger')
        return None

    def _group(self, row):
        result = dict(row)
        result['members'] = [member['agent_id'] for member in self.db.execute('SELECT agent_id FROM chat_group_members WHERE group_id=? ORDER BY agent_id', (row['id'],))]
        return result

    def _room(self, row):
        result = dict(row)
        result['roundId'] = result.pop('round_id')
        result['closedAt'] = result.pop('closed_at')
        result['agentId'] = result.pop('agent_id', None)
        owner = self.get_agent(result['agentId']) if result['agentId'] else None
        result['ownerName'] = owner['name'] if owner else None
        return result

    def create_group(self, creator, name, members):
        if creator != 'human' and self.get_agent(creator) is None:
            raise ValueError('Unknown group creator')
        if not isinstance(name, str) or not name.strip() or len(name.strip()) > 80:
            raise ValueError('Group name must contain 1 to 80 characters')
        if not isinstance(members, (list, tuple)) or len(members) > 10 or not all(isinstance(member, str) for member in members):
            raise ValueError('Group members must be a list of up to 10 investor IDs')
        member_ids = set(members)
        if creator != 'human':
            member_ids.add(creator)
        if not member_ids or len(member_ids) > 10 or not member_ids.issubset({agent['id'] for agent in self.list_agents()}):
            raise ValueError('Groups require 1 to 10 valid investor members')
        group_id = 'group:' + uuid.uuid4().hex
        timestamp = utcnow()
        with self._lock:
            self.db.execute('BEGIN IMMEDIATE')
            try:
                self.db.execute('INSERT INTO chat_groups(id,name,creator,timestamp) VALUES(?,?,?,?)', (group_id, name.strip(), creator, timestamp))
                self.db.executemany('INSERT INTO chat_group_members(group_id,agent_id) VALUES(?,?)', [(group_id, member) for member in sorted(member_ids)])
                self.db.execute('COMMIT')
            except BaseException:
                self.db.execute('ROLLBACK')
                raise
        return {'id': group_id, 'name': name.strip(), 'creator': creator, 'members': sorted(member_ids), 'timestamp': timestamp}

    def open_round_room(self, round_id, number, agent_id=None):
        if not isinstance(round_id, str) or not round_id.strip() or len(round_id) > 200:
            raise ValueError('Round ID must contain 1 to 200 characters')
        if isinstance(number, bool) or not isinstance(number, int) or not 1 <= number <= 1_000_000_000:
            raise ValueError('Round number must be a positive integer')
        owner = self.get_agent(agent_id) if agent_id is not None else None
        if agent_id is not None and owner is None:
            raise ValueError('Unknown room owner')
        round_id = round_id.strip()
        room_id, timestamp = 'round:' + round_id, utcnow()
        with self._lock:
            self.db.execute('BEGIN IMMEDIATE')
            try:
                existing = self.db.execute('SELECT * FROM chat_rooms WHERE round_id=?', (round_id,)).fetchone()
                if existing:
                    if existing['agent_id'] != agent_id:
                        raise ValueError('Round room belongs to a different investor')
                    self.db.execute('COMMIT')
                    return self._room(existing)
                self.db.execute("UPDATE chat_rooms SET status='closed',closed_at=? WHERE status='active' AND agent_id IS ?", (timestamp, agent_id))
                name = f"{owner['name']} · Cycle {number}" if owner else f'Round {number}'
                self.db.execute('INSERT INTO chat_rooms(id,round_id,number,name,status,timestamp,agent_id) VALUES(?,?,?,?,?,?,?)', (room_id, round_id, number, name, 'active', timestamp, agent_id))
                room = self._room(self.db.execute('SELECT * FROM chat_rooms WHERE id=?', (room_id,)).fetchone())
                self.db.execute('COMMIT')
                return room
            except BaseException:
                self.db.execute('ROLLBACK')
                raise

    def chat_state(self, agent_id=None):
        if agent_id is not None and self.get_agent(agent_id) is None:
            raise ValueError('Unknown investor')
        with self._lock:
            sql, args = 'SELECT * FROM chat_groups', ()
            if agent_id is not None:
                sql += ' WHERE id IN (SELECT group_id FROM chat_group_members WHERE agent_id=?)'
                args = (agent_id,)
            groups = [self._group(row) for row in self.db.execute(sql + ' ORDER BY timestamp,id', args)]
            rooms = [self._room(row) for row in self.db.execute('SELECT * FROM chat_rooms ORDER BY rowid DESC LIMIT 100')]
            active_rooms = [self._room(row) for row in self.db.execute("SELECT * FROM chat_rooms WHERE status='active' ORDER BY rowid DESC")]
            known_rooms = {room['id'] for room in rooms}
            rooms.extend(room for room in active_rooms if room['id'] not in known_rooms)
            return {'groups': groups, 'rooms': rooms, 'activeRooms': active_rooms, 'activeRoom': active_rooms[0] if active_rooms else None}

    def snapshot_equity(self, agent_id, equity):
        value = decimal_value(equity, 'equity')
        if value < 0:
            raise ValueError('Equity cannot be negative')
        with self._lock:
            self.db.execute('INSERT INTO equity_history(agent_id,equity,timestamp) VALUES(?,?,?)', (agent_id, float(value), utcnow()))

    def equity_history(self, agent_id=None):
        sql, args = 'SELECT agent_id,equity,timestamp FROM equity_history', ()
        if agent_id is not None:
            sql, args = sql + ' WHERE agent_id=?', (agent_id,)
        with self._lock:
            rows = self.db.execute(sql + ' ORDER BY id DESC LIMIT 5000', args).fetchall()
            return [{'agentId': row['agent_id'], 'equity': row['equity'], 'timestamp': row['timestamp']} for row in reversed(rows)]

    def set_agent_status(self, agent_id, status, error=None):
        with self._lock:
            if self.db.execute('UPDATE agents SET status=?,error=? WHERE id=?', (str(status)[:80], str(error)[:2000] if error else None, agent_id)).rowcount != 1:
                raise ValueError('Unknown investor')

    def set_setting(self, key, value):
        payload = json.dumps(value, ensure_ascii=False, allow_nan=False)
        with self._lock:
            self.db.execute('INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value', (str(key), payload))

    def get_setting(self, key, default=None):
        with self._lock:
            row = self.db.execute('SELECT value FROM settings WHERE key=?', (str(key),)).fetchone()
            return json.loads(row['value']) if row else default

    def seed_wallets(self, wallets, *, world_id):
        """Fund a pristine historical database from a cash-only snapshot.

        No main wallet is debited. All ten investor balances are required; the
        canonical main database and existing positions/trades are rejected.
        Branch baselines persist across restarts and are never seeded twice.
        """
        world_id = self._identifier(world_id, 'World ID')
        if world_id == 'main':
            raise ValueError('Historical wallet funding requires a separate world ID')
        canonical_main = Path(__file__).resolve().parent.parent / 'data' / 'arena.db'
        if self.path is not None and self.path.resolve() == canonical_main.resolve():
            raise ValueError('Main wallets cannot be seeded with historical funding')
        expected = {agent[0] for agent in AGENTS}
        if not isinstance(wallets, dict) or set(wallets) != expected:
            raise ValueError('Historical funding requires all ten investor cash balances')
        funded = {}
        for agent_id, value in wallets.items():
            amount = decimal_value(value, 'initial wallet cash')
            if amount < 0 or amount > Decimal('1000000000'):
                raise ValueError('Initial wallet cash must be between 0 and 1 billion VC')
            funded[agent_id] = micros(amount)
        baselines = {agent_id: money(amount) for agent_id, amount in funded.items()}
        with self._lock:
            self.db.execute('BEGIN IMMEDIATE')
            try:
                existing = self.get_setting('branchWorldId')
                if existing is not None:
                    if existing == world_id and self.get_setting('branchInitialCapital') == baselines:
                        self.db.execute('COMMIT')
                        return {'worldId': world_id, 'wallets': baselines, 'initialCapital': money(sum(funded.values()))}
                    raise ValueError('Historical wallets have already been funded')
                if any(self.db.execute(f'SELECT 1 FROM {table} LIMIT 1').fetchone() for table in ('trades', 'positions', 'lots', 'world_transfers')):
                    raise ValueError('Historical funding requires a pristine trading ledger')
                self.db.executemany('UPDATE agents SET cash_micros=? WHERE id=?', [(amount, agent_id) for agent_id, amount in funded.items()])
                self.set_setting('branchWorldId', world_id)
                self.set_setting('branchInitialCapital', baselines)
                self.db.execute('DELETE FROM equity_history')
                timestamp = utcnow()
                self.db.executemany('INSERT INTO equity_history(agent_id,equity,timestamp) VALUES(?,?,?)', [(agent_id, baselines[agent_id], timestamp) for agent_id in sorted(expected)])
                self.add_event('world_funded', None, 'Historical wallets funded from the main-world available-cash snapshot', {'worldId': world_id, 'baselineCash': baselines, 'totalInitialCapital': money(sum(funded.values()))})
                self.db.execute('COMMIT')
                return {'worldId': world_id, 'wallets': baselines, 'initialCapital': money(sum(funded.values()))}
            except BaseException:
                self.db.execute('ROLLBACK')
                raise

    @staticmethod
    def _world_transfer(row):
        return {'id': row['id'], 'agentId': row['agent_id'], 'worldId': row['world_id'],
                'decisionId': row['decision_id'], 'deltaVC': money(row['delta_micros']),
                'cashBefore': money(row['cash_before_micros']), 'cashAfter': money(row['cash_after_micros']),
                'status': row['status'], 'error': row['error'],
                'baselineEquity': money(row['baseline_micros']) if row['baseline_micros'] is not None else None,
                'finalEquity': money(row['final_micros']) if row['final_micros'] is not None else None,
                'details': json.loads(row['details']) if row['details'] else None,
                'timestamp': row['timestamp']}

    def world_transfers(self, limit=100, agent_id=None, world_id=None):
        filters, args = [], []
        if agent_id is not None:
            filters.append('agent_id=?')
            args.append(agent_id)
        if world_id is not None:
            filters.append('world_id=?')
            args.append(world_id)
        sql = 'SELECT * FROM world_transfers'
        if filters:
            sql += ' WHERE ' + ' AND '.join(filters)
        args.append(max(0, min(int(limit), 10000)))
        with self._lock:
            return [self._world_transfer(row) for row in self.db.execute(sql + ' ORDER BY id DESC LIMIT ?', args)]

    def apply_world_delta(self, agent_id, deltaVC, world_id, decision_id, *,
                          baseline_equity=None, final_equity=None, details=None, deadline_at=None, now=None):
        """Apply an investor's historical net PnL once, entirely or not at all.

        Success/failure consumes the investor's one choice for this world.
        Exact retries return the existing audit result. A cash-shortfall returns
        status='failed' and never silently clips losses or changes holdings.
        The controller supplies genuine historical baseline/final marks; when
        supplied together, their micro-VC difference must equal the transfer.
        """
        world_id = self._identifier(world_id, 'World ID')
        decision_id = self._identifier(decision_id, 'Decision ID')
        if world_id == 'main' or self.get_setting('branchWorldId') is not None:
            raise ValueError('Historical PnL may only transfer to the main-world ledger')
        value = decimal_value(deltaVC, 'world profit/loss')
        if abs(value) > Decimal('1000000000'):
            raise ValueError('World profit/loss exceeds supported accounting limits')
        delta = micros(value)
        baseline = final = None
        if (baseline_equity is None) != (final_equity is None):
            raise ValueError('Supply both historical baseline and final equity')
        if baseline_equity is not None:
            baseline_value = decimal_value(baseline_equity, 'historical baseline')
            final_value = decimal_value(final_equity, 'historical final equity')
            if min(baseline_value, final_value) < 0 or max(baseline_value, final_value) > Decimal('1000000000'):
                raise ValueError('Historical marks must be between 0 and 1 billion VC')
            baseline, final = micros(baseline_value), micros(final_value)
            if final - baseline != delta:
                raise ValueError('Transfer must equal historical final equity minus its baseline')
        payload = json.dumps(details, ensure_ascii=False, allow_nan=False) if details is not None else None
        deadline = None
        if deadline_at is not None:
            deadline = datetime.fromisoformat(deadline_at.replace('Z', '+00:00'))
            if deadline.tzinfo is None:
                raise ValueError('Merge deadline must specify its timezone')
        wall_now = now or (lambda: datetime.now(timezone.utc))
        with self._lock:
            self.db.execute('BEGIN IMMEDIATE')
            try:
                agent = self.db.execute('SELECT cash_micros FROM agents WHERE id=?', (agent_id,)).fetchone()
                if agent is None:
                    raise ValueError('Unknown investor')
                existing = self.db.execute('SELECT * FROM world_transfers WHERE agent_id=? AND world_id=?', (agent_id, world_id)).fetchone()
                if existing:
                    if existing['decision_id'] != decision_id or existing['delta_micros'] != delta or existing['baseline_micros'] != baseline or existing['final_micros'] != final:
                        raise ValueError('This investor has already made a transfer choice for this world')
                    self.db.execute('COMMIT')
                    return self._world_transfer(existing)
                before = agent['cash_micros']
                after = before + delta
                status, error = 'applied', None
                if deadline is not None and wall_now() >= deadline:
                    status, error, after = 'failed', 'Five-minute merge window expired before the ledger writer lock was acquired', before
                elif after < 0:
                    status, error, after = 'failed', 'Main wallet has insufficient cash to fund the full historical loss; no partial transfer was made', before
                elif after > 1_000_000_000 * MICROS:
                    status, error, after = 'failed', 'Transfer exceeds the supported main-wallet accounting limit', before
                if status == 'applied':
                    self.db.execute('UPDATE agents SET cash_micros=? WHERE id=?', (after, agent_id))
                transfer_id = self.db.execute('INSERT INTO world_transfers(agent_id,world_id,decision_id,delta_micros,cash_before_micros,cash_after_micros,status,error,baseline_micros,final_micros,details,timestamp) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)', (agent_id, world_id, decision_id, delta, before, after, status, error, baseline, final, payload, utcnow())).lastrowid
                result = self._world_transfer(self.db.execute('SELECT * FROM world_transfers WHERE id=?', (transfer_id,)).fetchone())
                self.add_event('world_transfer' if status == 'applied' else 'world_transfer_failed', agent_id,
                    f'Historical net profit/loss transfer: {money(delta):+.6f} VC' if status == 'applied' else error, result)
                self.db.execute('COMMIT')
                return result
            except BaseException:
                self.db.execute('ROLLBACK')
                raise

    def reset(self):
        """Only the explicit paused dashboard action calls this destructive reset."""
        with self._lock:
            self.db.execute('BEGIN IMMEDIATE')
            try:
                for table in ('trades', 'lots', 'positions', 'speaking_sessions', 'messages', 'chat_group_members', 'chat_groups', 'chat_rooms', 'world_transfers', 'events', 'equity_history'):
                    self.db.execute(f'DELETE FROM {table}')
                baselines = self.get_setting('branchInitialCapital', {})
                self.db.executemany("UPDATE agents SET cash_micros=?,status='idle',error=NULL WHERE id=?", [(micros(baselines.get(agent[0], 800_000)), agent[0]) for agent in AGENTS])
                for agent in AGENTS:
                    self.db.execute('INSERT INTO equity_history(agent_id,equity,timestamp) VALUES(?,?,?)', (agent[0], baselines.get(agent[0], 800_000), utcnow()))
                self.db.execute('COMMIT')
            except BaseException:
                self.db.execute('ROLLBACK')
                raise

    def record_trade(self, *, agent_id, action, instrument_id, quantity, price,
                     currency, fx, reason, quote_timestamp, source, acquired_date,
                     enforce_t1=False, round_id=None, timestamp=None):
        """Commit a validated quote atomically; balances are revalidated here.

        round_id is an order execution ID. For callers passing a round identifier,
        an order fingerprint distinguishes separate orders in the same round.
        Identical intended orders should use distinct tool-call execution IDs.
        """
        quantity = decimal_value(quantity, 'quantity')
        price = decimal_value(price, 'price')
        fx = decimal_value(fx, 'FX rate')
        if action not in {'buy', 'sell'} or quantity <= 0 or quantity > MAX_QUANTITY or price <= 0 or fx <= 0:
            raise ValueError('Invalid trade amount')
        if quantity.as_tuple().exponent < -8:
            raise ValueError('Quantity supports at most 8 decimal places')
        amount = micros(quantity * price * fx)
        if amount <= 0 or amount > 100_000_000 * MICROS:
            raise ValueError('Trade amount is outside supported limits')
        fingerprint = json.dumps([agent_id, str(round_id), action, instrument_id, str(quantity.normalize()), str(reason)], separators=(',', ':'))
        execution_key = hashlib.sha256(fingerprint.encode()).hexdigest() if round_id is not None else None
        timestamp = timestamp or utcnow()
        with self._lock:
            self.db.execute('BEGIN IMMEDIATE')
            try:
                agent = self.db.execute('SELECT * FROM agents WHERE id=?', (agent_id,)).fetchone()
                if agent is None:
                    raise ValueError('Unknown investor')
                if execution_key and self.db.execute('SELECT 1 FROM trades WHERE execution_key=?', (execution_key,)).fetchone():
                    raise ValueError('Duplicate order execution key')
                position = self.db.execute('SELECT * FROM positions WHERE agent_id=? AND instrument_id=?', (agent_id, instrument_id)).fetchone()
                old_qty = Decimal(position['quantity']) if position else Decimal(0)
                old_cost = position['cost_micros'] if position else 0
                realized = 0
                if action == 'buy':
                    if agent['cash_micros'] < amount:
                        raise ValueError('Insufficient virtual cash; margin is disabled')
                    if position is None and self.db.execute('SELECT COUNT(*) FROM positions WHERE agent_id=?', (agent_id,)).fetchone()[0] >= 100:
                        raise ValueError('Maximum 100 distinct positions per investor')
                    new_qty, new_cost, new_cash = old_qty + quantity, old_cost + amount, agent['cash_micros'] - amount
                    if new_qty > MAX_QUANTITY:
                        raise ValueError('Position exceeds supported quantity limit')
                    self.db.execute('INSERT INTO lots(agent_id,instrument_id,quantity,acquired_date) VALUES(?,?,?,?)', (agent_id, instrument_id, str(quantity), acquired_date))
                else:
                    if old_qty < quantity:
                        raise ValueError('Insufficient holdings; short selling is disabled')
                    lots = self.db.execute('SELECT * FROM lots WHERE agent_id=? AND instrument_id=? ORDER BY acquired_date,id', (agent_id, instrument_id)).fetchall()
                    eligible = [lot for lot in lots if not enforce_t1 or lot['acquired_date'] < acquired_date]
                    if sum((Decimal(lot['quantity']) for lot in eligible), Decimal(0)) < quantity:
                        raise ValueError('A-share T+1: shares bought today are unavailable to sell')
                    remaining = quantity
                    for lot in eligible:
                        consumed = min(remaining, Decimal(lot['quantity']))
                        left = Decimal(lot['quantity']) - consumed
                        if left:
                            self.db.execute('UPDATE lots SET quantity=? WHERE id=?', (str(left), lot['id']))
                        else:
                            self.db.execute('DELETE FROM lots WHERE id=?', (lot['id'],))
                        remaining -= consumed
                        if not remaining:
                            break
                    removed_cost = old_cost if quantity == old_qty else int((Decimal(old_cost) * quantity / old_qty).to_integral_value(rounding=ROUND_HALF_UP))
                    new_qty, new_cost, new_cash = old_qty - quantity, old_cost - removed_cost, agent['cash_micros'] + amount
                    realized = amount - removed_cost
                    if new_cash > 1_000_000_000 * MICROS:
                        raise ValueError('Cash exceeds supported accounting limit')
                self.db.execute('UPDATE agents SET cash_micros=? WHERE id=?', (new_cash, agent_id))
                if new_qty:
                    self.db.execute('INSERT INTO positions(agent_id,instrument_id,quantity,cost_micros,currency) VALUES(?,?,?,?,?) ON CONFLICT(agent_id,instrument_id) DO UPDATE SET quantity=excluded.quantity,cost_micros=excluded.cost_micros,currency=excluded.currency', (agent_id, instrument_id, str(new_qty), new_cost, currency))
                else:
                    self.db.execute('DELETE FROM positions WHERE agent_id=? AND instrument_id=?', (agent_id, instrument_id))
                trade_id = self.db.execute('INSERT INTO trades(agent_id,action,instrument_id,quantity,price,currency,fx,amount_micros,realized_micros,reason,timestamp,quote_timestamp,source,execution_key,round_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (agent_id, action, instrument_id, str(quantity), str(price), currency, str(fx), amount, realized, str(reason)[:4000], timestamp, quote_timestamp, str(source)[:300], execution_key, str(round_id)[:300] if round_id is not None else None)).lastrowid
                row = self.db.execute('SELECT * FROM trades WHERE id=?', (trade_id,)).fetchone()
                self.db.execute('COMMIT')
                return self._trade(row)
            except BaseException:
                self.db.execute('ROLLBACK')
                raise
