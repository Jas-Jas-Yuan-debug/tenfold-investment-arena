# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Jas-Jas-Yuan-debug

from fastapi.testclient import TestClient
import pytest

DUMMY_API_KEY = "test-only-provider-key-not-a-secret"

@pytest.fixture
def client(monkeypatch, tmp_path):
    import app as module
    from arena.store import Store
    ledger = Store(tmp_path / 'api.db')
    class Markets:
        catalog = {'BTC-USD': {'id': 'BTC-USD'}}
        async def overview(self):
            return {'markets': [], 'coverage': {}, 'featured': []}
        async def search(self, **kw):
            return {'items': [], 'total': 0, 'coverage': {}}
    class Executor:
        async def portfolios(self):
            return [{**a, 'equity': a['cash'], 'pnl': 0, 'pnlPct': 0, 'positions': []} for a in ledger.list_agents()]
    class Runtime:
        running = False
        async def start(self): self.running = True
        async def pause(self): self.running = False
        def status(self): return {'running': self.running, 'inProgress': False}
        async def run_round(self): pass
    async def provider(force=False):
        return {'connected': True, 'models': ['gpt-6-astra', 'gpt-6.1-sol'], 'baseUrl': module.BASE_URL}
    monkeypatch.setattr(module, 'API_KEY', DUMMY_API_KEY)
    monkeypatch.setattr(module, 'store', ledger)
    monkeypatch.setattr(module, 'markets', Markets())
    monkeypatch.setattr(module, 'executor', Executor())
    monkeypatch.setattr(module, 'runtime', Runtime())
    monkeypatch.setattr(module, 'check_provider', provider)
    return TestClient(module.app)

def test_initial_virtual_balances_and_server_side_credentials(client):
    response = client.get('/api/state')
    assert response.status_code == 200
    state = response.json()
    assert len(state['agents']) == 10
    assert state['totals']['initialCapital'] == state['totals']['equity'] == 8000000
    assert all(a['cash'] == 800000 for a in state['agents'])
    assert sum(a['model'] == 'gpt-6-astra' for a in state['agents']) == 5
    assert sum(a['model'] == 'gpt-6.1-sol' for a in state['agents']) == 5
    assert all(a['effort'] == 'max' for a in state['agents'])
    assert 'apiKey' not in response.text and DUMMY_API_KEY not in response.text

def test_cross_site_mutation_blocked(client):
    response = client.post('/api/simulation/start', json={}, headers={'origin': 'https://example.com'})
    assert response.status_code == 403
    assert not client.get('/api/health').json()['runtime']['running']
    assert client.post('/api/simulation/start', json={}, headers={'sec-fetch-site':'cross-site'}).status_code == 403

def test_dns_rebinding_blocked(client):
    assert client.get('/api/health', headers={'host':'example.com'}).status_code == 403

def test_non_json_mutation_blocked(client):
    assert client.post('/api/simulation/start', content='x').status_code == 415

def test_settings_and_messages_validate_inputs(client):
    assert client.post('/api/settings', json={'intervalSeconds':1}).status_code == 422
    assert client.post('/api/settings', json={'intervalSeconds':300}).status_code == 200
    assert client.post('/api/messages', json={'recipient':'missing','content':'hello'}).status_code == 404
    assert client.post('/api/messages', json={'sender':'astra-1','content':'hello'}).status_code == 400
    assert client.post('/api/messages', json={'content':'  '}).status_code == 400
    response = client.post('/api/messages', json={'recipient':'all','kind':'distraction','content':'Look over here!'})
    assert response.status_code == 200
    assert 'Look over here!' in client.get('/api/state').text

def test_reset_requires_pause(client):
    assert client.post('/api/simulation/start', json={}).status_code == 200
    assert client.post('/api/simulation/reset', json={}).status_code == 409
    assert client.post('/api/simulation/pause', json={}).status_code == 200
    assert client.post('/api/simulation/reset', json={}).status_code == 200


def test_reset_during_pause_cleanup_keeps_ledger(client, monkeypatch):
    import app as module
    module.store.add_message('human', 'all', 'tip', 'Preserve this record')
    monkeypatch.setattr(module.runtime, 'status', lambda: {'running': False, 'inProgress': False, 'pausing': True, 'canReset': False})
    assert client.post('/api/simulation/reset', json={}).status_code == 409
    assert module.store.messages()[0]['content'] == 'Preserve this record'
    assert sum(a['cash'] for a in module.store.list_agents()) == 8000000


def test_continuous_start_does_not_interrupt_manual_evaluation(client, monkeypatch):
    import app as module
    monkeypatch.setattr(module.runtime, 'status', lambda: {'running': False, 'inProgress': True})
    assert client.post('/api/simulation/start', json={}).status_code == 409
    assert not module.runtime.running


def test_manual_evaluation_rejected_while_continuous_workers_start(client, monkeypatch):
    import app as module
    monkeypatch.setattr(module.runtime, 'status', lambda: {'running': True, 'inProgress': False})
    assert client.post('/api/simulation/round', json={}).status_code == 409
