# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Jas-Jas-Yuan-debug

"""Read/control adapter for the already-running original localhost arena.

Closing this adapter only closes its HTTP client. It never pauses or replaces
the original service's investor tasks. Provider slots below govern only new
companion-world requests, since another process owns the original slots.
"""
from __future__ import annotations

import asyncio
import copy
from datetime import datetime, timezone
from urllib.parse import urlparse

import httpx


class RemoteRuntimeError(RuntimeError):
    def __init__(self, message, status_code=503):
        super().__init__(message)
        self.status_code = status_code


class RemoteRuntime:
    _CONTROL_PATHS = {'/api/simulation/start', '/api/simulation/pause', '/api/simulation/round',
                      '/api/simulation/reset', '/api/settings'}

    def __init__(self, base_url):
        parsed = urlparse(base_url)
        if parsed.scheme != 'http' or parsed.hostname not in {'127.0.0.1', 'localhost', '::1'} or parsed.username or parsed.password:
            raise ValueError('MAIN_RUNTIME_URL must point to the original localhost HTTP service')
        self.base_url = base_url.rstrip('/')
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(8, connect=3), trust_env=False, follow_redirects=False)
        self._model_slots = asyncio.Semaphore(3)
        self._search_slots = asyncio.Semaphore(3)
        self._status = {'mode': 'continuous', 'running': None, 'inProgress': None,
                        'round': 0, 'apiConnected': None, 'agentStates': {}, 'lastError': 'Original runtime has not been checked'}
        self._connected = False
        self._checked_at = None
        self._error = 'Original runtime has not been checked'
        self._lock = asyncio.Lock()

    def status(self):
        return copy.deepcopy({**self._status, 'remote': True, 'remoteConnected': self._connected,
                              'remoteCheckedAt': self._checked_at, 'remoteError': self._error,
                              'modelConcurrencyScope': 'per-world', 'maxConcurrentModelsPerWorld': 3})

    async def refresh(self):
        async with self._lock:
            try:
                response = await self._client.get(self.base_url + '/api/health')
                response.raise_for_status()
                if len(response.content) > 2_000_000:
                    raise ValueError('Original service health response is too large')
                payload = response.json()
                status = payload.get('runtime')
                if not isinstance(status, dict):
                    raise ValueError('Original service did not supply runtime state')
                self._status = copy.deepcopy(status)
                self._connected, self._error = True, None
            except Exception as exc:
                self._connected = False
                self._error = f'{type(exc).__name__}: original localhost runtime unavailable'
            self._checked_at = datetime.now(timezone.utc).isoformat()
            return self.status()

    async def control(self, path, body=None):
        if path not in self._CONTROL_PATHS:
            raise ValueError('Unsupported original runtime control')
        try:
            response = await self._client.post(self.base_url + path, json=body or {}, timeout=45)
        except httpx.HTTPError as exc:
            raise RemoteRuntimeError('Original localhost runtime control is unavailable') from exc
        try:
            payload = response.json()
        except ValueError:
            payload = {}
        if not 200 <= response.status_code < 300:
            detail = payload.get('detail', 'Original service rejected the request') if isinstance(payload, dict) else 'Original service rejected the request'
            raise RemoteRuntimeError(str(detail)[:1000], response.status_code if response.status_code >= 400 else 502)
        await self.refresh()
        return payload

    async def start(self):
        return await self.control('/api/simulation/start')

    async def pause(self):
        return await self.control('/api/simulation/pause')

    async def run_round(self):
        return await self.control('/api/simulation/round')

    async def reset(self):
        return await self.control('/api/simulation/reset')

    async def set_interval(self, seconds):
        return await self.control('/api/settings', {'intervalSeconds': seconds})

    async def close(self):
        await self._client.aclose()
