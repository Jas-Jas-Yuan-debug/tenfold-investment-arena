#!/bin/zsh
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Jas-Jas-Yuan-debug

set -e
cd "${0:A:h}"
if [[ ! -x .venv/bin/python ]]; then
  uv venv --python 3.12 .venv
  uv pip install --python .venv/bin/python -r requirements.txt
fi
exec .venv/bin/python -m uvicorn app:app --host 127.0.0.1 --port "${PORT:-8787}" --no-access-log
