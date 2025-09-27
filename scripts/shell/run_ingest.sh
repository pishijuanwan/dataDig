#!/bin/zsh
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)

cd "$PROJECT_ROOT"

if [ -f ".venv/bin/activate" ]; then
  source .venv/bin/activate
fi

python -m src.app "$@"
