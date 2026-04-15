#!/bin/bash

#!/usr/bin/env bash
set -euo pipefail

VENV_DIR=".venv"
REQUIRED_MIN="3.7"
REQUIRED_MAX="3.14"

echo "🔍 Locating Python..."

# Prefer python3, fall back to python
if command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python3)"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python)"
else
  echo "❌ Python not found. Please install Python >= 3.8."
  exit 1
fi

PY_VERSION="$($PYTHON_BIN -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"

echo "🐍 Found Python $PY_VERSION at $PYTHON_BIN"

# Version check: > 3.7 and < 3.13
$PYTHON_BIN - <<EOF
import sys
min_v = (3, 8)
max_v = (3, 14)
cur_v = sys.version_info[:2]

if not (min_v <= cur_v < max_v):
    print(f"❌ Python {cur_v[0]}.{cur_v[1]} not supported. "
          f"Require >= {min_v[0]}.{min_v[1]} and < {max_v[0]}.0")
    sys.exit(1)
EOF

echo "✅ Python version is supported"

# Create virtual environment if missing
if [ ! -d "$VENV_DIR" ]; then
  echo "📦 Creating virtual environment in $VENV_DIR"
  "$PYTHON_BIN" -m venv "$VENV_DIR"
else
  echo "📦 Virtual environment already exists"
fi

# Activate virtual environment
# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

echo "⚡ Virtual environment activated: $VENV_DIR"

# Check pip is installed
if ! python -m pip --version >/dev/null 2>&1; then
  echo "⚠️  pip not found, bootstrapping with ensurepip"
  python -m ensurepip --upgrade || {
    echo "❌ Failed to bootstrap pip. Your Python build may be incomplete."
    echo "   If using conda, try: conda install pip"
    exit 1
  }
fi
# Upgrade pip (safe + recommended)
python -m pip install --upgrade pip >/dev/null

# Ensure required libraries
ensure_package() {
  PKG="$1"
  if ! python - <<EOF >/dev/null 2>&1
import importlib.util
exit(0 if importlib.util.find_spec("$PKG") else 1)
EOF
  then
    echo "📥 Installing $PKG"
    python -m pip install "$PKG"
  else
    echo "✅ $PKG already installed"
  fi
}

ensure_package invoke

if command -v uv >/dev/null 2>&1; then
  uv pip install -r requirements.txt
else
  pip install -r requirements.txt
fi

echo "🎉 Environment ready!"
echo "   Python: $(python --version)"
echo "   invoke: $(invoke --version 2>/dev/null || echo installed)"
echo "   uv: $(uv --version 2>/dev/null || echo 'not installed')"

echo "Check which tasks you can run with:"
echo "   inv --list"
echo "Run a task with:"
echo "   inv <task_name>"
echo "Run the air quality example end-to-end with:"
echo "   inv aq-all"
