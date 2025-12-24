#!/usr/bin/env bash

# Detect if script is being sourced or executed
# When sourced, use 'return' instead of 'exit' to avoid closing the shell
(return 0 2>/dev/null) && SOURCED=1 || SOURCED=0

# Only use strict mode when executed (not sourced)
# set -e would exit the terminal when sourced
if [ "$SOURCED" -eq 0 ]; then
  set -euo pipefail
else
  set -uo pipefail
fi

# Helper function to exit or return based on how script was invoked
exit_script() {
  if [ "$SOURCED" -eq 1 ]; then
    return "$1"
  else
    exit "$1"
  fi
}

VENV_DIR=".venv"
REQUIRED_MIN="3.8"
REQUIRED_MAX="3.13"

echo "🔍 Locating Python..."

# Prefer python3, fall back to python
if command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python3)"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="$(command -v python)"
else
  echo "❌ Python not found. Please install Python >= 3.8."
  exit_script 1
fi

PY_VERSION="$($PYTHON_BIN -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"

echo "🐍 Found Python $PY_VERSION at $PYTHON_BIN"

# Version check: > 3.7 and < 3.13
if ! $PYTHON_BIN - <<EOF
import sys
min_v = (3, 8)
max_v = (3, 13)
cur_v = sys.version_info[:2]

if not (min_v <= cur_v < max_v):
    print(f"❌ Python {cur_v[0]}.{cur_v[1]} not supported. "
          f"Require >= {min_v[0]}.{min_v[1]} and < {max_v[0]}.0")
    sys.exit(1)
EOF
then
  exit_script 1
fi

echo "✅ Python version is supported"

# Check and setup .env file
ENV_FILE="../.env"
ENV_EXAMPLE="../.env.example"

echo ""
echo "🔐 Checking environment configuration..."

if [ ! -f "$ENV_FILE" ]; then
  if [ -f "$ENV_EXAMPLE" ]; then
    echo "📝 Creating .env file from .env.example"
    cp "$ENV_EXAMPLE" "$ENV_FILE"
    echo "✅ Created $ENV_FILE"
  else
    echo "⚠️  Warning: $ENV_EXAMPLE not found, skipping .env creation"
  fi
fi

# Check if HOPSWORKS_API_KEY is set
if [ -f "$ENV_FILE" ]; then
  if ! grep -q "^HOPSWORKS_API_KEY=.\+" "$ENV_FILE" 2>/dev/null; then
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "⚠️  IMPORTANT: Hopsworks API Key Required"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    echo "To use this project, you need a Hopsworks API key:"
    echo ""
    echo "  1. Create a free account at: https://run.hopsworks.ai"
    echo "  2. Create a new project (or use an existing one)"
    echo "  3. Go to Settings → API Keys → Generate new key"
    echo "  4. Copy the API key"
    echo "  5. Edit $ENV_FILE and set:"
    echo "     HOPSWORKS_API_KEY=<your-api-key-here>"
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
  else
    echo "✅ HOPSWORKS_API_KEY found in $ENV_FILE"
  fi

  # Check and prompt for HOPSWORKS_PROJECT
  if ! grep -q "^HOPSWORKS_PROJECT=.\+" "$ENV_FILE" 2>/dev/null; then
    echo ""
    echo "📋 Hopsworks Project Configuration"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    echo "Please enter your Hopsworks project name."
    echo "This is the project you created (or will create) at https://run.hopsworks.ai"
    echo ""
    read -p "Enter your Hopsworks project name: " PROJECT_NAME

    if [ -n "$PROJECT_NAME" ]; then
      # Update or add HOPSWORKS_PROJECT in .env file
      if grep -q "^#HOPSWORKS_PROJECT=" "$ENV_FILE" 2>/dev/null; then
        # Uncomment and set the value
        sed -i.bak "s/^#HOPSWORKS_PROJECT=.*/HOPSWORKS_PROJECT=$PROJECT_NAME/" "$ENV_FILE"
      elif grep -q "^HOPSWORKS_PROJECT=" "$ENV_FILE" 2>/dev/null; then
        # Update existing uncommented line
        sed -i.bak "s/^HOPSWORKS_PROJECT=.*/HOPSWORKS_PROJECT=$PROJECT_NAME/" "$ENV_FILE"
      else
        # Add new line after HOPSWORKS_API_KEY
        sed -i.bak "/^HOPSWORKS_API_KEY=/a\\
HOPSWORKS_PROJECT=$PROJECT_NAME" "$ENV_FILE"
      fi
      rm -f "${ENV_FILE}.bak"
      echo "✅ Set HOPSWORKS_PROJECT=$PROJECT_NAME in $ENV_FILE"
    else
      echo "⚠️  No project name entered. You can set it later in $ENV_FILE"
    fi
    echo ""
  else
    CURRENT_PROJECT=$(grep "^HOPSWORKS_PROJECT=" "$ENV_FILE" | cut -d'=' -f2)
    echo "✅ HOPSWORKS_PROJECT set to: $CURRENT_PROJECT"
  fi
else
  echo "⚠️  Warning: $ENV_FILE not found"
fi

echo ""
# Create virtual environment if missing or incomplete
if [ ! -f "$VENV_DIR/bin/activate" ]; then
  if [ -d "$VENV_DIR" ]; then
    echo "📦 Removing incomplete virtual environment"
    rm -rf "$VENV_DIR"
  fi
  echo "📦 Creating virtual environment in $VENV_DIR"
  if ! "$PYTHON_BIN" -m venv "$VENV_DIR"; then
    echo "❌ Failed to create virtual environment"
    exit_script 1
  fi
else
  echo "📦 Virtual environment already exists"
fi

# Activate virtual environment
# shellcheck disable=SC1091
if ! source "$VENV_DIR/bin/activate"; then
  echo "❌ Failed to activate virtual environment"
  exit_script 1
fi

echo "⚡ Virtual environment activated: $VENV_DIR"

# Check pip is installed
# Upgrade pip (safe + recommended)
if ! python -m pip install --upgrade pip >/dev/null 2>&1; then
  #echo "⚠️  Warning: Failed to upgrade pip, continuing anyway"
  python -m ensurepip --upgrade || {
    echo "❌ Failed to bootstrap pip. Your Python build may be incomplete."
    echo "   If using conda, try: conda install pip"
    exit_script 1
  }
fi

# Upgrade pip, setuptools, and wheel (best practice for clean venv)
#echo "📦 Upgrading pip, setuptools, and wheel..."
if ! python -m pip install -q --upgrade pip setuptools wheel; then
  echo "⚠️  Warning: Failed to upgrade pip/setuptools/wheel"
  echo "   This may cause issues with package installations"
  echo "   Attempting to continue..."
fi

# Ensure required libraries
ensure_package() {
  PKG="$1"
  if ! python - <<EOF >/dev/null 2>&1
import importlib.util
exit(0 if importlib.util.find_spec("$PKG") else 1)
EOF
  then
    echo "📥 Installing $PKG"
    if ! python -m pip install "$PKG"; then
      echo "❌ Failed to install $PKG"
      exit_script 1
    fi
  fi
}

ensure_package invoke
ensure_package uv

if ! uv pip install -r requirements.txt; then
  echo "❌ Failed to install requirements"
  exit_script 1
fi

echo "🎉 Environment ready!"
echo "   Python: $(python --version)"
echo "   invoke: $(invoke --version 2>/dev/null || echo installed)"
echo "   uv: $(uv --version)"
echo ""
echo "Activate your virtual environment with:"
echo "   source $VENV_DIR/bin/activate"

echo "Check which tasks you can run with:"
echo "   inv --list"
echo "Run a task with:"
echo "   inv <task_name>"
echo "Run the end-to-end example with:"
echo "   inv all"

# Restore shell options when sourced to avoid affecting the parent shell
if [ "$SOURCED" -eq 1 ]; then
  set +uo pipefail
fi
