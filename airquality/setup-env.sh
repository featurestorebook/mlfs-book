#!/usr/bin/env bash

# Detect if script is being sourced or executed
# When sourced, use 'return' instead of 'exit' to avoid closing the shell
(return 0 2>/dev/null) && SOURCED=1 || SOURCED=0

# Require sourcing so that venv activation persists in user's shell
if [ "$SOURCED" -eq 0 ]; then
  echo ""
  echo "❌ This script must be sourced, not executed."
  echo ""
  echo "   Run it like this:"
  echo "      source setup-env.sh"
  echo "   Or:"
  echo "      . setup-env.sh"
  echo ""
  exit 1
fi

set -uo pipefail

# Helper function to return from sourced script
exit_script() {
  return "$1"
}

VENV_DIR=".venv"
REQUIRED_MIN="3.10"
REQUIRED_MAX="3.13"
PREFERRED_PYTHON="3.12"

echo "🔍 Locating Python..."

# Function to check if a Python version is valid
check_python_version() {
  local python_bin="$1"
  $python_bin - <<EOF 2>/dev/null
import sys
min_v = (3, 10)
max_v = (3, 13)
cur_v = sys.version_info[:2]
sys.exit(0 if min_v <= cur_v < max_v else 1)
EOF
}

# Function to get Python version string
get_python_version() {
  local python_bin="$1"
  $python_bin -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")' 2>/dev/null
}

# Function to find a valid Python installation
find_valid_python() {
  # Check common Python installations in order of preference
  local candidates=(
    "python3.12"
    "python3.11"
    "python3.10"
    "python3"
    "python"
  )

  # On macOS, also check Homebrew paths
  if [ "$(uname -s)" = "Darwin" ]; then
    candidates=(
      "/opt/homebrew/bin/python3.12"
      "/opt/homebrew/bin/python3.11"
      "/opt/homebrew/bin/python3.10"
      "/usr/local/bin/python3.12"
      "/usr/local/bin/python3.11"
      "/usr/local/bin/python3.10"
      "${candidates[@]}"
    )
  fi

  for candidate in "${candidates[@]}"; do
    if command -v "$candidate" >/dev/null 2>&1; then
      if check_python_version "$candidate"; then
        command -v "$candidate"
        return 0
      fi
    elif [ -x "$candidate" ]; then
      if check_python_version "$candidate"; then
        echo "$candidate"
        return 0
      fi
    fi
  done

  return 1
}

# Function to install a valid Python version
install_valid_python() {
  local os_type="$(uname -s)"

  echo "🔧 Attempting to install Python $PREFERRED_PYTHON..."

  if [ "$os_type" = "Darwin" ]; then
    # macOS: Use Homebrew
    if ! command -v brew >/dev/null 2>&1; then
      echo "❌ Homebrew is not installed. Cannot auto-install Python."
      echo "   Install Homebrew first: /bin/bash -c \"\$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)\""
      return 1
    fi

    echo "📥 Installing Python $PREFERRED_PYTHON via Homebrew..."
    if brew install "python@$PREFERRED_PYTHON"; then
      # Return the path to the newly installed Python
      if [ -x "/opt/homebrew/bin/python$PREFERRED_PYTHON" ]; then
        echo "/opt/homebrew/bin/python$PREFERRED_PYTHON"
      elif [ -x "/usr/local/bin/python$PREFERRED_PYTHON" ]; then
        echo "/usr/local/bin/python$PREFERRED_PYTHON"
      else
        command -v "python$PREFERRED_PYTHON"
      fi
      return 0
    else
      echo "❌ Failed to install Python via Homebrew"
      return 1
    fi

  elif [ "$os_type" = "Linux" ]; then
    # Linux: Detect distribution
    if [ -f /etc/os-release ]; then
      . /etc/os-release
      local distro_id="$ID"

      case "$distro_id" in
        ubuntu|debian)
          echo "📥 Installing Python $PREFERRED_PYTHON via deadsnakes PPA..."

          # Add deadsnakes PPA for newer Python versions
          if ! sudo add-apt-repository -y ppa:deadsnakes/ppa; then
            echo "❌ Failed to add deadsnakes PPA"
            return 1
          fi

          if sudo apt-get update && sudo apt-get install -y "python$PREFERRED_PYTHON" "python$PREFERRED_PYTHON-venv" "python$PREFERRED_PYTHON-dev"; then
            command -v "python$PREFERRED_PYTHON"
            return 0
          else
            echo "❌ Failed to install Python $PREFERRED_PYTHON"
            return 1
          fi
          ;;

        rhel|centos|fedora|rocky|almalinux)
          echo "📥 Installing Python $PREFERRED_PYTHON via dnf..."

          # Format version without dot for package name (e.g., python312)
          local pkg_version="${PREFERRED_PYTHON//./}"

          if sudo dnf install -y "python$pkg_version" "python$pkg_version-devel"; then
            command -v "python$PREFERRED_PYTHON"
            return 0
          else
            echo "❌ Failed to install Python $PREFERRED_PYTHON"
            return 1
          fi
          ;;

        *)
          echo "❌ Unsupported Linux distribution: $distro_id"
          echo "   Please manually install Python >= $REQUIRED_MIN and < $REQUIRED_MAX"
          return 1
          ;;
      esac
    else
      echo "❌ Cannot detect Linux distribution"
      return 1
    fi
  else
    echo "❌ Unsupported operating system: $os_type"
    return 1
  fi
}

# Try to find a valid Python first
PYTHON_BIN=""

# Check if python3 or python exists and is valid
if command -v python3 >/dev/null 2>&1; then
  INITIAL_PYTHON="$(command -v python3)"
elif command -v python >/dev/null 2>&1; then
  INITIAL_PYTHON="$(command -v python)"
else
  INITIAL_PYTHON=""
fi

if [ -n "$INITIAL_PYTHON" ]; then
  PY_VERSION="$(get_python_version "$INITIAL_PYTHON")"
  echo "🐍 Found Python $PY_VERSION at $INITIAL_PYTHON"

  if check_python_version "$INITIAL_PYTHON"; then
    PYTHON_BIN="$INITIAL_PYTHON"
    echo "✅ Python version is supported"
  else
    echo "⚠️  Python $PY_VERSION is not supported (requires >= $REQUIRED_MIN and < $REQUIRED_MAX)"
    echo ""
    echo "🔍 Searching for a valid Python installation..."

    # Try to find another valid Python
    if VALID_PYTHON="$(find_valid_python)"; then
      PYTHON_BIN="$VALID_PYTHON"
      PY_VERSION="$(get_python_version "$PYTHON_BIN")"
      echo "✅ Found valid Python $PY_VERSION at $PYTHON_BIN"
    else
      echo "⚠️  No valid Python found. Attempting to install one..."
      echo ""

      # Try to install a valid Python
      if INSTALLED_PYTHON="$(install_valid_python)"; then
        PYTHON_BIN="$INSTALLED_PYTHON"
        PY_VERSION="$(get_python_version "$PYTHON_BIN")"
        echo "✅ Successfully installed Python $PY_VERSION at $PYTHON_BIN"
      else
        echo ""
        echo "❌ Could not find or install a valid Python version."
        echo "   Please manually install Python >= $REQUIRED_MIN and < $REQUIRED_MAX"
        exit_script 1
      fi
    fi
  fi
else
  echo "⚠️  No Python found. Searching for valid installations..."

  # Try to find a valid Python
  if VALID_PYTHON="$(find_valid_python)"; then
    PYTHON_BIN="$VALID_PYTHON"
    PY_VERSION="$(get_python_version "$PYTHON_BIN")"
    echo "✅ Found valid Python $PY_VERSION at $PYTHON_BIN"
  else
    echo "⚠️  No valid Python found. Attempting to install one..."
    echo ""

    # Try to install a valid Python
    if INSTALLED_PYTHON="$(install_valid_python)"; then
      PYTHON_BIN="$INSTALLED_PYTHON"
      PY_VERSION="$(get_python_version "$PYTHON_BIN")"
      echo "✅ Successfully installed Python $PY_VERSION at $PYTHON_BIN"
    else
      echo ""
      echo "❌ Could not find or install a valid Python version."
      echo "   Please manually install Python >= $REQUIRED_MIN and < $REQUIRED_MAX"
      exit_script 1
    fi
  fi
fi

echo ""
echo "🐍 Using Python $PY_VERSION at $PYTHON_BIN"

# Create .python-version file so uv uses the correct Python
echo "$PY_VERSION" > .python-version
echo "📝 Created .python-version file for uv"

# Check and install system dependencies required for hopsworks (twofish)
echo ""
echo "🔧 Checking system dependencies..."

OS_TYPE="$(uname -s)"

install_system_deps() {
  if [ "$OS_TYPE" = "Linux" ]; then
    # Detect Linux distribution
    if [ -f /etc/os-release ]; then
      . /etc/os-release
      DISTRO_ID="$ID"

      case "$DISTRO_ID" in
        ubuntu|debian)
          echo "📦 Checking Ubuntu/Debian dependencies..."
          MISSING_DEPS=""

          # Check for build-essential
          if ! dpkg -s build-essential >/dev/null 2>&1; then
            MISSING_DEPS="build-essential"
          fi

          # Check for python3-dev
          if ! dpkg -s python3-dev >/dev/null 2>&1; then
            MISSING_DEPS="$MISSING_DEPS python3-dev"
          fi

          if [ -n "$MISSING_DEPS" ]; then
            echo "📥 Installing missing dependencies:$MISSING_DEPS"
            if sudo apt-get update && sudo apt-get install -y $MISSING_DEPS; then
              echo "✅ Successfully installed system dependencies"
            else
              echo "❌ Failed to install system dependencies"
              echo "   Please manually run: sudo apt install build-essential python3-dev"
              exit_script 1
            fi
          else
            echo "✅ All system dependencies already installed"
          fi
          ;;

        rhel|centos|fedora|rocky|almalinux)
          echo "📦 Checking Red Hat/CentOS/Fedora dependencies..."
          MISSING_DEPS=""

          # Check for Development Tools group (equivalent to build-essential)
          if ! dnf group list installed 2>/dev/null | grep -q "Development Tools"; then
            MISSING_DEPS="@development-tools"
          fi

          # Check for python3-devel
          if ! rpm -q python3-devel >/dev/null 2>&1; then
            MISSING_DEPS="$MISSING_DEPS python3-devel"
          fi

          if [ -n "$MISSING_DEPS" ]; then
            echo "📥 Installing missing dependencies:$MISSING_DEPS"
            if sudo dnf install -y $MISSING_DEPS; then
              echo "✅ Successfully installed system dependencies"
            else
              echo "❌ Failed to install system dependencies"
              echo "   Please manually run: sudo dnf install -y @development-tools python3-devel"
              exit_script 1
            fi
          else
            echo "✅ All system dependencies already installed"
          fi
          ;;

        *)
          echo "⚠️  Unknown Linux distribution: $DISTRO_ID"
          echo "   Please ensure you have C/C++ build tools and Python development headers installed"
          echo "   These are required for the hopsworks Python client (twofish dependency)"
          ;;
      esac
    else
      echo "⚠️  Cannot detect Linux distribution"
      echo "   Please ensure you have C/C++ build tools and Python development headers installed"
    fi

  elif [ "$OS_TYPE" = "Darwin" ]; then
    echo "📦 Checking macOS dependencies..."

    # Check for Xcode Command Line Tools
    if ! xcode-select -p >/dev/null 2>&1; then
      echo "📥 Installing Xcode Command Line Tools..."
      echo "   This provides the necessary build tools for Python packages"

      # Trigger the Xcode Command Line Tools installer
      xcode-select --install

      echo ""
      echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
      echo "⚠️  ACTION REQUIRED"
      echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
      echo ""
      echo "Please complete the Xcode Command Line Tools installation in the"
      echo "popup dialog, then re-run this script."
      echo ""
      echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
      echo ""
      exit_script 1
    else
      echo "✅ Xcode Command Line Tools already installed"
    fi

    # Check for Homebrew
    if ! command -v brew >/dev/null 2>&1; then
      echo ""
      echo "❌ Homebrew is not installed"
      echo "   Homebrew is required to install librdkafka for confluent-kafka"
      echo ""
      echo "   Install Homebrew with:"
      echo "   /bin/bash -c \"\$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)\""
      echo ""
      exit_script 1
    fi

    # Check for librdkafka (required for confluent-kafka)
    if ! brew list librdkafka >/dev/null 2>&1; then
      echo "📥 Installing librdkafka (required for confluent-kafka)..."
      if brew install librdkafka; then
        echo "✅ Successfully installed librdkafka"
      else
        echo "❌ Failed to install librdkafka"
        echo "   Please manually run: brew install librdkafka"
        exit_script 1
      fi
    else
      echo "✅ librdkafka already installed"
    fi

    # Check for libomp (required for LightGBM and other ML libraries)
    if ! brew list libomp >/dev/null 2>&1; then
      echo "📥 Installing libomp (required for LightGBM)..."
      if brew install libomp; then
        echo "✅ Successfully installed libomp"
      else
        echo "❌ Failed to install libomp"
        echo "   Please manually run: brew install libomp"
        exit_script 1
      fi
    else
      echo "✅ libomp already installed"
    fi
  else
    echo "⚠️  Unknown operating system: $OS_TYPE"
    echo "   Please ensure you have C/C++ build tools and Python development headers installed"
  fi
}

# Run the system dependency installation
install_system_deps

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
  # Skip Hopsworks credential checks if running inside Hopsworks (PROJECT_PATH is set)
  if [ -n "${PROJECT_PATH:-}" ]; then
    echo "✅ Running inside Hopsworks, skipping credential checks"
  else
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
    printf "Enter your Hopsworks project name: "
    read PROJECT_NAME

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

  # Check for HOPSWORKS_HOST
  if ! grep -q "^HOPSWORKS_HOST=.\+" "$ENV_FILE" 2>/dev/null; then
    echo ""
    echo "⚠️  Warning: HOPSWORKS_HOST not set in $ENV_FILE"
    echo "   Please set it to your Hopsworks cluster host (e.g., eu-west.cloud.hopsworks.ai)"
    echo ""
  else
    CURRENT_HOST=$(grep "^HOPSWORKS_HOST=" "$ENV_FILE" | cut -d'=' -f2)
    echo "✅ HOPSWORKS_HOST set to: $CURRENT_HOST"
  fi
  fi  # End of PROJECT_PATH check
else
  echo "⚠️  Warning: $ENV_FILE not found"
fi

# Skip venv setup if running inside Hopsworks (PROJECT_PATH is set)
if [ -n "${PROJECT_PATH:-}" ]; then
  echo ""
  echo "✅ Running inside Hopsworks, skipping venv setup"
  echo ""
  # Restore shell options to avoid affecting the parent shell
  set +uo pipefail
else
  echo ""
  # Create virtual environment if missing, incomplete, or using wrong Python version
  RECREATE_VENV=0

  if [ ! -f "$VENV_DIR/bin/activate" ]; then
    if [ -d "$VENV_DIR" ]; then
      echo "📦 Removing incomplete virtual environment"
      rm -rf "$VENV_DIR"
    fi
    RECREATE_VENV=1
  elif [ -f "$VENV_DIR/bin/python" ]; then
    # Check if existing venv uses a valid Python version
    VENV_PY_VERSION="$(get_python_version "$VENV_DIR/bin/python")"
    if ! check_python_version "$VENV_DIR/bin/python"; then
      echo "📦 Existing virtual environment uses Python $VENV_PY_VERSION (requires >= $REQUIRED_MIN and < $REQUIRED_MAX)"
      echo "📦 Removing virtual environment to recreate with Python $PY_VERSION"
      rm -rf "$VENV_DIR"
      RECREATE_VENV=1
    else
      echo "📦 Virtual environment already exists with Python $VENV_PY_VERSION"
    fi
  else
    echo "📦 Virtual environment missing Python binary, recreating"
    rm -rf "$VENV_DIR"
    RECREATE_VENV=1
  fi

  if [ "$RECREATE_VENV" -eq 1 ]; then
    echo "📦 Creating virtual environment in $VENV_DIR with Python $PY_VERSION"
    if ! "$PYTHON_BIN" -m venv "$VENV_DIR"; then
      echo "❌ Failed to create virtual environment"
      exit_script 1
    fi
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

  # Refresh shell's command hash table so uv command is found
  hash -r

  if ! uv pip install -U -r requirements.txt; then
    echo "❌ Failed to install requirements"
    exit_script 1
  fi

  # Refresh shell's command hash table so newly installed commands are found
  hash -r

  echo "🎉 Environment ready!"
  echo "   Python: $(python --version)"
  echo "   invoke: $(invoke --version 2>/dev/null || echo installed)"
  echo "   uv: $(uv --version)"
  echo ""
  echo "✅ Virtual environment is active"
  echo ""
  echo "Check which tasks you can run with:"
  echo "   inv --list"
  echo "Run a task with:"
  echo "   inv <task_name>"
  echo "Run the end-to-end example with:"
  echo "   inv all"

  # Restore shell options to avoid affecting the parent shell
  set +uo pipefail

  # Ensure venv is activated in the current shell
  source "$VENV_DIR/bin/activate"

  # Refresh command hash so uv, pip, and other venv commands are found
  hash -r
fi
