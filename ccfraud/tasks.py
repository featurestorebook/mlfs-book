from invoke import task, call, Collection, Program, exceptions
import os
import sys
import signal
from pathlib import Path

# Monkey-patch ParseError to provide better error messages
_original_parse_error_str = exceptions.ParseError.__str__

def _custom_parse_error_str(self):
    """Custom error message for ParseError."""
    original_msg = _original_parse_error_str(self)

    # Check if it's an unknown command error
    if "No idea what" in original_msg:
        import re
        import subprocess
        match = re.search(r"No idea what ['\"]([^'\"]+)['\"]", original_msg)
        cmd_name = match.group(1) if match else "unknown"

        # Get list of available tasks
        try:
            result = subprocess.run(
                ['inv', '--list'],
                capture_output=True,
                text=True,
                timeout=2
            )
            task_list = result.stdout
        except:
            task_list = "(run 'inv --list' to see available commands)"

        return f"""❌ Command not found: '{cmd_name}'

{task_list}"""

    return original_msg

# Apply the monkey patch
exceptions.ParseError.__str__ = _custom_parse_error_str


VENV_DIR = Path(".venv")

def _in_hopsworks():
    return os.environ.get("PROJECT_PATH") is not None

def _require_outside_hopsworks(task_name):
    """Exit with an error if running inside Hopsworks."""
    if _in_hopsworks():
        print(f"ERROR: 'inv {task_name}' can only be run outside Hopsworks.")
        sys.exit(1)

def uv_run(cmd):
    """Wrap a command with 'uv run' locally, or run directly in Hopsworks."""
    if _in_hopsworks():
        return cmd
    return f"uv run {cmd}"

def uv_pip(args):
    """Wrap 'uv pip <args>' locally, or use 'pip <args>' in Hopsworks."""
    if _in_hopsworks():
        return f"pip {args}"
    return f"uv pip {args}"

def check_venv():
    """Check if a virtual environment exists and is active."""

    # Skip this check if you are running the code in a Hopsworks cluster
    if os.environ.get("PROJECT_PATH"):
        return

    # 1. Create venv if it doesn't exist
    if not VENV_DIR.exists():
        print("🔧 There is no virtual environment. Did you run the setup step yet?")
        print("👉 ./setup.sh")
        sys.exit(2)

    virtual_env = os.environ.get("VIRTUAL_ENV")
    venv_path = str(VENV_DIR.resolve())

    if virtual_env != venv_path:
        print("🐍 Virtual environment is NOT active.")
        print()
        print("👉 Activate it with:")
        print(f"   source {VENV_DIR}/bin/activate")
        sys.exit(1)


def run_interruptible(c, command, pty=True):
    """Run a command with proper Ctrl-C handling.

    Args:
        c: Invoke context
        command: Command string to execute
        pty: Whether to use a pseudo-terminal (default: True)
    """
    try:
        c.run(command, pty=pty)
    except KeyboardInterrupt:
        print("\n\n⚠️  Task interrupted by user (Ctrl-C)")
        sys.exit(130)  # Standard exit code for SIGINT


##########################################
# Credit Card Fraud ML System
##########################################

@task
def clean(c):
    """Removes all feature groups, feature views, models, deployments for this example."""
    check_venv()
    with c.cd(".."):
        print("#################################################")
        print("################## Cleanup ######################")
        print("#################################################")
        run_interruptible(c, uv_run("python mlfs/clean_hopsworks_resources.py cc"), pty=False)


@task
def datamart(c, mode="backfill", entities="all", num_transactions=500000, fraud_rate=0.0001, start_date=None, end_date=None):
    """Generate and insert synthetic data.

    Args:
        mode: Generation mode ('backfill' or 'incremental')
        entities: Comma-separated list of entities to generate (default: 'all')
        num_transactions: Number of transactions to generate (default: 500000)
        fraud_rate: Fraud rate as decimal (default: 0.0001)
        start_date: Start date for transactions in YYYY-MM-DD format (default: 30 days ago)
        end_date: End date for transactions in YYYY-MM-DD format (default: today)

    Examples:
        inv datamart                                    # Full backfill (previous 30 days)
        inv datamart --start-date=2025-11-01 --end-date=2025-12-01
        inv datamart --mode=incremental --entities=transactions --num-transactions=10000
        inv datamart --fraud-rate=0.001                # Higher fraud rate
    """
    from datetime import datetime, timedelta

    check_venv()
    print("#################################################")
    print("########## Parameterized Data Generation ########")
    print("#################################################")

    # Calculate default date range (previous 30 days)
    if end_date is None:
        end_dt = datetime.now()
        end_date = end_dt.strftime('%Y-%m-%d')
    else:
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')

    if start_date is None:
        start_dt = end_dt - timedelta(days=30)
        start_date = start_dt.strftime('%Y-%m-%d')

    # Build command
    cmd = uv_run(f"python ccfraud/1_data_generator.py --mode {mode} --start-date {start_date} --end-date {end_date}")

    if entities != "all":
        # Convert comma-separated to space-separated for argparse
        entities_list = entities.replace(",", " ")
        cmd += f" --entities {entities_list}"

    if num_transactions != 500000:
        cmd += f" --num-transactions {num_transactions}"

    if fraud_rate != 0.0001:
        cmd += f" --fraud-rate {fraud_rate}"

    print(f"Transaction date range: {start_date} to {end_date}")
    print(f"\nRunning: {cmd}\n")
    run_interruptible(c, cmd)

@task
def stream_transactions(c, transactions_per_sec=10, fraud_rate=0.005):
    """Insert stream of cc transactions until Ctrl-C.

    Publishes to credit_card_transactions feature group, which automatically
    syncs to Kafka for Feldera real-time pipeline.

    Args:
        transactions_per_sec: Transactions per second (range: 1-100, default: 10)
        fraud_rate: Fraud rate as decimal (default: 0.005 = 0.5%)

    Examples:
        inv stream-transactions                           # 10 TPS
        inv stream-transactions --transactions-per-sec=50 # 50 TPS
        inv stream-transactions --transactions-per-sec=1  # 1 TPS

    Press Ctrl-C to stop gracefully.
    """
    check_venv()

    # Validate parameters
    if transactions_per_sec < 1 or transactions_per_sec > 100:
        print(f"ERROR: transactions-per-sec must be between 1-100 (got {transactions_per_sec})")
        sys.exit(1)

    print("#################################################")
    print("######### Streaming Transaction Producer ########")
    print("#################################################")
    print(f"Rate: {transactions_per_sec} transactions/second")
    print(f"Fraud rate: {fraud_rate*100:.2f}%")
    print(f"Publishing to: credit_card_transactions FG → Kafka\n")

    cmd = (uv_run(f"python ccfraud/1_data_generator.py --mode streaming "
           f"--transactions-per-sec {transactions_per_sec} "
           f"--fraud-rate {fraud_rate}"))

    run_interruptible(c, cmd)

@task
def features(c, current_date=None, wait=False):
    """A batch feature pipeline to create features.

    Args:
        current_date: Current date in YYYY-MM-DD format (default: today's date)
        wait: Wait for data to be synced to backend (default: False)

    Examples:
        inv features                              # Uses today's date
        inv features --current-date=2025-12-15   # Specific date
        inv features --wait                       # Wait for sync to complete
    """
    from datetime import datetime

    check_venv()
    print("#################################################")
    print("######### Incremental Feature Pipeline ##########")
    print("#################################################")
    #run_interruptible(c, "./fix.sh", pty=False)

    run_interruptible(c, uv_pip("install -U hopsworks"), pty=False)

    # Default to today's date if not provided
    if current_date is None:
        current_date = datetime.now().strftime('%Y-%m-%d')

    cmd = uv_run(f"python ccfraud/3-batch-feature-pipeline.py --current-date {current_date}")
    if wait:
        cmd += " --wait"
    print(f"Current date: {current_date}")
    print(f"Wait for sync: {wait}")
    print(f"\nRunning: {cmd}\n")
    run_interruptible(c, cmd)

@task
def train(c, model="xgboost", test_start=None):
    """Training pipeline for credit card fraud prediction.

    Args:
        model: Model type to train: 'xgboost', 'catboost', or 'neuralnet' (default: xgboost)
        test_start: Start date for test split in YYYY-MM-DD format (default: 7 days ago)

    Examples:
        inv train                              # Train XGBoost (default)
        inv train --model=catboost             # Train CatBoost
        inv train --model=neuralnet            # Train Neural Network
        inv train --test-start=2026-01-10      # Specific test split date
        inv train --model=catboost --test-start=2026-01-10  # Combined
    """
    from datetime import datetime, timedelta

    check_venv()

    # Map model types to notebook paths
    notebook_map = {
        'xgboost': 'notebooks/4-training-cc-fraud-pipeline.ipynb',
        'catboost': 'notebooks/4b-training-catboost-fraud-model.ipynb',
        'neuralnet': 'notebooks/4b-training-nn-fraud-model.ipynb',
    }

    # Validate model choice
    model = model.lower()
    if model not in notebook_map:
        valid_models = ', '.join(notebook_map.keys())
        print(f"ERROR: Invalid model '{model}'. Choose from: {valid_models}")
        sys.exit(1)

    notebook = notebook_map[model]
    model_display = {'xgboost': 'XGBoost', 'catboost': 'CatBoost', 'neuralnet': 'Neural Network'}

    print("#################################################")
    print(f"######## Training Pipeline ({model_display[model]}) ########")
    print("#################################################")

    # Default to 7 days ago if not provided
    if test_start is None:
        test_start_dt = datetime.now() - timedelta(days=7)
        test_start = test_start_dt.strftime('%Y-%m-%d 00:00')
    elif ' ' not in test_start:
        test_start = f"{test_start} 00:00"

    print(f"Model: {model_display[model]}")
    print(f"Notebook: {notebook}")
    print(f"Test split start date: {test_start}")
    run_interruptible(c, uv_pip("install -r requirements.txt"), pty=False)

    print("\nInstalling requirements...")
    run_interruptible(c, uv_pip("install -r requirements.txt"), pty=False)

    cmd = uv_run(
        f'papermill {notebook} '
        f'{notebook} '
        f'-p test_start "{test_start}"'
    )
    print(f"\nRunning: {cmd}\n")
    run_interruptible(c, cmd)

@task
def inference(c):
    """Launch Streamlit UI for interactive fraud prediction.

    Provides an interactive web interface to:
    - Configure transaction generation parameters
    - Generate synthetic transactions
    - Write transactions to the feature group
    - Get fraud predictions from deployed model
    - View results with fraud status

    Examples:
        inv inference
    """
    check_venv()
    print("#################################################")
    print("#############  Fraud Prediction UI ###############")
    print("#################################################")
    run_interruptible(c, uv_run("streamlit run ccfraud/streamlit_app.py"))
    print("Starting Streamlit app at http://localhost:8501")

@task
def feldera_start(c):
    """Start Feldera Docker container as a background process."""
    _require_outside_hopsworks("feldera-start")
    check_venv()
    print("#################################################")
    print("#######  Starting Feldera Container  ############")
    print("#################################################")
    run_interruptible(c, "bash scripts/1a-run-feldera.sh start")

@task
def feldera_stop(c):
    """Stop Feldera Docker container."""
    _require_outside_hopsworks("feldera-stop")
    check_venv()
    print("#################################################")
    print("#######  Stopping Feldera Container  ############")
    print("#################################################")
    run_interruptible(c, "bash scripts/1a-run-feldera.sh stop")

@task(pre=[feldera_start])
def feldera(c):
    """Create/deploy streaming feature pipeline with Feldera."""
    _require_outside_hopsworks("feldera")
    check_venv()
    print("#################################################")
    print("#######  Feldera Streeaming Pipeline ############")
    print("#################################################")
    print("Upgrading feldera to latest version...")
    run_interruptible(c, uv_pip("install -U feldera"), pty=False)
    print("\nRunning streaming feature pipeline...")
    run_interruptible(c, uv_run("python ccfraud/2-feldera-streaming-feature-pipeline.py"))

@task
def test(c):
    """Run all unit tests using pytest."""
    check_venv()
    print("#################################################")
    print("############### Running Tests ###################")
    print("#################################################")
    run_interruptible(c, uv_run("pytest tests/ -v"))

@task(pre=[datamart, feldera, call(features, wait=True), train, inference])
def all(c):
    """datamart, feldera, features (with wait), train, inference."""
    pass
