from invoke import task, Collection, Program, exceptions
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

def check_venv():
    """Check if a virtual environment exists and is active."""

    # 1. Create venv if it doesn't exist
    if not VENV_DIR.exists():
        print("🔧 There is no virtual environment. Did you run the setup step yet?")
        print("👉 ./setup-env.sh")
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
        run_interruptible(c, "uv run python mlfs/clean_hopsworks_resources.py cc", pty=False)


@task
def backfill_1(c, mode="backfill", entities="all", num_transactions=500000, fraud_rate=0.0001, start_date=None, end_date=None):
    """Generate synthetic data using parameterized Python script.

    Args:
        mode: Generation mode ('backfill' or 'incremental')
        entities: Comma-separated list of entities to generate (default: 'all')
        num_transactions: Number of transactions to generate (default: 500000)
        fraud_rate: Fraud rate as decimal (default: 0.0001)
        start_date: Start date for transactions in YYYY-MM-DD format (default: 30 days ago)
        end_date: End date for transactions in YYYY-MM-DD format (default: today)

    Examples:
        inv backfill-1                                    # Full backfill (previous 30 days)
        inv backfill-1 --start-date=2025-11-01 --end-date=2025-12-01
        inv backfill-1 --mode=incremental --entities=transactions --num-transactions=10000
        inv backfill-1 --fraud-rate=0.001                # Higher fraud rate
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
    cmd = f"uv run python ccfraud/data_generator.py --mode {mode} --start-date {start_date} --end-date {end_date}"

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
def transactions(c, start_date=None, end_date=None, num_transactions=20000):
    """Generate credit card transactions within a time range (uses existing entities).

    Args:
        start_date: Start date for transactions in YYYY-MM-DD format (default: 24 hours ago)
        end_date: End date for transactions in YYYY-MM-DD format (default: now)
        num_transactions: Number of transactions to generate (default: 20000)

    Examples:
        inv transactions                                    # Previous 24 hours
        inv transactions --start-date=2025-12-01 --end-date=2025-12-25
        inv transactions --num-transactions=50000
    """
    from datetime import datetime, timedelta

    check_venv()
    print("#################################################")
    print("######### Generate CC Transactions ##############")
    print("#################################################")

    # Calculate default time range (previous 24 hours)
    if end_date is None:
        end_dt = datetime.now()
        end_date = end_dt.strftime('%Y-%m-%d')
    else:
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')

    if start_date is None:
        start_dt = end_dt - timedelta(days=1)
        start_date = start_dt.strftime('%Y-%m-%d')
    else:
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')

    cmd = (f"uv run python ccfraud/data_generator.py --mode incremental "
           f"--entities transactions --num-transactions {num_transactions} "
           f"--start-date {start_date} --end-date {end_date}")

    print(f"Time range: {start_date} to {end_date}")
    print(f"\nRunning: {cmd}\n")
    run_interruptible(c, cmd)

@task
def features_3(c):
    """Runs a daily scheduled batch feature pipeline for for credit card transaction data."""
    check_venv()
    print("#################################################")
    print("######### Incremental Feature Pipeline ##########")
    print("#################################################")
    run_interruptible(c, "uv run python ccfraud/3-batch-feature-pipeline.py")

@task
def train_4(c):
    """Training pipeline for credit card fraud prediction model."""
    check_venv()
    print("#################################################")
    print("############# Training Pipeline #################")
    print("#################################################")
    run_interruptible(c, "uv run ipython notebooks/4-training-cc-fraud-pipeline.ipynb")

@task
def inference_5(c):
    """Deploys an online inference pipeline for credit card fraud prediction."""
    check_venv()
    print("#################################################")
    print("#############  Inference Pipeline ###############")
    print("#################################################")
    run_interruptible(c, "uv run ipython notebooks/4-deployment-pipeline.ipynb", pty=False)

@task
def feldera_start(c):
    """Start Feldera Docker container locally."""
    check_venv()
    print("#################################################")
    print("#######  Starting Feldera Container  ############")
    print("#################################################")
    run_interruptible(c, "bash scripts/1a-run-feldera.sh start")

@task
def feldera_stop(c):
    """Stop Feldera Docker container."""
    check_venv()
    print("#################################################")
    print("#######  Stopping Feldera Container  ############")
    print("#################################################")
    run_interruptible(c, "bash scripts/1a-run-feldera.sh stop")

@task(pre=[feldera_start])
def feldera_2(c):
    """Streaming feature pipeline started locally with Feldera."""
    check_venv()
    print("#################################################")
    print("#######  Feldera Streeaming Pipeline ############")
    print("#################################################")
    run_interruptible(c, "uv run ipython notebooks/2-feldera-streaming-feature-pipeline.ipynb")

@task
def test(c):
    """Run all unit tests using pytest."""
    check_venv()
    print("#################################################")
    print("############### Running Tests ###################")
    print("#################################################")
    run_interruptible(c, "uv run pytest tests/ -v")

@task(pre=[backfill_1, feldera_2, features_3, train_4, inference_5])
def all(c):
    """Runs feature/training pipelines, deploys credit card model."""
    pass
