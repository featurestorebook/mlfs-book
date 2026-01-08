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
def cc_gen_kafka(c):
    """Run Kafka transactions generator notebook."""
    check_venv()
    print("#################################################")
    print("########## Incremental Data Generation ##########")
    print("#################################################")
    run_interruptible(c, "uv run ipython notebooks/transactions_synthetic_kafka_generator.ipynb")


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
def backfill(c, mode="backfill", entities="all", num_transactions=500000, fraud_rate=0.0001):
    """Generate synthetic data using parameterized Python script.

    Args:
        mode: Generation mode ('backfill' or 'incremental')
        entities: Comma-separated list of entities to generate (default: 'all')
        num_transactions: Number of transactions to generate (default: 500000)
        fraud_rate: Fraud rate as decimal (default: 0.0001)

    Examples:
        inv gen                                    # Full backfill
        inv gen --mode=incremental --entities=transactions --num-transactions=10000
        inv gen --fraud-rate=0.001                # Higher fraud rate
    """
    check_venv()
    print("#################################################")
    print("########## Parameterized Data Generation ########")
    print("#################################################")

    # Build command
    cmd = f"uv run python ccfraud/data_generator.py --mode {mode}"

    if entities != "all":
        # Convert comma-separated to space-separated for argparse
        entities_list = entities.replace(",", " ")
        cmd += f" --entities {entities_list}"

    if num_transactions != 500000:
        cmd += f" --num-transactions {num_transactions}"

    if fraud_rate != 0.0001:
        cmd += f" --fraud-rate {fraud_rate}"

    print(f"\nRunning: {cmd}\n")
    run_interruptible(c, cmd)

@task
def features(c):
    """Runs a daily scheduled batch feature pipeline for for credit card transaction data."""
    check_venv()
    print("#################################################")
    print("######### Incremental Feature Pipeline ##########")
    print("#################################################")
    run_interruptible(c, "uv run python ccfraud/3-batch-feature-pipeline.py")

@task
def train(c):
    """Training pipeline for credit card fraud prediction model."""
    check_venv()
    print("#################################################")
    print("############# Training Pipeline #################")
    print("#################################################")
    run_interruptible(c, "uv run ipython notebooks/4-training-cc-fraud-pipeline.ipynb")

@task
def inference(c):
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

@task
def feldera(c):
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

@task(pre=[backfill, features, train, inference])
def all(c):
    """Runs feature/training pipelines, deploys credit card model."""
    pass
