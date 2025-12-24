from invoke import task, Collection, Program, exceptions
import os
import sys
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

VENV_DIR= Path(".venv")

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

    
##########################################
# Titanic Batch ML System
##########################################

@task
def clean(c):
    """Removes all feature groups, feature views, models, deployments for this example."""
    check_venv()
    with c.cd(".."):
        print("#################################################")
        print("################## Cleanup   ####################")
        print("#################################################")
        c.run("uv run python mlfs/clean_hopsworks_resources.py titanic")

@task
def backfill(c):
    """Creates the feature groups and inserts synthetic data."""
    check_venv()
    print("#################################################")
    print("########## Backfill Feature Pipeline   ##########")
    print("#################################################")
    c.run("uv run ipython notebooks/1-titanic-feature-group-backfill.ipynb")

@task
def features(c):
    """Incremental addition of synthetic data to the feature groups."""
    check_venv()
    print("#################################################")
    print("######### Incremental Feature Pipeline  #########")
    print("#################################################")
    c.run("uv run ipython notebooks/3-scheduled-titanic-feature-pipeline-daily.ipynb")

@task
def train(c):
    """Creates a feature view, gets training data, and trains and saves the XGBoost model."""
    check_venv()
    print("#################################################")
    print("############# Training Pipeline #################")
    print("#################################################")
    c.run("uv run ipython notebooks/2-titanic-training-pipeline.ipynb")

@task
def inference(c):
    """Downloads model and feature view, Uses feature view to get inference data and predicts with the model."""
    check_venv()
    print("#################################################")
    print("#############  Inference Pipeline ###############")
    print("#################################################")
    c.run("uv run ipython notebooks/4-scheduled-titanic-batch-inference-daily.ipynb")


@task(pre=[backfill, features, train, inference])
def all(c):
    """Runs all the FTI pipelines in order."""
    pass


