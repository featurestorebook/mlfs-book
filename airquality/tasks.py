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

def _in_hopsworks():
    return os.environ.get("PROJECT_PATH") is not None

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
        # Disable IPython extension that causes issues in Hopsworks
        config_path = Path("/srv/hops/anaconda/.ipython/profile_default/ipython_config.py")
        if config_path.exists():
            lines = config_path.read_text().splitlines()
            updated = []
            target = "c.InteractiveShellApp.extensions = ['attach_config_extension']"
            for line in lines:
                stripped = line.strip()
                if stripped == target:
                    updated.append(f"# {line}")
                else:
                    updated.append(line)
            config_path.write_text("\n".join(updated) + "\n")
        return

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
# Air Quality Batch ML System
##########################################

@task
def clean(c):
    """Deletes feature groups, feature views, models for air quality."""
    check_venv()
    with c.cd(".."):
        print("#################################################")
        print("################## Cleanup   ####################")
        print("#################################################")
        c.run(uv_run("python mlfs/clean_hopsworks_resources.py aq"))

@task
def backfill(c):
    """Creates feature groups, backfills air quality and weather data."""
    check_venv()
    print("#################################################")
    print("########## Backfill Feature Pipeline   ##########")
    print("#################################################")
    c.run(uv_run("ipython notebooks/1_air_quality_feature_backfill.ipynb"))

@task
def features(c):
    """Incremental ingestion of historical weather data, air quality data, and weather forecast data."""
    check_venv()
    print("#################################################")
    print("######### Incremental Feature Pipeline  #########")
    print("#################################################")
    c.run(uv_run("ipython notebooks/2_air_quality_feature_pipeline.ipynb"))

@task
def train(c, test_days=30, min_train_days=180):
    """Creates feature view, reads training data with feature view, trains and saves XGBoost model to predict air quality.

    Args:
        test_days: Number of days to use for test set (default: 30)
        min_train_days: Minimum number of days required for training set (default: 180)

    Examples:
        inv train                           # Default: 30 test days
        inv train --test-days=60            # 60 test days
        inv train --test-days=14            # 14 test days
    """
    check_venv()
    print("#################################################")
    print("############# Training Pipeline #################")
    print("#################################################")
    print(f"Configuration: test_days={test_days}, min_train_days={min_train_days}")

    # Set environment variables for the notebook to read
    env = os.environ.copy()
    env['TEST_DAYS'] = str(test_days)
    env['MIN_TRAIN_DAYS'] = str(min_train_days)

    c.run(uv_run("ipython notebooks/3_air_quality_training_pipeline.ipynb"), env=env)

@task
def inference(c):
    """Batch inference program that reads weather forecast from feature store, then predicts air quality, outputs PNG forecasts."""
    check_venv()
    print("#################################################")
    print("#############  Inference Pipeline ###############")
    print("#################################################")
    c.run(uv_run("ipython notebooks/2_air_quality_feature_pipeline.ipynb"))
    c.run(uv_run("ipython notebooks/4_air_quality_batch_inference.ipynb"))


@task
def llm(c):
    """Uses function calling to answer questions about air quality."""
    check_venv()
    c.run(uv_run("ipython notebooks/5_function_calling.ipynb"))

@task
def test(c):
    """Run all unit tests using pytest."""
    check_venv()
    print("#################################################")
    print("############### Running Tests ###################")
    print("#################################################")
    c.run(uv_run("pytest tests/ -v"))

@task(pre=[backfill, train, features, inference])
def all(c):
    """Run all feature/training/inference pipelines."""
    pass


