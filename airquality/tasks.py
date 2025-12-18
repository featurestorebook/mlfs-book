from invoke import task
import os
import sys
from pathlib import Path

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
# Air Quality Batch ML System
##########################################

@task
def aq_clean(c):
    check_venv()
    with c.cd(".."):
        c.run("uv run python mlfs/clean_hopsworks_resources.py aq")

@task
def aq_backfill(c):
    check_venv()
    c.run("uv run ipython notebooks/1_air_quality_feature_backfill.ipynb")

@task
def aq_features(c):
    check_venv()
    c.run("uv run ipython notebooks/2_air_quality_feature_pipeline.ipynb")

@task
def aq_train(c):
    check_venv()
    c.run("uv run ipython notebooks/3_air_quality_training_pipeline.ipynb")

@task
def aq_inference(c):
    check_venv()
    c.run("uv run ipython notebooks/2_air_quality_feature_pipeline.ipynb")
    c.run("uv run ipython notebooks/4_air_quality_batch_inference.ipynb")


@task
def aq_llm(c):
    check_venv()
    c.run("uv run ipython notebooks/5_function_calling.ipynb")

@task(pre=[aq_backfill, aq_features, aq_train, aq_inference])
def aq_all(c):
    pass


