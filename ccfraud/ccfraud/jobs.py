#!/usr/bin/env python3
"""
Create, run and stop the Hopsworks jobs of the credit card fraud streaming system.

  datamart       ccfraud-datamart         PYTHON   1_data_generator.py --mode backfill
                 merchants, banks, accounts, cards and the transaction history (runs to completion)
  features       ccfraud-features         PYTHON   3-batch-feature-pipeline.py
                 per-transaction features into cc_trans_fg (runs to completion)
  train          ccfraud-train            PYTHON   run_notebook.py <training notebook>
                 trains, registers and deploys the model (runs to completion)
  transactions   ccfraud-transactions     PYTHON   1b-transaction-generator-job.py
                 writes N transactions/min to credit_card_transactions (runs until stopped)
  backfill-aggs  ccfraud-backfill-aggs    PYSPARK  2-spark-streaming-feature-pipeline.py --mode backfill
                 sliding-window aggregates over the transaction history (runs to completion)
  streaming-aggs ccfraud-streaming-aggs   PYSPARK  2-spark-streaming-feature-pipeline.py --mode stream
                 Spark Structured Streaming job writing cc_trans_aggs_fg (runs until stopped)

Inside Hopsworks the repo is on HopsFS, so a job runs the repo's script in place.
Outside Hopsworks the scripts (and the ccfraud package they import) are uploaded to
Resources/mlfs-book first, keeping the repo layout.

Usage:
    python ccfraud/jobs.py start transactions --args "--transactions-per-min 100"
    python ccfraud/jobs.py start backfill-aggs --wait
    python ccfraud/jobs.py start streaming-aggs
    python ccfraud/jobs.py stop transactions streaming-aggs
    python ccfraud/jobs.py status
"""

import argparse
import sys
from pathlib import Path

import hopsworks

ccfraud_pkg_dir = Path(__file__).absolute().parent  # ccfraud/ccfraud/
ccfraud_project_dir = ccfraud_pkg_dir.parent  # ccfraud/
root_dir = ccfraud_project_dir.parent  # mlfs-book/

JOBS = {
    "datamart": {
        "name": "ccfraud-datamart",
        "type": "PYTHON",
        "script": "1_data_generator.py",
        "args": "--mode backfill",
        # Faker is not in python-feature-pipeline: clone it and install requirements-jobs.txt
        "environment": "ccfraud-pipeline",
        "memory": 8192,
    },
    "features": {
        "name": "ccfraud-features",
        "type": "PYTHON",
        "script": "3-batch-feature-pipeline.py",
        "args": "--wait",
        "environment": "ccfraud-pipeline",
        "memory": 8192,
    },
    "train": {
        "name": "ccfraud-train",
        "type": "PYTHON",
        "script": "run_notebook.py",
        "args": "notebooks/4-training-cc-fraud-pipeline.ipynb",
        "environment": "ccfraud-pipeline",
        "memory": 8192,
    },
    "transactions": {
        "name": "ccfraud-transactions",
        "type": "PYTHON",
        "script": "1b-transaction-generator-job.py",
        "args": "--transactions-per-min 100",
        "environment": "python-feature-pipeline",
    },
    "backfill-aggs": {
        "name": "ccfraud-backfill-aggs",
        "type": "PYSPARK",
        "script": "2-spark-streaming-feature-pipeline.py",
        "args": "--mode backfill",
        "environment": "spark-feature-pipeline",
    },
    "streaming-aggs": {
        "name": "ccfraud-streaming-aggs",
        "type": "PYSPARK",
        "script": "2-spark-streaming-feature-pipeline.py",
        "args": "--mode stream",
        "environment": "spark-feature-pipeline",
    },
}

# Files a job needs when they have to be uploaded (paths relative to the repo root)
UPLOAD_FILES = [
    "ccfraud/ccfraud/__init__.py",
    "ccfraud/ccfraud/synth_transactions.py",
    "ccfraud/ccfraud/1_data_generator.py",
    "ccfraud/ccfraud/3-batch-feature-pipeline.py",
    "ccfraud/ccfraud/features/__init__.py",
    "ccfraud/ccfraud/features/cc_trans_fg.py",
    "ccfraud/ccfraud/run_notebook.py",
    "ccfraud/notebooks/4-training-cc-fraud-pipeline.ipynb",
    "ccfraud/notebooks/4b-training-nn-fraud-model.ipynb",
    "ccfraud/notebooks/ccfraud-predictor.py",
    "ccfraud/notebooks/ccfraud-nn-predictor.py",
    "ccfraud/requirements.txt",
    "ccfraud/ccfraud/1b-transaction-generator-job.py",
    "ccfraud/ccfraud/2-spark-streaming-feature-pipeline.py",
]
UPLOAD_ROOT = "Resources/mlfs-book"

RUNNING_STATES = {"INITIALIZING", "RUNNING", "ACCEPTED", "NEW", "NEW_SAVING", "SUBMITTED",
                  "STARTING_APP_MASTER", "GENERATING_SECURITY_MATERIAL"}


def _login():
    env_file = root_dir / ".env"
    if env_file.exists():
        sys.path.insert(0, str(root_dir))
        from mlfs import config
        config.HopsworksSettings(_env_file=str(env_file))
    return hopsworks.login()


def _app_path(project, script: str) -> str:
    """HopsFS path of the script: the repo file itself when the repo lives on HopsFS."""
    local = ccfraud_pkg_dir / script
    if str(local).startswith("/hopsfs/"):
        return f"hdfs:///Projects/{project.name}/{str(local).removeprefix('/hopsfs/')}"
    dataset_api = project.get_dataset_api()
    for rel in UPLOAD_FILES:
        target_dir = f"{UPLOAD_ROOT}/{Path(rel).parent.as_posix()}"
        if not dataset_api.exists(target_dir):
            dataset_api.mkdir(target_dir)
        dataset_api.upload(str(root_dir / rel), target_dir, overwrite=True)
    return f"hdfs:///Projects/{project.name}/{UPLOAD_ROOT}/ccfraud/ccfraud/{script}"


def _running_executions(job):
    return [e for e in job.get_executions() if e.state in RUNNING_STATES]


def start(project, key: str, args: str | None, wait: bool):
    spec = JOBS[key]
    job_api = project.get_job_api()
    job = job_api.get_job(spec["name"])
    if job is not None and _running_executions(job):
        print(f"{spec['name']} is already running")
        return
    config = job.config if job is not None else job_api.get_configuration(spec["type"])
    config["appPath"] = _app_path(project, spec["script"])
    config["environmentName"] = spec["environment"]
    config["defaultArgs"] = args if args is not None else spec["args"]
    if "memory" in spec:
        config["resourceConfig"]["memory"] = spec["memory"]
    job = job_api.create_job(spec["name"], config)
    print(f"Starting {spec['name']}: {config['appPath']} {config['defaultArgs']}")
    execution = job.run(await_termination=wait)
    if wait and not execution.success:
        sys.exit(f"{spec['name']} finished in state {execution.state}")


def stop(project, key: str):
    job = project.get_job_api().get_job(JOBS[key]["name"])
    if job is None:
        print(f"{JOBS[key]['name']} does not exist")
        return
    running = _running_executions(job)
    for execution in running:
        execution.stop()
    print(f"Stopped {len(running)} execution(s) of {job.name}")


def status(project):
    job_api = project.get_job_api()
    for spec in JOBS.values():
        job = job_api.get_job(spec["name"])
        if job is None:
            print(f"{spec['name']:28s} not created")
            continue
        executions = job.get_executions()
        latest = max(executions, key=lambda e: e.id).state if executions else "never run"
        print(f"{spec['name']:28s} {latest}")


def main(argv=None):
    parser = argparse.ArgumentParser(description="Manage the ccfraud streaming jobs in Hopsworks")
    sub = parser.add_subparsers(dest="command", required=True)
    p_start = sub.add_parser("start", help="Create/update a job and run it")
    p_start.add_argument("job", choices=JOBS)
    p_start.add_argument("--args", default=None, help="Job arguments (default: the job's default arguments)")
    p_start.add_argument("--wait", action="store_true", help="Wait for the execution to finish")
    p_stop = sub.add_parser("stop", help="Stop the running executions of jobs")
    p_stop.add_argument("jobs", nargs="+", choices=JOBS)
    sub.add_parser("status", help="Show the latest execution state of each job")
    args = parser.parse_args(argv)

    project = _login()
    if args.command == "start":
        start(project, args.job, args.args, args.wait)
    elif args.command == "stop":
        for key in args.jobs:
            stop(project, key)
    else:
        status(project)


if __name__ == "__main__":
    main()
