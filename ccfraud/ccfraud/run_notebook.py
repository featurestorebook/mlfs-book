#!/usr/bin/env python3
"""
Execute a ccfraud notebook with papermill, as a Hopsworks job.

The notebook runs with the ccfraud project dir as its working directory (as `inv train`
does), and the executed copy, with its outputs, is written next to the notebook as
<name>.output.ipynb, so the source notebook is left unchanged.

    python ccfraud/run_notebook.py notebooks/4-training-cc-fraud-pipeline.ipynb -p test_start "2026-09-30 00:00"
"""

import argparse
from pathlib import Path

import papermill as pm

ccfraud_project_dir = Path(__file__).absolute().parent.parent  # ccfraud/


def main(argv=None):
    parser = argparse.ArgumentParser(description="Execute a notebook with papermill")
    parser.add_argument("notebook", help="Notebook path, relative to the ccfraud project dir")
    parser.add_argument("-p", "--parameter", nargs=2, action="append", default=[], metavar=("NAME", "VALUE"),
                        help="Notebook parameter (repeatable)")
    args = parser.parse_args(argv)

    notebook = ccfraud_project_dir / args.notebook
    output = notebook.with_suffix(".output.ipynb")
    print(f"Executing {notebook} -> {output} with {dict(args.parameter)}")
    pm.execute_notebook(
        str(notebook),
        str(output),
        parameters=dict(args.parameter),
        cwd=str(ccfraud_project_dir),
        kernel_name="python3",
        log_output=True,
    )


if __name__ == "__main__":
    main()
