### O'Reilly book - Building Machine Learning Systems with a feature store: batch, real-time, and LLMs
Author: Jim Dowling

## Introduction to ML
If you don't understand what a feature is, read this [brief introduction to ML](./introduction_to_supervised_ml.pdf) that didn't make it into the book. 

## ML System Examples

These example ML systems from the book have been tested on Ubuntu, Apple, and Windows Subsystem for Linux (WSL) with Ubuntu.
Invoke is a Python-native alternative to Make.

The example ML systems have the same steps to run them:

    source setup-env.sh 
    inv [backfill | features | train | inference | all]

 * `source setup-env.sh' installs a .venv in the directory and installs dependencies from a local requirements.txt file with `uv`. For the credit-card system, it also installs Feldera using Docker.
 * `inv [backfill | features | train | inference | all]` to `invoke` to the ML pipelines.

# Run Titanic Example 

    cd titanic
    source set-env.sh
    inv all

# Run Air Quality Tutorial

    cd airquality
    source set-env.sh
    inv all

# Run Air Quality Tutorial

    cd airquality
    source set-env.sh
    inv all


See [tutorial instructions here](https://docs.google.com/document/d/1YXfM1_rpo1-jM-lYyb1HpbV9EJPN6i1u6h2rhdPduNE/edit?usp=sharing)
    
# Dashboards

[Dashboards for Example ML Systems](https://featurestorebook.github.io/mlfs-book/)

