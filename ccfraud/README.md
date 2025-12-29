## Credit Card Fraud Predcition

This project builds a Real-Time Credit Card Fraud Prediction Service using synthetic data.


## How to run Instructions

    source set-env.sh
    inv backfill
    inv feldera-start  # Runs feldera in a Docker container in the foreground
    inv feldera
    inv features
    inv train
    