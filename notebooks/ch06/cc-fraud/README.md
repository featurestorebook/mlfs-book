# Hopsworks real-time credit card fraud detection tutorial

## Introduction
In this guide we will demonstrate how to build a real-time machine learning system detecting credit card fraud.

The guide is divided into the following sections:
- **Data Generation**: Generate a sample of credit card user profiles and transactions
- **Feature Pipeline**: Compute the features and create the feature groups
- **Training Pipeline**: Assemble the training dataset, train and validate the model
- **Inference Pipeline**: Deploy the model and make requests

### Prerequisites
Before running the different notebooks, make sure you have installed the following Python libraries:

```
pip install hopsworks==3.7.0
pip install bytewax==0.19.1
pip install faker==24.4.0
```

### Data Generation

Run the `data_generation/generate_historical_data.ipynb` notebook to generate 2 CSV files:
- `historical_transactions.csv`: Containing historical transactions
- `kyc.csv`: Containing informations about the different accounts

You can save the above csv files in Hopsworks or S3.

<span style="background-color: #FFFF00">Please note that you will have to adjust the location of the CSV files in the feature pipelines notebooks</span>

### Feature Pipeline

The feature pipeline directory contains the code to compute the different feature groups and populate the feature store.
While the first run is static, you can use the data generator above to generate new data and update the feature groups (in real-time)

I suggest you execute the notebooks in the following order:

#### Profiles
Uses the data in the `kyc.csv` file to create the necessary profiles features.

#### Transactions
Uses the data in the `historical_transactions.csv` file to create the necessary transactions features.

#### Profiles Last Transactions
Uses the data in the `historical_transactions.csv` file to compute the time and location of the last transaction for each account

#### Profiles Activity 5M
This is a streaming application that computes averages of transaction amounts for each account on a 5 minutes window.
This feature group has 3 parts:
- `profiles_activity_5m_setup.ipynb`: This creates the scaffolding for the feature group
- `transaction_stream_simulation.ipynb`: This noteook creates a Kafka topic on Hopsworks and simulates a stream of transaction from credit cards. It starts by replaying the old transactions to compute the correct state of each account.
- `bytewax_profile_activity_5m.py`: This code leverages [bytewax](https://docs.bytewax.io/stable/guide/index.html) to do stateful streaming computation without requiring any Spark Streaming application

For the `bytewax_profile_activity_5m.py`, you should populate the follwoing part in the code:
```python
project = hopsworks.login(
    host="",
    project="",
    api_key_value="",
)
```

You can run the Bytewax streaming application by running the following Python command:

```bash
python -m bytewax.run "bytewax_profile_activity_5min:get_flow"
```

<span style="background-color: #FFFF00">Please note that you will have to schedule (or trigger manually) the job named: profiles_activity_5m_1_offline_fg_materialization for the feature group data to be available in the offline feature store for training</span>

### Training Pipeline
The training pipeline contains the code to join the different features together, generate a training dataset, train a model and registered with the feature store.
- `fraud_model_fv.ipynb`: This notebook joins the necessary features together and register the feature view. It also generates a training dataset split by time.
- `model_training,ipynb`: This notebook trains the model on the training dataset and registers the model with the Hopsworks model registry.

### Inference Pipeline

This creates a KServe based deployment on Hopsworks. It deploys the model and provide and endpoint where you can send predictions. While you run your own KServe deployment, you can extract the code to fetch the data from the online feature store and compute the on-demand features.

Additionally the `inference_debug.ipynb` notebook contains only the part retrieving the online feature data and computing the on-demand features.



### Why not Poetry?
Poetry is too slow for many large production projects. It can take hundreds of seconds for a `poetry lock --no-update` command to finish.
Poetry is too strict when you have unresolvable dependency conflicts. It can happen that you have to override conflicting dependencies, particularly in large projects. If any downstream dependency inadvertently specificies pandas==2.1.0, any dependency that specifies a different version of Pandas will cause a conflict, such as pandas > 2.1.0. Sometimes people make mistakes when defining dependencies, and you need to be able to work around those mistakes.


### Pipeline and AI App Testing

This will typically be built into a docker image and deployed. This is not a Python library that may be used by many other Python programs, so we should strictly pin all dependency versions. You should create a lockfile to ensure that the code you write and test in dev is identical to the code you run in prod.
We validate the lockfile for every PR to main, ensuring that no new packages have been added to the pyproject.toml, making the lockfile out of date.
