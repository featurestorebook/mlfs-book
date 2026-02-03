import os
import numpy as np
import pandas as pd
import hopsworks
import joblib
from datetime import datetime


class Predict(object):

    def __init__(self, async_logger):
        # Get feature store handle
        project = hopsworks.login()
        self.mr = project.get_model_registry()

        # Retrieve the feature view from the model
        retrieved_model = self.mr.get_best_model(name="cc_fraud_xgboost_model", metric="f1_score", direction="max")
        self.feature_view = retrieved_model.get_feature_view()
        self.feature_view.init_feature_logger(async_logger)

        # Load the unified pipeline (preprocessor + XGBoost model)
        self.pipeline = joblib.load(os.environ["MODEL_FILES_PATH"] + "/cc_fraud_pipeline.pkl")
        print("Initialization Complete - Loaded unified pipeline (preprocessor + model)")

    def predict(self, inputs):
        cc_num = inputs[0][0]
        amount = inputs[0][1]
        merchant_id = inputs[0][2]
        ip_address = inputs[0][3]
        card_present = inputs[0][4]
        t_id = inputs[0][5]

        # Get raw feature vector from feature view
        # entry contains serving keys to look up features from online store
        # passed_features provides request-time feature values
        feature_df = self.feature_view.get_feature_vector(
            entry={"cc_num": cc_num, "merchant_id": merchant_id},  # , "aggs_cc_num": cc_num
            request_parameters={
                "t_id": t_id,
                "amount": amount,
                "ip_address": ip_address,
                "card_present": card_present
            },
            return_type="pandas",
            allow_missing=True,
        )

        # Pipeline handles preprocessing (imputation + encoding) and prediction
        predictions = self.pipeline.predict(feature_df)

        predictions = [bool(prediction) for prediction in predictions]

        # Log predictions with feature vector
        self.feature_view.log(feature_df, 
                              predictions=predictions,
                              serving_keys=[{"cc_num": cc_num, "merchant_id": merchant_id}],
                              event_time=[datetime.now()])

        return predictions