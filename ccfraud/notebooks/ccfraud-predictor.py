import os
import numpy as np
import pandas as pd
import hopsworks
import joblib


class Predict(object):

    def __init__(self):
        # Get feature store handle
        project = hopsworks.login()
        self.mr = project.get_model_registry()

        # Retrieve the feature view from the model
        retrieved_model = self.mr.get_best_model(name="cc_fraud_xgboost_model", metric="f1_score", direction="max")
        self.feature_view = retrieved_model.get_feature_view()

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
            request_params={
                "t_id": t_id,
                "amount": amount,
                "ip_address": ip_address,
                "card_present": card_present
            },
            return_type = "pandas"
        )

        # untransformed_vector = fv.get_feature_vector({"id": 1}, transform=False)
        # transformed_vector = fv.transform(untransformed_vector)
        # feature_view.log(untransformed_vector)
        # feature_view.log(transformed_features=transformed_vector)untransformed_vector = fv.get_feature_vector({"id": 1}, transform=False)
        # transformed_vector = fv.transform(untransformed_vector)
        # feature_view.log(untransformed_vector)
        # feature_view.log(transformed_features=transformed_vector)        

        # feature_names = [f.name for f in self.feature_view.features]
        # feature_df = pd.DataFrame([feature_vector], columns=feature_names)

        # Pipeline handles preprocessing (imputation + encoding) and prediction
        return self.pipeline.predict(feature_df).tolist()
