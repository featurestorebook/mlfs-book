import os
import numpy as np
import hopsworks
import joblib


class Predict(object):

    def __init__(self):
        # Get feature store handle
        project = hopsworks.login()
        self.mr = project.get_model_registry()

        # Retrieve the feature view from the model
        retrieved_model = self.mr.get_best_model(name="cc_fraud_catboost_model", metric="f1_score", direction="max")
        self.feature_view = retrieved_model.get_feature_view()

        # Load the trained CatBoost model
        self.model = joblib.load(os.environ["MODEL_FILES_PATH"] + "/cc_fraud_catboost.pkl")
        print("Initialization Complete")

    def predict(self, inputs):
        cc_num = inputs[0][0]
        amount = inputs[0][1]
        merchant_id = inputs[0][2]
        ip_address = inputs[0][3]
        card_present = inputs[0][4]

        feature_vector = self.feature_view.get_feature_vector(entry={"cc_num": cc_num},
                                                              parameters={"merchant_id": merchant_id,
                                                                          "amount": amount,
                                                                          "ip_address": ip_address,
                                                                          "card_present": card_present
                                                                         }
                                                             )
        return self.model.predict(np.asarray(feature_vector).reshape(1, -1)).tolist() # Numpy Arrays are not JSON serializable
