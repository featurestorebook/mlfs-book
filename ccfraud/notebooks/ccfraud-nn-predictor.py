import os
import numpy as np
import pandas as pd
import hopsworks
import joblib
import torch
import torch.nn as nn
from datetime import datetime


class FraudDetectorNN(nn.Module):
    """Neural network architecture for fraud detection."""

    def __init__(self, input_dim, dropout_rate=0.3):
        super().__init__()
        self.network = nn.Sequential(
            nn.Linear(input_dim, 15000),
            nn.ReLU(),
            nn.Dropout(dropout_rate),
            nn.Linear(15000, 10000),
            nn.ReLU(),
            nn.Dropout(dropout_rate),
            nn.Linear(10000, 5000),
            nn.ReLU(),
            nn.Dropout(dropout_rate),
            nn.Linear(5000, 1)
        )

    def forward(self, x):
        return self.network(x)


class Predict(object):

    def __init__(self):
        # Get feature store handle
        project = hopsworks.login()
        self.mr = project.get_model_registry()

        # Retrieve the feature view from the model
        retrieved_model = self.mr.get_best_model(name="cc_fraud_nn_model", metric="f1_score", direction="max")
        self.feature_view = retrieved_model.get_feature_view()

        # Set device (CPU for inference - avoid GPU memory issues in serving)
        self.device = torch.device('cpu')

        # Load model checkpoint
        # weights_only=False is required for PyTorch 2.6+ when loading checkpoints
        # that contain numpy scalars (like class_weight_ratio)
        model_files_path = os.environ["MODEL_FILES_PATH"]
        checkpoint = torch.load(
            os.path.join(model_files_path, "cc_fraud_nn_model.pt"),
            map_location=self.device,
            weights_only=False
        )

        # Initialize and load PyTorch model
        self.model = FraudDetectorNN(
            input_dim=checkpoint['input_dim'],
            dropout_rate=checkpoint['dropout_rate']
        ).to(self.device)
        self.model.load_state_dict(checkpoint['model_state_dict'])
        self.model.eval()

        # Load imputer for handling missing values
        self.imputer = joblib.load(os.path.join(model_files_path, "imputer.pkl"))

        # Load feature names to ensure correct column ordering
        self.feature_names = joblib.load(os.path.join(model_files_path, "feature_names.pkl"))

        print("Initialization Complete - Loaded PyTorch Neural Network model")

    def predict(self, inputs):
        cc_num = inputs[0][0]
        amount = inputs[0][1]
        merchant_id = inputs[0][2]
        ip_address = inputs[0][3]
        card_present = inputs[0][4]
        t_id = inputs[0][5]

        # Get feature vector from feature view
        # entry contains serving keys to look up features from online store
        # passed_features provides request-time feature values
        feature_df = self.feature_view.get_feature_vector(
            entry={"cc_num": cc_num, "merchant_merchant_id": merchant_id},
            request_parameters={
                "t_id": t_id,
                "amount": amount,
                "ip_address": ip_address,
                "card_present": card_present
            },
            return_type="pandas",
            allow_missing=True,
        )

        # Ensure correct column ordering
        feature_df = feature_df[self.feature_names]

        # Impute missing values
        X = self.imputer.transform(feature_df)
        X_tensor = torch.FloatTensor(X).to(self.device)

        # Get predictions
        with torch.no_grad():
            probs = torch.sigmoid(self.model(X_tensor)).cpu().numpy().squeeze()

        # Convert to boolean predictions (threshold 0.5)
        if probs.ndim == 0:
            probs = np.array([probs])
        predictions = [bool(prob >= 0.5) for prob in probs]

        return predictions
