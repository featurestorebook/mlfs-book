"""
Air Quality Prediction - Training Pipeline with Lagged Features
================================================================

This script adds lagged PM2.5 features (1, 2, 3 days) to improve model performance.

Run this in Jupyter Notebook or as a Python script.
"""

# %% [markdown]
# # Training Pipeline with Lagged Features
# 
# ## Adding 1-day, 2-day, and 3-day lagged PM2.5 features

# %% Setup environment
import sys
from pathlib import Path
import os

root_dir = Path().absolute()
if root_dir.parts[-1:] == ('airquality',):
    root_dir = Path(*root_dir.parts[:-1])
if root_dir.parts[-1:] == ('notebooks',):
    root_dir = Path(*root_dir.parts[:-1])
root_dir = str(root_dir)

if root_dir not in sys.path:
    sys.path.append(root_dir)
print(f"Root directory: {root_dir}")

from mlfs import config
if os.path.exists(f"{root_dir}/.env"):
    settings = config.HopsworksSettings(_env_file=f"{root_dir}/.env")

# %% Imports
from datetime import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
from sklearn.model_selection import train_test_split
import hopsworks
from mlfs.airquality import util
import json
import warnings
warnings.filterwarnings("ignore")

# %% Connect to Hopsworks
if settings.HOPSWORKS_API_KEY is not None:
    api_key = settings.HOPSWORKS_API_KEY.get_secret_value()
    os.environ['HOPSWORKS_API_KEY'] = api_key
    
project = hopsworks.login()
fs = project.get_feature_store()

secrets = hopsworks.get_secrets_api()
location_str = secrets.get_secret("SENSOR_LOCATION_JSON").value
location = json.loads(location_str)
country = location['country']
city = location['city']
street = location['street']

print(f"✅ Connected to: {city}, {street}")

# %% Load Data
print("\n" + "="*60)
print("STEP 1: Loading Data from Feature Groups")
print("="*60)

air_quality_fg = fs.get_feature_group(name='air_quality', version=1)
weather_fg = fs.get_feature_group(name='weather', version=1)

air_quality_df = air_quality_fg.read()
weather_df = weather_fg.read()

print(f"Air quality records: {len(air_quality_df)}")
print(f"Weather records: {len(weather_df)}")

# %% Add Lagged Features
print("\n" + "="*60)
print("STEP 2: Adding Lagged PM2.5 Features (1, 2, 3 days)")
print("="*60)

air_quality_with_lags = util.add_lagged_features(
    air_quality_df,
    target_column='pm25',
    lags=[1, 2, 3]
)

print(f"\n✅ Lagged features created!")
print(f"New columns: {[col for col in air_quality_with_lags.columns if 'lag_' in col]}")
print(f"\nSample data with lags:")
print(air_quality_with_lags[['date', 'pm25', 'lag_1_pm25', 'lag_2_pm25', 'lag_3_pm25']].head(10))

# %% Merge with Weather Data
print("\n" + "="*60)
print("STEP 3: Merging Air Quality and Weather Data")
print("="*60)

merged_df = pd.merge(
    air_quality_with_lags,
    weather_df,
    on=['date', 'city'],
    how='inner'
)

print(f"Merged data: {len(merged_df)} records")
print(f"Date range: {merged_df['date'].min()} to {merged_df['date'].max()}")

# %% Prepare Features
print("\n" + "="*60)
print("STEP 4: Preparing Training Data")
print("="*60)

# Define feature sets
baseline_features = [
    'temperature_2m_mean',
    'precipitation_sum',
    'wind_speed_10m_max',
    'wind_direction_10m_dominant'
]

enhanced_features = baseline_features + [
    'lag_1_pm25',
    'lag_2_pm25',
    'lag_3_pm25'
]

target = 'pm25'

print(f"Baseline features (4): {baseline_features}")
print(f"Enhanced features (7): {enhanced_features}")

# Prepare data
X_baseline = merged_df[baseline_features]
X_enhanced = merged_df[enhanced_features]
y = merged_df[target]

# Train-test split
X_train_base, X_test_base, y_train, y_test = train_test_split(
    X_baseline, y, test_size=0.2, random_state=42
)

X_train_enh, X_test_enh, _, _ = train_test_split(
    X_enhanced, y, test_size=0.2, random_state=42
)

print(f"\nTrain samples: {len(X_train_base)}")
print(f"Test samples: {len(X_test_base)}")

# %% Train Baseline Model
print("\n" + "="*60)
print("STEP 5: Training BASELINE Model (Weather Only)")
print("="*60)

model_baseline = XGBRegressor(
    n_estimators=100,
    max_depth=5,
    learning_rate=0.1,
    random_state=42
)

model_baseline.fit(X_train_base, y_train)
y_pred_baseline = model_baseline.predict(X_test_base)

mse_baseline = mean_squared_error(y_test, y_pred_baseline)
rmse_baseline = np.sqrt(mse_baseline)
mae_baseline = mean_absolute_error(y_test, y_pred_baseline)
r2_baseline = r2_score(y_test, y_pred_baseline)

print("\n✅ Baseline Model Results:")
print(f"  MSE:  {mse_baseline:.4f}")
print(f"  RMSE: {rmse_baseline:.4f}")
print(f"  MAE:  {mae_baseline:.4f}")
print(f"  R²:   {r2_baseline:.4f}")

# %% Train Enhanced Model
print("\n" + "="*60)
print("STEP 6: Training ENHANCED Model (Weather + Lags)")
print("="*60)

model_enhanced = XGBRegressor(
    n_estimators=100,
    max_depth=5,
    learning_rate=0.1,
    random_state=42
)

model_enhanced.fit(X_train_enh, y_train)
y_pred_enhanced = model_enhanced.predict(X_test_enh)

mse_enhanced = mean_squared_error(y_test, y_pred_enhanced)
rmse_enhanced = np.sqrt(mse_enhanced)
mae_enhanced = mean_absolute_error(y_test, y_pred_enhanced)
r2_enhanced = r2_score(y_test, y_pred_enhanced)

print("\n✅ Enhanced Model Results:")
print(f"  MSE:  {mse_enhanced:.4f}")
print(f"  RMSE: {rmse_enhanced:.4f}")
print(f"  MAE:  {mae_enhanced:.4f}")
print(f"  R²:   {r2_enhanced:.4f}")

# %% Performance Comparison
print("\n" + "="*80)
print("STEP 7: PERFORMANCE COMPARISON")
print("="*80)

comparison = pd.DataFrame({
    'Model': ['Baseline (Weather Only)', 'Enhanced (Weather + Lags)'],
    'Features': [4, 7],
    'MSE': [mse_baseline, mse_enhanced],
    'RMSE': [rmse_baseline, rmse_enhanced],
    'MAE': [mae_baseline, mae_enhanced],
    'R²': [r2_baseline, r2_enhanced]
})

print(comparison.to_string(index=False))
print("="*80)

# Calculate improvements
mse_improve = ((mse_baseline - mse_enhanced) / mse_baseline * 100)
rmse_improve = ((rmse_baseline - rmse_enhanced) / rmse_baseline * 100)
mae_improve = ((mae_baseline - mae_enhanced) / mae_baseline * 100)
r2_improve = ((r2_enhanced - r2_baseline) / abs(r2_baseline) * 100) if r2_baseline != 0 else 0

print("\n📈 PERFORMANCE IMPROVEMENT:")
print(f"  MSE decreased by:  {mse_improve:+.2f}%")
print(f"  RMSE decreased by: {rmse_improve:+.2f}%")
print(f"  MAE decreased by:  {mae_improve:+.2f}%")
print(f"  R² improved by:    {r2_improve:+.2f}%")

if r2_enhanced > r2_baseline:
    print("\n✅ CONCLUSION: Lagged features IMPROVED model performance!")
else:
    print("\n⚠️ CONCLUSION: Lagged features did NOT improve performance.")

# %% Feature Importance
print("\n" + "="*60)
print("STEP 8: Feature Importance Analysis")
print("="*60)

feature_importance = pd.DataFrame({
    'feature': enhanced_features,
    'importance': model_enhanced.feature_importances_
}).sort_values('importance', ascending=False)

print("\nFeature Importance (sorted):")
print(feature_importance.to_string(index=False))

# Plot feature importance
plt.figure(figsize=(10, 6))
plt.barh(feature_importance['feature'], feature_importance['importance'])
plt.xlabel('Importance')
plt.title('Feature Importance - Enhanced Model')
plt.gca().invert_yaxis()
plt.tight_layout()

# Save plot
img_dir = f"{root_dir}/notebooks/airquality/air_quality_model/images"
os.makedirs(img_dir, exist_ok=True)
plt.savefig(f"{img_dir}/feature_importance_with_lags.png")
print(f"\n📊 Feature importance plot saved!")
plt.show()

# %% Visualize Predictions
print("\n" + "="*60)
print("STEP 9: Visualizing Predictions")
print("="*60)

fig, axes = plt.subplots(2, 2, figsize=(15, 12))

# Baseline - Scatter
axes[0, 0].scatter(y_test, y_pred_baseline, alpha=0.5)
axes[0, 0].plot([0, y_test.max()], [0, y_test.max()], 'r--')
axes[0, 0].set_xlabel('Actual PM2.5')
axes[0, 0].set_ylabel('Predicted PM2.5')
axes[0, 0].set_title(f'Baseline (R²={r2_baseline:.4f})')
axes[0, 0].grid(True, alpha=0.3)

# Enhanced - Scatter
axes[0, 1].scatter(y_test, y_pred_enhanced, alpha=0.5, color='green')
axes[0, 1].plot([0, y_test.max()], [0, y_test.max()], 'r--')
axes[0, 1].set_xlabel('Actual PM2.5')
axes[0, 1].set_ylabel('Predicted PM2.5')
axes[0, 1].set_title(f'Enhanced (R²={r2_enhanced:.4f})')
axes[0, 1].grid(True, alpha=0.3)

# Residuals
residuals_base = y_test - y_pred_baseline
residuals_enh = y_test - y_pred_enhanced

axes[1, 0].scatter(y_pred_baseline, residuals_base, alpha=0.5)
axes[1, 0].axhline(y=0, color='r', linestyle='--')
axes[1, 0].set_xlabel('Predicted PM2.5')
axes[1, 0].set_ylabel('Residuals')
axes[1, 0].set_title('Baseline - Residuals')
axes[1, 0].grid(True, alpha=0.3)

axes[1, 1].scatter(y_pred_enhanced, residuals_enh, alpha=0.5, color='green')
axes[1, 1].axhline(y=0, color='r', linestyle='--')
axes[1, 1].set_xlabel('Predicted PM2.5')
axes[1, 1].set_ylabel('Residuals')
axes[1, 1].set_title('Enhanced - Residuals')
axes[1, 1].grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig(f"{img_dir}/model_comparison.png")
print("📊 Comparison plots saved!")
plt.show()

# %% Save Model
print("\n" + "="*60)
print("STEP 10: Saving Enhanced Model")
print("="*60)

model_dir = f"{root_dir}/notebooks/airquality/air_quality_model_with_lags"
os.makedirs(model_dir, exist_ok=True)

model_enhanced.save_model(f"{model_dir}/model.json")

metadata = {
    'model_type': 'XGBoost Regressor',
    'features': enhanced_features,
    'n_features': len(enhanced_features),
    'performance': {
        'mse': float(mse_enhanced),
        'rmse': float(rmse_enhanced),
        'mae': float(mae_enhanced),
        'r2': float(r2_enhanced)
    },
    'training_date': str(datetime.now()),
    'comparison': {
        'baseline_r2': float(r2_baseline),
        'enhanced_r2': float(r2_enhanced),
        'improvement_percent': float(r2_improve)
    }
}

with open(f"{model_dir}/metadata.json", 'w') as f:
    json.dump(metadata, f, indent=2)

print(f"✅ Model saved to: {model_dir}")

# %% Generate Report
print("\n" + "="*80)
print("FINAL SUMMARY REPORT")
print("="*80)

report = f"""
AIR QUALITY PREDICTION - LAGGED FEATURES EXPERIMENT
{'='*80}

LOCATION: {city}, {street}
DATE: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

{'='*80}
MODEL COMPARISON
{'='*80}

Baseline Model (Weather Only - 4 features):
  MSE:  {mse_baseline:.4f}
  RMSE: {rmse_baseline:.4f}
  MAE:  {mae_baseline:.4f}
  R²:   {r2_baseline:.4f}

Enhanced Model (Weather + Lags - 7 features):
  MSE:  {mse_enhanced:.4f}
  RMSE: {rmse_enhanced:.4f}
  MAE:  {mae_enhanced:.4f}
  R²:   {r2_enhanced:.4f}

{'='*80}
IMPROVEMENT
{'='*80}
MSE:  {mse_improve:+.2f}%
RMSE: {rmse_improve:+.2f}%
MAE:  {mae_improve:+.2f}%
R²:   {r2_improve:+.2f}%

{'='*80}
TOP 3 FEATURES
{'='*80}
"""

for idx, row in feature_importance.head(3).iterrows():
    report += f"{row['feature']:30s} {row['importance']:.4f}\n"

report += f"\n{'='*80}\n"

if r2_enhanced > r2_baseline:
    report += """✅ CONCLUSION: Adding lagged PM2.5 features IMPROVED the model!

The lagged features capture temporal patterns in air quality, allowing the
model to use yesterday's and recent days' pollution levels as predictors.
This is especially useful because air quality tends to persist over days.

RECOMMENDATION: Use the enhanced model for production deployment.
"""
else:
    report += """⚠️ CONCLUSION: Lagged features did NOT improve performance.

Possible reasons:
1. Overfitting on training data
2. Data leakage issues
3. High multicollinearity

RECOMMENDATION: Further investigation needed. Consider:
- Cross-validation
- Feature selection
- Different lag periods
"""

print(report)

# Save report
with open(f"{model_dir}/performance_report.txt", 'w') as f:
    f.write(report)

print(f"\n✅ Report saved to: {model_dir}/performance_report.txt")

print("\n" + "="*80)
print("✅ EXPERIMENT COMPLETED!")
print("="*80)
print("\nGenerated files:")
print(f"  1. {model_dir}/model.json")
print(f"  2. {model_dir}/metadata.json")
print(f"  3. {model_dir}/performance_report.txt")
print(f"  4. {img_dir}/feature_importance_with_lags.png")
print(f"  5. {img_dir}/model_comparison.png")
print("\n" + "="*80)

