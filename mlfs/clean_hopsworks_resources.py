import hopsworks
from config import settings

project = hopsworks.login() # api_key_value=settings.HOPSWORKS_API_KEY.get_secret_value()

# Get feature store, deployment registry, model registry
fs = project.get_feature_store()
ms = project.get_model_serving()
mr = project.get_model_registry()


# Delete all deployments
for deployment_name in [
    "",
    "",
    "",
    "",
]:
    try:
        deployment = ms.get_deployment(name=deployment_name)
        print(f"Deleting deployment: {deployment.name}")
        deployment.stop()
        try:
            deployment.delete()
        except Exception:
            print(f"Problem deleting deployment: {deployment_name}.")
    except Exception:
        print("No deployments to delete.")

# List all models
for model_name in [
    "",
    "",
    "",
    "",
]:
    try:
        models = mr.get_models(name=model_name)
        for model in models:
            print(f"Deleting model: {model.name} (version: {model.version})")
            try:
                model.delete()
            except Exception:
                print(f"Failed to delete model {model_name}.")
    except Exception:
        print("No  models to delete.")


for feature_view in [
    "",
    "",
    "",
    "",
    "",
]:
    # Get all feature views
    try:
        feature_views = fs.get_feature_views(name=feature_view)
    except:
        print(f"Couldn't find feature view: {feature_view}. Skipping...")
        feature_views = []

    # Delete each feature view
    for fv in feature_views:
        print(f"Deleting feature view: {fv.name} (version: {fv.version})")
        try:
            fv.delete()
        except Exception:
            print(f"Failed to delete feature view {fv.name}.")

for feature_group in [
    "account_details",
    "bank_details",
    "merchant_details",
    "credit_card_transactions",
    "card_details",
    "cc_fraud",
    "cc_trans_aggs_fg",
    "cc_trans_fg",
    "merchant_fg",
    "account_fg",
    "bank_fg",
]:
    # Get all feature groups
    try:
        feature_groups = fs.get_feature_groups(name=feature_group)
    except:
        print(f"Couldn't find feature group: {feature_view}. Skipping...")
        feature_groups = []

    # Delete each feature group
    for fg in feature_groups:
        print(f"Deleting feature group: {fg.name} (version: {fg.version})")
        try:
            fg.delete()
        except:
            print(f"Failed to delete feature group {fv.name}.")
