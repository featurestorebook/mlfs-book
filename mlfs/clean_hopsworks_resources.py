import hopsworks
import sys

files_to_clean=""
if len(sys.argv) != 2:
    print("Usage: <prog> project_to_clean (e.g., cc or aq or titanic)")
    sys.exit(1)

files_to_clean = sys.argv[1]

print(f"Cleaning project: {files_to_clean}")

project = hopsworks.login(engine="python") 

# Get feature store, deployment registry, model registry
fs = project.get_feature_store()
ms = project.get_model_serving()
mr = project.get_model_registry()
kafka_api = project.get_kafka_api()

def delete_deployment(deployment_name):
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

def delete_model(model_name):
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

def delete_feature_view(feature_view):
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

def delete_feature_group(feature_group):
    # Get all feature groups
    try:
        feature_groups = fs.get_feature_groups(name=feature_group)
    except:
        print(f"Couldn't find feature group: {feature_group}. Skipping...")
        feature_groups = []

    # Delete each feature group
    for fg in feature_groups:
        print(f"Deleting feature group: {fg.name} (version: {fg.version})")
        try:
            fg.delete()
        except:
            print(f"Failed to delete feature group {fv.name}.")

    try:
        kafka_topics = kafka_api.get_topics()
        for topic in kafka_topics:
            if topic.name == feature_group:
                topic.delete()
                print(f"Deleting kafka topic {feature_group}")
    except:
        print(f"Couldn't find any kafka topics. Skipping...")

    try:
        schema = kafka_api.get_schema(feature_group, 1)
        if schema is not None:
            schema.delete()
    except:
        print(f"Couldn't find kafka schema: {feature_group}. Skipping...")


if files_to_clean == "cc":

    # Delete all deployments
    for deployment_name in [
        "",
    ]:
        delete_deployment(deployment_name)
    # List all models
    for model_name in [
        "",
    ]:
        delete_model(model_name)
    
    
    for feature_view in [
        #"",
    ]:
        delete_feature_view(feature_view)
    
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
        "cc_trans_aggs_fg"
    ]:
        delete_feature_group(feature_group)

    KAFKA_TOPIC_NAME = f"{project.name}_real_time_live_transactions"
    SCHEMA_NAME = "live_transactions_schema"
    try:
        kafka_topics = kafka_api.get_topics()
        for topic in kafka_topics:
            if topic.name == KAFKA_TOPIC_NAME:
                topic.delete()
    except:
        print(f"Couldn't find kafka topic: {KAFKA_TOPIC_NAME}. Skipping...")

    try:
        schema = kafka_api.get_schema(SCHEMA_NAME, 1)
        if schema is not None:
            schema.delete()
            print(f"Deleted kafka schema {SCHEMA_NAME}")
    except:
        print(f"Couldn't find kafka schema: {SCHEMA_NAME}. Skipping...")


elif files_to_clean == "aq":
    delete_model("air_quality_xgboost_model")    
    delete_feature_view("air_quality_fv")
    for feature_group in [
        "air_quality",
        "weather",
        "air_quality_fv_1_logging_transformed",
        "air_quality_fv_1_logging_untransformed",
        "aq_predictions"
    ]:
        delete_feature_group(feature_group)

elif files_to_clean == "titanic":
    delete_model("titanic")
    delete_feature_view("titanic")
    delete_feature_group("titanic")

else:
    print(f"Couldn't find target to clean files for: {files_to_clean}. Valid options include 'cc', 'aq', and 'titanic'")
