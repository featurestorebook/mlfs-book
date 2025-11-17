#        conda create -n mlfs python==3.11	
#        conda activate mlsfs
#	conda install twofish clang -y

# include .env
# export $(shell sed 's/=.*//' .env)
	
check-venv:
	@if [ -n "$$CONDA_DEFAULT_ENV" ]; then \
		echo "You are in a Conda environment: $$CONDA_DEFAULT_ENV"; \
	elif python -c 'import sys; exit(0 if hasattr(sys, "real_prefix") or (hasattr(sys, "base_prefix") and sys.base_prefix != sys.prefix) else 1)'; then \
		echo "You are running in a Python virtual environment."; \
	else \
		echo "No virtual environment or Conda environment detected. Please create and activate one first, then re-'make install'"; \
		exit 1; \
	fi 

install: check-venv
	pip install -r requirements.txt

install-recommender: check-venv
	pip install -r requirements-llm.txt

cc-start-ui:
	python -m streamlit streamlit_app.py
 

cc-clean:
	python mlfs/clean_hopsworks_resources.py cc

cc-datagen:
	ipython notebooks/ccfraud/0-data-generation-with-polars.ipynb

cc-gen-kafka:
	ipython notebooks/ccfraud/transactions_synthetic_kafka_generator.ipynb

cc-features:
	ipython notebooks/ccfraud/1-batch-polars-feature-pipeline.ipynb

cc-streaming-features:
	ipython notebooks/ccfraud/1-streaming-feature-pipeline-feldera.ipynb

cc-train:
	ipython notebooks/ccfraud/

cc-deploy:
	ipython notebooks/ccfraud/

cc-all: cc-datagen cc-features cc-streaming-features cc-train cc-deploy

CSV_PATH := air-quality-data-holmsjo.csv \
            air-quality-data-vaxjo.csv \
            air-quality-data-tingsryds-kommun.csv \
            air-quality-data-vaxjovagen.csv \
            air-quality-data-klackens-ostergrand.csv

AQICN_URLS := https://api.waqi.info/feed/A59899 \
              https://api.waqi.info/feed/A63646 \
              https://api.waqi.info/feed/A59650 \
              https://api.waqi.info/feed/A61867 \
              https://api.waqi.info/feed/A415030

AQICN_COUNTRY := sweden sweden sweden sweden sweden
AQICN_CITY    := karlskrona kronoberg tingsryds-kommun halmstad halmstad
AQICN_STREET  := holmsjö vaxjo g-651 växjövägen klackens-östergränd

# Compute list length (all lists assumed equal length)
NUM_ITEMS := $(words $(CSV_PATH))
# NUM_ITEMS := 2

# Enable secondary expansion for dynamic variable evaluation
.SECONDEXPANSION:

aq-clean:
	python mlfs/clean_hopsworks_resources.py aq

# aq-features:
# 	ipython notebooks/airquality/1_air_quality_feature_backfill.ipynb
aq-features: $(addprefix run-, $(shell seq 1 $(NUM_ITEMS)))

# Rule for each index
# Use secondary expansion to evaluate word function with the index
run-%:
	@idx=$*; \
	csv=$(word $*, $(CSV_PATH)); \
	url=$(word $*, $(AQICN_URLS)); \
	country=$(word $*, $(AQICN_COUNTRY)); \
	city=$(word $*, $(AQICN_CITY)); \
	street=$(word $*, $(AQICN_STREET)); \
	echo "Running index $$idx:"; \
	echo "  CSV      = $$csv"; \
	echo "  URL      = $$url"; \
	echo "  COUNTRY  = $$country"; \
	echo "  CITY     = $$city"; \
	echo "  STREET   = $$street"; \
	time ipython notebooks/airquality/1_air_quality_feature_backfill.ipynb \
		"$$csv" "$$url" "$$country" "$$city" "$$street"

aq-train: $(addprefix train-, $(shell seq 1 $(NUM_ITEMS)))

# Rule for training with each city
train-%:
	@idx=$*; \
	city=$(word $*, $(AQICN_CITY)); \
	echo "Training for index $$idx:"; \
	echo "  CITY     = $$city"; \
	echo "  STREET   = $$street"; \
	ipython notebooks/airquality/3_air_quality_training_pipeline.ipynb \
		"$$city"

aq-inference: $(addprefix inference-, $(shell seq 1 $(NUM_ITEMS)))

# Rule for inference with each city
inference-%:
	@idx=$*; \
	city=$(word $*, $(AQICN_CITY)); \
	echo "Inference with city $$city for index $$idx:"; \
	echo "  CITY     = $$city"; \
	echo "  STREET   = $$street"; \
	ipython notebooks/airquality/2_air_quality_feature_pipeline.ipynb \
		"$$city" \
	ipython notebooks/airquality/4_air_quality_batch_inference.ipynb \
		"$$city"

aq-llm: $(addprefix llm-, $(shell seq 1 $(NUM_ITEMS)))

# Rule for LLM function calling with each city
llm-%:
	@idx=$*; \
	city=$(word $*, $(AQICN_CITY)); \
	echo "LLM function calling with city $$city for index $$idx:"; \
	echo "  CITY     = $$city"; \
	echo "  STREET   = $$street"; \
	ipython notebooks/airquality/5_function_calling.ipynb \
		""$$city"

aq-all: aq-features aq-train aq-inference

titanic-clean:
	python mlfs/clean_hopsworks_resources.py titanic

titanic-features:
	ipython notebooks/titanic/1-titanic-feature-group-backfill.ipynb

titanic-train:
	ipython notebooks/titanic/2-titanic-training-pipeline.ipynb

titanic-inference:
	ipython notebooks/titanic/scheduled-titanic-feature-pipeline-daily.ipynb
	ipython notebooks/titanic/scheduled-titanic-batch-inference-daily.ipynb

titanic-all: titanic-features titanic-train titanic-inference

