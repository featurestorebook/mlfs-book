#        conda create -n mlfs python==3.11	
#        conda activate mlsfs
#	conda install twofish clang -y

include .env
export $(shell sed 's/=.*//' .env)
	
check-venv:
	@if [ -n "$$CONDA_DEFAULT_ENV" ]; then \
		echo "You are in a Conda environment: $$CONDA_DEFAULT_ENV"; \
	elif python -c 'import sys; exit(0 if hasattr(sys, "real_prefix") or (hasattr(sys, "base_prefix") and sys.base_prefix != sys.prefix) else 1)'; then \
		echo "You are running in a Python virtual environment."; \
	else \
		echo "No virtual environment or Conda environment detected. Please create and activate one first, then re-run 'make install'"; \
		exit 1; \
	fi 

install: check-venv
	pip install uv
	uv pip install -r requirements.txt

install-recommender: check-venv
	uv pip install -r requirements-llm.txt

cc-start-ui:
	uv run python -m streamlit run streamlit_app.py
 

cc-purge:
	uv run python mlfs/clean_hopsworks_resources.py cc

cc-datagen:
	uv run ipython notebooks/ccfraud/0-data-generation-with-polars.ipynb

cc-gen-kafka:
	uv run ipython notebooks/ccfraud/transactions_synthetic_kafka_generator.ipynb

cc-features:
	uv run ipython notebooks/ccfraud/1-batch-polars-feature-pipeline.ipynb

cc-streaming-features:
	uv run ipython notebooks/ccfraud/1-streaming-feature-pipeline-feldera.ipynb

cc-train:
	uv run ipython notebooks/ccfraud/

cc-deploy:
	uv run ipython notebooks/ccfraud/

cc-all: cc-datagen cc-features cc-streaming-features cc-train cc-deploy

aq-purge:
	uv run python mlfs/clean_hopsworks_resources.py aq

aq-features:
	uv run ipython notebooks/airquality/1_air_quality_feature_backfill.ipynb

aq-train:
	uv run ipython notebooks/airquality/3_air_quality_training_pipeline.ipynb

aq-inference:
	uv run ipython notebooks/airquality/2_air_quality_feature_pipeline.ipynb
	uv run ipython notebooks/airquality/4_air_quality_batch_inference.ipynb

aq-llm:
	uv run ipython notebooks/airquality/5_function_calling.ipynb

aq-all: aq-features aq-train aq-inference

titanic-purge:
	uv run python mlfs/clean_hopsworks_resources.py titanic

titanic-features:
	uv run ipython notebooks/titanic/1-titanic-feature-group-backfill.ipynb

titanic-train:
	uv run ipython notebooks/titanic/2-titanic-training-pipeline.ipynb

titanic-inference:
	uv run ipython notebooks/titanic/scheduled-titanic-feature-pipeline-daily.ipynb
	uv run ipython notebooks/titanic/scheduled-titanic-batch-inference-daily.ipynb

titanic-all: titanic-features titanic-train titanic-inference

