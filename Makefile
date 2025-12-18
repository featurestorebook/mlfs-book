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
	ipython --InteractiveShellApp.extensions=[] notebooks/ccfraud/0-data-generation-with-polars.ipynb

cc-gen-kafka:
	ipython  --InteractiveShellApp.extensions=[] notebooks/ccfraud/transactions_synthetic_kafka_generator.ipynb

cc-features:
	ipython  --InteractiveShellApp.extensions=[] notebooks/ccfraud/3-batch-feature-pipeline.ipynb

cc-streaming-features:
	./scripts/1a-run-feldera.sh
	ipython  --InteractiveShellApp.extensions=[] notebooks/ccfraud/1b-synthetic-stream-transactions.ipynb

cc-train:
	ipython  --InteractiveShellApp.extensions=[] notebooks/ccfraud/

cc-deploy:
	ipython  --InteractiveShellApp.extensions=[] notebooks/ccfraud/

cc-all: cc-datagen cc-features cc-streaming-features cc-train cc-deploy

aq-clean:
	python mlfs/clean_hopsworks_resources.py aq

aq-features:
	ipython  --InteractiveShellApp.extensions=[] notebooks/airquality/1_air_quality_feature_backfill.ipynb

aq-train:
	ipython  --InteractiveShellApp.extensions=[] notebooks/airquality/3_air_quality_training_pipeline.ipynb

aq-inference:
	ipython  --InteractiveShellApp.extensions=[] notebooks/airquality/2_air_quality_feature_pipeline.ipynb
	ipython  --InteractiveShellApp.extensions=[] notebooks/airquality/4_air_quality_batch_inference.ipynb

aq-llm:
	ipython  --InteractiveShellApp.extensions=[] notebooks/airquality/5_function_calling.ipynb

aq-all: aq-features aq-train aq-inference

titanic-clean:
	python mlfs/clean_hopsworks_resources.py titanic

titanic-features:
	ipython  --InteractiveShellApp.extensions=[] notebooks/titanic/1-titanic-feature-group-backfill.ipynb

titanic-train:
	ipython  --InteractiveShellApp.extensions=[] notebooks/titanic/2-titanic-training-pipeline.ipynb

titanic-inference:
	ipython  --InteractiveShellApp.extensions=[] notebooks/titanic/scheduled-titanic-feature-pipeline-daily.ipynb
	ipython  --InteractiveShellApp.extensions=[] notebooks/titanic/scheduled-titanic-batch-inference-daily.ipynb

titanic-all: titanic-features titanic-train titanic-inference

