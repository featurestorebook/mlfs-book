#        conda create -n mlfs python==3.11	
#        conda activate mlsfs
#	conda install twofish clang -y

include .env
export $(shell sed 's/=.*//' .env)

install:
	@if [ -n "$$CONDA_DEFAULT_ENV" ]; then \
		echo "You are in a Conda environment: $$CONDA_DEFAULT_ENV"; \
	elif python -c 'import sys; exit(0 if hasattr(sys, "real_prefix") or (hasattr(sys, "base_prefix") and sys.base_prefix != sys.prefix) else 1)'; then \
		echo "You are running in a Python virtual environment."; \
	else \
		echo "No virtual environment or Conda environment detected. Please create and activate one first, then re-run 'make install'"; \
		exit 1; \
	fi 
	pip install uv
	uv pip install --all-extras --system --requirement pyproject.toml

start-ui:
	uv run python -m streamlit run streamlit_app.py
 
cc-purge:
	uv run python mlfs/clean_hopsworks_resources.py cc

all: feature-engineering train-retrieval train-ranking create-embeddings create-deployments schedule-materialization-jobs

cc-data-generation:
	uv run ipython notebooks/ccfraud/0-data-generation-with-polars.ipynb

cc-feature-backfill:
	uv run ipython notebooks/ccfraud/1-batch-polars-feature-pipeline.ipynb

cc-streaming-features:
	uv run ipython notebooks/ccfraud/1-streaming-feature-pipeline-feldera.ipynb
cc-training:
	uv run ipython notebooks/ccfraud/

cc-deploy:
	uv run ipython notebooks/ccfraud/

aq-purge:
	uv run python mlfs/clean_hopsworks_resources.py aq

titanic-purge:
	uv run python mlfs/clean_hopsworks_resources.py titanic

