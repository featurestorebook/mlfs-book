from enum import Enum
from pathlib import Path
from typing import Literal

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class FraudDatasetSize(Enum):
    LARGE = "LARGE"
    MEDIUM = "MEDIUM"
    SMALL = "SMALL"


class HopsworksSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore'
    )
    
    MLFS_DIR: Path = Path(__file__).parent

    # Hopsworks
    HOPSWORKS_API_KEY: SecretStr | None = None
    HOPSWORKS_PROJECT: str | None = None
    
    # Air Quality
    AQICN_API_KEY: SecretStr | None = None
    AQICN_COUNTRY: str = "sweden"
    AQICN_CITY: str = "stockholm"
    AQICN_STREET: str = "hornsgatan-108"
    AQICN_URL: str = "https://api.waqi.info/feed/@10009"
    
    # Other API Keys
    FELDERA_API_KEY: SecretStr | None = None    
    OPENAI_API_KEY: SecretStr | None = None
    OPENAI_MODEL_ID: str = "gpt-4o-mini"



    # Feature engineering
    FRAUD_DATA_SIZE: FraudDatasetSize = FraudDatasetSize.SMALL
    EMBEDDING_MODEL: str = "all-MiniLM-L6-v2"

    # Personalized Recommendations
    TWO_TOWER_MODEL_EMBEDDING_SIZE: int = 16
    TWO_TOWER_MODEL_BATCH_SIZE: int = 2048
    TWO_TOWER_NUM_EPOCHS: int = 10
    TWO_TOWER_WEIGHT_DECAY: float = 0.001
    TWO_TOWER_LEARNING_RATE: float = 0.01
    TWO_TOWER_DATASET_VALIDATON_SPLIT_SIZE: float = 0.1
    TWO_TOWER_DATASET_TEST_SPLIT_SIZE: float = 0.1

    RANKING_DATASET_VALIDATON_SPLIT_SIZE: float = 0.1
    RANKING_LEARNING_RATE: float = 0.2
    RANKING_ITERATIONS: int = 100
    RANKING_SCALE_POS_WEIGHT: int = 10
    RANKING_EARLY_STOPPING_ROUNDS: int = 5

    # Inference
    RANKING_MODEL_TYPE: Literal["ranking", "llmranking"] = "ranking"
    CUSTOM_HOPSWORKS_INFERENCE_ENV: str = "custom_env_name"
    
