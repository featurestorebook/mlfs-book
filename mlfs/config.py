from enum import Enum
from pathlib import Path
from typing import Literal

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict
import os

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

    # For hopsworks.login(), set as environment variables if they are not already set as env variables
    HOPSWORKS_API_KEY: SecretStr | None = None
    HOPSWORKS_PROJECT: str | None = None
    HOPSWORKS_HOST: str | None = None

    # Air Quality
    AQICN_API_KEY: SecretStr | None = None
    AQICN_COUNTRY: str | None = None
    AQICN_CITY: str | None = None
    AQICN_STREET: str | None = None
    AQICN_URL: str | None = None
    
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

    def model_post_init(self, __context):
        """Runs after the model is initialized."""
        print("HopsworksSettings initialized!")

        # Set environment variables if not already set
        if os.getenv("HOPSWORKS_API_KEY") is None:
            if self.HOPSWORKS_API_KEY is not None:
                os.environ['HOPSWORKS_API_KEY'] = self.HOPSWORKS_API_KEY.get_secret_value()
        if os.getenv("HOPSWORKS_PROJECT") is None:
            if self.HOPSWORKS_PROJECT is not None:
                os.environ['HOPSWORKS_PROJECT'] = self.HOPSWORKS_PROJECT
        if os.getenv("HOPSWORKS_HOST") is None:
            if self.HOPSWORKS_HOST is not None:
                os.environ['HOPSWORKS_HOST'] = self.HOPSWORKS_HOST

        # --- Check required .env values ---
        missing = []
        # 1. HOPSWORKS_API_KEY
        api_key = self.HOPSWORKS_API_KEY or os.getenv("HOPSWORKS_API_KEY")
        if not api_key:
            missing.append("HOPSWORKS_API_KEY")
        # 2. AQICN_API_KEY
        aqicn_api_key = self.AQICN_API_KEY or os.getenv("AQICN_API_KEY")
        if not aqicn_api_key:
            missing.append("AQICN_API_KEY")
        # 3. AQICN_COUNTRY
        aqicn_country = self.AQICN_COUNTRY or os.getenv("AQICN_COUNTRY")
        if not aqicn_country:
            missing.append("AQICN_COUNTRY")
        # 4. AQICN_CITY
        aqicn_city = self.AQICN_CITY or os.getenv("AQICN_CITY")
        if not aqicn_city:
            missing.append("AQICN_CITY")
        # 5. AQICN_STREET
        aqicn_street = self.AQICN_STREET or os.getenv("AQICN_STREET")
        if not aqicn_street:
            missing.append("AQICN_STREET")
        # 6. AQICN_URL
        aqicn_url = self.AQICN_URL or os.getenv("AQICN_URL")
        if not aqicn_url:
            missing.append("AQICN_URL")

        if missing:
            raise ValueError(
                "The following required settings are missing from your environment (.env or system):\n  " +
                "\n  ".join(missing)
            )    