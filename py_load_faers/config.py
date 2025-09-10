# -*- coding: utf-8 -*-
"""
This module handles the configuration management for the FAERS loader.

It uses a hierarchical configuration approach, allowing settings to be loaded
from a YAML file, environment variables, and CLI arguments.
"""
import os
import yaml
from pathlib import Path
from typing import Optional, Literal
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DatabaseSettings(BaseModel):
    """Configuration for the target database connection."""

    type: str = Field(
        "postgresql", description="The type of database (e.g., postgresql, redshift)."
    )
    host: str = "localhost"
    port: int = 5432
    user: str = "user"
    password: str = "password"
    dbname: str = "faers"


class DownloaderSettings(BaseModel):
    """Configuration for the FAERS data downloader."""

    download_dir: str = Field(
        "./downloads", description="Directory to store downloaded FAERS files."
    )
    retries: int = Field(3, description="Number of retry attempts for downloads.")
    timeout: int = Field(60, description="Timeout in seconds for download requests.")


class ProcessingSettings(BaseModel):
    """Configuration for data processing."""

    chunk_size: int = Field(500_000, description="Number of records to process in a single chunk.")
    staging_format: Literal["csv", "parquet"] = Field(
        "parquet",
        description=("The intermediate file format for staging data before loading."),
    )


class AppSettings(BaseSettings):
    """
    Main application settings.

    Settings are loaded from the following sources in order of precedence:
    1. Environment variables (e.g., `PY_LOAD_FAERS_DB__HOST=...`)
    2. YAML configuration file (`config.yaml` or path specified by `CONFIG_FILE` env var)
    3. Default values defined in this class.
    """

    model_config = SettingsConfigDict(
        env_prefix="PY_LOAD_FAERS_",
        env_nested_delimiter="__",
        env_file_encoding="utf-8",
    )

    db: DatabaseSettings = Field(default_factory=DatabaseSettings)
    downloader: DownloaderSettings = Field(default_factory=DownloaderSettings)
    processing: ProcessingSettings = Field(default_factory=ProcessingSettings)
    log_level: str = Field("INFO", description="Logging level (e.g., DEBUG, INFO, WARNING).")

    @classmethod
    def from_yaml(cls, path: Path) -> "AppSettings":
        """Load configuration from a YAML file."""
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found at: {path}")

        with open(path, "r") as f:
            config_data = yaml.safe_load(f)

        return cls.model_validate(config_data)


def load_config(profile: Optional[str] = None, config_file: Optional[str] = None) -> AppSettings:
    """
    Load application configuration.

    It loads settings from a YAML file and then overrides with any
    environment variables. A specific profile can be selected from the config file.

    :param profile: The configuration profile to load (e.g., 'dev', 'prod').
    :param config_file: Path to a specific YAML config file.
    :return: An instance of AppSettings.
    """
    # This is a workaround to make pydantic-settings prioritize env vars over file settings
    # when dealing with nested models. We first load from the environment, then update
    # with the file settings if the env var is not set.

    # 1. Load from environment variables first
    env_settings = AppSettings()

    # 2. Load from YAML file
    cfg_path_str = config_file or os.environ.get("CONFIG_FILE", "config.yaml")
    cfg_path = Path(cfg_path_str)

    if cfg_path.exists():
        with open(cfg_path, "r") as f:
            yaml_data = yaml.safe_load(f) or {}

        profile_data = yaml_data.get(profile, {}) if profile else yaml_data

        # Update the settings from the file, but only if not set by environment variables
        if profile_data:
            file_settings = AppSettings.model_validate(profile_data)
            # This is a simple merge. A more complex deep merge could be used if needed.
            if os.getenv("PY_LOAD_FAERS_DB__HOST") is None:
                env_settings.db.host = file_settings.db.host
            if os.getenv("PY_LOAD_FAERS_DB__PORT") is None:
                env_settings.db.port = file_settings.db.port
            if os.getenv("PY_LOAD_FAERS_DB__USER") is None:
                env_settings.db.user = file_settings.db.user
            if os.getenv("PY_LOAD_FAERS_DB__PASSWORD") is None:
                env_settings.db.password = file_settings.db.password
            if os.getenv("PY_LOAD_FAERS_DB__DBNAME") is None:
                env_settings.db.dbname = file_settings.db.dbname
            if os.getenv("PY_LOAD_FAERS_DOWNLOADER__DOWNLOAD_DIR") is None:
                env_settings.downloader.download_dir = file_settings.downloader.download_dir
            if os.getenv("PY_LOAD_FAERS_LOG_LEVEL") is None:
                env_settings.log_level = file_settings.log_level

    return env_settings


# Example of how to create a default config file for users
DEFAULT_CONFIG = """
# Default configuration for py-load-faers
# You can create profiles like 'dev', 'staging', 'prod'
dev:
  db:
    host: localhost
    port: 5432
    user: postgres
    password: password
    dbname: faers_dev
  downloader:
    download_dir: /tmp/faers_downloads
  log_level: DEBUG

prod:
  db:
    host: prod.db.server.com
    port: 5432
    user: faers_loader
    password: "" # Should be set via ENV var: PY_LOAD_FAERS_DB__PASSWORD
    dbname: faers
  downloader:
    download_dir: /data/faers
  log_level: INFO
"""
