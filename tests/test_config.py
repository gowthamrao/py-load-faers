# -*- coding: utf-8 -*-
"""
Tests for the configuration module.
"""
import os
from pathlib import Path
import pytest
from pytest import MonkeyPatch
from py_load_faers.config import load_config, AppSettings

# A sample config file for testing
SAMPLE_CONFIG_YAML = """
dev:
  db:
    host: localhost
    port: 5432
    user: dev_user
    password: dev_password
    dbname: faers_dev
  downloader:
    download_dir: /tmp/dev_downloads
  log_level: DEBUG

prod:
  db:
    host: prod.db.server.com
    port: 5433
    user: prod_user
    password: prod_password
    dbname: faers_prod
"""


@pytest.fixture
def sample_config_file(tmp_path: Path) -> Path:
    """Create a sample config.yaml file in a temporary directory."""
    config_path = tmp_path / "config.yaml"
    config_path.write_text(SAMPLE_CONFIG_YAML)
    return config_path


def test_load_config_from_file_and_profile(sample_config_file: Path) -> None:
    """Test that configuration is loaded correctly from a specific profile in the YAML file."""
    settings = load_config(profile="prod", config_file=str(sample_config_file))

    assert isinstance(settings, AppSettings)
    assert settings.db.host == "prod.db.server.com"
    assert settings.db.port == 5433
    assert settings.db.user == "prod_user"
    # These fields are not in the 'prod' profile, so they should have their default values
    assert settings.downloader.download_dir == "./downloads"
    assert settings.log_level == "INFO"


def test_load_config_with_env_var_override(
    sample_config_file: Path, monkeypatch: MonkeyPatch
) -> None:
    """Test that environment variables override settings from the config file."""
    # Set environment variables to override the 'dev' profile
    monkeypatch.setenv("PY_LOAD_FAERS_DB__HOST", "env_host")
    monkeypatch.setenv("PY_LOAD_FAERS_DB__PORT", "9999")
    monkeypatch.setenv("PY_LOAD_FAERS_LOG_LEVEL", "WARNING")

    settings = load_config(profile="dev", config_file=str(sample_config_file))

    assert settings.db.host == "env_host"
    assert settings.db.port == 9999
    assert settings.db.user == "dev_user"  # This should still come from the file
    assert settings.log_level == "WARNING"


def test_load_config_defaults_when_no_file() -> None:
    """Test that default settings are used when no config file is present."""
    # Assuming no config.yaml exists in the root of the test execution dir
    if os.path.exists("config.yaml"):
        os.remove("config.yaml")

    settings = load_config()

    assert isinstance(settings, AppSettings)
    assert settings.db.host == "localhost"
    assert settings.downloader.download_dir == "./downloads"
    assert settings.log_level == "INFO"
