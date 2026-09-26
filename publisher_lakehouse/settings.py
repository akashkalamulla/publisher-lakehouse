"""Explicit application configuration loading.

Importing this module performs no file reads and creates no directories.  Call
the loader functions at the application boundary instead.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


PROJECT_ROOT = Path(__file__).parent.parent
DEFAULT_ENV_FILE = PROJECT_ROOT / ".env"
DEFAULT_CONFIG_FILE = PROJECT_ROOT / "config" / "config.ini"


class EnvironmentSettings(BaseSettings):
    """Secrets and deployment-specific values loaded only from the environment."""

    # No default: PostgreSQL is the target, and a SQLite default here would be
    # exactly the silent fallback that hides a broken connection string.
    database_url: str
    data_dir: Path = Path("data")
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    capture_fixtures: bool = False

    model_config = SettingsConfigDict(
        case_sensitive=False,
        env_file=None,
        env_prefix="",
        extra="ignore",
        frozen=True,
    )

    @field_validator("log_level", mode="before")
    @classmethod
    def normalise_log_level(cls, value: object) -> object:
        return value.upper() if isinstance(value, str) else value


class LakeSettings(BaseSettings):
    """S3 API connection used only by lake commands."""

    s3_endpoint_url: str = "http://localhost:9000"
    s3_access_key: str
    s3_secret_key: SecretStr
    lake_bucket: str = "lakehouse"

    model_config = SettingsConfigDict(
        case_sensitive=False,
        env_file=None,
        env_prefix="",
        extra="ignore",
        frozen=True,
    )


class DetailsSettings(BaseModel):
    output_path: Path

    model_config = ConfigDict(extra="forbid", frozen=True)

    @field_validator("output_path", mode="before")
    @classmethod
    def strip_trailing_separators(cls, value: object) -> object:
        """Normalise INI paths before platform-specific ``Path`` parsing."""

        return value.rstrip("\\/") if isinstance(value, str) else value


class PathSettings(BaseModel):
    url_details: Path
    raw_html_dir: Path
    bronze_dir: Path
    export_dir: Path

    model_config = ConfigDict(extra="forbid", frozen=True)


class IngestionSettings(BaseModel):
    bronze_flush_every: int
    manifest_flush_every: int

    model_config = ConfigDict(extra="forbid", frozen=True)

    @field_validator("bronze_flush_every", "manifest_flush_every")
    @classmethod
    def require_positive_batch_size(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("flush sizes must be greater than zero")
        return value


class RefreshSettings(BaseModel):
    journal_days: int
    issue_days: int
    article_days: int

    model_config = ConfigDict(extra="forbid", frozen=True)

    @field_validator("journal_days", "issue_days", "article_days")
    @classmethod
    def require_non_negative_window(cls, value: int) -> int:
        if value < 0:
            raise ValueError("refresh windows cannot be negative")
        return value


class OperatorSettings(BaseModel):
    """Operator-tunable behavior loaded only from ``config/config.ini``."""

    details: DetailsSettings
    paths: PathSettings
    ingestion: IngestionSettings
    refresh: RefreshSettings

    model_config = ConfigDict(extra="forbid", frozen=True)


class Settings(BaseModel):
    environment: EnvironmentSettings
    operator: OperatorSettings

    model_config = ConfigDict(extra="forbid", frozen=True)


def load_environment_settings(
    env_file: str | Path | None = DEFAULT_ENV_FILE,
) -> EnvironmentSettings:
    """Load environment-only settings, optionally including a dotenv file."""

    return EnvironmentSettings(_env_file=env_file, _env_file_encoding="utf-8")


def load_lake_settings(
    env_file: str | Path | None = DEFAULT_ENV_FILE,
) -> LakeSettings:
    """Load lake settings independently of the ingestion database URL."""

    return LakeSettings(_env_file=env_file, _env_file_encoding="utf-8")


def load_operator_settings(
    config_file: str | Path = DEFAULT_CONFIG_FILE,
) -> OperatorSettings:
    """Read and validate operator behavior from an INI file."""

    import configparser

    path = Path(config_file)
    parser = configparser.ConfigParser(interpolation=None)
    if not parser.read(path, encoding="utf-8"):
        raise FileNotFoundError(f"Operator config file not found: {path}")

    required_sections = ("DETAILS", "paths", "ingestion", "refresh")
    missing = [section for section in required_sections if section not in parser]
    if missing:
        joined = ", ".join(missing)
        raise ValueError(f"Operator config is missing section(s): {joined}")

    return OperatorSettings.model_validate(
        {
            "details": dict(parser["DETAILS"]),
            "paths": dict(parser["paths"]),
            "ingestion": dict(parser["ingestion"]),
            "refresh": dict(parser["refresh"]),
        }
    )


def load_settings(
    config_file: str | Path = DEFAULT_CONFIG_FILE,
    env_file: str | Path | None = DEFAULT_ENV_FILE,
) -> Settings:
    """Load both configuration domains explicitly at the application boundary."""

    return Settings(
        environment=load_environment_settings(env_file),
        operator=load_operator_settings(config_file),
    )
