import enum
from pathlib import Path
from tempfile import gettempdir

from pydantic_settings import BaseSettings, SettingsConfigDict
from yarl import URL

TEMP_DIR = Path(gettempdir())


class LogLevel(str, enum.Enum):  # noqa: WPS600
    """Possible log levels."""

    NOTSET = "NOTSET"
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    FATAL = "FATAL"


class Settings(BaseSettings):
    """
    Application settings.

    These parameters can be configured
    with environment variables.
    """

    host: str = "127.0.0.1"
    port: int = 8000
    # quantity of workers for uvicorn
    workers_count: int = 1
    # Enable uvicorn reloading
    reload: bool = False

    # Current environment
    environment: str = "dev"

    log_level: LogLevel = LogLevel.INFO

    # Variables for Nimble API
    nimble_token: str = "NxkA2RlXS3NiR8SKwRdDmroA992jgu"
    nimble_contacts_ids_endpoint: str = "https://api.nimble.com/api/v1/contacts/ids/"
    nimble_contacts_endpoint: str = "https://api.nimble.com/api/v1/contacts/"

    # Variables for the database
    db_host: str = "localhost"
    db_port: int = 5432
    db_user: str = "nimblesync"
    db_pass: str = "nimblesync"
    db_base: str = "nimblesync"
    db_echo: bool = False

    # Variables for database seed
    seed_contact_csv_path: str = "seed/Nimble Contacts - Sheet1.csv"

    @property
    def db_url(self) -> URL:
        """
        Assemble database URL from settings.

        :return: database URL.
        """
        return URL.build(
            scheme="postgresql",
            host=self.db_host,
            port=self.db_port,
            user=self.db_user,
            password=self.db_pass,
            path=f"/{self.db_base}",
        )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="NIMBLESYNC_",
        env_file_encoding="utf-8",
    )


settings = Settings()
