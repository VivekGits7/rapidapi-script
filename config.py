import os
import re

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from dotenv import dotenv_values


_RAPIDAPI_KEY_PATTERN = re.compile(r"^RAPIDAPI_KEY_(\d+)$")


class Settings(BaseSettings):
    # ==================== SERVER CONFIGURATION ====================
    APP_NAME: str = Field(default="RapidAPI Auto Parts Dumper", description="Application name")
    HOST: str = Field(default="0.0.0.0", description="Server host")
    PORT: int = Field(default=8000, description="Server port")
    ENVIRONMENT: str = Field(default="development", description="Environment")
    LOG_LEVEL: str = Field(default="INFO", description="Logging level")

    # ==================== CORS CONFIGURATION ====================
    FRONTEND_URL: str = Field(default="http://localhost:3000", description="Frontend URL")

    # ==================== RAPIDAPI CONFIGURATION ====================
    # Keys are discovered dynamically from .env / os.environ — any number of
    # `RAPIDAPI_KEY_<n>` entries work. No fixed upper bound. See
    # `rapidapi_keys` property below.
    RAPIDAPI_HOST: str = Field(default="auto-parts-catalog.p.rapidapi.com", description="RapidAPI host header")
    RAPIDAPI_BASE_URL: str = Field(default="https://auto-parts-catalog.p.rapidapi.com", description="RapidAPI base URL")
    RAPIDAPI_TIMEOUT: float = Field(default=30.0, description="HTTP request timeout (seconds)")

    DEFAULT_LANG_ID: int = Field(default=4, description="Primary/English language ID (4 = English (GB))")
    DEFAULT_COUNTRY_FILTER_ID: int = Field(default=80, description="Default country filter ID (80 = Egypt)")
    DEFAULT_TYPE_ID: int = Field(default=1, description="Default vehicle type ID (1 = Passenger Car)")

    # ==================== BILINGUAL (English + Arabic) ====================
    # Every text endpoint is fetched in English (DEFAULT_LANG_ID) AND Arabic
    # (ARABIC_LANG_ID) in parallel; the Arabic text is merged into *_ar columns
    # on the same row. TecDoc has one Arabic (42); there is no Egyptian dialect.
    BILINGUAL: bool = Field(default=True, description="Also fetch + store Arabic (_ar columns) alongside English")
    ARABIC_LANG_ID: int = Field(default=42, description="Arabic language ID (42 = ar)")

    # ==================== CRAWL LIMITATIONS (store-all, complete top-N) ====================
    # ALL vehicles + articles are STORED. These caps only decide how many get the
    # expensive deep crawl / full details — the rest stay completed=false and can
    # be filled later by raising the cap (future extensibility).
    # Vehicles are ranked latest-first by construction date; the top N are crawled.
    MAX_VEHICLES_PER_MODEL: int = Field(default=5, description="Latest N vehicles per model to fully crawl (0 = all)")
    MAX_ARTICLES_PER_CATEGORY: int = Field(default=2, description="Top N articles per (vehicle, category) to fetch full details for (0 = all)")

    # ==================== DATABASE CONFIGURATION ====================
    POSTGRES_DB_HOST: str = Field(default="localhost", description="PostgreSQL host")
    POSTGRES_DB_PORT: int = Field(default=5432, description="PostgreSQL port")
    POSTGRES_DB_NAME: str = Field(default="autoparts", description="Database name")
    POSTGRES_DB_USER: str = Field(default="autoparts", description="Database user")
    POSTGRES_DB_PASSWORD: str = Field(default="", description="Database password")

    # ==================== DUMP TUNING ====================
    # Plan limits: 20 requests/second, 100,000 requests/month (hard).
    RAPIDAPI_RATE_LIMIT_PER_SEC: int = Field(default=20, description="Global RapidAPI rate limit (requests/second) — plan hard limit")
    MONTHLY_REQUEST_HARD_LIMIT: int = Field(default=100_000, description="RapidAPI monthly request hard limit (plan)")
    MONTHLY_REQUEST_SAFETY_BUFFER: int = Field(default=500, description="Stop this many calls BEFORE the monthly hard limit, to never overshoot")
    COOLDOWN_429_SEC: int = Field(default=60, description="Cooldown after 429 (per-minute rate limit)")
    COOLDOWN_403_SEC: int = Field(default=3600, description="Cooldown after 403 (likely quota exhausted)")
    COOLDOWN_5XX_SEC: int = Field(default=10, description="Cooldown after 5xx server error")
    KEY_EXHAUSTION_THRESHOLD_SEC: int = Field(
        default=21600,
        description="If all keys are cooling for longer than this (seconds), assume quota exhaustion and pause the job",
    )
    MAX_TRANSIENT_RETRIES: int = Field(default=3, description="Max retries per request on transient errors")
    BATCH_SIZE: int = Field(default=100, description="Article ids per batch for specs/OEM endpoints")

    # ==================== TARGET FILTER (dump_targets.csv) ====================
    DUMP_TARGETS_CSV: str = Field(default="dump_targets.csv", description="Path to the make/model target seed CSV")

    # ==================== CATEGORY SKIP (optional; only filter besides make+model) ====================
    # The ONLY dump filter is make + model (dump_targets.csv). Everything under a
    # targeted model — ALL vehicles, ALL categories, ALL articles — is dumped.
    # This optional list just skips known-junk TecDoc categories; empty = skip nothing.
    BLOCKED_CATEGORY_EXTERNAL_IDS: str = Field(default="", description="Comma-separated TecDoc category ids to skip (optional; empty = none)")

    # ==================== MEDIA + S3 IMAGE MIRRORING ====================
    # Primary article image (api_image_url) is always captured during the article
    # crawl. MEDIA_ENABLED additionally pulls EXTRA images per article (per-article
    # call — expensive). S3_ENABLED mirrors images to our bucket; when False the
    # s3_image_url mirror step is a no-op stub until creds are wired.
    MEDIA_ENABLED: bool = Field(default=False, description="Pull extra per-article media images (expensive, opt-in)")
    S3_ENABLED: bool = Field(default=False, description="Enable downloading RapidAPI images and re-uploading to our S3")
    AWS_REGION: str = Field(default="", description="AWS region of the S3 bucket")
    AWS_ACCESS_KEY_ID: str = Field(default="", description="AWS access key id")
    AWS_SECRET_ACCESS_KEY: str = Field(default="", description="AWS secret access key")
    AWS_BUCKET_NAME: str = Field(default="", description="Target S3 bucket for mirrored images")
    S3_ARTICLE_MEDIA_FOLDER: str = Field(default="dev/article_media", description="S3 key prefix/folder for mirrored article images")

    # ==================== DERIVED PROPERTIES ====================
    @property
    def request_interval_sec(self) -> float:
        """Seconds to wait between API calls to hold RAPIDAPI_RATE_LIMIT_PER_SEC globally."""
        rate = max(1, self.RAPIDAPI_RATE_LIMIT_PER_SEC)
        return 1.0 / rate

    @property
    def monthly_request_ceiling(self) -> int:
        """Stop making calls once the month's usage reaches this (hard limit minus buffer)."""
        return max(0, self.MONTHLY_REQUEST_HARD_LIMIT - self.MONTHLY_REQUEST_SAFETY_BUFFER)

    @property
    def blocked_category_ids(self) -> set[int]:
        out: set[int] = set()
        for tok in (self.BLOCKED_CATEGORY_EXTERNAL_IDS or "").split(","):
            tok = tok.strip()
            if tok:
                try:
                    out.add(int(tok))
                except ValueError:
                    pass
        return out

    @property
    def database_url(self) -> str:
        return (
            f"postgresql://{self.POSTGRES_DB_USER}:{self.POSTGRES_DB_PASSWORD}"
            f"@{self.POSTGRES_DB_HOST}:{self.POSTGRES_DB_PORT}/{self.POSTGRES_DB_NAME}"
        )

    @property
    def rapidapi_keys(self) -> list[str]:
        """Discover RapidAPI keys from .env + os.environ.

        Accepts both a single `RAPIDAPI_KEY` and any number of numbered
        `RAPIDAPI_KEY_<n>` entries. The bare key comes first, then the numbered
        ones in order. Add/remove keys in `.env` — no code change. Empties + dups
        filtered.
        """
        merged: dict[str, str] = {}
        # .env values first, then live env wins (handy for prod where keys
        # come from real env vars rather than a checked-in file).
        for k, v in (dotenv_values(".env") or {}).items():
            if v is not None:
                merged[k] = v
        merged.update(os.environ)

        ordered: list[str] = []
        # Bare single key first
        bare = merged.get("RAPIDAPI_KEY")
        if bare:
            ordered.append(bare)
        # Then numbered keys RAPIDAPI_KEY_1, _2, ...
        numbered: list[tuple[int, str]] = []
        for var, val in merged.items():
            m = _RAPIDAPI_KEY_PATTERN.match(var)
            if m and val:
                numbered.append((int(m.group(1)), val))
        numbered.sort(key=lambda t: t[0])
        ordered.extend(v for _, v in numbered)

        # De-dup while preserving order
        seen: set[str] = set()
        return [k for k in ordered if not (k in seen or seen.add(k))]

    @property
    def is_production(self) -> bool:
        return self.ENVIRONMENT.lower() == "production"

    @property
    def is_development(self) -> bool:
        return self.ENVIRONMENT.lower() == "development"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )


settings = Settings()
