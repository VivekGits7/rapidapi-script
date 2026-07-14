"""Make / model seeding from dump_targets (NO API calls).

In the line approach the makes + models we dump come straight from
`dump_targets.csv` (it already carries the TecDoc `tec_manufacturer_id` and
`tec_model_id`). So we seed the manufacturer, the (manufacturer × PC) junction,
and the model row directly — no `/manufacturers/list` or `/models/list` call.

The expensive API work (vehicles → categories → articles → enrich) starts from
these seeded rows. All upserts are idempotent.
"""

import datetime as _dt
from typing import Optional

from config import settings
from dumper.http_client import api_get
from dumper.id_utils import new_id
from dumper.state import TYPE_ID_TO_CODE, VehicleTypeCode
from logger import get_logger
from services.db import execute_command, execute_query_one

logger = get_logger("dumper.seed")

# Model metadata (AR name + production years) per make — fetched once per make per run.
_models_meta_cache: dict[tuple, dict] = {}
# Brand image per make — fetched once per make per run (was once per TARGET: 508 calls where 91 suffice).
_mfg_image_cache: dict[int, Optional[str]] = {}


def _parse_date(value) -> Optional[_dt.date]:
    """Parse a 'YYYY-MM-DD' API date string into a date; None on anything unparseable."""
    if not value:
        return None
    try:
        return _dt.datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


async def fetch_models_meta(api_type_id: int, mfg_external_id: int, country_filter_id: int) -> dict:
    """Return {modelId: {"name_ar", "year_from", "year_to"}} for a make.

    ONE models-list call per make per run (cached) — it carries both the AR name
    (localizes body-style words: Bus→حافلة) AND modelYearFrom/modelYearTo (language-
    neutral dates). Returns {} on any failure.
    """
    key = (api_type_id, mfg_external_id, country_filter_id)
    if key in _models_meta_cache:
        return _models_meta_cache[key]
    out: dict[int, dict] = {}
    path = (
        f"/models/list/type-id/{api_type_id}/manufacturer-id/{mfg_external_id}"
        f"/lang-id/{settings.ARABIC_LANG_ID}/country-filter-id/{country_filter_id}"
    )
    data = await api_get(path)
    models = data.get("models") if isinstance(data, dict) else None
    if isinstance(models, list):
        for m in models:
            if not isinstance(m, dict):
                continue
            try:
                mid = int(m.get("modelId")) if m.get("modelId") is not None else None
            except (TypeError, ValueError):
                mid = None
            if mid is not None:
                out[mid] = {
                    "name_ar": m.get("modelName"),
                    "year_from": _parse_date(m.get("modelYearFrom")),
                    "year_to": _parse_date(m.get("modelYearTo")),
                }
    _models_meta_cache[key] = out
    return out


async def fetch_model_image(api_type_id: int, model_external_id: int) -> Optional[str]:
    """Return the RapidAPI model image URL for a model (GET /models/type-id/{t}/model-id/{id}
    → `modelImage`). One call per model. Returns None on failure / no image."""
    path = f"/models/type-id/{api_type_id}/model-id/{model_external_id}"
    data = await api_get(path)
    img = data.get("modelImage") if isinstance(data, dict) else None
    return img or None


async def get_active_vehicle_type() -> Optional[dict]:
    """Return the ACTIVE vehicle_type row (uuid + external id + code) for this run.

    The active type is `settings.DEFAULT_TYPE_ID` (1=PC, 2=CV, ...), which the CLI
    `--vehicle-type` flag sets at process start. Seeded by the reference phase;
    returns None if reference hasn't run yet or the id is unknown.
    """
    type_code = TYPE_ID_TO_CODE.get(int(settings.DEFAULT_TYPE_ID), VehicleTypeCode.PC.value)
    row = await execute_query_one(
        """
        SELECT vehicle_type_id, vehicle_types_external_id AS api_type_id, type_code
        FROM rapid_api_vehicle_types
        WHERE type_code = $1
        """,
        type_code,
    )
    return dict(row) if row else None


async def fetch_manufacturer_image(mfg_external_id: int) -> Optional[str]:
    """Return the RapidAPI brand image URL (GET /manufacturers/find-by-id/{id} → `image`).
    One call per make PER RUN (cached — a make's models all share the brand image).
    Returns None on failure / no image."""
    if mfg_external_id in _mfg_image_cache:
        return _mfg_image_cache[mfg_external_id]
    path = f"/manufacturers/find-by-id/{mfg_external_id}"
    data = await api_get(path)
    img = data.get("image") if isinstance(data, dict) else None
    _mfg_image_cache[mfg_external_id] = img or None
    return img or None


async def upsert_manufacturer(
    external_id: int, name: Optional[str], image_api_url: Optional[str] = None,
) -> Optional[str]:
    """Insert/return the manufacturer_id (uuid) for a TecDoc make external id.
    `image_api_url` is the RapidAPI brand image; stored (COALESCE = keep existing)."""
    existing = await execute_query_one(
        "SELECT manufacturer_id FROM rapid_api_manufacturers WHERE manufacturers_external_id = $1",
        external_id,
    )
    if existing:
        if image_api_url:
            await execute_command(
                "UPDATE rapid_api_manufacturers SET manufacturer_image_api_url = COALESCE(manufacturer_image_api_url, $2), updated_at = NOW() WHERE manufacturer_id = $1",
                existing["manufacturer_id"], image_api_url,
            )
        return existing["manufacturer_id"]

    mfg_id = await new_id("manufacturers")
    await execute_command(
        """
        INSERT INTO rapid_api_manufacturers (manufacturer_id, manufacturers_external_id, manufacturer_name, manufacturer_image_api_url)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (manufacturers_external_id) DO UPDATE SET
            manufacturer_name = COALESCE(EXCLUDED.manufacturer_name, rapid_api_manufacturers.manufacturer_name),
            manufacturer_image_api_url = COALESCE(EXCLUDED.manufacturer_image_api_url, rapid_api_manufacturers.manufacturer_image_api_url),
            updated_at = NOW()
        """,
        mfg_id,
        external_id,
        name or "",
        image_api_url,
    )
    row = await execute_query_one(
        "SELECT manufacturer_id FROM rapid_api_manufacturers WHERE manufacturers_external_id = $1",
        external_id,
    )
    return row["manufacturer_id"] if row else None


async def upsert_mvt(manufacturer_id: str, vehicle_type_id: str, type_code: str) -> Optional[str]:
    """Insert/return the (manufacturer × vehicle_type) junction id (uuid)."""
    existing = await execute_query_one(
        """
        SELECT mvt_id FROM rapid_api_manufacturer_vehicle_types
        WHERE manufacturer_id = $1 AND vehicle_type_id = $2
        """,
        manufacturer_id,
        vehicle_type_id,
    )
    if existing:
        return existing["mvt_id"]

    mvt_id = await new_id("mvt", type_code)
    await execute_command(
        """
        INSERT INTO rapid_api_manufacturer_vehicle_types (mvt_id, manufacturer_id, vehicle_type_id, models_fetched_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (manufacturer_id, vehicle_type_id) DO NOTHING
        """,
        mvt_id,
        manufacturer_id,
        vehicle_type_id,
    )
    row = await execute_query_one(
        """
        SELECT mvt_id FROM rapid_api_manufacturer_vehicle_types
        WHERE manufacturer_id = $1 AND vehicle_type_id = $2
        """,
        manufacturer_id,
        vehicle_type_id,
    )
    return row["mvt_id"] if row else None


async def upsert_model(
    model_external_id: int,
    model_name: Optional[str],
    manufacturer_id: str,
    vehicle_type_id: str,
    model_name_ar: Optional[str] = None,
    year_from=None,
    year_to=None,
    model_image_api_url: Optional[str] = None,
) -> Optional[dict]:
    """Insert/return the model row (uuid + external id) for a TecDoc model.

    `model_name` is the EN name; `model_name_ar` the lang-42 name. `year_from`/`year_to`
    come from the models list (modelYearFrom/To); `model_image_api_url` from the model
    image endpoint. Seeded with lang/country from settings.
    """
    existing = await execute_query_one(
        """
        SELECT model_id, models_external_id, vehicles_fetched_at
        FROM rapid_api_models
        WHERE models_external_id = $1 AND manufacturer_id = $2 AND vehicle_type_id = $3
          AND lang_id = $4 AND country_filter_id = $5
        """,
        model_external_id,
        manufacturer_id,
        vehicle_type_id,
        settings.DEFAULT_LANG_ID,
        settings.DEFAULT_COUNTRY_FILTER_ID,
    )
    if existing:
        # Fill any newly-available metadata on an already-seeded row (COALESCE = keep existing).
        await execute_command(
            """
            UPDATE rapid_api_models
               SET model_name_ar       = COALESCE($2, model_name_ar),
                   year_from           = COALESCE($3, year_from),
                   year_to             = COALESCE($4, year_to),
                   model_image_api_url = COALESCE($5, model_image_api_url),
                   updated_at = NOW()
             WHERE model_id = $1
            """,
            existing["model_id"], model_name_ar, year_from, year_to, model_image_api_url,
        )
        return dict(existing)

    model_id = await new_id("models", VehicleTypeCode.PC.value)
    await execute_command(
        """
        INSERT INTO rapid_api_models
          (model_id, models_external_id, model_name_en, model_name_ar, year_from, year_to,
           model_image_api_url, manufacturer_id, vehicle_type_id, lang_id, country_filter_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (models_external_id, manufacturer_id, vehicle_type_id, lang_id, country_filter_id) DO NOTHING
        """,
        model_id,
        model_external_id,
        model_name or "",
        model_name_ar,
        year_from,
        year_to,
        model_image_api_url,
        manufacturer_id,
        vehicle_type_id,
        settings.DEFAULT_LANG_ID,
        settings.DEFAULT_COUNTRY_FILTER_ID,
    )
    row = await execute_query_one(
        """
        SELECT model_id, models_external_id, vehicles_fetched_at
        FROM rapid_api_models
        WHERE models_external_id = $1 AND manufacturer_id = $2 AND vehicle_type_id = $3
          AND lang_id = $4 AND country_filter_id = $5
        """,
        model_external_id,
        manufacturer_id,
        vehicle_type_id,
        settings.DEFAULT_LANG_ID,
        settings.DEFAULT_COUNTRY_FILTER_ID,
    )
    return dict(row) if row else None
