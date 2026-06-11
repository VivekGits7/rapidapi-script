"""Catch-all logger for API items that couldn't fit the typed schema.

Every malformed item from any RapidAPI response lands in
`rapid_api_unparsed_items` so nothing is ever silently lost. The raw payload
sits there as JSONB for inspection / re-processing later.

Reasons:
  * non_list_response       — outer items field wasn't a list/dict as expected
  * non_dict_item           — list contained a non-dict element (e.g. a string)
  * missing_external_id     — dict had no usable external id field
  * unparseable_external_id — id was present but couldn't cast to int

Safe to call from any phase. Never raises — logging failure here would
just compound the problem, so we swallow + log.
"""

import json
from enum import Enum
from typing import Any, Optional

from dumper.id_utils import new_id
from logger import get_logger
from services.db import execute_command

logger = get_logger("dumper.unparsed")


class UnparsedReason(str, Enum):
    NON_LIST_RESPONSE = "non_list_response"
    NON_DICT_ITEM = "non_dict_item"
    MISSING_EXTERNAL_ID = "missing_external_id"
    UNPARSEABLE_EXTERNAL_ID = "unparseable_external_id"


class UnparsedEntity(str, Enum):
    LANGUAGE = "language"
    COUNTRY = "country"
    VEHICLE_TYPE = "vehicle_type"
    MANUFACTURER = "manufacturer"
    MODEL = "model"
    VEHICLE = "vehicle"
    CATEGORY = "category"
    ARTICLE = "article"
    SPEC = "spec"
    OEM = "oem"
    MEDIA = "media"


async def log_unparsed(
    api_path: str,
    entity_type: UnparsedEntity | str,
    raw_item: Any,
    reason: UnparsedReason | str,
    parent_ref: Optional[dict] = None,
) -> None:
    """Persist a raw API item we couldn't parse into the typed schema."""
    try:
        unparsed_id = await new_id("unparsed_items")
        entity_str = entity_type.value if isinstance(entity_type, Enum) else entity_type
        reason_str = reason.value if isinstance(reason, Enum) else reason
        # JSONB columns in asyncpg need pre-serialized strings. default=str
        # handles dates, Decimal, etc.
        raw_json = json.dumps(raw_item, default=str, ensure_ascii=False)
        parent_json = json.dumps(parent_ref or {}, default=str, ensure_ascii=False)
        await execute_command(
            """
            INSERT INTO rapid_api_unparsed_items
              (unparsed_id, api_path, entity_type, parent_ref, raw_item, reason)
            VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6)
            """,
            unparsed_id,
            api_path,
            entity_str,
            parent_json,
            raw_json,
            reason_str,
        )
    except Exception as e:
        # Never raise from here — caller already had a parse problem; we
        # don't want to escalate it. Just log loudly.
        logger.error(
            f"log_unparsed FAILED (path={api_path}, entity={entity_type}, reason={reason}): {e}"
        )
