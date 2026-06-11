"""Enums + type-id↔type-code mappings used across the dumper."""

from enum import Enum


class StopRequested(Exception):
    """Raised at a cooperative checkpoint when a graceful stop was requested.

    Propagates up to dump_main, which marks the job 'paused'. All work up to the
    checkpoint is already committed, so resume continues from where it left off.
    """


class DumpStatus(str, Enum):
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"


class DumpPhase(str, Enum):
    IDLE = "idle"
    REFERENCE = "reference"
    MANUFACTURERS = "manufacturers"
    MODELS = "models"
    VEHICLES = "vehicles"
    CATEGORIES = "categories"
    ARTICLES = "articles"
    ENRICH = "enrich"
    MEDIA = "media"
    COMPLETED = "completed"
    FAILED = "failed"
    PAUSED = "paused"


class TargetStatus(str, Enum):
    """Per (make, model) target lifecycle in rapid_api_dump_targets."""

    PENDING = "pending"     # not started
    RESUMABLE = "resumable" # started, partially crawled — finish before new makes
    COMPLETE = "complete"   # fully dumped (vehicles → categories → articles → details)


class DumpEntityState(str, Enum):
    """rapid_api_dump_state — per-entity (vehicle/article) completion."""

    INCOMPLETE = "incomplete"
    COMPLETE = "complete"   # dumped all the way to the deepest stage it needs


class DumpStage(str, Enum):
    """rapid_api_dump_stage — how far an entity got in the hierarchy."""

    LISTED = "listed"          # vehicle stored / article listed
    CATEGORIES = "categories"  # vehicle: category tree fetched
    ARTICLES = "articles"      # vehicle: all leaf-category article lists fetched
    DETAILS = "details"        # article: complete-details fetched


class CursorKey(str, Enum):
    MODELS = "models"
    VEHICLES = "vehicles"
    CATEGORIES = "categories"
    ARTICLES = "articles"


class VehicleTypeCode(str, Enum):
    """Our short code for each RapidAPI vehicle type id."""

    PC = "PC"
    CV = "CV"
    MOTO = "MOTO"
    LCV = "LCV"
    DCAB = "DCAB"
    AXLE = "AXLE"
    ENG = "ENG"
    BUS = "BUS"
    AFT = "AFT"
    TRAC = "TRAC"
    VOEM = "VOEM"


# Map the API's numeric `type id` (1..11) to our short code.
# This is the only place this mapping lives — everything else looks it up here.
TYPE_ID_TO_CODE: dict[int, str] = {
    1: VehicleTypeCode.PC.value,
    2: VehicleTypeCode.CV.value,
    3: VehicleTypeCode.MOTO.value,
    4: VehicleTypeCode.LCV.value,
    5: VehicleTypeCode.DCAB.value,
    6: VehicleTypeCode.AXLE.value,
    7: VehicleTypeCode.ENG.value,
    8: VehicleTypeCode.BUS.value,
    9: VehicleTypeCode.AFT.value,
    10: VehicleTypeCode.TRAC.value,
    11: VehicleTypeCode.VOEM.value,
}

CODE_TO_TYPE_ID: dict[str, int] = {v: k for k, v in TYPE_ID_TO_CODE.items()}


# API-name as returned by `GET /types/list-vehicles-type` → our short code.
# Useful when we receive the API's `vehicleType` string and want our code.
API_NAME_TO_CODE: dict[str, str] = {
    "PC": "PC",
    "CV": "CV",
    "Motorcycle": "MOTO",
    "LCV": "LCV",
    "DriverCab": "DCAB",
    "Axle": "AXLE",
    "Engine": "ENG",
    "Bus": "BUS",
    "Aftermarket": "AFT",
    "Tractor": "TRAC",
    "Virtual OEM": "VOEM",
}
