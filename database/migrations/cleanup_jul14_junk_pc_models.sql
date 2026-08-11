-- Residue from reset_phantom_cv_bus_dump_state.sql: 8 more junk PC-typed model rows
-- were seeded by a feat/pc run on 2026-07-14 (zero vehicles, not on the PC list) and
-- escaped the previous cleanup's created_at >= 2026-08-10 filter. Expected: 8 deleted.

BEGIN;

DELETE FROM rapid_api_models m
USING rapid_api_vehicle_types vt
WHERE vt.vehicle_type_id = m.vehicle_type_id
  AND vt.type_code = 'PC'
  AND NOT EXISTS (SELECT 1 FROM rapid_api_vehicles v WHERE v.model_id = m.model_id)
  AND NOT EXISTS (SELECT 1 FROM rapid_api_dump_targets pt
                  WHERE pt.vehicle_type_id = 1
                    AND pt.tec_model_id = m.models_external_id);

DELETE FROM rapid_api_manufacturer_vehicle_types j
USING rapid_api_vehicle_types vt
WHERE vt.vehicle_type_id = j.vehicle_type_id
  AND vt.type_code = 'PC'
  AND NOT EXISTS (SELECT 1 FROM rapid_api_models m
                  WHERE m.manufacturer_id = j.manufacturer_id
                    AND m.vehicle_type_id = j.vehicle_type_id);

COMMIT;

-- Post-check. Expect 1: the lone legit PC-list model that genuinely has no vehicles.
SELECT COUNT(*) AS zero_vehicle_pc_models
FROM rapid_api_models m
JOIN rapid_api_vehicle_types vt ON vt.vehicle_type_id = m.vehicle_type_id
WHERE vt.type_code = 'PC'
  AND NOT EXISTS (SELECT 1 FROM rapid_api_vehicles v WHERE v.model_id = m.model_id);
