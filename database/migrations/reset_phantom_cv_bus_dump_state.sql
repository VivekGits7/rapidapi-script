-- Repair phantom completions from the 2026-08-10/11 runs on the old feat/pc code.
-- That code claimed CV (type 2) and BUS (type 8) targets with no vehicle-type filter,
-- seeded their models under the PC type, fetched PC vehicles for truck/bus models
-- (empty), and marked all of them complete with zero data.
--
-- Expected results:
--   step 1: 713 CV targets  -> pending   (complete but no CV model row)
--   step 2: 234 BUS targets -> pending   (all phantom; bus phase is parked anyway)
--   step 3:   6 CV targets  -> resumable (real data, but 159 vehicles left uncrawled
--             from the old top-5 cap; MAX_VEHICLES_PER_MODEL=0 now wants them all)
--   step 4: 734 junk PC-typed model rows deleted (zero vehicles, created 08-10/11)
--   step 5: PC junction rows for makes left with no PC models deleted

BEGIN;

-- 1) Phantom CV targets: complete but no CV-typed model row exists for them
UPDATE rapid_api_dump_targets t
SET status = 'pending', started_at = NULL, completed_at = NULL,
    last_error = NULL, updated_at = NOW()
WHERE t.vehicle_type_id = 2
  AND t.status = 'complete'
  AND NOT EXISTS (
      SELECT 1
      FROM rapid_api_models m
      JOIN rapid_api_vehicle_types vt ON vt.vehicle_type_id = m.vehicle_type_id
      WHERE vt.vehicle_types_external_id = 2
        AND m.models_external_id = t.tec_model_id
  );

-- 2) All BUS targets: every one was phantom-completed in the same sweep
UPDATE rapid_api_dump_targets
SET status = 'pending', started_at = NULL, completed_at = NULL,
    last_error = NULL, updated_at = NOW()
WHERE vehicle_type_id = 8;

-- 3) Real CV targets whose model still has uncrawled vehicles (old top-5 cap leftovers)
UPDATE rapid_api_dump_targets t
SET status = 'resumable', completed_at = NULL, updated_at = NOW()
WHERE t.vehicle_type_id = 2
  AND t.status = 'complete'
  AND EXISTS (
      SELECT 1
      FROM rapid_api_models m
      JOIN rapid_api_vehicle_types vt ON vt.vehicle_type_id = m.vehicle_type_id
      JOIN rapid_api_manufacturers mf ON mf.manufacturer_id = m.manufacturer_id
      JOIN rapid_api_vehicles v ON v.model_id = m.model_id
      WHERE vt.vehicle_types_external_id = 2
        AND mf.manufacturers_external_id = t.tec_manufacturer_id
        AND m.models_external_id = t.tec_model_id
        AND v.dump_state = 'incomplete'
        AND v.is_fuel_excluded = FALSE
  );

-- 4) Junk PC-typed model rows seeded for CV/BUS models: zero vehicles attached, so
--    nothing cascades. The PC-list exclusion is belt and braces; a real PC model
--    already existed before August and is never matched by created_at >= 08-10.
DELETE FROM rapid_api_models m
USING rapid_api_vehicle_types vt
WHERE vt.vehicle_type_id = m.vehicle_type_id
  AND vt.type_code = 'PC'
  AND m.created_at >= '2026-08-10'
  AND NOT EXISTS (SELECT 1 FROM rapid_api_vehicles v WHERE v.model_id = m.model_id)
  AND NOT EXISTS (SELECT 1 FROM rapid_api_dump_targets pt
                  WHERE pt.vehicle_type_id = 1
                    AND pt.tec_model_id = m.models_external_id);

-- 5) PC junctions for makes that now have no PC models (CV/BUS-only brands)
DELETE FROM rapid_api_manufacturer_vehicle_types j
USING rapid_api_vehicle_types vt
WHERE vt.vehicle_type_id = j.vehicle_type_id
  AND vt.type_code = 'PC'
  AND NOT EXISTS (SELECT 1 FROM rapid_api_models m
                  WHERE m.manufacturer_id = j.manufacturer_id
                    AND m.vehicle_type_id = j.vehicle_type_id);

COMMIT;

-- Post-checks. Expect: type 1 = 508 complete; type 2 = 122 complete, 6 resumable,
-- 713 pending; type 8 = 234 pending. Junk models remaining must be 0.
SELECT vehicle_type_id, status, COUNT(*) AS n
FROM rapid_api_dump_targets GROUP BY 1, 2 ORDER BY 1, 2;

SELECT COUNT(*) AS junk_pc_models_remaining
FROM rapid_api_models m
JOIN rapid_api_vehicle_types vt ON vt.vehicle_type_id = m.vehicle_type_id
WHERE vt.type_code = 'PC'
  AND NOT EXISTS (SELECT 1 FROM rapid_api_vehicles v WHERE v.model_id = m.model_id);
