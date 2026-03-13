WITH
  -- 1. LOAD MAP & ASSIGN WEIGHT LIMITS
  location_map AS (
    SELECT
      location_id,
      SUBSTR(location_id, 1, 5) AS bay_id,
      SUBSTR(location_id, 6, 2) AS level_id,
      LB_zone, 
      SAFE_CAST(LB_aisle AS INT64) AS aisle_int,
      CASE 
        WHEN (LB_zone = 'ZONE_A' AND SAFE_CAST(LB_aisle AS INT64) BETWEEN 10 AND 20)
          OR (LB_zone = 'ZONE_B' AND SAFE_CAST(LB_aisle AS INT64) BETWEEN 30 AND 40) 
          THEN 'Region_Front'
        WHEN LB_zone = 'ZONE_A' AND SAFE_CAST(LB_aisle AS INT64) BETWEEN 80 AND 90 
          THEN 'Region_Back'
        ELSE 'Unknown'
      END AS working_region,
      SAFE_CAST(length AS FLOAT64) AS rack_length,
      SAFE_CAST(width AS FLOAT64) AS rack_width,
      SAFE_CAST(height AS FLOAT64) AS rack_height_limit,
      CASE
        WHEN SUBSTR(location_id, 6, 2) = '01' THEN 9999999 
        WHEN description IN ('HEAVY_DUTY_RACK', 'OVERSIZE_RACK') THEN 6000
        ELSE 4000
      END AS bay_weight_limit
    FROM `your-gcp-project-ops.capacity_management.XY_EmptyLocations`
    WHERE wh_id = '999'
      AND (
        (LB_zone = 'ZONE_B' AND SAFE_CAST(LB_aisle AS INT64) BETWEEN 30 AND 40)
        OR (LB_zone = 'ZONE_A' AND SAFE_CAST(LB_aisle AS INT64) BETWEEN 10 AND 20)
        OR (LB_zone = 'ZONE_A' AND SAFE_CAST(LB_aisle AS INT64) BETWEEN 80 AND 90))
  ),

  -- 2. INVENTORY, HEIGHTS & WEIGHTS
  inventory_data AS (
    SELECT
      s.item_number,
      s.location_id,
      map.bay_id,
      map.level_id,
      map.LB_zone,
      map.aisle_int,
      map.working_region,
      s.actual_qty,
      map.rack_height_limit,
      map.bay_weight_limit,
      (s.actual_qty * d.unit_weight) + 40 AS pallet_weight, -- Generic 40lb pallet weight
      
      -- DYNAMIC TI-HI LOGIC
      (
        CEIL(
          s.actual_qty / (
            GREATEST(
              1,
              CASE 
                WHEN d.unit_length > map.rack_length THEN 0 
                WHEN FLOOR(60 / d.unit_length) < 1 THEN 1  
                WHEN FLOOR(72 / d.unit_length) >= 2 THEN LEAST(FLOOR(72 / d.unit_length), FLOOR(map.rack_length / d.unit_length))
                ELSE LEAST(FLOOR(60 / d.unit_length), FLOOR(map.rack_length / d.unit_length))
              END
            )
            * 
            GREATEST(
              1,
              CASE
                WHEN FLOOR(48 / d.unit_width) >= 2 THEN FLOOR(48 / d.unit_width)
                ELSE FLOOR(40 / d.unit_width)
              END
            )
          )
        ) * d.unit_height
      ) + 4 AS current_stack_height -- Generic 4" pallet base
    FROM `your-gcp-project-bulk.schema.t_stored_item` s
    JOIN location_map map ON s.location_id = map.location_id
    JOIN (
        SELECT ItemID, AVG(Height) AS unit_height, AVG(Width) AS unit_width, AVG(Length) AS unit_length, AVG(Weight) AS unit_weight 
        FROM `your-gcp-project-data.schema.tbl_item_box` GROUP BY ItemID
    ) d ON s.item_id = d.ItemID
    WHERE s.wh_id = '999'
      AND s.fifo_date < DATE_SUB(CURRENT_DATE('America/New_York'), INTERVAL 3 MONTH)
  ),

  -- 2.5: CALCULATE EXISTING BAY WEIGHTS
  current_bay_weights AS (
    SELECT bay_id, SUM(pallet_weight) AS starting_bay_weight
    FROM inventory_data
    GROUP BY bay_id
  ),

  -- 3. THE GREEDY VIRTUAL STACKER
  simulation AS (
    SELECT
      s.item_number, s.location_id AS source_loc, d.location_id AS dest_loc, d.bay_id AS dest_bay,
      s.actual_qty AS source_qty, d.actual_qty AS dest_qty,
      CAST(s.current_stack_height AS INT64) AS height_source, CAST(d.current_stack_height AS INT64) AS height_dest_start,
      d.rack_height_limit, d.bay_weight_limit, cbw.starting_bay_weight, s.pallet_weight AS moving_weight,
      ABS(s.aisle_int - d.aisle_int) AS distance_penalty,
      
      SUM(s.current_stack_height) OVER (
          PARTITION BY d.location_id ORDER BY ABS(s.aisle_int - d.aisle_int) ASC, s.actual_qty ASC, s.location_id
      ) AS cumulative_added_height,
      
      SUM(s.pallet_weight) OVER (
          PARTITION BY d.bay_id ORDER BY ABS(s.aisle_int - d.aisle_int) ASC, s.actual_qty ASC, s.location_id
      ) AS cumulative_added_bay_weight

    FROM inventory_data s
    JOIN inventory_data d ON s.item_number = d.item_number
      AND s.working_region = d.working_region 
      AND (s.actual_qty < d.actual_qty OR (s.actual_qty = d.actual_qty AND s.location_id < d.location_id))
    JOIN current_bay_weights cbw ON d.bay_id = cbw.bay_id
  )

-- 4. FINAL EXECUTION LIST & ROUTING
SELECT
  item_number, source_loc AS Move_From, dest_loc AS Move_To, source_qty AS Qty_Moving, dest_qty AS Qty_At_Destination,
  distance_penalty AS Aisle_Distance, height_dest_start AS Height_Before, height_source AS Height_Adding,
  (height_dest_start + cumulative_added_height) AS Predicted_Total_Height, rack_height_limit AS Limit_at_Dest,
  moving_weight AS Weight_Moving, starting_bay_weight AS Dest_Bay_Starting_Weight,
  (starting_bay_weight + cumulative_added_bay_weight) AS Predicted_Total_Bay_Weight, bay_weight_limit AS Limit_at_Dest_Bay,
  'APPROVED' AS Decision
FROM simulation
WHERE (height_dest_start + cumulative_added_height) < rack_height_limit
  AND (starting_bay_weight + cumulative_added_bay_weight) <= bay_weight_limit
QUALIFY ROW_NUMBER() OVER (PARTITION BY source_loc ORDER BY distance_penalty ASC, dest_qty DESC, dest_loc) = 1
ORDER BY dest_loc, Predicted_Total_Height;
