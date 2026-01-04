CREATE OR REPLACE PROCEDURE `project-ad7287ce-8651-4583-931.nimbus.sp_transform_ev_data`()
BEGIN
  MERGE `project-ad7287ce-8651-4583-931.nimbus.central_london_ev_charging_points_target` AS T
  USING (
    -- Deduplicate and validate landing data
    SELECT * EXCEPT(row_num)
    FROM (
      SELECT *, 
             ROW_NUMBER() OVER (PARTITION BY ID, conn_ID ORDER BY insert_datetime DESC) as row_num
      FROM `project-ad7287ce-8651-4583-931.nimbus.central_london_ev_charging_points_landing`
      WHERE AddressInfo_Latitude BETWEEN 51.2 AND 51.7
        AND AddressInfo_Longitude BETWEEN -0.5 AND 0.3
    ) WHERE row_num = 1
  ) AS S
  ON T.station_id = S.ID AND T.connection_id = S.conn_ID

  -- Update existing records
  WHEN MATCHED THEN
    UPDATE SET 
      T.connection_type = S.conn_ConnectionType_Title,
      T.power_kw = CAST(S.conn_PowerKW AS FLOAT64),
      T.voltage = CAST(S.conn_Voltage AS INT64),
      T.amps = CAST(S.conn_Amps AS INT64),
      T.charger_speed_category = CASE 
          WHEN S.conn_PowerKW <= 3.7 THEN 'Slow'
          WHEN S.conn_PowerKW <= 22 THEN 'Fast'
          WHEN S.conn_PowerKW <= 50 THEN 'Rapid'
          ELSE 'Ultra-Rapid' 
      END,
      T.status = COALESCE(S.conn_StatusType_Title, 'Unknown'),
      T.is_operational = CAST(S.conn_StatusType_IsOperational AS BOOL),
      T.location_name = S.AddressInfo_Title,
      T.address = S.AddressInfo_AddressLine1,
      T.town = S.AddressInfo_Town,
      T.postcode = S.AddressInfo_Postcode,
      T.postcode_district = REGEXP_EXTRACT(S.AddressInfo_Postcode, r'^([A-Z]{1,2}[0-9R][0-9A-Z]?)'),
      T.latitude = S.AddressInfo_Latitude,
      T.longitude = S.AddressInfo_Longitude,
      T.usage_type = S.UsageType_Title,
      T.raw_usage_cost = S.UsageCost,
      T.is_free_to_use = CASE WHEN LOWER(S.UsageCost) LIKE '%free%' OR S.UsageCost = '0' THEN TRUE ELSE FALSE END,
      T.source_deleted_flag = FALSE,
      T.updated_at = CURRENT_TIMESTAMP()

  -- Insert new records
  WHEN NOT MATCHED THEN
    INSERT (
      station_id, connection_id, uuid, connection_type, power_kw, voltage, amps, 
      charger_speed_category, status, is_operational, location_name, address, 
      town, postcode, postcode_district, latitude, longitude, usage_type, 
      raw_usage_cost, is_free_to_use, source_deleted_flag, inserted_at, updated_at
    )
    VALUES (
      S.ID, S.conn_ID, S.UUID, S.conn_ConnectionType_Title, S.conn_PowerKW, 
      CAST(S.conn_Voltage AS INT64), CAST(S.conn_Amps AS INT64),
      CASE 
          WHEN S.conn_PowerKW <= 3.7 THEN 'Slow'
          WHEN S.conn_PowerKW <= 22 THEN 'Fast'
          WHEN S.conn_PowerKW <= 50 THEN 'Rapid'
          ELSE 'Ultra-Rapid' 
      END,
      S.conn_StatusType_Title, S.conn_StatusType_IsOperational, S.AddressInfo_Title, 
      S.AddressInfo_AddressLine1, S.AddressInfo_Town, S.AddressInfo_Postcode, 
      REGEXP_EXTRACT(S.AddressInfo_Postcode, r'^([A-Z]{1,2}[0-9R][0-9A-Z]?)'),
      S.AddressInfo_Latitude, S.AddressInfo_Longitude, S.UsageType_Title, 
      S.UsageCost,
      CASE WHEN LOWER(S.UsageCost) LIKE '%free%' OR S.UsageCost = '0' THEN TRUE ELSE FALSE END,
      FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
    )

  -- Soft delete records missing from source
  WHEN NOT MATCHED BY SOURCE THEN
    UPDATE SET 
      T.source_deleted_flag = TRUE,
      T.updated_at = CURRENT_TIMESTAMP();
END;