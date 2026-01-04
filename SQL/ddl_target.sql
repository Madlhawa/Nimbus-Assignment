CREATE OR REPLACE TABLE `project-ad7287ce-8651-4583-931.nimbus.central_london_ev_charging_points_target`
(
  -- Identifiers
  station_id INT64 OPTIONS(description="The unique identifier for the physical station location"),
  connection_id INT64 OPTIONS(description="The unique identifier for a specific plug at a station"),
  uuid STRING,
  
  -- Technical Details
  connection_type STRING,
  power_kw FLOAT64,
  voltage INT64,
  amps INT64,
  charger_speed_category STRING OPTIONS(description="Slow, Fast, Rapid, or Ultra-Rapid based on kW"),
  
  -- Status
  status STRING,
  is_operational BOOL,
  
  -- Location Details
  location_name STRING,
  address STRING,
  town STRING,
  postcode STRING,
  postcode_district STRING OPTIONS(description="The outcode/sector for geospatial aggregation"),
  latitude FLOAT64,
  longitude FLOAT64,
  
  -- Usage and Commercials
  usage_type STRING,
  raw_usage_cost STRING,
  is_free_to_use BOOL,

  -- CDC / Tracking Flags
  source_deleted_flag BOOL OPTIONS(description="True if this point was missing from the latest API extract"),
  inserted_at TIMESTAMP OPTIONS(description="When this record was first created in the target table"),
  updated_at TIMESTAMP OPTIONS(description="When this record was last modified or validated by the MERGE process")
)
-- Partition by date for cost-efficient historical analysis
PARTITION BY DATE(inserted_at)
-- Cluster by location or operator for faster queries
CLUSTER BY postcode_district, status;