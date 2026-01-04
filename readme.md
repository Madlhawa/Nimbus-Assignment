# EV Charging Points Data Pipeline

(Please go through the [Assingment_Explanation.pdf](./Assignment_Explanation.pdf) file for the assignment evaluation.)

A data pipeline for extracting, transforming, and loading Electric Vehicle (EV) charging point data from the OpenChargeMap API into Google BigQuery. This project implements a daily ETL pipeline using Apache Airflow, Google Cloud Storage, and BigQuery with comprehensive data quality checks and change data capture (CDC) capabilities.


## Project Overview

(Please go through the [Assingment_Explanation.pdf](./Assignment_Explanation.pdf) file for the assignment evaluation.)

This pipeline extracts EV charging station data for Central London from the OpenChargeMap API, transforms it to meet business requirements, and loads it into a BigQuery data warehouse. The solution follows best practices for data engineering, including:

- **ETL Pipeline**: Automated daily extraction, transformation, and loading
- **Data Quality**: Comprehensive validation checks at multiple stages
- **CDC**: Change Data Capture with soft delete capabilities
- **Scalability**: Cloud-native architecture using Google Cloud Platform
- **Monitoring**: Data quality checks and error handling

## Architecture

```
OpenChargeMap API → Python Extraction → Google Cloud Storage (GCS)
                                                 ↓
                                          BigQuery Landing Table
                                                 ↓
                                          Data Quality Checks
                                                 ↓
                                          BigQuery Target Table
                                                 ↓
                                          Archive to GCS
                                                 ↓
                                          Power BI Analysis
```

## Project Structure

```
.
├── Dags/                              # Apache Airflow DAG definitions
│   ├── ev_charging_points_daily.py   # Main DAG orchestration
│   └── logic.py                       # Extraction and transformation logic
├── SQL/                               # BigQuery SQL scripts
│   ├── ddl_landing.sql               # Landing table schema
│   ├── ddl_target.sql                # Target table schema
│   └── sp_landing_to_target_ev_data.sql  # Transformation stored procedure
├── Data/                              # Sample data files
│   └── ev_charging_data_normalized.csv
├── Analysis.pbix                      # Power BI dashboard
└── readme.md                          # This file
```

## Data Flow

### 1. Extraction (`Dags/logic.py`)
- Fetches EV charging points data from OpenChargeMap API
- Filters by Central London coordinates (51.51°N, -0.09°W) with 5km radius
- Normalizes nested JSON structure into flat DataFrame
- Saves normalized CSV to Google Cloud Storage

**Key Fields Extracted:**
- Unique identifier per charging point (ID, UUID)
- Latitude/Longitude coordinates
- Operator/Network information
- Charger type and specifications (power, voltage, amps)
- Status (operational vs. others)
- Connection details

### 2. Landing Stage (`SQL/ddl_landing.sql`)
- Raw data loaded into BigQuery landing table
- Write disposition: WRITE_TRUNCATE (full refresh)
- All source fields preserved for audit trail
- Includes `insert_datetime` timestamp for tracking

### 3. Data Quality Checks
**Landing Stage Checks:**
- Null validation: ID and conn_ID must not be null
- Coordinate validation: Latitude between 50.0-53.0, Longitude between -0.5-0.5
- Duplicate detection: No duplicate connection IDs

**Target Stage Checks:**
- Null validation: station_id must not be null
- Power validation: power_kw must be >= 0

### 4. Transformation (`SQL/sp_landing_to_target_ev_data.sql`)
The stored procedure performs:

- **Deduplication**: Uses ROW_NUMBER() to keep latest record per station/connection
- **Geographic Filtering**: Validates coordinates within Central London bounds
- **Business Logic Transformations**:
  - Charger speed categorization (Slow ≤3.7kW, Fast ≤22kW, Rapid ≤50kW, Ultra-Rapid >50kW)
  - Free usage detection from usage cost strings
  - Postcode district extraction using regex
  - Type conversions and data type casting

- **CDC (Change Data Capture)**:
  - MERGE operation (INSERT/UPDATE)
  - Soft delete: Sets `source_deleted_flag = TRUE` for records missing from source
  - Timestamp tracking: `inserted_at` and `updated_at` fields

### 5. Target Table (`SQL/ddl_target.sql`)
- Clean, business-ready schema
- Partitioned by `inserted_at` date for cost optimization
- Clustered by `postcode_district` and `status` for query performance
- Includes CDC tracking flags

## Key Features

### Data Quality
- ✅ Multi-stage validation (landing and target)
- ✅ Coordinate bounds checking
- ✅ Null value detection
- ✅ Duplicate prevention
- ✅ Type safety with explicit casting

### Change Data Capture (CDC)
- ✅ MERGE-based upsert logic
- ✅ Soft delete tracking (`source_deleted_flag`)
- ✅ Temporal tracking (`inserted_at`, `updated_at`)
- ✅ Historical data preservation

### Performance Optimization
- ✅ BigQuery table partitioning by date
- ✅ Clustering by postcode_district and status
- ✅ Efficient MERGE operations
- ✅ Parallel data quality checks

### Operational Excellence
- ✅ Daily scheduled execution
- ✅ Error handling and retry logic
- ✅ Data archiving to GCS
- ✅ Comprehensive logging

## Setup and Configuration

### Prerequisites
- Google Cloud Platform account
- Apache Airflow (Cloud Composer recommended)
- BigQuery dataset access
- Google Cloud Storage bucket
- OpenChargeMap API key

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd "Nimbus Assignment"
   ```

2. **Set up Google Cloud Resources**
   - Create a GCS bucket
   - Create a BigQuery dataset named `nimbus`
   - Set up Cloud Composer environment (for Airflow)

3. **Configure Airflow Variables**
   Set the OpenChargeMap API key in Airflow Variables:
   ```bash
   # In Airflow UI or via CLI
   airflow variables set openchargemap_api_key "your-api-key-here"
   ```

4. **Deploy SQL Scripts**
   Execute SQL scripts in BigQuery console in this order:
   1. `SQL/ddl_landing.sql`
   2. `SQL/ddl_target.sql`
   3. `SQL/sp_landing_to_target_ev_data.sql`

5. **Update Configuration**
   Edit `Dags/ev_charging_points_daily.py` with your project details:
   ```python
   BUCKET_NAME = 'your-gcs-bucket-name'
   PROJECT_ID = 'your-gcp-project-id'
   DATASET_ID = 'nimbus'
   ```

6. **Deploy DAG**
   Copy the `Dags/` folder to your Airflow DAGs directory or Cloud Composer DAGs bucket

### Dependencies

**Python Packages:**
- `apache-airflow>=2.0.0`
- `pandas>=1.3.0`
- `requests>=2.25.0`
- `google-cloud-bigquery`
- `google-cloud-storage`

**Airflow Providers:**
- `apache-airflow-providers-google>=8.0.0`

## Usage

### Running the Pipeline

The pipeline runs automatically on a daily schedule (`@daily`). To trigger manually:

1. **Via Airflow UI:**
   - Navigate to the Airflow web interface
   - Find the `ev_charging_points_daily` DAG
   - Click "Trigger DAG"

2. **Via Airflow CLI:**
   ```bash
   airflow dags trigger ev_charging_points_daily
   ```

### Monitoring

Monitor pipeline execution through:
- **Airflow UI**: Task status, logs, and Gantt charts
- **BigQuery Console**: Query target table for data validation
- **GCS Console**: Check archived files in `archive/` prefix

### Querying the Data

**Sample Queries:**

```sql
-- Count operational charging points by speed category
SELECT 
    charger_speed_category,
    COUNT(*) as count
FROM `project-ad7287ce-8651-4583-931.nimbus.central_london_ev_charging_points_target`
WHERE is_operational = TRUE
GROUP BY charger_speed_category
ORDER BY count DESC;

-- Find charging points by postcode district
SELECT 
    postcode_district,
    COUNT(*) as charging_points,
    COUNT(DISTINCT station_id) as unique_stations
FROM `project-ad7287ce-8651-4583-931.nimbus.central_london_ev_charging_points_target`
WHERE postcode_district IS NOT NULL
GROUP BY postcode_district
ORDER BY charging_points DESC;

-- Track changes over time
SELECT 
    DATE(inserted_at) as date,
    COUNT(*) as new_records,
    SUM(CASE WHEN source_deleted_flag = TRUE THEN 1 ELSE 0 END) as deleted_records
FROM `project-ad7287ce-8651-4583-931.nimbus.central_london_ev_charging_points_target`
GROUP BY date
ORDER BY date DESC;
```

## Data Schema

### Landing Table Schema
The landing table (`central_london_ev_charging_points_landing`) contains raw, unprocessed data with all source fields preserved. Key fields include:
- Connection details (ID, Type, Power, Voltage, Amps)
- Status information
- Address information (Title, Address, Town, Postcode, Coordinates)
- Operator and usage type information

### Target Table Schema
The target table (`central_london_ev_charging_points_target`) contains clean, transformed data optimized for analytics:

| Field | Type | Description |
|-------|------|-------------|
| `station_id` | INT64 | Unique identifier for the physical station |
| `connection_id` | INT64 | Unique identifier for a specific plug |
| `connection_type` | STRING | Type of connection (e.g., "Type 2", "CCS") |
| `power_kw` | FLOAT64 | Charging power in kilowatts |
| `charger_speed_category` | STRING | Categorized speed: Slow/Fast/Rapid/Ultra-Rapid |
| `status` | STRING | Operational status |
| `is_operational` | BOOL | Whether the charger is operational |
| `location_name` | STRING | Name of the charging location |
| `address` | STRING | Full address |
| `postcode_district` | STRING | Extracted postcode district for aggregation |
| `latitude` / `longitude` | FLOAT64 | Geographic coordinates |
| `is_free_to_use` | BOOL | Whether charging is free |
| `source_deleted_flag` | BOOL | CDC flag for soft deletes |
| `inserted_at` / `updated_at` | TIMESTAMP | CDC timestamps |

## Pipeline Tasks

The Airflow DAG consists of the following tasks:

1. **run_ev_extraction**: Extracts data from OpenChargeMap API and saves to GCS
2. **load_to_bigquery_landing**: Loads CSV from GCS to BigQuery landing table
3. **check_landing_columns**: Validates data quality (nulls, coordinates)
4. **check_landing_duplicates**: Checks for duplicate records
5. **transform_to_target**: Executes stored procedure to transform data
6. **check_target_data_quality**: Validates transformed data
7. **archive_to_gcs**: Archives processed CSV file with date suffix

## Visualization

The `Analysis.pbix` Power BI file provides interactive dashboards for:
- Geographic distribution of charging points
- Charger speed category analysis
- Operational status tracking
- Postcode district breakdown
- Temporal trends and changes

## Troubleshooting

### Common Issues

**API Key Error:**
- Verify the API key is set in Airflow Variables
- Check API key validity at OpenChargeMap website

**BigQuery Permission Errors:**
- Ensure service account has BigQuery Data Editor and Job User roles
- Verify dataset and table permissions

**Data Quality Check Failures:**
- Review landing table data for anomalies
- Check coordinate ranges and null values
- Validate source API data quality

**GCS Access Errors:**
- Verify bucket permissions
- Check service account has Storage Admin role
- Ensure bucket name is correct in configuration

## Future Enhancements

- [ ] Add data lineage tracking
- [ ] Implement alerting for pipeline failures
- [ ] Add data profiling reports
- [ ] Implement incremental processing options
- [ ] Add unit tests for transformation logic
- [ ] Expand geographic coverage beyond Central London
- [ ] Add data freshness monitoring

## License

This project is part of a technical assignment.

## Contact

For questions or issues, please refer to the project documentation or contact the development team.
