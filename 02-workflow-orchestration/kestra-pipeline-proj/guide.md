# NYC Taxi Data Pipeline with Kestra

This guide walks through setting up a data pipeline using Kestra to process NYC Taxi trip data.

## Prerequisites

- Docker and Docker Compose installed
- Basic understanding of SQL and YAML
- Git installed

## Project Setup

1. Clone the Data Engineering Zoomcamp repository:
```bash
git clone https://github.com/DataTalksClub/data-engineering-zoomcamp.git
```

2. Navigate to the workflow orchestration section and follow the [local setup instructions](https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/02-workflow-orchestration#6-additional-resources)

## Project Structure

```
kestra-pipeline-proj/
├── flows/
│   └── 02_postgres_taxi.yaml    # Main pipeline definition
└── README.md                    # This file
```

## Pipeline Overview

This pipeline demonstrates:
1. Data extraction from HTTP REST API
2. Data transformation using Python
3. Loading data into PostgreSQL
4. Data deduplication and merging

### Key Components

1. **Dynamic Inputs**
   - Taxi type (yellow/green)
   - Year (2019/2020)
   - Month (01-12)

2. **Main Tasks**
   - `set_label`: Store metadata
   - `extract`: Download and decompress data
   - `if_yellow_taxi`/`if_green_taxi`: Conditional processing
   - `purge_files`: Cleanup

3. **Database Operations**
   - Create tables
   - Load data
   - Add unique IDs
   - Merge data

## Pipeline Implementation

### 1. Dynamic Input Configuration
```yaml
inputs:
  - id: taxi
    type: SELECT
    displayName: Select taxi type
    values: [yellow, green]
    defaults: yellow
  
  - id: year
    type: SELECT
    displayName: Select year
    values: ["2019", "2020"]
    defaults: "2019"
  
  - id: month
    type: SELECT
    displayName: Select month
    values: ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    defaults: "01"
```

### 2. Variable Definitions
```yaml
variables:
  file: "{{inputs.taxi}}_tripdata_{{inputs.year}}-{{inputs.month}}.csv"
  staging_table: "public.{{inputs.taxi}}_tripdata_staging"
  table: "public.{{inputs.taxi}}_tripdata"
  data: "{{outputs.extract.outputFiles[inputs.taxi ~ '_tripdata_' ~ inputs.year ~ '-' ~ inputs.month ~ '.csv']}}"
```

### 3. Data Extraction Task
```yaml
- id: extract
  type: io.kestra.plugin.scripts.shell.Commands
  outputFiles:
    - "*.csv"
  taskRunner:
    type: io.kestra.plugin.core.runner.Process
  commands:
    - wget -qO- https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{{inputs.taxi}}/{{render(vars.file)}}.gz | gunzip > {{render(vars.file)}}
```

### 4. Data Processing Example (Green Taxi)
```yaml
# Create staging table
- id: green_create_staging_table
  type: io.kestra.plugin.jdbc.postgresql.Queries
  sql: |
    CREATE TABLE IF NOT EXISTS {{render(vars.staging_table)}} (
      unique_row_id          text,
      filename               text,
      VendorID              text,
      lpep_pickup_datetime  timestamp,
      # ... other columns ...
    );

# Add unique identifiers
- id: green_add_unique_id_and_filename
  type: io.kestra.plugin.jdbc.postgresql.Queries
  sql: |
    UPDATE {{render(vars.staging_table)}}
    SET 
      unique_row_id = md5(
        COALESCE(CAST(VendorID AS text), '') ||
        COALESCE(CAST(lpep_pickup_datetime AS text), '') || 
        # ... other fields for uniqueness ...
      ),
      filename = '{{render(vars.file)}}';
```

### 5. Data Merging Example
```sql
MERGE INTO {{render(vars.table)}} AS T
USING {{render(vars.staging_table)}} AS S
ON T.unique_row_id = S.unique_row_id
WHEN NOT MATCHED THEN
  INSERT (unique_row_id, filename, /* other columns */)
  VALUES (S.unique_row_id, S.filename, /* other values */);
```

## Pipeline Flow Diagram

```mermaid
graph LR
    A[Start] --> B[Input Selection]
    B --> C{Select Taxi Type}
    
    subgraph "Yellow Taxi Process"
        C -->|Yellow| D1[Extract Data]
        D1 -->|CSV| F1[Create Tables]
        F1 --> G1[Load to Staging]
        G1 --> H1[Add Unique IDs]
        H1 --> I1[Merge to Main]
    end
    
    subgraph "Green Taxi Process"
        C -->|Green| D2[Extract Data]
        D2 -->|CSV| F2[Create Tables]
        F2 --> G2[Load to Staging]
        G2 --> H2[Add Unique IDs]
        H2 --> I2[Merge to Main]
    end
    
    I1 --> J[Purge Files]
    I2 --> J
    J --> K[End]

    %% Data flow annotations
    D1 -.->|"yellow_tripdata_*.csv"| G1
    D2 -.->|"green_tripdata_*.csv"| G2
```

## Running the Pipeline

1. Start Kestra and PostgreSQL:
```bash
docker-compose up -d
```

2. Access Kestra UI:
```
http://localhost:8080
```

3. Create new trigger with following parameters:
   - Namespace: `zc`
   - Flow: `02_postgres_taxi`
   - Select inputs as needed

## Database Configuration

PostgreSQL connection details (configured in YAML):
```yaml
url: jdbc:postgresql://host.docker.internal:5432/postgres-zc
username: kestra
password: k3str4
```

## Data Schema

### Yellow Taxi
```sql
-- Main columns
VendorID               text
tpep_pickup_datetime   timestamp
tpep_dropoff_datetime  timestamp
passenger_count        integer
trip_distance          double precision
-- ... other columns ...

-- Added columns for deduplication
unique_row_id          text    -- MD5 hash of key fields
filename               text    -- Source file tracking
```

### Green Taxi
```sql
-- Main columns
VendorID               text
lpep_pickup_datetime   timestamp
lpep_dropoff_datetime  timestamp
passenger_count        integer
trip_distance          double precision
-- ... other columns ...

-- Added columns for deduplication
unique_row_id          text    -- MD5 hash of key fields
filename               text    -- Source file tracking
```

## Understanding COALESCE Usage

In our pipeline, we use COALESCE for generating unique row IDs. Here's why:

```sql
unique_row_id = md5(
    COALESCE(CAST(VendorID AS text), '') ||
    COALESCE(CAST(pickup_datetime AS text), '') || 
    COALESCE(CAST(dropoff_datetime AS text), '') ||
    -- ...other fields
)
```

### Why COALESCE?

1. **NULL Handling**: COALESCE returns the first non-NULL value in a list
   - If VendorID is NULL → returns empty string ''
   - Prevents NULL values from breaking the concatenation

2. **Data Quality**:
   - Ensures consistent unique ID generation even with missing data
   - Empty string is better than NULL for concatenation

3. **Deduplication Logic**:
   - Creates consistent hashes for same trips
   - Handles both clean and messy data reliably

Example:
```sql
-- Without COALESCE
'123' || NULL || '456'  → NULL

-- With COALESCE
'123' || COALESCE(NULL, '') || '456'  → '123456'
```

## Useful Links

- [NYC TLC Trip Data](https://github.com/DataTalksClub/nyc-tlc-data/releases)
- [Kestra Documentation](https://kestra.io/docs)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)

## Notes

- The pipeline uses staging tables to prevent data corruption
- Deduplication is handled via MD5 hashes of key fields
- Data is merged using PostgreSQL's MERGE statement

## Scheduled Pipeline Implementation

The scheduled version (`02_postgres_taxi_scheduled.yaml`) automates the data ingestion process with the following modifications:

### Key Changes from Base Pipeline

1. **Simplified Input Parameters**
   - Removed manual year/month selections
   - Only taxi type remains as input parameter
   - Date parameters derived from trigger timing

2. **Dynamic Date Variables**
   ```yaml
   variables:
     file: "{{inputs.taxi}}_tripdata_{{trigger.date | date('yyyy-MM')}}.csv"
   ```

3. **Concurrency Control**
   ```yaml
   concurrency:
     limit: 1  # Prevents parallel execution issues
   ```

4. **Automated Scheduling**
   ```yaml
   triggers:
     - id: green_schedule
       type: io.kestra.plugin.core.trigger.Schedule
       cron: "0 9 1 * *"  # 9 AM, 1st of each month
       inputs:
         taxi: green

     - id: yellow_schedule
       type: io.kestra.plugin.core.trigger.Schedule
       cron: "0 10 1 * *"  # 10 AM, 1st of each month
       inputs:
         taxi: yellow
   ```

### Scheduled Flow Diagram

```mermaid
%%{init: {
    'theme': 'base',
    'themeVariables': {
        'primaryColor': '#b3e5fc',
        'secondaryColor': '#ffccbc',
        'tertiaryColor': '#c8e6c9',
        'noteTextColor': '#1a237e'
    }
}}%%
graph TD
    classDef trigger fill:#e1bee7,stroke:#4a148c,stroke-width:2px
    classDef task fill:#bbdefb,stroke:#0d47a1,stroke-width:2px
    classDef storage fill:#ffccbc,stroke:#bf360c,stroke-width:2px
    classDef condition fill:#fff9c4,stroke:#f57f17,stroke-width:2px

    T1[Green Taxi Trigger\n9 AM Monthly]:::trigger --> S1[Set Labels]:::task
    T2[Yellow Taxi Trigger\n10 AM Monthly]:::trigger --> S1
    
    S1 --> E[Extract Data]:::task
    E --> C{Taxi Type?}:::condition
    
    C -->|Green| G1[Create Tables]:::task
    G1 --> G2[Load Staging]:::task
    G2 --> G3[Add Unique IDs]:::task
    G3 --> G4[Merge Data]:::task
    
    C -->|Yellow| Y1[Create Tables]:::task
    Y1 --> Y2[Load Staging]:::task
    Y2 --> Y3[Add Unique IDs]:::task
    Y3 --> Y4[Merge Data]:::task
    
    G4 --> P[Purge Files]:::task
    Y4 --> P
    
    G4 --> DB[(PostgreSQL)]:::storage
    Y4 --> DB
    
    subgraph "Concurrency Control"
        direction LR
        L1[Limit: 1]
    end
    
    %% File handling annotations
    E -.->|Download| F1[CSV File]
    F1 -.->|Process| G2
    F1 -.->|Process| Y2
    F1 -.->|Clean up| P
```

### Backfilling Data

To process historical data:

1. From Kestra UI:
   - Navigate to scheduled flow
   - Click "Execute"
   - Add label `backfill:true`
   - Set appropriate trigger date
   - Execute flow

2. Monitor backfill progress:
   - Check execution status
   - Verify data in PostgreSQL
   - Confirm proper deduplication

### Schedule Management

1. **Execution Order**
   - Green taxi data: 9 AM on 1st of month
   - Yellow taxi data: 10 AM on 1st of month
   - One hour gap prevents resource contention

2. **Concurrency Control**
   - Single execution at a time
   - Prevents data conflicts
   - Queues concurrent triggers


