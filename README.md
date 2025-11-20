# ETL Sales Target Pipeline

## Overview

ETL pipeline untuk memindahkan data sales target dari Oracle Database ke PostgreSQL menggunakan Apache Airflow. Pipeline ini menangani ekstraksi, transformasi, dan loading data dengan mekanisme error handling dan validation yang robust.

## Architecture

```
Oracle Database (Source) → Apache Airflow → PostgreSQL (Target)
      ↓
APPS.KHS_CUSTOMERS_TRX_TARGETS → mb.sales_target
```

## Features

### 🔄 **Data Pipeline**
- **Incremental Loading** - Daily scheduled extraction
- **Bulk Processing** - Chunk-based processing (10,000 records per batch)
- **Data Validation** - Row count verification between source and target
- **Transaction Safety** - Atomic commits dengan rollback capability

### 🛡️ **Error Handling & Validation**
- Oracle client initialization validation
- Source table existence check
- Row count mismatch detection
- Connection management dengan proper cleanup
- Comprehensive logging dan monitoring

### 🔧 **Technical Specifications**
- **Source**: Oracle Database (APPS schema)
- **Target**: PostgreSQL (mb schema)
- **Batch Size**: 10,000 records
- **Schedule**: Daily at midnight
- **Retry Policy**: 1 retry dengan 5 minutes delay

## Requirements

### System Dependencies
```bash
# Oracle Instant Client
/opt/oracle/instantclient/
  ├── libclntsh.so
  └── libocci.so

# Environment Variables
LD_LIBRARY_PATH=/opt/oracle/instantclient
ORACLE_HOME=/opt/oracle/instantclient
```

### Python Dependencies
```python
apache-airflow>=2.0.0
cx_Oracle>=8.0.0
SQLAlchemy>=1.4.0
psycopg2-binary>=2.9.0
```

## Airflow Connections

### Oracle Connection (`oracle_conn_id`)
```yaml
conn_type: oracle
host: oracle-server-host
port: 1521
schema: service_name
login: username
password: password
```

### PostgreSQL Connection (`postgres_conn_id`)
```yaml
conn_type: postgres
host: postgres-server-host
port: 5432
schema: database_name
login: username
password: password
```

## Database Schema

### Source Table (Oracle)
```sql
-- APPS.KHS_CUSTOMERS_TRX_TARGETS
COLUMNS:
  id NUMBER
  product_id NUMBER
  target_month NUMBER
  target_year NUMBER
  target_amount NUMBER
```

### Target Table (PostgreSQL)
```sql
-- mb.sales_target
CREATE TABLE IF NOT EXISTS sales_target (
    id NUMERIC,
    product_id NUMERIC,
    target_month NUMERIC,
    target_year NUMERIC,
    target_amount NUMERIC
);
```

## Pipeline Execution Flow

### 1. **Initialization Phase**
```python
# Initialize Oracle client dengan library validation
init_oracle_client()

# Verify source table existence
verify_table_exists(connection, "APPS", "KHS_CUSTOMERS_TRX_TARGETS")
```

### 2. **Extraction Phase**
```python
# Stream results dengan chunking
result = connection.execute(
    text("SELECT * FROM APPS.KHS_CUSTOMERS_TRX_TARGETS"),
    stream_results=True,
    yield_per=10000
)
```

### 3. **Loading Phase**
```python
# Batch insertion dengan transaction management
INSERT INTO sales_target (id, product_id, target_month, target_year, target_amount)
VALUES (:id, :product_id, :target_month, :target_year, :target_amount)
```

### 4. **Validation Phase**
```python
# Row count verification
oracle_count = total_count
postgres_count = transferred_count

if oracle_count != postgres_count:
    raise ValueError("Row count mismatch detected")
```

## Error Handling Mechanisms

### Connection Errors
- Oracle client library validation
- Database connection timeout handling
- Connection cleanup in finally block

### Data Integrity Errors
- Source table existence check
- Row count verification
- Transaction rollback on failure

### Performance Issues
- Stream results untuk large datasets
- Batch processing untuk memory management
- Chunk-based insertion

## Monitoring & Logging

### Key Log Points
- Oracle client initialization status
- Connection establishment
- Row count statistics
- Batch processing progress
- Validation results
- Error details dengan stack traces

### Performance Metrics
- Total processing time
- Records processed per second
- Memory usage monitoring
- Network transfer rates

## Deployment

### Airflow DAG Configuration
```python
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_sales_target',
    schedule_interval='0 0 * * *',  # Daily at midnight
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'oracle', 'postgres', 'sales_target']
)
```

### Environment Setup
1. Install Oracle Instant Client
2. Configure environment variables
3. Set up Airflow connections
4. Deploy DAG to Airflow server

## Troubleshooting

### Common Issues
1. **Oracle Client Not Found**
   - Verify Instant Client installation path
   - Check library file permissions
   - Validate environment variables

2. **Connection Timeouts**
   - Verify network connectivity
   - Check database availability
   - Validate connection credentials

3. **Row Count Mismatch**
   - Check for concurrent modifications
   - Verify table permissions
   - Validate data types compatibility

### Debug Mode
Enable detailed logging untuk troubleshooting:
```python
logging.basicConfig(level=logging.DEBUG)
```

## Maintenance

### Regular Checks
- Monitor Airflow task execution history
- Review error logs untuk pattern detection
- Validate data consistency between source and target
- Update connection credentials as needed

### Performance Optimization
- Adjust chunk size berdasarkan system resources
- Monitor database performance metrics
- Optimize query performance dengan indexing

## Security Considerations

- Credential management melalui Airflow Connections
- Network security antara source dan target databases
- Data encryption in transit
- Access control untuk database resources

---

**ETL Sales Target Pipeline** - Robust data integration solution untuk daily sales target synchronization antara Oracle dan PostgreSQL dengan comprehensive error handling dan performance optimization.
