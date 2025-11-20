from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import logging
import cx_Oracle
import os
from sqlalchemy.exc import SQLAlchemyError

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def init_oracle_client():
    """Initialize Oracle Client with validation"""
    try:
        instantclient_path = "/opt/oracle/instantclient"

        if not os.path.exists(instantclient_path):
            raise FileNotFoundError(f"Oracle Instant Client not found at {instantclient_path}")

        required_files = ['libclntsh.so', 'libocci.so']
        for file in required_files:
            if not os.path.exists(os.path.join(instantclient_path, file)):
                raise FileNotFoundError(f"Required Oracle file {file} not found in {instantclient_path}")

        os.environ["LD_LIBRARY_PATH"] = instantclient_path
        os.environ["ORACLE_HOME"] = instantclient_path

        cx_Oracle.init_oracle_client(lib_dir=instantclient_path)
        logging.info(f"Oracle client initialized at {instantclient_path}")

    except Exception as e:
        logging.error(f"Oracle client initialization failed: {str(e)}", exc_info=True)
        raise

def verify_table_exists(connection, schema, table_name):
    """Check if Oracle table exists"""
    try:
        query = text("""
            SELECT COUNT(*)
            FROM all_tables
            WHERE owner = :schema
            AND table_name = :table_name
        """)
        result = connection.execute(query, {
            'schema': schema.upper(),
            'table_name': table_name.upper()
        }).scalar()
        return result > 0
    except Exception as e:
        logging.error(f"Error verifying table: {str(e)}")
        return False

def extract_and_load(**kwargs):
    """ETL: Oracle → PostgreSQL"""
    oracle_connection = None
    pg_connection = None

    try:
        init_oracle_client()

        # Oracle Connection
        oracle_conn = BaseHook.get_connection('oracle_conn_id')
        dsn = cx_Oracle.makedsn(
            host=oracle_conn.host,
            port=oracle_conn.port,
            service_name=oracle_conn.schema
        )
        oracle_uri = f"oracle+cx_oracle://{oracle_conn.login}:{oracle_conn.password}@{dsn}"
        oracle_engine = create_engine(oracle_uri)
        oracle_connection = oracle_engine.connect()

        oracle_connection.execute(text("SELECT 1 FROM DUAL"))
        logging.info("Connected to Oracle successfully")

        source_schema = "APPS"
        source_table = "KHS_CUSTOMERS_TRX_TARGETS"

        if not verify_table_exists(oracle_connection, source_schema, source_table):
            raise ValueError(f"Source table {source_schema}.{source_table} does not exist")

        # PostgreSQL Connection
        postgres_conn = BaseHook.get_connection('postgres_conn_id')
        pg_uri = f"postgresql+psycopg2://{postgres_conn.login}:{postgres_conn.password}@{postgres_conn.host}:{postgres_conn.port}/{postgres_conn.schema}"
        pg_engine = create_engine(pg_uri)
        pg_connection = pg_engine.connect()

        logging.info("Fetching data from Oracle...")

        total_count = oracle_connection.execute(
            text(f"SELECT COUNT(*) FROM {source_schema}.{source_table}")
        ).scalar()

        logging.info(f"Total rows available: {total_count}")

        if total_count == 0:
            logging.warning("No data found — ETL stopped.")
            return

        result = oracle_connection.execution_options(
            stream_results=True,
            yield_per=10000
        ).execute(text(f"SELECT * FROM {source_schema}.{source_table}"))

        columns = [col[0] for col in result.cursor.description]

        # Transaction
        trans = pg_connection.begin()

        try:
            pg_connection.execute(text("SET search_path TO mb"))

            # === NEW SALES TARGET TABLE ===
            pg_connection.execute(text("""
                CREATE TABLE IF NOT EXISTS sales_target (
                    id NUMERIC,
                    product_id NUMERIC,
                    target_month NUMERIC,
                    target_year NUMERIC,
                    target_amount NUMERIC
                )
            """))

            pg_connection.execute(text('TRUNCATE TABLE sales_target'))

            chunk_size = 10000
            batch = []
            total_rows = 0

            cols_quoted = ','.join([f'"{c}"' for c in columns])
            params = ','.join([f':{c}' for c in columns])

            insert_stmt = text(f"""
                INSERT INTO sales_target ({cols_quoted})
                VALUES ({params})
            """)

            for row in result:
                row_dict = dict(zip(columns, row))
                batch.append(row_dict)

                if len(batch) >= chunk_size:
                    pg_connection.execute(insert_stmt, batch)
                    total_rows += len(batch)
                    logging.info(f"Inserted {total_rows}/{total_count} rows")
                    batch = []

            if batch:
                pg_connection.execute(insert_stmt, batch)
                total_rows += len(batch)

            transferred_count = pg_connection.execute(
                text('SELECT COUNT(*) FROM sales_target')
            ).scalar()

            logging.info(f"Verification — PostgreSQL row count: {transferred_count}")

            if transferred_count != total_count:
                raise ValueError(
                    f"Row mismatch! Oracle: {total_count}, PostgreSQL: {transferred_count}"
                )

            trans.commit()
            logging.info("ETL completed successfully!")

        except Exception as e:
            trans.rollback()
            logging.error(f"Transaction rolled back: {str(e)}", exc_info=True)
            raise

    except Exception as e:
        logging.error(f"ETL failed: {str(e)}", exc_info=True)
        raise

    finally:
        if oracle_connection:
            oracle_connection.close()
            logging.info("Oracle connection closed")
        if pg_connection:
            pg_connection.close()
            logging.info("PostgreSQL connection closed")

start_date = datetime.now().replace(
    hour=0, minute=0, second=0, microsecond=0
) - timedelta(days=1)

dag = DAG(
    'etl_sales_target',
    default_args=default_args,
    description='ETL Sales Target from Oracle to PostgreSQL',
    schedule_interval='0 0 * * *',
    start_date=start_date,
    catchup=False,
    tags=['etl', 'oracle', 'postgres', 'sales_target']
)

extract_and_load_task = PythonOperator(
    task_id='extract_and_load_sales_target',
    python_callable=extract_and_load,
    dag=dag,
)
