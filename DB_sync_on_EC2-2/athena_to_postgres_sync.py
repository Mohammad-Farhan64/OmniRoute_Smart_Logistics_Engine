import logging
import pandas as pd
from pyathena import connect
from sqlalchemy import create_engine, text
from datetime import datetime
import traceback
import sys
import os
import time

# =========================
# LOAD CONFIG
# =========================

sys.path.append('/home/ubuntu/enterprise_reporting_sync/config')

from config import *

# =========================
# LOGGING CONFIG
# =========================

LOG_PATH = '/home/ubuntu/enterprise_reporting_sync/logs'

os.makedirs(LOG_PATH, exist_ok=True)

logging.basicConfig(
    filename=f'{LOG_PATH}/sync.log',
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s'
)

# =========================
# JOB START
# =========================

job_start_time = datetime.now()

logging.info("=" * 100)
logging.info("ENTERPRISE REPORTING SYNC STARTED")

print("=" * 100)
print("ENTERPRISE REPORTING SYNC STARTED")

# =========================
# POSTGRES CONNECTION
# =========================

postgres_engine = create_engine(
    "postgresql+psycopg2://farhan:Farhan%40123@localhost:5432/telemetry_db",
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10
)

# =========================
# ATHENA CONNECTION
# =========================

athena_conn = connect(
    s3_staging_dir=S3_STAGING_DIR,
    region_name=ATHENA_REGION
)

# =========================
# SYNC PROCESS
# =========================

for athena_view, postgres_table in TABLE_MAPPING.items():

    table_start_time = datetime.now()

    retry_count = 3

    for attempt in range(1, retry_count + 1):

        try:

            logging.info("-" * 100)
            logging.info(f"STARTING TABLE SYNC: {athena_view}")
            logging.info(f"TARGET TABLE: {postgres_table}")
            logging.info(f"ATTEMPT: {attempt}")

            print(f"\nSYNCING: {athena_view}")

            # =========================
            # ATHENA QUERY
            # =========================

            query = f"""
            SELECT *
            FROM {ATHENA_DATABASE}.{athena_view}
            """

            # =========================
            # READ ATHENA DATA
            # =========================

            df = pd.read_sql(query, athena_conn)

            fetched_rows = len(df)

            logging.info(f"ROWS FETCHED: {fetched_rows}")

            print(f"Fetched Rows: {fetched_rows}")

            # =========================
            # DATA QUALITY CHECKS
            # =========================

            duplicate_count = df.duplicated().sum()

            null_count = int(df.isnull().sum().sum())

            logging.info(f"DUPLICATE ROWS DETECTED: {duplicate_count}")

            logging.info(f"NULL VALUES DETECTED: {null_count}")

            print(f"Duplicate Rows: {duplicate_count}")

            print(f"NULL Values: {null_count}")

            # =========================
            # REMOVE DUPLICATES
            # =========================

            before_dedup = len(df)

            df = df.drop_duplicates()

            after_dedup = len(df)

            removed_duplicates = before_dedup - after_dedup

            logging.info(
                f"DUPLICATES REMOVED: {removed_duplicates}"
            )

            # =========================
            # EMPTY DATAFRAME CHECK
            # =========================

            if df.empty:

                logging.warning(
                    f"EMPTY DATAFRAME RECEIVED FOR {athena_view}"
                )

                print(f"WARNING: Empty dataframe for {athena_view}")

                continue

            # =========================
            # REFRESH POSTGRES TABLE
            # =========================

            logging.info(
                f"REFRESHING POSTGRES TABLE: {postgres_table}"
            )

            with postgres_engine.begin() as conn:

                # DROP OLD TABLE
                conn.execute(
                    text(f"DROP TABLE IF EXISTS {postgres_table}")
                )

                # RECREATE TABLE FROM ATHENA DATA
                df.to_sql(
                    postgres_table,
                    conn,
                    if_exists='append',
                    index=False,
                    method='multi',
                    chunksize=1000
                )

            final_row_count = len(df)

            logging.info(
                f"POSTGRES REFRESH SUCCESSFUL: {postgres_table}"
            )

            logging.info(
                f"FINAL ROW COUNT INSERTED: {final_row_count}"
            )

            table_end_time = datetime.now()

            logging.info(
                f"TABLE EXECUTION TIME: "
                f"{table_end_time - table_start_time}"
            )

            print(f"SUCCESS: {postgres_table}")

            break

        except Exception as e:

            logging.error("=" * 100)
            logging.error(f"FAILED TABLE: {athena_view}")
            logging.error(f"ATTEMPT NUMBER: {attempt}")
            logging.error(str(e))
            logging.error(traceback.format_exc())

            print(f"FAILED: {athena_view}")
            print(str(e))

            if attempt < retry_count:

                logging.warning(
                    f"RETRYING TABLE: {athena_view}"
                )

                print(f"Retrying {athena_view}...")

                time.sleep(5)

            else:

                logging.critical(
                    f"MAX RETRIES EXCEEDED: {athena_view}"
                )

# =========================
# FINAL METRICS
# =========================

job_end_time = datetime.now()

total_execution_time = job_end_time - job_start_time

logging.info("=" * 100)
logging.info("ENTERPRISE REPORTING SYNC COMPLETED")
logging.info(f"TOTAL EXECUTION TIME: {total_execution_time}")
logging.info("=" * 100)

print("=" * 100)
print("SYNC COMPLETED")
print(f"TOTAL EXECUTION TIME: {total_execution_time}")
print("=" * 100)