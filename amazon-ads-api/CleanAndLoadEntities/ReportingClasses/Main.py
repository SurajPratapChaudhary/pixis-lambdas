import glob
import os
import shutil
import time
import random
from datetime import datetime
from pathlib import Path
from typing import Tuple

import numpy as np
import pandas as pd
import dask.dataframe as dask_df
import pyarrow.parquet as Parquet
import pyarrow as pa
import json
import psycopg2

from Helpers.GoogleCloud import GoogleCloud
from Helpers.LoadEntitiesToBigquery import LoadEntitiesToBigquery
# from includes.redis_helper import MyRedis
from ReportingClasses.BaseFileProcessor import BaseFileProcessor

from includes.reports import *
from includes.bugsnag_helper import *
from includes.constants import Constant


class AdsApiEntitiesProcessor(BaseFileProcessor):
    def __init__(self, s3_file_path: str, incoming_bucket_name: str):
        super().__init__(s3_file_path, incoming_bucket_name)

        # The following is the setter of Bigquery Schema
        entitiesConfig = TableConfig(Constant.TBL_TYPE_ENTITY, self.ad_type, self.entity_type)
        self.schema = entitiesConfig.getSchema()
        self.load_method = entitiesConfig.getLoadMethod()
        self.dateFormats = entitiesConfig.getDateFormats()
        self.row_count = 0

    @staticmethod
    def append_to_parquet_table(df, filepath=None, writer=None):
        table = pa.Table.from_pandas(df)
        if writer is None:
            writer = Parquet.ParquetWriter(filepath, table.schema)
        writer.write_table(table=table)
        return writer

    def printlog(self, msg):
        log_prefix = "_".join([
            self.client_id,
            self.profileId,
            self.ad_type,
            self.entity_type
        ])
        print(f"{log_prefix} ___ {msg}")

    def clean_data(self) -> Tuple[bool, str, str]:
        df = None
        encoding_error = ''
        encodings = ['utf-8', 'latin1', 'ISO-8859-1', 'cp1252']
        for _encoding in encodings:
            try:
                incoming_path = Path(self.incoming_local_filepath)
                incoming_file_ext = incoming_path.suffix.lower()
                if not incoming_file_ext in ['.gz', '.json', '.csv']:
                    notify_bugsnag(f"INPUT_FORMAT_UNSUPPORTED -> {self.s3_file_path}")
                    return False, 'INPUT_FORMAT_UNSUPPORTED', ''

                # Prepare output file paths
                output_base_path = incoming_path.parent
                timestamp = str(int(time.time())).replace(".", "_")
                parquet_file_name = incoming_path.with_name(incoming_path.stem).stem
                final_file_folder_path = output_base_path / parquet_file_name
                parquet_file_path = output_base_path / f"{parquet_file_name}_{timestamp}.parquet"

                if incoming_file_ext == '.gz' or incoming_file_ext == '.json':
                    compression = 'gzip' if incoming_file_ext == '.gz' else None
                    self.printlog(f"reading file with encoding [{_encoding}]")
                    df = dask_df.read_json(
                        self.incoming_local_filepath,
                        compression=compression,
                        encoding=_encoding,
                        lines=False
                    )
                elif incoming_file_ext == '.csv':
                    df = dask_df.read_csv(self.incoming_local_filepath)

                self.row_count = df.shape[0].compute()  # This computes the number of rows

                self.printlog("file loaded in df successfully")

                df = df.rename(columns=df_columns_new_name_mapping(df.columns))
                self.printlog("columns renamed successfully")

                # df['marketplace'] = self.marketplace
                df['last_updated'] = pd.Timestamp(datetime.now())
                profileIdIndex = 'advertiserId' if self.ad_type == 'DSP' else 'profileId'
                self.printlog(f"profileIdIndex is {profileIdIndex}")
                df[profileIdIndex] = self.profileId
                self.printlog("s3 key added as df columns successfully")

                # Normalize nested fields for entities BEFORE type casting (generic)
                df = self._normalize_entity_dataframe(df)

                df = self.type_cast_each_field(df=df)
                self.printlog("df type casted successfully")

                # Branch behavior based on DB_CONNECTION
                db_conn = os.getenv('DB_CONNECTION', 'pg').lower()
                if db_conn == 'pg':
                    # Write chunked CSVs and merge into a single CSV file
                    csv_file_path = output_base_path / f"{parquet_file_name}_{timestamp}.csv"
                    df.to_csv(str(final_file_folder_path / "part-*.csv"), index=False)
                    part_files = [f for f in glob.glob(str(final_file_folder_path) + "/*.csv", recursive=True)]
                    if len(part_files) == 0:
                        self.printlog("No CSV parts were produced; treating as empty output.")
                        shutil.rmtree(final_file_folder_path, ignore_errors=True)
                        return True, "CLEANED_SUCCESSFULLY", str(csv_file_path)
                    with open(csv_file_path, "w", encoding="utf-8") as out_f:
                        first = None
                        for p in part_files:
                            with open(p, "r", encoding="utf-8") as pf:
                                if first is None:
                                    # write header + data for first part
                                    shutil.copyfileobj(pf, out_f)
                                    first = False
                                else:
                                    # skip header line, then append remainder
                                    _ = pf.readline()
                                    shutil.copyfileobj(pf, out_f)
                            os.remove(p)
                    shutil.rmtree(final_file_folder_path, ignore_errors=True)
                    self.printlog(f"CSV file created at {csv_file_path}")
                    return True, "CLEANED_SUCCESSFULLY", str(csv_file_path)
                else:
                    # Default: write parquet chunks and merge to single parquet file for BigQuery
                    df.to_parquet(final_file_folder_path, engine='pyarrow', write_metadata_file=False)
                    self.printlog("df converted to parquet successfully")

                    parquetFiles = [f for f in glob.glob(str(final_file_folder_path) + "/*", recursive=True)]
                    if len(parquetFiles) >= 1:
                        writer = None
                        for parquetFile in parquetFiles:
                            parquetFileDF = pd.read_parquet(parquetFile)
                            writer = self.append_to_parquet_table(parquetFileDF, parquet_file_path, writer)
                            os.remove(parquetFile)
                        if writer:
                            writer.close()
                    shutil.rmtree(final_file_folder_path)

                    self.printlog("all parquet files merged successfully")

                    return True, "CLEANED_SUCCESSFULLY", str(parquet_file_path)

            except Exception as _exp:
                error_str = str(_exp)
                if "codec can't decode" in error_str or "codec cant decode" in error_str:
                    encoding_error = error_str
                    self.printlog(f"Encoding [{_encoding}] Failed. Retrying another one...")
                else:
                    notify_bugsnag(_exp)
                    return False, str(_exp), ''

        # if all encodings failed.
        if encoding_error != '':
            encodings_list = "] , [".join(encodings)
            error_msg = f"Cleaning FAILED. All encodings [{encodings_list}] Failed. ---> {encoding_error}"
            notify_bugsnag(error_msg)
            return False, error_msg, ''

    def type_cast_each_field(self, df):

        new_columns = []
        for df_column in df.columns:
            target_type = self.schema.get(df_column, None)

            if target_type is None:
                new_columns.append(df_column)
                continue
            try:
                target_type = target_type.lower()
                if target_type in ['int64', 'int']:
                    df.replace('', np.nan)
                    df[df_column] = df[df_column].fillna(0).astype('int64')
                elif target_type in ['float', 'float64']:
                    df.replace('', np.nan)
                    df[df_column] = df[df_column].fillna(0.0).astype('float64')
                elif target_type in ['str', 'string', 'varchar']:
                    df[df_column] = df[df_column].fillna('').astype(str)
                elif target_type in ['datetime', 'date', 'timestamp']:
                    dateFormat = self.dateFormats.get(target_type, None)
                    df[df_column] = df[df_column].map(
                        lambda x: None if x in ['none', 'null', ''] else pd.to_datetime(x, format=dateFormat),
                        meta=(df_column, 'datetime64[ns]'))
                elif target_type == 'bool':
                    df[df_column] = df[df_column].fillna(False).astype(bool)
            except Exception as e:
                self.printlog(f"Error converting column '{df_column}' to type '{target_type}': {e}")
                break
        if new_columns:
            new_columns_str = ",".join(new_columns)
            print(f"New Columns Found: [{new_columns_str}]")
            # Drop unknown columns so downstream loaders (BQ/PG) don't fail on schema mismatch
            try:
                df = df.drop(columns=new_columns, errors='ignore')
            except TypeError:
                # dask < 2023 may not support errors kw; fallback
                df = df.drop(columns=[c for c in new_columns if c in df.columns])
        return df

    def _normalize_entity_dataframe(self, df):
        # 1) Flatten extendedData across all entities if present
        if 'extendedData' in df.columns:
            def _get_from_ext(x, key):
                try:
                    return None if x is None else x.get(key)
                except Exception:
                    return None

            df['creationDateTime'] = df['extendedData'].map(
                lambda x: _get_from_ext(x, 'creationDateTime'), meta=('creationDateTime', 'object')
            )
            df['lastUpdateDateTime'] = df['extendedData'].map(
                lambda x: _get_from_ext(x, 'lastUpdateDateTime'), meta=('lastUpdateDateTime', 'object')
            )
            df['servingStatus'] = df['extendedData'].map(
                lambda x: _get_from_ext(x, 'servingStatus'), meta=('servingStatus', 'object')
            )

            def _details_to_str(x):
                try:
                    details = None if x is None else x.get('servingStatusDetails')
                    return None if details is None else json.dumps(details)
                except Exception:
                    return None

            df['servingStatusDetails'] = df['extendedData'].map(
                _details_to_str, meta=('servingStatusDetails', 'object')
            )
            df = df.drop(columns=['extendedData'])

        # 2) Entity-specific: campaign budget flattening
        if self.entity_type == Constant.ADS_CAMPAIGN and 'budget' in df.columns:
            budget_series = df['budget']

            def _get_budget_amt(x):
                try:
                    if x is None:
                        return None
                    val = x.get('budget')
                    return float(val) if val is not None else None
                except Exception:
                    return None

            def _get_budget_type(x):
                try:
                    return None if x is None else x.get('budgetType')
                except Exception:
                    return None

            df['budgetType'] = budget_series.map(_get_budget_type, meta=('budgetType', 'object'))
            df['budget'] = budget_series.map(_get_budget_amt, meta=('budget', 'float64'))

        if self.entity_type == Constant.ADS_PORTFOLIO and 'budget' in df.columns:
            budget_series = df['budget']

            def _get_policy(x):
                try:
                    return None if x is None else x.get('policy')
                except Exception:
                    return None

            def _get_currency_code(x):
                try:
                    return None if x is None else x.get('currencyCode')
                except Exception:
                    return None

            df['policy'] = budget_series.map(_get_policy, meta=('policy', 'object'))
            df['currencyCode'] = budget_series.map(_get_currency_code, meta=('currencyCode', 'object'))
            df = df.drop(columns=['budget'])

        # 3) Generic: stringify any complex values for STRING schema columns
        def _to_json_str(val):
            try:
                if isinstance(val, (list, dict)):
                    return json.dumps(val)
                return val if (val is None or isinstance(val, (str, int, float, bool))) else str(val)
            except Exception:
                return None

        for col, dtype in self.schema.items():
            try:
                if col in df.columns and isinstance(dtype, str) and dtype.strip().upper() == 'STRING':
                    df[col] = df[col].map(_to_json_str, meta=(col, 'object'))
            except Exception:
                # best-effort; skip columns that fail mapping
                pass

        return df

    def load_csv_to_postgres(self, csv_file_path: str):
        """
        Load a local CSV file into Postgres using a staging table + COPY, then merge into target.
        Assumes the target table already exists (created by CreateTable lambda) and has a schema
        compatible with the CSV header (business columns; id is auto-generated).
        """
        table_cfg = TableConfig(Constant.TBL_TYPE_ENTITY, self.ad_type, self.entity_type)
        schema_name = table_cfg.getDataset()
        target_table = f"{table_cfg.getTableName()}_{self.client_id}"
        staging_table = f'tmp_{datetime.now().strftime("%Y%m%d")}_{target_table}__stg_{int(time.time())}_{random.randint(100, 99999)}'

        db_host = os.getenv("DB_HOST")
        db_port = int(os.getenv("DB_PORT", "5432"))
        db_name = os.getenv("DB_DATABASE")
        db_user = os.getenv("DB_USERNAME")
        db_pass = os.getenv("DB_PASSWORD")

        if not all([db_host, db_name, db_user]):
            self.printlog("Postgres credentials are incomplete; skipping COPY load.")
            return

        # Read header to build column list for COPY (exclude auto id if present)
        with open(csv_file_path, "r", encoding="utf-8") as f:
            header = f.readline().strip()
            if not header:
                self.printlog("CSV appears empty; skipping COPY load.")
                return
            columns = [c.strip() for c in header.split(",") if c.strip() and c.strip().lower() != "id"]

        self.printlog(f"Starting staged load into {schema_name}.{target_table}")
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_pass,
        )
        try:
            with conn, conn.cursor() as cur:
                cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"')
                # Create temporary staging table with same structure as target in session temp schema
                cur.execute(f'CREATE TEMP TABLE "{staging_table}" (LIKE "{schema_name}"."{target_table}" INCLUDING ALL) ON COMMIT DROP')
                # Build COPY column list from TableConfig schema (normalized names), preserve CSV order and exclude id
                allowed_cols = list(table_cfg.getSchema().keys())
                allowed_cols_wo_id = [c for c in allowed_cols if c.lower() != 'id']
                copy_columns = [c for c in columns if c in allowed_cols_wo_id]
                cols_sql = ", ".join([f'"{c}"' for c in copy_columns])
                copy_sql = f'COPY "{staging_table}" ({cols_sql}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER \',\');'
                with open(csv_file_path, "r", encoding="utf-8") as data_f:
                    cur.copy_expert(copy_sql, data_f)
                self.printlog("COPY into staging table completed successfully.")
                # Merge staged rows into target
                self.merge_into_postgres(cur, schema_name, staging_table, target_table, columns, table_cfg)
        finally:
            conn.close()

    def merge_into_postgres(self, cur, target_schema_name: str, staging_table: str, target_table: str, columns: list, table_cfg: 'TableConfig'):
        """
        Merge rows from staging table into target table.
        Strategy:
          - If TableConfig.loadMethod == 'merge': UPSERT (ON CONFLICT DO UPDATE) using merge columns.
          - Else: DELETE using business keys + INSERT from staging (snapshot overwrite).
        """
        load_method = (table_cfg.getLoadMethod() or "").lower()
        key_cols = [c for c in (table_cfg.getMergeColumns() or []) if c in columns]
        # Build column lists
        insert_cols_sql = ", ".join([f'"{c}"' for c in columns])
        non_key_cols = [c for c in columns if c not in set(key_cols)]

        # If no keys available, fall back to simple INSERT ALL
        if not key_cols or load_method != "merge":
            # Snapshot-style: delete matching by keys if available, then insert
            if key_cols:
                join_cond = " AND ".join([f't."{c}" = s."{c}"' for c in key_cols])
                delete_sql = f'DELETE FROM "{target_schema_name}"."{target_table}" t USING "{staging_table}" s WHERE {join_cond}'
                self.printlog(f"Executing DELETE with join on keys: {key_cols}")
                cur.execute(delete_sql)
            insert_sql = f'INSERT INTO "{target_schema_name}"."{target_table}" ({insert_cols_sql}) SELECT {insert_cols_sql} FROM "{staging_table}"'
            self.printlog("Executing INSERT from staging to target")
            cur.execute(insert_sql)
            return

        # Ensure a unique index exists for upsert keys (optional but recommended)
        idx = f'{target_table}_bk_uniq'
        idx_cols = ", ".join([f'"{c}"' for c in key_cols])
        try:
            cur.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS "{idx}" ON "{target_schema_name}"."{target_table}" ({idx_cols})')
        except Exception as e:
            self.printlog(f"Warning: could not ensure unique index for upsert keys {key_cols}: {e}")

        # Build ON CONFLICT DO UPDATE clause
        if non_key_cols:
            set_clause = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in non_key_cols])
        else:
            # No non-key columns -> do nothing on conflict
            set_clause = ""

        upsert_sql = (
            f'INSERT INTO "{target_schema_name}"."{target_table}" ({insert_cols_sql}) '
            f'SELECT {insert_cols_sql} FROM "{staging_table}" '
            f'ON CONFLICT ({idx_cols}) '
            + (f'DO UPDATE SET {set_clause}' if set_clause else 'DO NOTHING')
        )
        self.printlog(f"Executing UPSERT with keys {key_cols}")
        cur.execute(upsert_sql)

    def process(self):

        self.download_s3_file()
        clean_success, clean_response, cleaned_filepath = self.clean_data()

        if clean_success:
            if os.getenv('DB_CONNECTION', 'pg').lower() == 'pg':
                self.printlog(f"DB_CONNECTION is pg. Skipping GCS/BQ path.")
                self.printlog(f"cleaned_filepath: {cleaned_filepath}")
                try:
                    self.load_csv_to_postgres(cleaned_filepath)
                except Exception as e:
                    self.printlog(f"Postgres COPY failed: {e}")
            else:
                self.printlog(f"DB_CONNECTION is not pg. Loading in BQ.")
                self.printlog(f"Cleaning SUCCESSFUL. Parquet file created [{cleaned_filepath}] .")
                google_cloud_obj = GoogleCloud()
                gcs_cleaned_filepath = f"{self.s3_file_path.split('.')[0]}.parquet"
                upload_success, upload_response = google_cloud_obj.upload_file_to_gcs(
                    local_file_path=cleaned_filepath,
                    gcs_file_path=gcs_cleaned_filepath
                )

                if upload_success:
                    self.printlog("parquet file uploaded to gcs successfully.")

                    if self.load_method == 'redis':
                        print("Redis load_method is under construction....")
                    else:
                        if self.row_count > 0:
                            gcs_uri = f"gs://{google_cloud_obj.bucket}/{gcs_cleaned_filepath}"
                            loadEntitiesToBQ = LoadEntitiesToBigquery(
                                gcs_file_path=gcs_uri,
                                marketplace=self.marketplace,
                                client_id=self.client_id,
                                profileId=self.profileId,
                                entity_filename=self.s3_file_path,
                                entity_type=self.entity_type,
                                ad_type=self.ad_type
                            )
                            loadEntitiesToBQ.run()
                        else:
                            self.printlog(f"File IS EMPTY. No need to load in BQ.")
        else:
            self.printlog(f"Cleaning FAILED: {clean_response}")
