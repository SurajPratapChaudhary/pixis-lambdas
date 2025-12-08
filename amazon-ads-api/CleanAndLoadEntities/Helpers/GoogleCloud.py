import logging
import time
from typing import Union, List, Any

from pandas import DataFrame

from Helpers.UtilHelper import Utils, ProjectConfigManager, BugsnagHelper
from google.cloud import bigquery, storage

from includes.constants import Constant


class GoogleCloud(object):

    def __init__(self, credential_file=Constant.GOOGLE_CLOUD_CREDENTIALS):
        self.project_name = Utils.get_env("GOOGLE_CLOUD_PROJECT_ID", None)
        self.bucket = Utils.get_env('GOOGLE_CLOUD_BUCKET', '')
        self.credentials_json_file_path = ProjectConfigManager.get_project_file_or_folder_path(
            file_or_dir_flag='GOOGLE_CREDENTIALS'
        )
        self.bq_client = bigquery.Client.from_service_account_json(str(self.credentials_json_file_path))
        self.gcs_client = storage.Client.from_service_account_json(str(self.credentials_json_file_path))

    def get_table_name(self, table_name, dataset_id, project=False):
        project = self.project_name if project else None
        return self.get_qualified_table_name(
            table_name=table_name,
            dataset_id=dataset_id,
            project=project
        )

    @staticmethod
    def get_qualified_table_name(table_name, dataset_id, project=None):
        return f"{project}.{dataset_id}.{table_name}" if project is not None else f"{dataset_id}.{table_name}"

    @staticmethod
    def check_if_retry_error_message(error_message):
        errors = [
            'ConnectionResetError',
            'Exceeded rate limits',
            'too many table update operations for this table',
            'Retrying may solve the problem',
            'Too many DML statements',
            'Quota exceeded',
            'table exceeded quota for imports or query appends per table'
            'table exceeded quota for Number of partition modifications',
            'Resources exceeded',
            'concurrent update',
            'An internal error occurred'
        ]
        for error in errors:
            if error.lower() in error_message.lower():
                return True
        return False

    def execute_bq_query(self, query, max_retries=10, backoff_factor=15):
        """
        Executes a BigQuery query and automatically retries on failure.

        Parameters:
        - bq_client (google.cloud.bigquery.client.Client): The BigQuery client.
        - query (str): SQL query string to be executed.
        - max_retries (int): Maximum number of retries on failure.
        - backoff_factor (int): Backoff factor for retries (in seconds).

        Returns:
        - tuple: A tuple containing a boolean indicating success and the query result or error message.
        """
        for attempt in range(1, max_retries + 1):
            try:
                query_job = self.bq_client.query(query)
                rows = query_job.result()
                return True, rows
            except Exception as e:
                error_message = str(e)
                if self.check_if_retry_error_message(error_message):
                    if attempt == max_retries:
                        return False, f"Query failed after {attempt} attempts: {error_message}"
                    print(
                        f"Attempt {attempt} failed, retrying in {backoff_factor * attempt} seconds. Error: {error_message}")
                    time.sleep(backoff_factor * attempt)
                else:
                    return False, f"Query failed after {attempt} attempts: {error_message}"
        return False, "Query failed after all retries."

    def run_raw_query(self, query: str, result_formation: str = 'df') -> Union[
        DataFrame, List[Any], bigquery.table.RowIterator]:
        """
        Executes a BigQuery SQL query and returns the result in the specified format.

        Parameters:
        - bq_client (google.cloud.bigquery.client.Client): The BigQuery client.
        - query (str): The SQL query string to be executed.
        - format (str): The format of the query result. Options are 'df' for DataFrame, 'list' for a list of
            rows, or any other string for a RowIterator object.

        Returns:
        - Union[DataFrame, List[Any], bigquery.table.RowIterator]: The query result in the specified format.
        """
        query_job = self.bq_client.query(query)
        result = query_job.result()

        if result_formation == 'df':
            return result.to_dataframe()
        elif result_formation == 'list':
            return list(result)
        else:
            return result

    def upload_file_to_gcs(self, local_file_path, gcs_file_path, max_retries=5, backoff_factor=15):
        """
        Uploads a file to Google Cloud Storage, with retry logic on failure.

        Parameters:
        - gcs_client (google.cloud.storage.client.Client): The GCS client.
        - bucket_name (str): The name of the GCS bucket.
        - local_file_path (str): The path to the local file to be uploaded.
        - gcs_file_path (str): The target path in the GCS bucket.
        - max_retries (int): Maximum number of retry attempts.
        - backoff_factor (int): Backoff factor for retry attempts (in seconds).

        Returns:
        - bool: True if the upload was successful, False otherwise.
        - str: A success message or an error message.
        """
        for attempt in range(1, max_retries + 1):
            try:
                print(f"Uploading {local_file_path} to GCS (Attempt {attempt})")
                logging.info(f"Uploading {local_file_path} to GCS (Attempt {attempt})")
                bucket = self.gcs_client.bucket(self.bucket)
                blob = bucket.blob(gcs_file_path)
                blob.upload_from_filename(local_file_path)
                return True, "File uploaded successfully."
            except Exception as e:
                error_message = str(e)
                print(f"Attempt {attempt} failed: {error_message}")
                logging.info(f"Attempt {attempt} failed: {error_message}")
                if attempt < max_retries:
                    sleep_time = backoff_factor * attempt
                    print(f"Retrying in {sleep_time} seconds...")
                    logging.info(f"Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                else:
                    return False, f"Upload failed after {attempt} attempts: {error_message}"

    def load_parquet_in_table(self, uris, table_name, table_schema='', dataset_id=''):
        bugsnag_on_retry = False
        success = True
        response = "success"
        table_ref = self.bq_client.dataset(dataset_id).table(table_name)
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        job_config.source_format = bigquery.SourceFormat.PARQUET
        job_config.autodetect = True
        job_config.schema_update_options = [
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
        ]
        if table_schema != "":
            job_config.schema = table_schema

        tries = 0
        max_tries = 10
        retry = True
        while retry:
            try:
                destination_table = self.bq_client.get_table(table_ref)
                current_rows_count = destination_table.num_rows
                load_job = self.bq_client.load_table_from_uri(
                    uris,
                    table_ref,
                    job_config=job_config
                )
                print("Starting job {} for table [ {} ]".format(load_job.job_id, table_name))
                flag = ''
                while flag != 'DONE':
                    job = self.bq_client.get_job(load_job.job_id)
                    flag = job.state
                    print('this status of job', job.state)
                    time.sleep(2)
                load_job.result()
                success = True
                response = "success"
                retry = False
                destination_table = self.bq_client.get_table(table_ref)
                print("Success in BQ Table Load --> [ {} ] ({} rows added) ".format(
                    table_name, destination_table.num_rows - current_rows_count))
            except Exception as e:
                error_str = str(e)
                success = False
                retry = self.check_if_retry_error_message(error_str)
                if retry:
                    tries = tries + 1
                    if tries <= max_tries:
                        response = "Retrying={} in {} sec. Error [ {} ] in Load parquet [{}]".format(tries, 15 * tries,
                                                                                                     error_str,
                                                                                                     table_name)
                        print(response)
                        if bugsnag_on_retry:
                            BugsnagHelper.notify_bugsnag(response, {
                                'table': table_name,
                                'uri': uris
                            })
                        time.sleep(15 * tries)
                    else:
                        retry = False
                        response = "Failed after {} tries. Error [ {} ] in Load parquet [{}]".format(tries,
                                                                                                     error_str,
                                                                                                     table_name)
                        print(response)
                        # notify_bugsnag(response, {
                        #     'table': table_name,
                        #     'uri': uris
                        # })
                else:
                    response = "Error Ads-report ETL - [ {} ] in Load flatten_json [{}]".format(
                        error_str,
                        table_name
                    )
                    print(response)
                    BugsnagHelper.notify_bugsnag(response, {
                        'table': table_name,
                        'uri': uris
                    })

        if success:
            print(
                "Ads-report ETL Load Job finished with {} for table [ {} ]".format("Success" if success else "Error",
                                                                                     table_name))
        else:
            success = False
        return success, response
