import json
import argparse
from google.cloud import bigquery, storage
from dotenv import load_dotenv
from pathlib import Path
import os

from includes.reports import TableConfig
from includes.constants import Constant
import boto3
from Helpers.postgress_ops import PostgresOps

current_dir = Path(__file__).resolve().parent
current_dir = str(current_dir)
env_path = "{}/.env".format(current_dir)

if os.path.exists(env_path):
    load_dotenv(dotenv_path=env_path, verbose=True)

bq_credentials_path = "{}".format(os.getenv("GOOGLE_CLOUD_CREDENTIALS_PATH"))


class BigQuery(object):

    def __init__(self):
        self.project_id = os.getenv("GOOGLE_CLOUD_PROJECT_ID")
        self.client = bigquery.Client.from_service_account_json(bq_credentials_path)
        self.gcs_client = storage.Client.from_service_account_json(bq_credentials_path)

    def get_full_table_id(self, table_name, dataset_id, with_project_id=True):
        if with_project_id:
            return "{}.{}.{}".format(self.project_id, dataset_id, table_name)
        else:
            return "{}.{}".format(dataset_id, table_name)

    def create_bucket(self, bucket_name):
        # Initialize client
        bucket = self.gcs_client.bucket(bucket_name)
        # Create the bucket
        new_bucket = self.gcs_client.create_bucket(bucket, project=self.project_id)
        print(f"✅ Bucket '{new_bucket.name}' created in location '{new_bucket.location}'.")

    def list_buckets(self):
        buckets = self.gcs_client.list_buckets(project=self.project_id)
        print("📦 Buckets in project:")
        for bucket in buckets:
            print(f" - {bucket.name}")

    def create_table(self, dataset, tableName, schema, partitionColumn=None, clusterColumn=None):

        full_table_id = self.get_full_table_id(table_name=tableName, dataset_id=dataset)
        bq_schema = []
        for columnName, dataType in schema.items():
            bq_schema.append(bigquery.SchemaField(columnName, dataType))

        retry = True
        retry_count = 0

        while retry and retry_count < 10:
            try:
                print("Creating table {}".format(full_table_id))
                bq_table = bigquery.Table(full_table_id, schema=bq_schema)
                if partitionColumn is not None:
                    bq_table.time_partitioning = bigquery.TimePartitioning(
                        field=partitionColumn,
                        type_=bigquery.TimePartitioningType.DAY
                    )
                if clusterColumn is not None:
                    bq_table.clustering_fields = clusterColumn
                created_table = self.client.create_table(bq_table)
                print("Created table {}".format(full_table_id))
                retry = False
            except Exception as e:
                tableCreationError = str(e)
                if 'Already Exists' in tableCreationError:
                    retry = False
                    print("BQ Table {} already exists.".format(full_table_id))
                else:
                    print("Error in Creating BQ Table [{}] : {}".format(full_table_id, tableCreationError))
                    retry = self.check_if_retry_error_message(tableCreationError)
            retry_count += 1

    def fix_table_schema(self, dataset, tableName, schema):

        full_table_id = self.get_full_table_id(table_name=tableName, dataset_id=dataset)
        bq_schema = []
        for columnName, dataType in schema.items():
            bq_schema.append(bigquery.SchemaField(columnName, dataType))

        retry = True
        retry_count = 0

        original_schema = []
        new_schema = []
        table = ''

        while retry and retry_count < 10:
            try:

                table = self.client.get_table(full_table_id)  # Make an API request.

                new_schema = table.schema[:]  # Get existing schema
                original_schema = table.schema
                original_schema_cols = []
                for field in original_schema:
                    original_schema_cols.append(field.name)

                alterSchema = False
                for columnName in schema.keys():
                    if columnName not in original_schema_cols:
                        alterSchema = True
                        new_schema.append(bigquery.SchemaField(columnName, schema.get(columnName, 'STRING')))
                if alterSchema == True:
                    table.schema = new_schema
                    table = self.client.update_table(table, ["schema"])
                    print('Schema Fixed successfully for table [' + full_table_id + ']')
                else:
                    print('Schema already fixed for table [' + full_table_id + ']')
                retry = False
            except Exception as e:
                tableSchemaError = str(e)
                print("Error in Altering schema for BQ Table [{}] : {}".format(full_table_id, tableSchemaError))
                retry = self.check_if_retry_error_message(tableSchemaError)
            retry_count += 1

    def delete_table(self, table_name, dataset):
        full_table_id = self.get_full_table_id(table_name=table_name, dataset_id=dataset)
        self.client.delete_table(full_table_id, not_found_ok=True)
        print("BQ Table [{}] deleted successfully".format(full_table_id))

    def check_if_retry_error_message(self, error_message):
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


def dispatch_etl_jobs(profile_id):
    data = {
        'p': str(profile_id),
        'backfill': 'historical'
    }
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName=f'ads-dispatch-report-request')
    print("Dispatching job for Profile [{}] - data {}".format(profile_id, data))
    queue.send_message(
        MessageBody=json.dumps(data, default=str)
    )


def lambda_handler(event, context):

    if 'profile_type' not in event:
        event['profile_type'] = ''

    profile_type = event['profile_type']
    if profile_type == '':
        print("Profile type is not specified to create tables.")
        return

    if 'action' not in event:
        event['action'] = 'create_table'

    if 'table_type' not in event:
        table_types = ''
    else:
        table_types = event['table_type']

    if table_types == '':
        table_types = [Constant.TBL_TYPE_REPORT, Constant.TBL_TYPE_ENTITY]
    else:
        table_types = table_types.split(',')

    if 'client_id' not in event:
        event['client_id'] = ''

    if 'tables' not in event:
        tables = []
    else:
        tables = event['tables'].split(',')

    if 'ad_type' not in event:
        ad_types = []
    else:
        ad_types = event['ad_type'].split(',')

    if 'run_etl' not in event:
        event['run_etl'] = False

    if 'profile_id' not in event:
        event['profile_id'] = ''

    action = event['action']
    db_connection = event.get('db_connection', os.getenv('DB_CONNECTION', 'pg')).lower()

    if action == 'create_table' or action == 'delete-create_table':
        client_id = event['client_id']

        for table_type in table_types:

            ad_types = TableConfig.getAdTypes(table_type,ad_types)

            print("{} - {} ad types -> {}".format(profile_type, table_type, ad_types))

            for ad_type in ad_types:

                tableNames = TableConfig.getTableNames(table_type,ad_type,givenTables=tables)

                print("client id: {} - ad_type -> {} - table type -> {} - table names -> {}".format(client_id,
                ad_type,
                table_type,
                tableNames))

                for tableName in tableNames:

                    tableConfig = TableConfig(table_type, ad_type, table_name=tableName)

                    tableName = tableConfig.getTableName()
                    if table_type != Constant.TBL_TYPE_GENERAL:
                        tableName = "{}_{}".format(tableName, client_id)

                    ops = PostgresOps() if db_connection == 'pg' else BigQuery()
                    if action == 'delete-create_table':
                        ops.delete_table(table_name=tableName, dataset=tableConfig.getDataset())
                    ops.create_table(
                        dataset=tableConfig.getDataset(),
                        tableName=tableName,
                        schema=tableConfig.getSchema(),
                        partitionColumn=tableConfig.getPartitionColumn(),
                        clusterColumn=tableConfig.getClusterColumn()
                    )

        # dispatch the etl jobs for the new created client
        if event['run_etl']:
            print("Dispatching job for Client [{}] Profile [{}]".format(client_id, event['profile_id']))
            dispatch_etl_jobs(event['profile_id'])

    # elif action == 'fix_schema':
    #     client_id = event['client_id']
    #     for ad_type in ad_types:
    #         reportTypes = tableConfig.getReportTypes(ad_type=ad_type, givenReportTypes=event['reportTypes'])
    #         print("client id: {} - ad_type -> {} - report types -> {}".format(client_id, ad_type, reportTypes))
    #         for reportType in reportTypes:
    #             reportConfig = ReportConfig(ad_type=ad_type, reportType=reportType)
    #             tableName = reportConfig.getTableName()
    #             tableName = "{}_{}".format(tableName, client_id)
    #
    #             bq = BigQuery()
    #             bq.fix_table_schema(
    #                 dataset=reportConfig.getDataset(),
    #                 tableName=tableName,
    #                 schema=reportConfig.getSchema()
    #             )
    elif action == 'create_bucket':
        bucket_name = event['bucket']
        bq = BigQuery()
        bq.create_bucket(
            bucket_name=bucket_name
        )
    elif action == 'list_buckets':
        bq = BigQuery()
        bq.list_buckets()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Create Tables for Ads API (BigQuery or Postgres)")

    parser.add_argument("-a", "--action", help="Action", default='create_table')
    parser.add_argument("-cid", "--client_id", help="Client Id", default=1)
    parser.add_argument("-pt", "--profile_type", help="Profile Type", default='seller')
    parser.add_argument("-tt", "--table_type", help="Table Type", default='')
    parser.add_argument("-at", "--ad_type", help="Ad Type", default='')
    parser.add_argument("-tbl", "--tables", help="Table Names", default='')
    parser.add_argument("-bkt", "--bucket", help="Bucket Name", default='')
    parser.add_argument("-dbc", "--db_connection", help="db_connection warehouse: BigQuery or Postgres", default=os.getenv('DB_CONNECTION', 'pg'))

    args = parser.parse_args()

    event = {
    "profile_type": args.profile_type,
    "client_id": args.client_id,
    "table_type": ""
    }
    lambda_handler(event, "abc")
