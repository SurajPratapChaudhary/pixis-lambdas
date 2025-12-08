import time
import random
import string

from google.cloud import storage, bigquery
from datetime import datetime, timedelta, timezone
import uuid
import os

from Helpers.GoogleCloud import GoogleCloud
from includes.reports import *
from includes.constants import Constant


class MergeTable(object):
    def __init__(self,
                 marketplace,
                 client_id,
                 profileId,
                 table_name,
                 dataset_id,
                 ad_type,
                 entity_type,
                 gcs_file_path,
                 merge_conditions,
                 load_method
                 ):
        self.marketplace = marketplace
        self.client_id = client_id
        self.profileId = profileId
        self.table_name = table_name
        self.dataset_id = dataset_id
        self.ad_type = ad_type
        self.entity_type = entity_type
        self.gcs_file_path = gcs_file_path
        self.merge_conditions = merge_conditions
        self.load_method = load_method

        self.gc = GoogleCloud()
        self.temp_table_name = self.get_temp_table_name()
        self.full_temp_table_id = self.gc.get_table_name(
            table_name=self.temp_table_name,
            dataset_id=self.dataset_id,
            project=True
        )

        self.full_table_id = self.gc.get_table_name(
            table_name=self.table_name,
            dataset_id=self.dataset_id,
            project=True
        )

        try:
            self.target_table_schema = self.gc.bq_client.get_table(self.full_table_id).schema
        except Exception as e:
            tableSchemaError = str(e)
            print("MergeTableTask Schema Error {} - Error {}".format(self.full_table_id, tableSchemaError))
            self.target_table_schema = None

    def get_temp_table_name(self):
        today = datetime.now().strftime('%Y%m%d')
        timestamp = datetime.now().strftime("%H%M%S%f")
        str = list(string.ascii_letters + string.digits)
        random.shuffle(str)
        shuffled_string = ''.join(random.choice(''.join(str)) for _ in range(16))
        unique_string = f"{timestamp}{shuffled_string}"
        return f"temp_{today}_{self.table_name}_{self.client_id}__{unique_string}"

    def run(self):
        # this is temporary to remove duplicate from the target table
        # self.gc.deleteDuplicateRows(self.target_table, self.match_condition)
        if self.target_table_schema is not None:
            if self.load_method == 'merge':
                data_loaded = self.load_temp_table()
                print("data loaded in temp table: ", data_loaded)
                if data_loaded:
                    self.merge_table()
                else:
                    print("MergeTableTask Load Job Failed for [{}] ".format(self.full_temp_table_id))
            else:
                data_loaded = self.delete_and_load_table()
                if data_loaded:
                    print("MergeTableTask Successful for table [{}] ".format(self.full_table_id))
                else:
                    print("MergeTableTask Load Job Failed for [{}] ".format(self.full_table_id))

    def load_temp_table(self):
        temp_table_expiry = 900
        print("MergeTableTask Creating temp table {} from {}".format(self.temp_table_name, self.full_table_id))
        table = bigquery.Table(self.full_temp_table_id, schema=self.target_table_schema)
        table.expires = datetime.now(timezone.utc) + timedelta(seconds=temp_table_expiry)
        temp_table_created = self.gc.bq_client.create_table(table)  # table with schema

        print(f"Temp table created for {temp_table_expiry} seconds --> {self.full_temp_table_id}")
        print(f"MergeTableTask Loading data to temp table {self.full_temp_table_id} from file")
        return self.gc.load_parquet_in_table(
            uris=self.gcs_file_path,
            table_name=self.temp_table_name,
            table_schema=self.target_table_schema,
            dataset_id=self.dataset_id
        )

    def delete_and_load_table(self):
        print(f"MergeTableTask deleting data from table {self.full_table_id}")
        delete_query = ''
        if delete_query != '':
            print(delete_query)
            delete_status, delete_resp = self.gc.execute_bq_query(delete_query)
            if delete_status:
                print(f"MergeTableTask old data deleted Successfully from table {self.full_table_id}")
                print(f"MergeTableTask Loading data into table {self.full_table_id} from file")
                return self.gc.load_parquet_in_table(
                    uris=self.gcs_file_path,
                    table_name=self.table_name,
                    table_schema=self.target_table_schema,
                    dataset_id=self.dataset_id
                )
        else:
            print("NO delete_query FOUND.")
            return False

    def merge_table(self):
        print("MergeTableTask Merging two tables [{} --> {}]".format(self.temp_table_name, self.full_table_id))
        temp_table_selection_query = f"SELECT DISTINCT *  FROM `{self.full_temp_table_id}`"
        merge_conditions = self.get_merge_query_where_conditions()
        delete_from_source_clause = ''
        if len(self.profileId) != 0 and False:
            profileIdColumn = 'advertiserId' if self.ad_type == 'dsp' else 'profileId'
            delete_from_source_clause = f"WHEN NOT MATCHED BY SOURCE AND {profileIdColumn} = {self.profileId} THEN DELETE"

        query = f"""MERGE  
                {self.full_table_id} target_table using ( {temp_table_selection_query} ) source_table
                ON {merge_conditions}
                WHEN NOT MATCHED BY TARGET THEN INSERT ROW
                {delete_from_source_clause}
                WHEN MATCHED THEN {self.merge_update()} 
                """

        run_successfully = self.gc.execute_bq_query(query)
        if run_successfully:
            print(f"MERGE SUCCESSFUL on table -> [{self.full_table_id}]")
        else:
            print(f"MERGE FAILED on table -> [{self.full_table_id}]")

    def get_merge_query_where_conditions(self):
        where_conditions = []
        for condition in self.merge_conditions:
            if condition is None:
                where_conditions.append("FALSE")
            else:
                where_conditions.append("source_table." + str(condition) + " = target_table." + str(condition))
        return " AND ".join(where_conditions)

    def merge_insert(self):
        table_col_names = " , ".join([col.name for col in self.target_table_schema])
        return "INSERT ( {} ) VALUES ( {} ) ".format(table_col_names, table_col_names)

    def merge_update(self):
        target_col_names = " , ".join(
            ["`" + col.name + "` = source_table." + col.name for col in self.target_table_schema])
        return "UPDATE SET {} ".format(target_col_names)
