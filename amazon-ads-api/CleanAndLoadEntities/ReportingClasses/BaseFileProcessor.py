import logging
from typing import List, Dict

import boto3

from abc import ABC, abstractmethod
from pathlib import Path

from includes.constants import Constant
import pandas as pd
from botocore.exceptions import NoCredentialsError, ClientError
from google.cloud.bigquery import SchemaField
from Helpers.UtilHelper import ProjectConfigManager, Utils, DateHelper


class BaseFileProcessor(ABC):
    """Coordinates processing of data according to a general schema."""
    prefix = 'entities_'
    entities_schema_fields: List[SchemaField] = []

    def __init__(self, _s3_file_path: str, _incomingBucketName: str):

        self.entity_type = None
        self.marketplace = None
        self.country = None
        self.client_id = None
        self.profileId = None
        self.advertiserId = None

        self.s3_file_path = _s3_file_path
        self._set_properties_from_s3_path()

        self.incoming_bucket_name = _incomingBucketName
        self.incoming_local_filepath = ProjectConfigManager.get_project_file_or_folder_path(
            s3_file_path=self.s3_file_path,
            file_or_dir_flag='DOWNLOAD'
        )
        print("incoming_local_filepath: ", self.incoming_local_filepath)
        print("s3_file_path: ", self.s3_file_path)
        print("entity_type: ", self.entity_type)

    @abstractmethod
    def process(self):
        pass

    def _set_properties_from_s3_path(self):
        extracted_data = Utils.extract_details_from_s3_path(s3_path=self.s3_file_path)
        self.ad_type = extracted_data.get("ad_type", '')
        self.entity_type = extracted_data.get("entity_type")
        self.country = extracted_data.get("country")
        self.marketplace = Constant.MARKETPLACES.get(self.country, self.country)
        self.client_id = extracted_data.get("client_id")
        self.profileId = extracted_data.get("profileId")

    def get_cleaned_file_path(self) -> Path:
        return self.incoming_local_filepath / f"{self.incoming_local_filepath}_cleaned.parquet"

    def download_s3_file(self):
        try:
            s3 = boto3.client('s3',
                              aws_access_key_id=Utils.get_env("AWS_ACCESS_KEY", ''),
                              aws_secret_access_key=Utils.get_env("AWS_SECRET_KEY", ''),
                              region_name=Utils.get_env("AWS_REGION", 'us-east-1'))
            logging.info("Starting download of file from S3: %s", self.s3_file_path)
            s3.download_file(self.incoming_bucket_name, self.s3_file_path, self.incoming_local_filepath)
            logging.info("Successfully downloaded file <%s> to local path <%s>", self.s3_file_path,
                         self.incoming_local_filepath)
            print("Successfully downloaded file <%s> to local path <%s>", self.s3_file_path,
                  self.incoming_local_filepath)
        except NoCredentialsError:
            logging.error("Credentials not available for AWS S3.")
        except ClientError as e:
            logging.error("Client error encountered during S3 download: %s", e)
        except Exception as e:
            logging.error("Unexpected error during S3 file download: %s", e)

    def validate_file_content(self) -> pd.DataFrame or None:
        try:
            if self.incoming_local_filepath.suffix == '.json':
                df = pd.read_json(self.incoming_local_filepath)
            elif self.incoming_local_filepath.suffix == '.csv':
                df = pd.read_csv(self.incoming_local_filepath)
            elif self.incoming_local_filepath.suffix == '.zip':
                df = pd.read_csv(self.incoming_local_filepath, compression='zip')
            elif self.incoming_local_filepath.suffix == '.gz':
                df = pd.read_csv(self.incoming_local_filepath, sep='\t', compression='gzip')
            else:
                logging.error("Unsupported file extension: %s", self.incoming_local_filepath.suffix)
                return None

            if self._has_invalid_columns(df):
                return None

            return df
        except pd.errors.EmptyDataError:
            logging.error("File is empty or contains only headers.")
        except Exception as e:
            logging.error("Error loading file: %s", e, exc_info=True)

        return None

    @staticmethod
    def _has_invalid_columns(df: pd.DataFrame) -> bool:
        if "Error" in df.columns:
            logging.error("File contains an 'Error' column.")
            return True
        all_columns = df.columns.str.replace(' ', '')
        if any('<html>' in col for col in all_columns):
            logging.error("File contains HTML content in column names.")
            return True
        return False

    @classmethod
    def add_schema_field(cls, name: str, field_type: str):
        cls.entities_schema_fields.append(SchemaField(name, field_type))

    @property
    def table_schema(self):
        return self.entities_schema_fields

    @table_schema.setter
    def table_schema(self, fields_dict: Dict[str, str]):
        """Sets the analytics schema using a dictionary of field names and types."""
        # Clear the existing schema before adding new fields
        self.entities_schema_fields.clear()
        for name, field_type in fields_dict.items():
            self.add_schema_field(name, field_type)
