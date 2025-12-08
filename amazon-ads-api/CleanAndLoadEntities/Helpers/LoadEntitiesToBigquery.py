from Helpers.MergeTable import MergeTable
from includes.reports import TableConfig
from includes.constants import Constant


class LoadEntitiesToBigquery:
    def __init__(self,
                 gcs_file_path,
                 marketplace,
                 client_id,
                 profileId,
                 entity_filename,
                 entity_type,
                 ad_type):
        self.gcs_file_path = gcs_file_path
        self.marketplace = marketplace
        self.client_id = client_id
        self.profileId = profileId
        self.entity_filename = entity_filename
        self.entity_type = entity_type
        self.ad_type = ad_type

        self.entities_config_obj = TableConfig(Constant.TBL_TYPE_ENTITY,ad_type, entity_type)
        self.table_name = self.entities_config_obj.getTableName()
        self.table_name = f"{self.table_name}_{self.client_id}"
        self.dataset_id = self.entities_config_obj.getDataset()
        self.load_method = self.entities_config_obj.getLoadMethod()
        self.merge_conditions = self.entities_config_obj.getMergeColumns()

    def run(self):
        self.run_merge_task()

    def run_merge_task(self):
        log_prefix = f"{self.client_id}_{self.profileId}_{self.ad_type}_{self.entity_type}"
        print("\n\n-------------------------------")
        merge_table = MergeTable(
            marketplace=self.marketplace,
            client_id=self.client_id,
            profileId=self.profileId,
            table_name=self.table_name,
            dataset_id=self.dataset_id,
            ad_type=self.ad_type,
            entity_type=self.entity_type,
            gcs_file_path=self.gcs_file_path,
            merge_conditions=self.merge_conditions,
            load_method=self.load_method
        )
        merge_table.run()
        print("\n\n")
