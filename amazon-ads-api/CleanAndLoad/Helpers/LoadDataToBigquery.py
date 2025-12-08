from Helpers.MergeTable import MergeTable
from includes.reports import TableConfig
from includes.constants import Constant


class LoadDataToBigquery:
    def __init__(self,
                 gcs_file_path,
                 marketplace,
                 client_id,
                 profileId,
                 start_date,
                 end_date,
                 report_filename,
                 report_type,
                 ad_type):
        self.gcs_file_path = gcs_file_path
        self.marketplace = marketplace
        self.client_id = client_id
        self.profileId = profileId
        self.start_date = start_date
        self.end_date = end_date
        self.report_filename = report_filename
        self.report_type = report_type
        self.ad_type = ad_type

        self.report_config_obj = TableConfig(Constant.TBL_TYPE_REPORT, ad_type, report_type)
        self.table_name = self.report_config_obj.getTableName()
        self.table_name = f"{self.table_name}_{self.client_id}"
        self.dataset_id = self.report_config_obj.getDataset()
        self.load_method = self.report_config_obj.getLoadMethod()
        self.merge_conditions = self.report_config_obj.getMergeColumns()

    def run(self):
        self.run_merge_task()

    def run_merge_task(self):
        log_prefix = f"{self.client_id}_{self.profileId}_{self.ad_type}_{self.report_type}_{self.start_date}_{self.end_date}"
        print("\n\n-------------------------------")
        merge_table = MergeTable(
            marketplace=self.marketplace,
            start_date=self.start_date,
            end_date=self.end_date,
            client_id=self.client_id,
            profileId=self.profileId,
            table_name=self.table_name,
            dataset_id=self.dataset_id,
            ad_type=self.ad_type,
            report_type=self.report_type,
            gcs_file_path=self.gcs_file_path,
            merge_conditions=self.merge_conditions,
            load_method=self.load_method
        )
        merge_table.run()
        print("\n\n")
