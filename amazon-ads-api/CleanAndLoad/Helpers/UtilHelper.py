import os
import re
from datetime import datetime, timedelta
from pathlib import Path

import bugsnag
from dotenv import load_dotenv

# Initialize environment variables
load_dotenv()


class ProjectConfigManager:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent
    ENV_PATH = PROJECT_ROOT.parent / '.env'
    BUGSNAG_API_KEY = os.getenv("PYTHON_BUGSNAG_API_KEY", "")
    APP_ENV = os.getenv("APP_ENV", "local")

    @classmethod
    def is_env(cls, env_name):
        return cls.APP_ENV == env_name

    @classmethod
    def get_project_file_or_folder_path(cls, s3_file_path=None, file_or_dir_flag=None):
        downloads_folder = None
        if file_or_dir_flag == 'DOWNLOAD':
            downloads_folder = '/tmp'
        elif file_or_dir_flag in 'GOOGLE_CREDENTIALS':
            downloads_folder = str(cls.PROJECT_ROOT) + '/' + os.getenv('GOOGLE_CLOUD_CREDENTIALS_PATH', '')
        if s3_file_path:
            local_file_name = f"{Path(s3_file_path).name}"
            return f"{downloads_folder}/{local_file_name}"
        else:
            return downloads_folder


class BugsnagHelper(object):
    def __init__(self):
        bugsnag.configure(
            api_key=ProjectConfigManager.BUGSNAG_API_KEY,
            release_stage=ProjectConfigManager.APP_ENV,
            project_root=str(ProjectConfigManager.PROJECT_ROOT),
        )

    @staticmethod
    def notify_bugsnag(exception, meta_data=None):
        if isinstance(exception, Exception):
            bugsnag.notify(exception, meta_data=meta_data)
        else:
            bugsnag.notify(Exception(str(exception)), meta_data=meta_data)


class DateHelper:
    @staticmethod
    def make_date_range(start_date, end_date):
        for n in range(int((end_date - start_date).days) + 1):
            yield start_date + timedelta(n)

    @staticmethod
    def get_dates_from_range(start_date, end_date, date_format="%Y-%m-%d"):
        return [single_date.strftime(date_format) for single_date in DateHelper.make_date_range(start_date, end_date)]

    @staticmethod
    def adjust_date_by_days(date_str, days=15, date_format="%Y-%m-%dT%H:%M:%SZ"):
        date = datetime.strptime(date_str, date_format)
        adjusted_date = date + timedelta(days=days)
        return adjusted_date.strftime(date_format)

    @staticmethod
    def convert_date_format(date_str, original_format='%Y%m%dT%H%M%SZ', target_format='%Y%m%d'):
        return datetime.strptime(date_str, original_format).strftime(target_format)

    @staticmethod
    def get_dates_from_file_name(file_name):
        date_pattern = r'date_range=(\d{8})(?:-(\d{8}))?'
        matches = re.findall(date_pattern, file_name)
        if matches:
            dates = matches[0]
            formatted_dates = []
            for date in dates:
                if date:
                    formatted_date = datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")
                    formatted_dates.append(formatted_date)
                else:
                    formatted_dates.append('')
            return formatted_dates
        else:
            return None


class Utils:
    @staticmethod
    def divide_list_into_chunks(lst, chunk_size):
        for i in range(0, len(lst), chunk_size):
            yield lst[i:i + chunk_size]

    @staticmethod
    def compare_strings(str1, str2, ignore_case=True):
        if ignore_case:
            str1, str2 = str1.lower(), str2.lower()
        return str1.strip() == str2.strip()

    @staticmethod
    def get_python_executable():
        return "python" if ProjectConfigManager.is_env('local') else f"{os.getenv('PYTHONPATH', '')}/python3"

    @staticmethod
    def interactive_testing_sleep(default_val=15):
        sleep_time = os.getenv('INTERACTIVE_TESTING_SLEEP', str(default_val))
        try:
            return int(sleep_time)
        except ValueError:
            return default_val

    @staticmethod
    def get_env(key, defaultVal=None):
        return os.getenv(key)

    @staticmethod
    def extract_details_from_s3_path(s3_path):
        # amazon-ads-api/reports/SP/campaign/US/2/1023871092830912830/20240320-20240320/StartDate=20240320_EndDate=20240320.tsv.gz
        segments = s3_path.split('/')
        ad_type = segments[2]
        _report_type = segments[3]
        if "__" in _report_type:
            report_type = _report_type.split("__")[0]
            report_options = _report_type.split("__")[1].split("_")
        else:
            report_type = _report_type
            report_options = []

        details = {
            'ad_type': ad_type,
            'report_type': report_type,
            'country': segments[4],
            'client_id': segments[5],
            'profileId': segments[6],
            'report_options': report_options
        }

        date_range = segments[7]
        start_date = date_range.split('-')[0]
        end_date = date_range.split('-')[1]

        details['start_date'] = start_date
        details['end_date'] = end_date

        return details

    @staticmethod
    def rename_columns(df):
        df.columns = df.columns.str.replace('-', '_', regex=False)
        return df
