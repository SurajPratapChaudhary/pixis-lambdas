import argparse, urllib.parse

from ReportingClasses.Main import AdsApiReportProcessor


def lambda_handler(file_event, context):
    s3_incoming_bucket, s3_incoming_file_prefix = get_file_meta_data(file_event)

    # to remove later on as we are not going to process advertised_product for now
    # if "advertised_product" in s3_incoming_file_prefix:
    #     return

    ads_reporting_obj = AdsApiReportProcessor(
        s3_file_path=s3_incoming_file_prefix,
        incoming_bucket_name=s3_incoming_bucket)
    ads_reporting_obj.process()


def get_file_meta_data(file_event):
    try:
        file_record = file_event["Records"][0]
        s3_object_key_encoded = file_record["s3"]["object"]["key"]
        s3_bucket_name = file_record["s3"]["bucket"]["name"]
        s3_object_key = urllib.parse.unquote_plus(s3_object_key_encoded)
        return s3_bucket_name, s3_object_key
    except KeyError as e:
        raise KeyError(f"Missing expected key in the event: {e}")
    except IndexError:
        raise IndexError("No records found in the event.")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="BigQuery Clean And Load Ads API")
    parser.add_argument("-k", "--key", help="Key", default='custom')
    parser.add_argument("-r", "--report", help="Report", default='campaign')
    parser.add_argument("-s", "--start_date", help="Start Date", default='20251012')
    parser.add_argument("-e", "--end_date", help="End Date", default='20251110')
    parser.add_argument("-p", "--profile_id", help="Profile Id", default='1299175532875224')
    parser.add_argument("-c", "--client_id", help="Client Id", default='1')
    parser.add_argument("-cn", "--country", help="Country", default='US')
    args = parser.parse_args()
    key = args.key

    from datetime import datetime, timedelta, date

    keys = {
        'custom': f"amazon-ads-api/reports/SP/{args.report}/{args.country}/{args.client_id}/{args.profile_id}/{args.start_date}-{args.end_date}/StartDate={args.start_date}_EndDate={args.end_date}.json.gz",
        'sp_c': f"amazon-ads-api/reports/SP/campaign/US/1/1299175532875224/20251028-20251028/StartDate=20251028_EndDate=20251028.json.gz",
        'sp_a': f"amazon-ads-api/reports/SP/adgroup/US/1/1299175532875224/20251028-20251028/StartDate=20251028_EndDate=20251028.json.gz",
        'sp_k': f"amazon-ads-api/reports/SP/keyword/US/1/1299175532875224/20251029-20251029/StartDate=20251029_EndDate=20251029.json.gz",
        'sp_t': f"amazon-ads-api/reports/SP/target/US/1/1299175532875224/20251028-20251028/StartDate=20251028_EndDate=20251028.json.gz",
    }

    if key != '':
        keys = {key: keys[key]} if key in keys else {}

    for k, value in keys.items():
        values = value if isinstance(value, list) else [value]
        for val in values:
            event = {'Records': [{'s3': {'bucket': {'name': 'peakroas-data-bucket'}, 'object': {'key': val}}}]}
            print(event)
            lambda_handler(event, "abc")
