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
        'sp_c': f"amazon-ads-api/reports/SP/campaign/US/1/794003192585523/20251130-20251209/StartDate=20251130_EndDate=20251209.json.gz",
        'sp_cp': f"amazon-ads-api/reports/SP/campaign_placement/US/1/794003192585523/20251130-20251209/StartDate=20251130_EndDate=20251209.json.gz",
        'sp_ag': f"amazon-ads-api/reports/SP/adgroup/US/1/794003192585523/20251130-20251209/StartDate=20251130_EndDate=20251209.json.gz",
        'sp_k': f"amazon-ads-api/reports/SP/keyword/US/1/794003192585523/20251130-20251209/StartDate=20251130_EndDate=20251209.json.gz",
        'sp_t': f"amazon-ads-api/reports/SP/target/US/1/794003192585523/20251130-20251209/StartDate=20251130_EndDate=20251209.json.gz",
        'sp_ap': f"amazon-ads-api/reports/SP/advertised_product/US/1/794003192585523/20251130-20251209/StartDate=20251130_EndDate=20251209.json.gz",
        'sp_pp': f"amazon-ads-api/reports/SP/purchased_product/US/1/794003192585523/20251130-20251209/StartDate=20251130_EndDate=20251209.json.gz",
        'sp_st': f"amazon-ads-api/reports/SP/search_term/US/1/794003192585523/20251130-20251209/StartDate=20251130_EndDate=20251209.json.gz",
        
        'sb_c': f"amazon-ads-api/reports/SB/campaign/US/1/794003192585523/20251130-20251209/StartDate=20251130_EndDate=20251209.json.gz",
        'sb_cp': f"amazon-ads-api/reports/SB/campaign_placement/US/1/794003192585523/20251130-20251209/StartDate=20251130_EndDate=20251209.json.gz",
        'sb_ag': f"amazon-ads-api/reports/SB/adgroup/US/1/794003192585523/20251130-20251209/StartDate=20251130_EndDate=20251209.json.gz",
        'sb_t': f"amazon-ads-api/reports/SB/target/US/1/794003192585523/20251130-20251209/StartDate=20251130_EndDate=20251209.json.gz",
        'sb_k': f"amazon-ads-api/reports/SB/keyword/US/1/794003192585523/20251130-20251209/StartDate=20251130_EndDate=20251209.json.gz",
        'sb_a': f"amazon-ads-api/reports/SB/ad/US/1/794003192585523/20251130-20251209/StartDate=20251130_EndDate=20251209.json.gz",
        'sb_pp': f"amazon-ads-api/reports/SB/purchased_product/US/1/794003192585523/20251130-20251209/StartDate=20251130_EndDate=20251209.json.gz",
        'sb_st': f"amazon-ads-api/reports/SB/search_term/US/1/794003192585523/20251130-20251209/StartDate=20251130_EndDate=20251209.json.gz",
        
        'sd_c': f"amazon-ads-api/reports/SD/campaign/US/1/794003192585523/20251130-20251209/StartDate=20251130_EndDate=20251209.json.gz",
        'sd_ag': f"amazon-ads-api/reports/SD/adgroup/US/1/794003192585523/20251130-20251209/StartDate=20251130_EndDate=20251209.json.gz",
        'sd_t': f"amazon-ads-api/reports/SD/target/US/1/794003192585523/20251130-20251209/StartDate=20251130_EndDate=20251209.json.gz",
        'sd_pp': f"amazon-ads-api/reports/SD/purchased_product/US/1/794003192585523/20251130-20251209/StartDate=20251130_EndDate=20251209.json.gz",
        'sd_ap': f"amazon-ads-api/reports/SD/advertised_product/US/1/794003192585523/20251130-20251209/StartDate=20251130_EndDate=20251209.json.gz",
    }

    if key != '':
        keys = {key: keys[key]} if key in keys else {}

    for k, value in keys.items():
        values = value if isinstance(value, list) else [value]
        for val in values:
            event = {'Records': [{'s3': {'bucket': {'name': 'pixis-data-bucket'}, 'object': {'key': val}}}]}
            print(event)
            lambda_handler(event, "abc")
