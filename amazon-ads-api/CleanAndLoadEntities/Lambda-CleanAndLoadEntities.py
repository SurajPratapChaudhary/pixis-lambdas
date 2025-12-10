import argparse, urllib.parse

from ReportingClasses.Main import AdsApiEntitiesProcessor


def lambda_handler(file_event, context):
    s3_incoming_bucket, s3_incoming_file_prefix = get_file_meta_data(file_event)

    ads_entities_obj = AdsApiEntitiesProcessor(
        s3_file_path=s3_incoming_file_prefix,
        incoming_bucket_name=s3_incoming_bucket)
    ads_entities_obj.process()


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
    
    parser = argparse.ArgumentParser(description="BigQuery Clean And Load Ads API Entities Data")

    parser.add_argument("-", "--entity", help="Entity", default='sp_p')
    args = parser.parse_args()
    entity = args.entity
    keys = {
        'sp_c': f"amazon-ads-api/entities/SP/campaign/US/1/794003192585523/campaign.json.gz",
        'sp_ag': f"amazon-ads-api/entities/SP/adgroup/US/1/794003192585523/adgroup.json.gz",
        'sp_t': f"amazon-ads-api/entities/SP/target/US/1/794003192585523/target.json.gz",
        'sp_k': f"amazon-ads-api/entities/SP/keyword/US/1/794003192585523/keyword.json.gz",
        'sp_p': f"amazon-ads-api/entities/SP/portfolio/US/1/794003192585523/portfolio.json.gz",
        'sp_pa': f"amazon-ads-api/entities/SP/productad/US/1/794003192585523/productad.json.gz",

        'sb_c': f"amazon-ads-api/entities/SB/campaign/US/1/794003192585523/campaign.json.gz",
        'sb_ag': f"amazon-ads-api/entities/SB/adgroup/US/1/794003192585523/adgroup.json.gz",
        'sb_t': f"amazon-ads-api/entities/SB/target/US/1/794003192585523/target.json.gz",
        'sb_k': f"amazon-ads-api/entities/SB/keyword/US/1/794003192585523/keyword.json.gz",
        'sb_a': f"amazon-ads-api/entities/SB/ad/US/1/794003192585523/ad.json.gz",

        'sd_c': f"amazon-ads-api/entities/SD/campaign/US/1/794003192585523/campaign.json.gz",
        'sd_ag': f"amazon-ads-api/entities/SD/adgroup/US/1/794003192585523/adgroup.json.gz",
        'sd_t': f"amazon-ads-api/entities/SD/target/US/1/794003192585523/target.json.gz",
        'sd_pa': f"amazon-ads-api/entities/SD/productad/US/1/794003192585523/productad.json.gz",
    }

    if entity != '':
        keys = {entity: keys[entity]} if entity in keys else {}

    for key, value in keys.items():
        values = value if isinstance(value, list) else [value]
        for val in values:
            event = {'Records': [{'s3': {'bucket': {'name': 'pixis-data-bucket'}, 'object': {'key': val}}}]}
            print(event)
            lambda_handler(event, "abc")
