import boto3
import argparse
import os,json
from dotenv import load_dotenv
import math

if os.path.exists(".env"):
    load_dotenv()

aws_access_key = os.environ.get('AWS_ACCESS_KEY', None)
aws_secret_key = os.environ.get('AWS_SECRET_KEY', None)
aws_region = os.environ.get('AWS_REGION', 'us-east-1')
subnet_ids = os.environ.get("ECS_SUBNETS", "").split(",")

security_groups = os.environ.get("ECS_SECURITY_GROUPS", "").split(",")

def get_queue_config_data():
    raw = os.environ.get("QUEUE_WORKER_SCALING_CONFIG", "[]")

    try:
        config = json.loads(raw)
        if not isinstance(config, list):
            print("QUEUE_WORKER_SCALING_CONFIG is not a list, ignoring")
            return []
        return config
    except Exception as e:
        print(f"Failed to parse QUEUE_WORKER_SCALING_CONFIG: {e}")
        return []

def lambda_handler(event, context):
    ecs_queue = get_queue_config_data()
    if aws_access_key:
        ecs_client = boto3.client('ecs',
                                  region_name=aws_region,
                                  aws_access_key_id=aws_access_key,
                                  aws_secret_access_key=aws_secret_key)
    else:
        ecs_client = boto3.client('ecs', region_name=aws_region)

    if aws_access_key:
        sqs_client = boto3.client('sqs',
                                  region_name=aws_region,
                                  aws_access_key_id=aws_access_key,
                                  aws_secret_access_key=aws_secret_key)
    else:
        sqs_client = boto3.client('sqs', region_name=aws_region)
    
    if ecs_queue:
        for sqs_queue_settings in ecs_queue:

            # if max worker is zero then skip that
            if sqs_queue_settings['max_workers'] <= 0:
                continue

            queue_url = sqs_queue_settings['queue']
            max_jobs_in_worker = sqs_queue_settings['max_jobs_in_worker']
            print("getting attributes of queue: {}".format(queue_url))
            response = sqs_client.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=['ApproximateNumberOfMessages']
            )
            message_count = int(response['Attributes']['ApproximateNumberOfMessages'])

            # Get the current number of tasks
            pending_tasks = ecs_client.list_tasks(
                cluster=sqs_queue_settings['cluster'],
                family=sqs_queue_settings['task'],
                desiredStatus='PENDING'
            )['taskArns']

            current_tasks = ecs_client.list_tasks(
                cluster=sqs_queue_settings['cluster'],
                family=sqs_queue_settings['task'],
                desiredStatus='RUNNING'
            )['taskArns']

            current_task_count = len(current_tasks) + len(pending_tasks)

            estimated_required_tasks = math.ceil(message_count / max_jobs_in_worker)

            # Calculate new tasks needed, not exceeding 20
            new_tasks_needed = min(
                estimated_required_tasks - current_task_count,
                sqs_queue_settings['max_workers'] - current_task_count
            )

            if new_tasks_needed <= 0:
                continue

            print(
                f"Queue : {queue_url} Cluster : {sqs_queue_settings['cluster']} SQS Messages : {message_count} - Tasks needed: {new_tasks_needed}")

            # Start new tasks if needed
            for _ in range(new_tasks_needed):
                ecs_client.run_task(cluster=sqs_queue_settings['cluster'],
                                    taskDefinition=sqs_queue_settings['task'],
                                    networkConfiguration={
                                        'awsvpcConfiguration': {
                                            'subnets': subnet_ids,
                                            'securityGroups': security_groups,
                                            'assignPublicIp': 'ENABLED'
                                        }
                                    },
                                    capacityProviderStrategy=[
                                        {
                                            'capacityProvider': 'FARGATE_SPOT',
                                            'weight': 1,
                                            'base': 0
                                        }]
                                    )


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Queue Worker Scaling")
    args = parser.parse_args()
    event = {}
    lambda_handler(event, "abc")
