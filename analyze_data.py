import os
import boto3
import json

s3_client = boto3.client('s3')


def process_file(file_name, bucket_name):
    local_path = '/tmp/' + os.path.basename(file_name)
    print(f"Loading file: {file_name} from S3")

    s3_client.download_file(bucket_name, file_name, local_path)

    print(f"Analysing file: {file_name}")

    output_filename = file_name.split(".")[0]
    output_file = '/tmp/processed_' + os.path.basename(output_filename) + '.csv'
    with open(output_file, 'w') as f:
        f.write('')

    s3_client.upload_file(output_file, bucket_name, f'processed/{os.path.basename(output_file)}')
    print(f"File processed and saved to: processed/{os.path.basename(output_file)}")


def lambda_handler(event, context):
    print("Received event:", json.dumps(event, indent=2))

    try:
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        file_key = event['Records'][0]['s3']['object']['key']

        print(f"Bucket: {bucket_name}, File: {file_key}")

        if file_key.endswith('.json'):
            process_file(file_key, bucket_name)
        else:
            print(f"Skipped file: {file_key}, it's not a JSON file.")

    except Exception as e:
        print(f"Error processing event: {e}")

    return {
        'statusCode': 200,
        'body': 'Processing complete'
    }