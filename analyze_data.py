import os

import boto3
import pandas as pd

from tools.config import CustomEnvironment

s3_client = boto3.client('s3')

bucket_name = CustomEnvironment.get_aws_s3_bucket()
files_directory = 'big-data-1/'


def process_file(file_name):
    local_path = '/tmp/' + os.path.basename(file_name)
    print(f"Loading file: {file_name} from S3")
    s3_client.download_file(bucket_name, file_name, local_path)

    data = pd.read_json(local_path)
    print(f"Analysing file: {file_name}")
    print(data.head())

    print(data.describe())
    output_filename = file_name.split(".")[0]
    output_file = '/tmp/processed_' + os.path.basename(output_filename) + '.csv'
    print(output_file)
    data.to_csv(output_file, index=False)
    s3_client.upload_file(output_file, bucket_name, f'processed/{os.path.basename(output_file)}')
    print(f"File processed and saved to: processed/{os.path.basename(output_file)}")


if __name__ == "__main__":
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=files_directory)
    if 'Contents' in response:
        for obj in response['Contents']:
            file_key = obj['Key']

            if file_key.endswith('.json'):
                process_file(file_key)
    else:
        print(f'No files in directory: {files_directory}')
