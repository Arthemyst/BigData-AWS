import datetime
import json
import os
import random
from tools.config import CustomEnvironment
import boto3


class DataGenerator:
    PAGES = ["/home", "/login", "/page1", "/page2", "/page3", "/page4", "/page5", "/page6", "/page7", "/page8", "/page9",
             "/page10"]

    @staticmethod
    def generate_data() -> dict:
        data = {
            "timestamp": datetime.datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S'),
            "user_id": random.randint(10000, 99999),
            "page": random.choice(DataGenerator.PAGES),
            "duration": random.randint(1, 2000)
        }
        return data

    @staticmethod
    def save_data_to_file() -> str:
        generated_data = DataGenerator.generate_data()

        filename = f"{datetime.datetime.utcnow().strftime('%Y-%m-%d')}.json"
        try:
            with open(filename, "r", encoding="utf-8") as file:
                existing_data = json.load(file)
        except (FileNotFoundError, json.JSONDecodeError):
            existing_data = []

        existing_data.append(generated_data)

        with open(filename, "w", encoding="utf-8") as file:
            json.dump(existing_data, file, indent=4)

        return filename


class DataUploader:
    S3_BUCKET = CustomEnvironment.get_aws_s3_bucket()

    @staticmethod
    def upload_to_s3(filename: str) -> None:
        if not os.path.exists(filename):
            print(f"❌ File {filename} does not exist! Upload not possible.")
            return

        s3_client = boto3.client("s3")

        try:
            s3_client.upload_file(filename, DataUploader.S3_BUCKET, f"big-data-1/{filename}")
            print(f"✅ File {filename} loaded to S3 t directory 'big-data-1/'.")

        except Exception as e:
            print(f"❌ Error during loading {filename} file to S3: {e}")


if __name__ == "__main__":
    for _ in range(100):
        filename = DataGenerator.save_data_to_file()
        DataUploader.upload_to_s3(filename)
