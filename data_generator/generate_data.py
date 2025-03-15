import datetime
import json
import random

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
    S3_BUCKET = "big-data-pipeline-bucket"

    @staticmethod
    def upload_to_s3(filename: str) -> None:
        s3_client = boto3.client("s3")

        try:
            s3_client.upload_file(filename, DataUploader.S3_BUCKET, f"data/{filename}")
            print(f"File {filename} uploaded to S3 to directory data/")
        except Exception as e:
            print(f"Error during file sending to S3: {e}")


if __name__ == "__main__":
    print(DataGenerator.generate_data())
    # filename = DataGenerator.save_data_to_file()
    # DataUploader.upload_to_s3(filename)
