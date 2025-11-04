import json

import pandas as pd
from google.cloud import storage


class GoogleCloudStorage:
    def __init__(self, project_id, bucket_name):
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.client = storage.Client(project_id)
        self.bucket = self.client.bucket(bucket_name)

    def read_json(self, filename):
        blob = self.bucket.blob(filename)
        with blob.open("r") as stream:
            result = json.load(stream)
        return result

    def read_pandas_json(self, filename, **kwargs):
        blob = self.bucket.blob(filename)
        with blob.open("r") as stream:
            result = pd.read_json(stream, **kwargs)
        return result

    def write_str(self, string, filename):
        blob = self.bucket.blob(filename)
        with blob.open("w") as f:
            f.write(string)

