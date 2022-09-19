import os

import boto3


def upload_to_s3(path):
    s3 = boto3.client('s3')
    s3.delete_object(
            Bucket="demo-create-bucket-tree", Key=path)
    for root, dirs, files in os.walk(path):
        for filename in files:
            if not filename.startswith("."):
                with open(os.path.join(root, filename), 'rb') as data:
                    s3.upload_fileobj(data, "demo-create-bucket-tree", os.path.join(root, filename))


