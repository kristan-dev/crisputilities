import boto3
from config import cfg #optional if you want to utilize a config file

# Let's you connect to an S3 bucket and pull contents from it. Even download a file there.


def S3ObjectDataSource(
    bucket: str,
    object_key: str,
    access_key_id: str,
    secret_access_key: str,
    chunk_size=500000,
):
    """
  A generator that reads and returns a s3 object in chunks.
  """
    s3 = boto3.client(
        "s3", aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key
    )
    body = s3.get_object(Bucket=bucket, Key=object_key)["Body"]

    while True:
        chunk = body.read(chunk_size)
        if chunk == b"":
            break
        else:
            yield chunk


class S3_Source:
    bucket: str = ""
    object_key: str = ""
    access_key_id: str = ""
    secret_access_key: str = ""
    prefix: str = "custom" # provide a value to prefix if you want to have a default pre-path to your S3 bucket. Example default-prefix-path/to-my-provided-path-in-params
    suffix: str = ".csv"

    def __init__(self, s3args):
        self.bucket = s3args["s3_bucket"]
        self.object_key = s3args["s3_key"]
        self.access_key_id = s3args["access_key_id"]
        self.secret_access_key = s3args["secret_access_key"]

    def S3ObjectDataSource(self):
        s3 = boto3.client(
            "s3",
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            region_name="eu-west-1"
        )

        return s3.get_object(Bucket=self.bucket, Key=self.object_key)

    def get_bucket_list(self):
        s3 = boto3.client(
            "s3",
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
        )

        # kwargs = {"Bucket": self.bucket, "Prefix": self.prefix}
        kwargs = {"Bucket": self.bucket, "Prefix": self.prefix}

        while True:
            resp = s3.list_objects_v2(**kwargs)
            for obj in resp["Contents"]:
                key = obj["Key"]
                if str(key).startswith(self.prefix) and str(key).endswith(self.suffix):
                    yield (obj["Key"], obj["LastModified"])

            try:
                kwargs["ContinuationToken"] = resp["NextContinuationToken"]
            except KeyError:
                break
    
    def download_file_as_temp(self, file_abspath: str):
        s3 = boto3.client(
            "s3",
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            )

        s3.download_file(self.bucket, self.object_key, file_abspath)


if __name__ == "__main__":
    # If you've set up a config file, use cfg
    # s3args = {
    #     "s3_bucket": "some-s3-bucket-name",
    #     "s3_key": r"custom/custom_upload_new.csv",
    #     "access_key_id": cfg["aws"]["bbr"]["access_key_id"],
    #     "secret_access_key": cfg["aws"]["bbr"]["secret_access_key"],
    # }

    s3args = {
        "s3_bucket": r"your-s3-bucket",
        "s3_key": r"path/to/your/bucket/folder", #pre-pended by the prefix property if you're using S3_Source. BEWARE.
        "access_key_id": "SOMEACCESSKEYID",
        "secret_access_key": "SOMESCRETACCESSKEY",
    }

    s3_class = S3_Source(s3args)

    file_list = []
    for key in s3_class.get_bucket_list():
        file_list.append(key)

    pass
