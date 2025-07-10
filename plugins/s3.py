# from airflow.exceptions import AirflowException
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# import logging

# def upload_to_s3(s3_conn_id, s3_bucket, s3_key, local_files_to_upload, replace):
#     """
#     Upload all of the files to S3
#     """
#     s3_hook = S3Hook(s3_conn_id)
#     dest_key = s3_key
#     for file in local_files_to_upload:
#         logging.info("Saving {} to {} in S3".format(file, dest_key))
#         s3_hook.load_file(
#             filename=file,
#             key=dest_key,
#             bucket_name=s3_bucket,
#             replace=replace
#         )

# def read_from_s3(bucket_name, key, s3_conn_id='aws_default'):
#     """
#     Read a file from S3 and return its content as a string
#     """
#     s3_hook = S3Hook(aws_conn_id=s3_conn_id)
#     logging.info(f"Reading from S3: s3://{bucket_name}/{key}")
#     obj = s3_hook.get_key(bucket_name=bucket_name, key=key)
#     return obj.get()['Body'].read().decode('utf-8')


from typing import BinaryIO, Union
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from typing import List


def upload_to_s3(
    key: str,
    bucket_name: str,
    data: Union[str, bytes, BinaryIO],
    aws_conn_id: str = "aws_conn_id",
    replace: bool = True,
) -> None:
    """Upload file/data to S3"""
    hook = S3Hook(aws_conn_id=aws_conn_id)
    if isinstance(data, str):
        hook.load_string(
            string_data=data, key=key, bucket_name=bucket_name, replace=replace
        )
    else:
        hook.load_bytes(
            bytes_data=data, key=key, bucket_name=bucket_name, replace=replace
        )


def read_from_s3(key: str, bucket_name: str, aws_conn_id: str = "aws_conn_id"):
    """Read data from S3"""
    hook = S3Hook(aws_conn_id=aws_conn_id)
    return hook.read_key(key=key, bucket_name=bucket_name)


def list_s3_keys(
    bucket_name: str, prefix: str, aws_conn_id: str = "aws_conn_id"
) -> List[str]:
    """주어진 S3 prefix의 모든 key값"""
    hook = S3Hook(aws_conn_id=aws_conn_id)
    return hook.list_keys(bucket_name=bucket_name, prefix=prefix)
