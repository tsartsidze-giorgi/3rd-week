from botocore.exceptions import ClientError
import os
import logging

PART_BYTE = 1024 * 10
def list_buckets(aws_s3_client) -> list:
    # https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_buckets.html
    return aws_s3_client.list_buckets()


def create_bucket(aws_s3_client, bucket_name, region) -> bool:
    location = {'LocationConstraint': region}
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/create_bucket.html
    response = aws_s3_client.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration=location
    )
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
        return True
    return False


def delete_bucket(aws_s3_client, bucket_name) -> bool:
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/delete_bucket.html
    response = aws_s3_client.delete_bucket(Bucket=bucket_name)
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 204:
        return True
    return False


def bucket_exists(aws_s3_client, bucket_name) -> bool:
    try:
        response = aws_s3_client.head_bucket(Bucket=bucket_name)
        status_code = response["ResponseMetadata"]["HTTPStatusCode"]
        if status_code == 200:
            return True
    except ClientError:
        # print(e)
        return False


def get_list_objects(aws_s3_client, bucket_name) -> str:
    lst = []
    for key in aws_s3_client.list_objects(Bucket=bucket_name)['Contents']:
        lst.append(key['Key'])
    return lst

def count_objects_in_bucket(objects):
    count = {}
    for obj in objects:
        ext = obj.split('.')

        if ext[-1] in count.keys():

            count[ext[-1]] += 1
        else:
            count[ext[-1]] = 1

    for i in count.keys():
        print(f"{i} {count[i]}",)


def upload_file(aws_s3_client, bucket_name, file_path):
    try:
        file_name = str(file_path).split("/")[-1]
        response = aws_s3_client.upload_file(file_path, bucket_name, file_name)

    except ClientError as e:
        logging.error(e)
        return False
    return True


def upload_file_obj(aws_s3_client,  bucket_name, filename):
    try:
        file_name = str(filename).split("/")[-1]
        with open(filename, "rb") as file:
            aws_s3_client.upload_fileobj(file, bucket_name, file_name)

    except ClientError as e:
        logging.error(e)
        return False
    return True



def upload_file_put(aws_s3_client, bucket_name, filename):
    try:
        file_name = str(filename).split("/")[-1]
        with open(filename, "rb") as file:
            aws_s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=file.read())

    except ClientError as e:
        logging.error(e)
        return False
    return True


def multipart_upload(aws_s3_client,bucket_name,file_name):
    key = str(file_name).split("/")[-1]

    mpu = aws_s3_client.create_multipart_upload(Bucket=bucket_name, Key=key,)
    mpu_id = mpu["UploadId"]

    parts = []
    uploaded_bytes = 0
    total_bytes = os.stat(file_name).st_size
    with open(file_name, "rb") as f:
        i = 1
        while True:
            data = f.read(PART_BYTE)
            if not len(data):
                break
            part = aws_s3_client.upload_part(Body=data, Bucket=bucket_name, Key=key, UploadId=mpu_id, PartNumber=i)
            parts.append({"PartNumber": i, "ETag": part["ETag"]})
            uploaded_bytes += len(data)
            print(f"{uploaded_bytes} of {total_bytes} uploaded")
            i += 1

    result = aws_s3_client.complete_multipart_upload(Bucket=bucket_name, Key=key, UploadId=mpu_id, MultipartUpload={"Parts": parts})
    print(result)

    return result


def download_file(aws_s3_client, buket_name, key,):
    try:
        file_name = str(key).split("/")[-1]
        aws_s3_client.download_file(buket_name, key, file_name)

    except ClientError as e:
        logging.error(e)
        return False
    return True



def delete_file(aws_s3_client, buket_name, key):
    try:
        client = aws_s3_client.client('s3')
        client.delete_object(Bucket=buket_name, Key=key)

    except ClientError as e:
        logging.error(e)
        return False
    return True
