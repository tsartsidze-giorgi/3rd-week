def set_object_access_policy(aws_s3_client, bucket_name, file_name):
    response = aws_s3_client.put_object_acl(
        ACL="public-read",
        Bucket=bucket_name,
        Key=file_name
    )
    status_code = response["ResponseMetadata"]["HTTPStatusCode"]
    if status_code == 200:
        return True
    return False


def set_expaired_object_policy(aws_s3_client, bucket_name):
    rule = {
        'ID': 'Delete old objects',
        'Filter': {
            'Prefix': "/*"
        },
        'Status': 'Enabled',
        'Expiration': {
            'Days': 120
        }
    }

    # Create the lifecycle configuration policy
    policy = {
        'Rules': [rule]
    }

    # Apply the policy to the bucket
    aws_s3_client.put_bucket_lifecycle_configuration(
        Bucket=bucket_name,
        LifecycleConfiguration=policy
    )
