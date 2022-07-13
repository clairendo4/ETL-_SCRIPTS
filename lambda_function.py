

import datetime
import io
import logging
import os

# import boto3
# import botocore.exceptions
import paramiko

logger = logging.getLogger()
logger.setLevel(os.getenv('LOGGING_LEVEL', 'DEBUG'))


SSH_HOST = os.environ['SSH_HOST']
SSH_USERNAME = os.environ['SSH_USERNAME']
SSH_PASSWORD = os.getenv('SSH_PASSWORD')
# # path to a private key file if any in s3... in 'bucket:key' format.
SSH_PRIVATE_KEY = os.getenv('SSH_PRIVATE_KEY')
assert SSH_PASSWORD or SSH_PRIVATE_KEY, "Missing SSH_PASSWORD or SSH_PRIVATE_KEY"
# optional
SSH_PORT = int(os.getenv('SSH_PORT', 22))
SSH_DIR = os.getenv('SSH_DIR')
# filename mask used for the remote file
SSH_FILENAME = os.getenv('SSH_FILENAME', 'data_{current_date}')


def on_trigger_event(event, context):
   
    if SSH_PRIVATE_KEY:
        key_obj = get_private_key(*SSH_PRIVATE_KEY.split(':'))
    else:
        key_obj = None

    # prefix all logging statements - otherwise impossible to filter out in
    # Cloudwatch
    logger.info(f"S3-SFTP: received trigger event")

    sftp_client, transport = connect_to_sftp(
        hostname=SSH_HOST,
        port=SSH_PORT,
        username=SSH_USERNAME,
        password=SSH_PASSWORD,
        pkey=key_obj
    )
    if SSH_DIR:
        sftp_client.chdir(SSH_DIR)
        logger.debug(f"S3-SFTP: Switched into remote SFTP upload directory")

    with transport:
        for s3_file in s3_files(event):
            filename = sftp_filename(SSH_FILENAME, s3_file)
            bucket = s3_file.bucket_name
            contents = ''
            try:
                logger.info(f"S3-SFTP: Transferring S3 file '{s3_file.key}'")
                transfer_file(sftp_client, s3_file, filename)
            except botocore.exceptions.BotoCoreError as ex:
                logger.exception(f"S3-SFTP: Error transferring S3 file '{s3_file.key}'.")
                contents = str(ex)
                filename = filename + '.x'
           
            logger.info(f"S3-SFTP: Deleting S3 file '{s3_file.key}'.")
            delete_file(s3_file)


def connect_to_sftp(hostname, port, username, password, pkey):
    """Connect to SFTP server and return client object."""
    transport = paramiko.Transport((hostname, port))
    transport.connect(username=username, password=password, pkey=pkey)
    client = paramiko.SFTPClient.from_transport(transport)
    logger.debug(f"S3-SFTP: Connected to remote SFTP server")
    return client, transport


def get_private_key(bucket, key):
    """
    Return an RSAKey object from a private key if any, stored on S3.
    """
    key_obj = boto3.resource('s3').Object(bucket, key)
    key_str = key_obj.get()['Body'].read().decode('utf-8')
    key = paramiko.RSAKey.from_private_key(io.StringIO(key_str))
    logger.debug(f"S3-SFTP: Retrieved private key from S3")
    return key


def s3_files(event):
 
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        event_category, event_subcat = record['eventName'].split(':')
        if event_category == 'ObjectCreated':
            logger.info(f"S3-SFTP: Received '{ event_subcat }' trigger on '{ key }'")
            yield boto3.resource('s3').Object(bucket, key)
        else:
            logger.warning(f"S3-SFTP: Ignoring invalid event: { record }")


def sftp_filename(file_mask, s3_file):
    """Create destination SFTP filename."""
    return file_mask.format(
        bucket=s3_file.bucket_name,
        key=s3_file.key.replace("_000", "new"),
        current_date=datetime.date.today().isoformat()
    )


def transfer_file(sftp_client, s3_file, filename):
    """
    Transfer S3 file to SFTP server.
    Args:
        sftp_client: paramiko.SFTPClient, connected to SFTP endpoint
        s3_file: boto3.Object representing the S3 file
        filename: string, the remote filename to use
    """
    with sftp_client.file(filename, 'w') as sftp_file:
        s3_file.download_fileobj(Fileobj=sftp_file)
    logger.info(f"S3-SFTP: Transferred '{ s3_file.key }' from S3 to SFTP as '{ filename }'")


def delete_file(s3_file):
    """
    Delete file from S3.
    """
    try:
        s3_file.delete()
    except botocore.exceptions.BotoCoreError as ex:
        logger.exception(f"S3-SFTP: Error deleting '{ s3_file.key }' from S3.")
    else:
        logger.info(f"S3-SFTP: Deleted '{ s3_file.key }' from S3")

