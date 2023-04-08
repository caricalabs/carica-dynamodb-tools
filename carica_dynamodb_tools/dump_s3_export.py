import gzip
import json
import multiprocessing
from multiprocessing import Queue
from typing import Tuple

import click
import sys
from botocore.response import StreamingBody
from click import BadParameter

import carica_dynamodb_tools.version
import carica_dynamodb_tools.version
from carica_dynamodb_tools.session import boto_session


def remove_protected_attrs(item: dict) -> dict:
    """
    Remove protected (AWS-only) attributes from a DynamoDB item.
    """
    attrs = [attr for attr in item.keys() if attr.startswith('aws:')]
    for attr in attrs:
        del item[attr]
    return item


def get_export_data_items(region: str, export_arn: str) -> Tuple[str, StreamingBody]:
    """
    :return: a tuple containing the bucket name and the response body
        to read the data files manifest JSON object
    """
    session = boto_session(region_name=region)
    dynamodb_client = session.client('dynamodb')
    s3_client = session.client('s3')

    desc = dynamodb_client.describe_export(ExportArn=export_arn)['ExportDescription']
    if desc['ExportFormat'] != 'DYNAMODB_JSON':
        print(f'ExportFormat is not DYNAMODB_JSON', file=sys.stderr)
        sys.exit(1)
    if desc['ExportStatus'] != 'COMPLETED':
        print(f'ExportStatus is not COMPLETED', file=sys.stderr)
        sys.exit(1)

    bucket = desc['S3Bucket']
    prefix = desc.get('S3Prefix', '')
    manifest_key = desc['ExportManifest']

    # Download the small export manifest JSON file
    resp = s3_client.get_object(Bucket=bucket, Key=f'{prefix}{manifest_key}')
    manifest = json.loads(resp['Body'].read())
    manifest_files_key = manifest['manifestFilesS3Key']

    # Open the data items manifest JSON file, but return the response so the caller
    # can stream lines out of it.
    resp = s3_client.get_object(Bucket=bucket, Key=manifest_files_key)
    return bucket, resp['Body']


def batch_worker(
    region: str,
    bucket: str,
    item_q: Queue,
    print_lock: multiprocessing.Lock,
) -> None:
    """
    Multiprocessing worker for dumping JSONL archives in S3.

    Quits when it reads a ``None`` from the queue.
    """
    session = boto_session(region_name=region)
    s3_client = session.client('s3')
    for manifest_item in iter(item_q.get, None):
        # The item is the contents of one manifestFilesS3Key file.  It looks like:
        # {
        #   'dataFileS3Key': 'AWSDynamoDB/01680958677849-381aef7c/data/s3rcacg63a6lfieybvp7dw357y.json.gz',
        #   'etag': 'ba00d841bd1eec340400e8d62c778aa3-1',
        #   'itemCount': 5344,
        #   'md5Checksum': 'R66Q93z6mjqdBW/mkgj0/A==',
        # }
        resp = s3_client.get_object(
            Bucket=bucket,
            Key=manifest_item['dataFileS3Key'],
            IfMatch=manifest_item['etag'],
        )
        with gzip.open(resp['Body'], 'rt') as data_item_lines:
            for data_item_line in data_item_lines:
                # Remove the wrapping "Item" property at the top level
                item = json.loads(data_item_line)['Item']
                item = remove_protected_attrs(item)
                item_json = json.dumps(item)
                with print_lock:
                    sys.stdout.write(item_json)
                    sys.stdout.write('\n')


@click.command()
@click.option('--region', '-r', help='AWS region name')
@click.option(
    '--procs', '-p', help='Number of processes to use', default=4, show_default=True
)
@click.argument('export-arn')
@click.version_option(version=carica_dynamodb_tools.version.__version__)
def cli(region: str, procs: int, export_arn: str):
    """
    Dump all items in a JSON-format S3 export of a DynamoDB table to stdout,
    one JSON item per line.

    When you export a DynamoDB table to S3 in JSON format, DynamoDB writes
    JSONL objects to S3, but those objects contain a serialized format of
    the DynamoDB item that places each item's data inside a top-level "Item"
    attribute at the root of its JSONL line.  The output of this command
    removes that nesting technique, returning each item's serialized data
    at the root of its JSONL line.  This makes the output compatible with
    the "dump" command.

    Protected attributes (those starting with "aws:") are not included in output.
    """
    num_procs = int(procs)
    if num_procs < 1:
        raise BadParameter('must be > 0', param_hint='procs')

    bucket, data_manifest_response = get_export_data_items(region, export_arn)

    # Limiting the queue size puts backpressure on the producer.
    manifest_item_q = multiprocessing.Queue(maxsize=num_procs * 10)
    print_lock = multiprocessing.Lock()
    proc_args = (
        region,
        bucket,
        manifest_item_q,
        print_lock,
    )
    procs = [
        multiprocessing.Process(target=batch_worker, args=proc_args)
        for _ in range(num_procs)
    ]

    for p in procs:
        p.start()

    # Read manifest items, putting one decoded item into the queue at a time.
    # Put blocks when the queue is full.
    for line in data_manifest_response.iter_lines():
        manifest_item_q.put(json.loads(line))

    for _ in procs:
        manifest_item_q.put(None)

    for p in procs:
        p.join()


if __name__ == '__main__':
    cli()
