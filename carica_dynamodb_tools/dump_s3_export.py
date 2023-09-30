import gzip
import multiprocessing
from multiprocessing import Queue
from typing import Tuple

import click
import orjson
import sys
from botocore.response import StreamingBody
from click import BadParameter

import carica_dynamodb_tools.version
import carica_dynamodb_tools.version
from carica_dynamodb_tools.session import boto_session
from carica_dynamodb_tools.utils import remove_protected_attrs


def get_export_data_items(
    region: str, export_arn: str
) -> Tuple[str, int, StreamingBody]:
    """
    :return: a tuple containing the bucket name, item count, and the response body
        to read the data files manifest JSON object
    """
    session = boto_session(region_name=region)
    dynamodb_client = session.client('dynamodb')
    s3_client = session.client('s3')

    desc = dynamodb_client.describe_export(ExportArn=export_arn)['ExportDescription']
    if desc['ExportFormat'] != 'DYNAMODB_JSON':
        print(f'ExportFormat is not DYNAMODB_JSON', file=sys.stderr)
        sys.exit(1)
    export_status = desc['ExportStatus']
    if export_status != 'COMPLETED':
        print(f'ExportStatus is {export_status}', file=sys.stderr)
        sys.exit(1)

    bucket = desc['S3Bucket']
    prefix = desc.get('S3Prefix', '')
    manifest_key = desc['ExportManifest']

    # Download the small export manifest JSON file
    resp = s3_client.get_object(Bucket=bucket, Key=f'{prefix}{manifest_key}')
    manifest = orjson.loads(resp['Body'].read())
    item_count = manifest['itemCount']
    manifest_files_key = manifest['manifestFilesS3Key']

    # Open the data items manifest JSON file, but return the response so the caller
    # can stream lines out of it.
    resp = s3_client.get_object(Bucket=bucket, Key=manifest_files_key)
    return bucket, item_count, resp['Body']


def batch_worker(
    region: str,
    bucket: str,
    item_q: Queue,
    print_lock: multiprocessing.Lock,
    item_total: multiprocessing.Value,
    error_total: multiprocessing.Value,
) -> None:
    """
    Multiprocessing worker for dumping JSONL archives in S3.

    Quits when it reads a ``None`` from the queue.
    """
    session = boto_session(region_name=region)
    s3_client = session.client('s3')
    for manifest_item in iter(item_q.get, None):
        # The manifest item is the contents of one manifestFilesS3Key file.  It looks like:
        # {
        #   'dataFileS3Key': 'AWSDynamoDB/01680958677849-381aef7c/data/s3rcacg63a6lfieybvp7dw357y.json.gz',
        #   'etag': 'ba00d841bd1eec340400e8d62c778aa3-1',
        #   'itemCount': 5344,
        #   'md5Checksum': 'R66Q93z6mjqdBW/mkgj0/A==',
        # }
        key = manifest_item['dataFileS3Key']
        etag = manifest_item['etag']
        try:
            resp = s3_client.get_object(Bucket=bucket, Key=key, IfMatch=etag)
            with gzip.open(resp['Body'], 'rt') as data_item_lines:
                for data_item_line in data_item_lines:
                    # Remove the wrapping "Item" property at the top level
                    item = orjson.loads(data_item_line)['Item']
                    item = remove_protected_attrs(item)
                    item_json = orjson.dumps(item)
                    with print_lock:
                        # Appending the newline and calling write one time, instead of
                        # writing the newline with a second call, prevents an apparent
                        # race where the underlying output doesn't stay consistent when
                        # multiple threads write to it quickly, even with print_lock.
                        # The effect of that condition is extra or misplaced newlines
                        # in the output, which we don't want.
                        sys.stdout.buffer.write(item_json + b'\n')
                    with item_total.get_lock():
                        item_total.value += 1
        except Exception as e:
            with error_total.get_lock():
                error_total.value += 1
            with print_lock:
                print(
                    f'Error getting s3://{bucket}/{key.lstrip("/")} etag={etag}: {e}',
                    file=sys.stderr,
                )


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

    bucket, item_count, data_manifest_response = get_export_data_items(
        region, export_arn
    )

    # Limiting the queue size puts backpressure on the producer.
    manifest_item_q = multiprocessing.Queue(maxsize=num_procs * 10)
    item_total = multiprocessing.Value('i')
    error_total = multiprocessing.Value('i')

    print_lock = multiprocessing.Lock()
    proc_args = (
        region,
        bucket,
        manifest_item_q,
        print_lock,
        item_total,
        error_total,
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
        manifest_item_q.put(orjson.loads(line))

    for _ in procs:
        manifest_item_q.put(None)

    for p in procs:
        p.join()

    error = False
    if item_total.value != item_count:
        print(
            f'Expected {item_count} items; {item_total.value} items in backup',
            file=sys.stderr,
        )
        error = True
    if error_total.value > 0:
        print(f'{error_total.value} errors getting item data', file=sys.stderr)
        error = True
    if error:
        sys.exit(1)


if __name__ == '__main__':
    cli()
