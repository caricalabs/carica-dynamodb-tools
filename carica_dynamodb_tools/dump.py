import json
import sys
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

import click
from click import BadParameter

import carica_dynamodb_tools.version
import carica_dynamodb_tools.version
from carica_dynamodb_tools.session import boto_session

print_lock = Lock()


def remove_protected_attrs(item: dict) -> dict:
    """
    Remove protected (AWS-only) attributes from a DynamoDB item.
    """
    attrs = [attr for attr in item.keys() if attr.startswith('aws:')]
    for attr in attrs:
        del item[attr]
    return item


def _parallel_scan_worker(region: str, table: str, num_threads: int, this_thread: int) -> None:
    try:
        session = boto_session(region_name=region)
        client = session.client('dynamodb')
        paginator = client.get_paginator('scan')
        for page in paginator.paginate(TableName=table, TotalSegments=num_threads, Segment=this_thread):
            for item in page['Items']:
                item = remove_protected_attrs(item)
                item_json = json.dumps(item)
                with print_lock:
                    sys.stdout.write(item_json)
                    sys.stdout.write('\n')
    except Exception as e:
        with print_lock:
            print(str(e), file=sys.stderr)


@click.command()
@click.option('--region', '-r', help='AWS region name')
@click.option(
    '--num-threads', '-n', help='Number of parallel scan threads', default=4, show_default=True
)
@click.argument('table')
@click.version_option(version=carica_dynamodb_tools.version.__version__)
def cli(region: str, table: str, num_threads: str):
    """
    Dump a DynamoDB table's items to stdout, one JSON item per line.
    """
    num_threads = int(num_threads)
    if num_threads < 1:
        raise BadParameter('must be > 0', param_hint='num_threads')

    e = ThreadPoolExecutor()
    for n in range(num_threads):
        e.submit(_parallel_scan_worker, region, table, num_threads, n)
    e.shutdown()


if __name__ == '__main__':
    cli()
