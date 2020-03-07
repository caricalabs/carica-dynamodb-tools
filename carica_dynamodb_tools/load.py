import json
import multiprocessing
import sys
from multiprocessing.queues import Queue

import boto3
import click
from botocore.exceptions import ClientError

import carica_dynamodb_tools.version
import carica_dynamodb_tools.version


def batch_write_items_worker(
    region: str, table: str, batch_q: Queue, print_lock: multiprocessing.Value
) -> None:
    session = boto3.session.Session(region_name=region)
    client = session.client('dynamodb')

    for batch in iter(batch_q.get, None):
        request_items = {table: [{'PutRequest': {'Item': item}} for item in batch]}
        try:
            client.batch_write_item(RequestItems=request_items)
        except ClientError as e:
            with print_lock.get_lock():
                print(str(e), file=sys.stderr)
                sys.stderr.flush()


@click.command()
@click.option('--region', '-r', help='AWS region name')
@click.option('--procs', '-p', help='Number of processes to use', default=4)
@click.argument('table')
@click.version_option(version=carica_dynamodb_tools.version.__version__)
def cli(region, procs, table):
    """
    Load items into a DynamoDB table from stdin, one JSON item per line.
    """
    # DynamoDB API limit
    BATCH_MAX_ITEMS = 25

    # Limiting the queue size puts backpressure on the producer.
    batch_q = multiprocessing.Queue(maxsize=int(procs) * 10)
    print_lock = multiprocessing.Value('b')
    proc_args = (region, table, batch_q, print_lock)
    procs = [
        multiprocessing.Process(target=batch_write_items_worker, args=proc_args)
        for _ in range(int(procs))
    ]

    for p in procs:
        p.start()

    batch = []
    while True:
        line = sys.stdin.readline()
        if line:
            batch.append(json.loads(line))

        # Enqueue a batch to be written there are no more items or the batch is full
        if not line or len(batch) == BATCH_MAX_ITEMS:
            batch_q.put(batch)
            batch = []

        if not line:
            break

    for _ in procs:
        batch_q.put(None)

    for p in procs:
        p.join()


if __name__ == '__main__':
    cli()
