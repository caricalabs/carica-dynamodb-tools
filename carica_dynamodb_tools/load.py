import json
import random

import boto3
import click
import multiprocessing
import sys
import time
from botocore.exceptions import ClientError
from click import BadParameter
from multiprocessing.queues import Queue

import carica_dynamodb_tools.version
import carica_dynamodb_tools.version

# DynamoDB API limit
BATCH_MAX_ITEMS = 25

# Initial retry delay in milliseconds
RETRY_DELAY_BASE_MS = 50

# Maximum retry delay in milliseconds
RETRY_DELAY_CAP_MS = 5000


def sleep_for_retry(attempt: int) -> None:
    """
    Sleep for an appropriate amount of time after a failed batch write.

    ``attempt`` should be 1 on the first call, 2 on the next call, etc.

    Implements "Full Jitter" from https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
    """
    max_delay_ms = min(RETRY_DELAY_CAP_MS, RETRY_DELAY_BASE_MS * pow(2, attempt - 1))
    jittered_delay_ms = random.randint(0, max_delay_ms)
    jittered_delay_secs = jittered_delay_ms / 1000.0
    time.sleep(jittered_delay_secs)


def batch_worker(
    region: str,
    table: str,
    batch_q: Queue,
    print_lock: multiprocessing.Lock,
    success_total: multiprocessing.Value,
    failure_total: multiprocessing.Value,
    retry_total: multiprocessing.Value,
) -> None:
    """
    Multiprocessing worker for writing batches of items.

    Quits when it reads a ``None`` from the queue.
    """
    session = boto3.session.Session(region_name=region)
    client = session.client('dynamodb')
    for batch in iter(batch_q.get, None):
        request_items = [{'PutRequest': {'Item': item}} for item in batch]

        # Iterate until all the items have been written.  If BatchWriteItem
        # returns a throttling error response, that means none of the items
        # were written, and boto will retry the call automatically.  If any of
        # the items were successfully written, BatchWriteItem doesn't return an
        # error response.  Instead its response contains the unprocessed items
        # and we retry those in the next attempt (after a short delay).
        attempt = 1
        while request_items:
            num_unprocessed = 0
            num_processed = 0
            num_failed = 0

            try:
                resp = client.batch_write_item(RequestItems={table: request_items})
                unprocessed_items = resp.get('UnprocessedItems', {}).get(table, [])
                num_unprocessed = len(unprocessed_items)
                num_processed = len(request_items) - num_unprocessed
                # Put unprocessed items back in the list to be retried.
                request_items = unprocessed_items
            except ClientError as e:
                # boto3 automatically retries on throttling errors, so if we're
                # here, it's for something else (validation, internal error,
                # permissions, etc.) that retrying probably won't solve.
                with print_lock:
                    print(str(e), file=sys.stderr)
                num_failed = len(request_items)
                # Abandon them to prevent an infinite retry loop.
                request_items = []

            # Update the totals
            with success_total.get_lock():
                success_total.value += num_processed
            with retry_total.get_lock():
                retry_total.value += num_unprocessed
            with failure_total.get_lock():
                failure_total.value += num_failed

            if num_unprocessed:
                sleep_for_retry(attempt)
                attempt += 1


@click.command()
@click.option('--region', '-r', help='AWS region name')
@click.option(
    '--procs', '-p', help='Number of processes to use', default=4, show_default=True
)
@click.option(
    '--report/--no-report', '-r', help='Write a JSON report to stdout when finished'
)
@click.argument('table')
@click.version_option(version=carica_dynamodb_tools.version.__version__)
def cli(region: str, procs: str, report: bool, table: str):
    """
    Load items into a DynamoDB table from stdin, one JSON item per line.
    """
    num_procs = int(procs)
    if num_procs < 1:
        raise BadParameter('must be > 0', param_hint='procs')

    # Limiting the queue size puts backpressure on the producer.
    batch_q = multiprocessing.Queue(maxsize=num_procs * 10)
    print_lock = multiprocessing.Lock()
    success_total = multiprocessing.Value('i')
    failure_total = multiprocessing.Value('i')
    retry_total = multiprocessing.Value('i')
    proc_args = (
        region,
        table,
        batch_q,
        print_lock,
        success_total,
        failure_total,
        retry_total,
    )
    procs = [
        multiprocessing.Process(target=batch_worker, args=proc_args)
        for _ in range(num_procs)
    ]

    for p in procs:
        p.start()

    # Read items from stdin, batch them up, and send batches to workers.
    batch = []
    while True:
        line = sys.stdin.readline()
        if line:
            batch.append(json.loads(line))

        # Send the batch to the worker when it's full or stdin is closed.
        # We don't worry about checking for total batch size here (in
        # serialized JSON bytes) because we assume DynamoDB would not have
        # supplied records that are too large to load during the dump process.
        if len(batch) == BATCH_MAX_ITEMS or not line:
            batch_q.put(batch)
            batch = []

        if not line:
            break

    for _ in procs:
        batch_q.put(None)

    for p in procs:
        p.join()

    if report:
        with success_total.get_lock(), failure_total.get_lock(), retry_total.get_lock():
            report = {
                'loaded': success_total.value,
                'failed': failure_total.value,
                'retried': retry_total.value,
            }
        print(json.dumps(report))


if __name__ == '__main__':
    cli()
