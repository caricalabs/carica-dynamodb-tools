import click
import sys
import time

import carica_dynamodb_tools.version
import carica_dynamodb_tools.version
from carica_dynamodb_tools.session import boto_session

WAIT_SLEEP_SECONDS = 5


@click.command()
@click.option('--region', '-r', help='AWS region name')
@click.option(
    '--wait',
    '-w',
    help='Wait until the export finishes',
    is_flag=True,
)
@click.argument('table')
@click.argument('bucket-name')
@click.version_option(version=carica_dynamodb_tools.version.__version__)
def cli(region: str, table: str, bucket_name: str, wait: bool):
    """
    Export a DynamoDB table to an S3 bucket at a point-in-time of "now".

    The export ARN is printed to stdout as soon as the export is created.
    You can feed it to carica-dynamodb-dump-s3-export to retrieve the dumped
    data when the export is COMPLETED.

    If you use the --wait flag, the export ARN is printed to stdout and the
    process waits until the export finishes.  If the export finishes with
    a COMPLETED status, the process's exit status is 0.  If the export
    finishes with a FAILED status, the process's exit status is 1.
    """
    session = boto_session(region_name=region)
    dynamodb_client = session.client('dynamodb')

    table_arn = dynamodb_client.describe_table(TableName=table)['Table']['TableArn']

    export_arn = dynamodb_client.export_table_to_point_in_time(
        TableArn=table_arn,
        ExportFormat='DYNAMODB_JSON',
        S3Bucket=bucket_name,
    )['ExportDescription']['ExportArn']

    print(export_arn)
    sys.stdout.flush()

    if wait:
        while True:
            desc = dynamodb_client.describe_export(ExportArn=export_arn)[
                'ExportDescription'
            ]
            status = desc['ExportStatus']
            if status == 'IN_PROGRESS':
                time.sleep(WAIT_SLEEP_SECONDS)
                continue
            elif status == 'COMPLETED':
                break
            else:
                print(f'ExportStatus is {status}', file=sys.stderr)
                sys.exit(1)


if __name__ == '__main__':
    cli()
