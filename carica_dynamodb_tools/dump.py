import click
import orjson
import sys

import carica_dynamodb_tools.version
import carica_dynamodb_tools.version
from carica_dynamodb_tools.session import boto_session
from carica_dynamodb_tools.utils import remove_protected_attrs


@click.command()
@click.option('--region', '-r', help='AWS region name')
@click.argument('table')
@click.version_option(version=carica_dynamodb_tools.version.__version__)
def cli(region, table):
    """
    Dump a DynamoDB table's items to stdout, one JSON item per line.

    Protected attributes (those starting with "aws:") are not included in output.
    """
    session = boto_session(region_name=region)
    client = session.client('dynamodb')
    paginator = client.get_paginator('scan')
    for page in paginator.paginate(TableName=table):
        for item in page['Items']:
            item = remove_protected_attrs(item)
            sys.stdout.buffer.write(orjson.dumps(item))
            sys.stdout.buffer.write(b'\n')


if __name__ == '__main__':
    cli()
