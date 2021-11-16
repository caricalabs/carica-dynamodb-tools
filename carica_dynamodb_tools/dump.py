import json
import sys

import click

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


@click.command()
@click.option('--region', '-r', help='AWS region name')
@click.argument('table')
@click.version_option(version=carica_dynamodb_tools.version.__version__)
def cli(region, table):
    """
    Dump a DynamoDB table's items to stdout, one JSON item per line.
    """
    session = boto_session(region_name=region)
    client = session.client('dynamodb')
    paginator = client.get_paginator('scan')
    for page in paginator.paginate(TableName=table):
        for item in page['Items']:
            item = remove_protected_attrs(item)
            json.dump(item, sys.stdout)
            sys.stdout.write('\n')


if __name__ == '__main__':
    cli()
