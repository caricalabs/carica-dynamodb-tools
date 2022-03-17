import csv
import json
import math
import sys
from typing import Tuple, Iterator, Iterable

import click

import carica_dynamodb_tools.version
from carica_dynamodb_tools.item_size import item_size


def generate_stats(attrs: Iterable[str]) -> Iterator[dict]:
    """Yield a dict containing stats for each item read from stdin."""
    for line in sys.stdin.readlines():
        item_stats = {}
        item = json.loads(line.strip())

        # Add attributes specified on the command-line.  Prefix them with
        # "attr." so they don't collide with other columns we add.
        for a in attrs:
            attr_val = item.get(a)
            item_stats['attr.' + a] = str(next(iter(attr_val.values()))) if attr_val else ''

        # Calculate the byte size of the item DynamoDB will charge us for, and
        # a few derived statistics.
        size = item_size(item)
        read_blocks_required = math.ceil(size / 4096)
        read_bytes_required = read_blocks_required * 4096
        write_blocks_required = math.ceil(size / 1024)
        write_bytes_required = read_blocks_required * 1024

        item_stats['size'] = size
        item_stats['read_units'] = read_blocks_required / 2
        item_stats['read_efficiency'] = size / read_bytes_required
        item_stats['read_excess'] = read_bytes_required - size
        item_stats['write_units'] = write_blocks_required
        item_stats['write_efficiency'] = size / write_bytes_required
        item_stats['write_excess'] = write_bytes_required - size
        yield item_stats


@click.command()
@click.option(
    '--format',
    '-f',
    type=click.Choice(['csv', 'json']),
    default='json',
    help='print statistics in this format',
)
@click.option(
    '--attr',
    '-a',
    help='include this attribute and its value in the output',
    multiple=True,
)
@click.version_option(version=carica_dynamodb_tools.version.__version__)
def cli(format: str, attr: Tuple[str]):
    """
    Print statistics about DynamoDB JSON item lines read from stdin.
    """
    if format == 'json':
        for item_stats in generate_stats(attr):
            json.dump(item_stats, sys.stdout)
    else:
        rows = []
        for item_stats in generate_stats(attr):
            rows.append(item_stats)

        fieldnames = set()
        for item_stats in rows:
            fieldnames.update(item_stats.keys())

        writer = csv.DictWriter(sys.stdout, fieldnames=sorted(fieldnames))
        writer.writeheader()
        writer.writerows(rows)


if __name__ == '__main__':
    cli()
