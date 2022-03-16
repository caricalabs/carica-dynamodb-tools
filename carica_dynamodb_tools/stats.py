import csv
import json
import math
import sys
from typing import Tuple, Any, Iterator, Iterable

import click

import carica_dynamodb_tools.version
from carica_dynamodb_tools.item_size import item_size


def attr_name(a: str) -> str:
    return 'attr.' + a


def attr_any_value(attr: dict) -> Any:
    return next(iter(attr.values()))


def generate_stats(attrs: Iterable[str]) -> Iterator[dict]:
    """Yield a dict containing stats for each item read from stdin."""
    for line in sys.stdin.readlines():
        item_stats = {}
        item = json.loads(line.strip())
        if attrs:
            item_stats.update({attr_name(a): attr_any_value(item[a]) for a in attrs})

        size = item_size(item)
        item_stats['size'] = size
        item_stats['read_units'] = math.ceil(size / 4096) / 2
        item_stats['write_units'] = math.ceil(size / 1024)
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
