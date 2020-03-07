#!/usr/bin/env python3
"""A setuptools based setup module.
See:
https://packaging.python.org/en/latest/distributing.html
https://github.com/pypa/sampleproject
"""

# Always prefer setuptools over distutils
from codecs import open
from os import path

from setuptools import setup, find_packages

import carica_dynamodb_tools.version

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='carica_dynamodb_tools',
    version=carica_dynamodb_tools.version.__version__,
    description='Tools to manage DynamoDB tables',
    long_description=long_description,
    url='https://github.com/caricalabs/carica-dynamodb-tools',
    author='Carica Labs, LLC',
    author_email='info@caricalabs.com',
    license='APL 2.0',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.7',
    ],
    keywords='dynamodb backup restore archive dump load',
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),
    install_requires=['boto3~=1.9.99', 'click~=6.7',],
    extras_require={'dev': ['check-manifest'], 'test': [],},
    package_data={},
    entry_points={
        'console_scripts': [
            'carica-dynamodb-dump=carica_dynamodb_tools.dump:cli',
            'carica-dynamodb-load=carica_dynamodb_tools.load:cli',
        ],
    },
)
