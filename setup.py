#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Setup script for rdflib-neo4j
"""
import os
import sys

if os.path.exists('MANIFEST'):
    os.remove('MANIFEST')

from setuptools import setup, find_packages

if sys.argv[-1] == 'setup.py':
    print("To install, run 'python setup.py install'")
    print()

if sys.version_info[:2] < (3, 6):
    print("Neo4j requires Python 3.6 or later (%d.%d detected)." %
          sys.version_info[:2])
    sys.exit(-1)

if __name__ == "__main__":
    setup(
        name="graphexpectations",
        version="0.0.1b0",
        author="Jesús Barrasa",
        author_email="jbarrasa@outlook.com",
        description="User friendly utilities to create data quality rules on a Neo4j graph",
        keywords="neo4j, data quality, SHACL, neosemantics, n10s",
        long_description="User friendly utilities to create data quality rules on a Neo4j graph",
        license="Apache 2",
        platforms="All",
        url="https://github.com/neo4j-labs/graphexpectations",
        install_requires=[
            'rdflib >= 6.0.0','neo4j >= 4.4.0',
        ],
        packages=find_packages(),
        zip_safe=False
    )
