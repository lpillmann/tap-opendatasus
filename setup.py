#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-opendatasus",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_opendatasus"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer-python",
        "elasticsearch==7.5.1",
        "elasticsearch-dsl==7.3.0",
    ],
    entry_points="""
    [console_scripts]
    tap-opendatasus=tap_opendatasus:main
    """,
    packages=["tap_opendatasus"],
    package_data={"schemas": ["tap_opendatasus/schemas/*.json"]},
    include_package_data=True,
)
