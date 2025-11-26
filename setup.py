# setup.py
from setuptools import setup, find_packages

setup(
    name='starburst-data-pipelines',
    version='0.1.0',
    description='Shared utilities for Starburst Data Product deployment',
    # Use 'packages' to automatically find the package in the src directory
    packages=find_packages(where='src'), 
    package_dir={'': 'src'},
)