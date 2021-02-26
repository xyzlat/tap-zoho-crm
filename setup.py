#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="tap-zoho-crm",
    version="0.0.3",
    description="tap to fetch zoho crm API",
    py_modules=["tap_zoho_crm"],
    install_requires=[
        "backoff==1.8.0",
        "requests==2.22.0",
        "singer-python==5.8.1",
        "python-dateutil==2.8.0"
    ],
    entry_points="""
          [console_scripts]
          tap-zoho-crm=tap_zoho_crm:main
      """,
    packages=["tap_zoho_crm"],
    package_data={"tap_zoho_crm": [
        "schemas/*.json"]},
    include_package_data=True,
)
