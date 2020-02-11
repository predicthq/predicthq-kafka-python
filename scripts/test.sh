#!/usr/bin/env bash

set -e
pip install -r requirements/test.txt
python -m pytest --cov=phq --cov-report xml
pylint phq --exit-zero
