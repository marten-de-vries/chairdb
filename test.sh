#!/bin/bash

flake8 microcouch tests --max-complexity=5
rm -f test.sqlite3
python -m pytest --cov=microcouch --cov=tests --cov-report html --cov-branch -vv
