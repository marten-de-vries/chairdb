#!/bin/bash

flake8 chairdb tests --max-complexity=5
rm -f test.sqlite3
python -m pytest --cov=chairdb --cov=tests --cov-report html --cov-branch -vv
