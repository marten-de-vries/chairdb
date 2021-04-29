#!/bin/bash

flake8 chairdb tests --max-complexity=5
python -m pytest --cov=chairdb --cov=tests --cov-report html --cov-branch -vv
