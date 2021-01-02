#!/bin/bash

flake8 microcouch
python -m pytest --cov=microcouch --cov=tests --cov-report html --cov-branch -vv
