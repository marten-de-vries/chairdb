#!/bin/bash

python -m pytest --cov=microcouch --cov=tests --cov-report html --cov-branch -vv
