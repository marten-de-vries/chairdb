#!/bin/bash
uvicorn chairdb.server:app --reload --reload-dir=chairdb
