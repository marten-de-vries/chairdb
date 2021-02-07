#!/bin/bash
#hypercorn chairdb.server:app --reload --access-log - --error-log -
uvicorn chairdb.server:app --reload --reload-dir=chairdb
