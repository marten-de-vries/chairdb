#!/bin/bash

docker run -e COUCHDB_USER=marten -e COUCHDB_PASSWORD=test --network="host" -v /home/marten/git/chairdb/integrationtest/data:/opt/couchdb/data couchdb
