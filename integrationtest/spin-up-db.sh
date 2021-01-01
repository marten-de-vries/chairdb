#!/bin/bash

docker run -e COUCHDB_USER=marten -e COUCHDB_PASSWORD=test --network="host" -v /home/marten/Bureaublad/couch/integrationtest/data:/opt/couchdb/data couchdb
