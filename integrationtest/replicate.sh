#!/bin/bash
curl http://marten:test@localhost:5984/_replicate -H "Content-Type: application/json" -d '{"source": "brassbandwirdum", "target": "http://localhost:8000/brassbandwirdum", "create_target": true}' -v
curl http://marten:test@localhost:5984/_replicate -H "Content-Type: application/json" -d '{"source": "activiteitenweger", "target": "http://localhost:8000/activiteitenweger", "create_target": true}' -v
