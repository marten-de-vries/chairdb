#!/bin/bash
uvicorn microcouch.server:app --reload --reload-dir=microcouch
