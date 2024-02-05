#!/bin/bash

ray start --head --dashboard-host "0.0.0.0" --disable-usage-stats

# TODO - modify logviewer config to point at the /tmp/ray/logs directory
#/home/python/log-viewer-1.0.5/logviewer.sh &

uvicorn insights.rest.main:app  --host "0.0.0.0" --port "8000" --reload --log-level trace