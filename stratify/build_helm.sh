#!/bin/bash
# This must be run from the projects home directory which has the child dir helm

CHART_SRC_DIR=$1
CHART_NAME=$2
CHART_VERSION=$3
APP_VERSION=$4

# TODO - Need to create this
APP_URL='https://isc-patrick.github.io/sds-insights/'

# Rmove old packages 
rm helm/*.tgz

helm package helm -d helm
helm repo index helm --url=${APP_URL}

cp helm/index.yaml ${CHART_SRC_DIR}/
cp helm/sds.yaml ${CHART_SRC_DIR}/

# Remove old packages
rm ${CHART_SRC_DIR}/*.tgz
cp helm/${CHART_NAME}-${CHART_VERSION}.tgz ${CHART_SRC_DIR}/

echo "You still need to add and commit from ${CHART_SRC_DIR} before deploying"
