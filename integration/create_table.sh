#!/bin/sh

set -eux
bq mk -t --project_id=${PROJECT_NAME} --schema=$(dirname $0)/schema.json ${DATASET_NAME}.${TABLE_NAME} 
