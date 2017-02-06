#!/bin/sh

TARGET="http://localhost:8080"

curl -X POST "$TARGET/add" \
    -d "name=sample_worker" \
    -d "id=123456780" \
    -d 'json={"message":"hello worker"}'
