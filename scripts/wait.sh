#!/bin/sh
set -eu

echo "Testing clickhouse ping"
timeout -s TERM 20 sh -c \
  'while [[ "$(curl -s -o /dev/null -L -w ''%{http_code}'' http://localhost:8123/ping)" != "200" ]];\
    do echo "Waiting..." && sleep 2;\
    done'
echo "OK!"
