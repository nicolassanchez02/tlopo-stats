#!/bin/sh

cd ../test
ln -fs /app/build/tlopostats .

set -e

if [ "$1" == "all" ]; then
    python test_av_mgr.py
    python test_collectors.py
    python test_rpc.py
    python test_redis_resilience.py
else
    python test_$1.py $2
fi
