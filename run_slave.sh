#!/bin/bash
echo "start bench slave..."
nohup bin/benchmark-worker   --port 9090   --stats-port 9091 > worker.log 2>&1 &