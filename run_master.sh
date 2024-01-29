#!/bin/bash
echo "start bench master..."
nohup bin/benchmark \
--drivers driver-pulsar/pulsar.yaml \
--workers http://11.149.254.164:9090,http://11.149.254.168:9090,http://11.149.254.174:9090,http://11.149.254.190:9090 \
workloads/perftest-100-topics-0-partition-1kb.yaml > master.log 2>&1 &