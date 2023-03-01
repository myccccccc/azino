#!/bin/bash

./txplanner_server -txindex_addrs="0.0.0.0:8002 0.0.0.0:8003" -minloglevel=2 &
sleep 3

./storage_server &
./txindex_server -txindex_addr="0.0.0.0:8002" -minloglevel=2 &
./txindex_server -txindex_addr="0.0.0.0:8003" -minloglevel=2 &