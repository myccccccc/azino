#!/bin/bash


./txplanner_server -listen_addr="0.0.0.0:8001"\
                   -txindex_addrs="0.0.0.0:8002 0.0.0.0:8003"\
                   -minloglevel=2 &
sleep 3

./storage_server -listen_addr="0.0.0.0:8000"\
                 -minloglevel=2 &


./txindex_server -listen_addr="0.0.0.0:8002"\
                 -txindex_addr="0.0.0.0:8002"\
                 -minloglevel=2 &

./txindex_server -listen_addr="0.0.0.0:8003"\
                 -txindex_addr="0.0.0.0:8003"\
                 -minloglevel=2 &