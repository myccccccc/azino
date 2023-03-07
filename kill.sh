#!/bin/bash

ps -ef | grep txplanner_server | grep -v grep |  awk '{print $2}' | xargs kill
ps -ef | grep storage_server | grep -v grep |  awk '{print $2}' | xargs kill
ps -ef | grep txindex_server | grep -v grep |  awk '{print $2}' | xargs kill

rm -rf azino_storage
rm -rf log_*