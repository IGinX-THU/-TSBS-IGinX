#!/usr/bin/env bash

message(){
    echo -e "\e[1;44m"$*"\e[0m"
}

run_zookeeper(){
    message run zookeeper
    docker run -d --name zookeeper -p 2181:2181 --rm zookeeper
}

kill_zookeeper(){
    docker stop zookeeper
    message remove zookeeper
}

run_iotdb12(){
    message run iotdb
    docker run -d --name iotdb -p 6667:6667 --rm apache/iotdb:0.12.6-node
}

kill_iotdb12(){
    docker stop iotdb
    message remove iotdb
}

PATH=$PWD/bin:$PATH

gen_data(){
    message gen data
    tsbs_generate_data $DATABASE_CFG --log-interval="10s" | gzip>iginx-data.gz
}

load_data(){
    message load data
    cat iginx-data.gz | gunzip | tsbs_load_iginx > tsbs_load_iginx.log 2>&1
}

run_queries(){
    for query in $*; do
        message gen query: $query
        tsbs_generate_queries $DATABASE_CFG --query-type="$query" --queries=100 --file iginx-queries-$query
        message run query: $query
        tsbs_run_queries_iginx --file iginx-queries-$query >iginx-queries-$query.log 2>&1
    done
}

DATABASE_CFG='--seed=123 --format=iginx --use-case=iot --scale=10 --timestamp-start=2016-01-01T00:00:00Z --timestamp-end=2016-01-04T00:00:00Z'

QUERIES=
QUERIES=$QUERIES"last-loc "
QUERIES=$QUERIES"low-fuel "
QUERIES=$QUERIES"high-load "
QUERIES=$QUERIES"stationary-trucks "
QUERIES=$QUERIES"long-driving-sessions "
QUERIES=$QUERIES"long-daily-sessions "
QUERIES=$QUERIES"avg-vs-projected-fuel-consumption "
QUERIES=$QUERIES"avg-daily-driving-duration "
QUERIES=$QUERIES"avg-daily-driving-session "
QUERIES=$QUERIES"avg-load "
QUERIES=$QUERIES"daily-activity "
QUERIES=$QUERIES"breakdown-frequency "

mkdir -p tmp && cd tmp
export IGINX_HOME=/mnt/c/Users/aqnii/source/repos/TSBS-IGinX/iginx/main
message start at `date`
run_iotdb12
run_zookeeper
CURR_DIR=$PWD
cd $IGINX_HOME
sbin/start_iginx.sh >$CURR_DIR/iginx.log 2>&1 &
IGINX_PID=$!
cd $CURR_DIR
message run iginx with pid=$IGINX_PID
gen_data
sleep 5
load_data 
run_queries $QUERIES
kill $IGINX_PID
message kill iginx with pid=$IGINX_PID
kill_zookeeper
kill_iotdb12
message finish at `date`