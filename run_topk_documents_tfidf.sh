#!/bin/bash

MODE=client 
MASTER=yarn 
NUM_EXECS=16
NUM_CORES=1
MEM_EXECS=3G
NUM_TESTS=10
WEIGHT=TFIDF
CLASS=TopK_Documents_TFIDF
JAR_FILE=Spark_code/target/scala-2.11/textbends_2.11-0.1.jar
CMP=/usr/hdp/current/spark2-client/bin/spark-submit

# data base
for db in `seq 1 5`
do
    db=$(($db * 500))
    # query
    for q in `seq 1 4`
    do
        # gender
        for g in `seq 0 1`
        do
            # words
            for w in `seq 1 3`
            do
                for i in `seq 1 $NUM_TESTS`
                do
                    # build the edges table in Hive
                    $CMP --master $MASTER --deploy-mode $MODE --num-executors $NUM_EXECS --executor-cores $NUM_CORES --executor-memory $MEM_EXECS --class $CLASS $JAR_FILE $db $q $g $w $i >> "results/TwitterDB"$db"K_"$WEIGHT"_TOPKD_q"$q"_"$w"w"
                    sleep 10
                done;
            done;
		done;
	done;
done;
