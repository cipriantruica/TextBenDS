#!/bin/bash

RPATH="./Hive_Results/TwitterDB"
QPATH="./Hive_Queries/*"


for db in `seq 1 5`
do
    db=$(($db * 500))
    dbname="twitterdb"$db"k"
    # echo $dbname    
    for query in $QPATH
    do
        # echo $query
        filename=$(basename "$query")
        filename="${filename%.*}"
        # echo $filename
        output=$RPATH$db"K_"$filename
        # echo $output
        for i in `seq 1 10`
        do
            start=`date +%s.%N`   
            hive --database $dbname -f $query
            end=`date +%s.%N`
            runtime=$(python -c "print(${end} - ${start})")
            printf $runtime", " >> $output
        done;
    done;
done;