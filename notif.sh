#!/bin/bash

usage="notif.sh <folder> <n_jobs> <upload|prep>"

if test "$#" -ne 3; then
    echo "Usage:" $usage
    exit
fi

WATCHED=$1
n_jobs=$2
cmd=$3
processes=


inotifywait -mre CLOSE_WRITE,CREATE,MOVED_TO --format $'%e\t%w%f' $WATCHED |
    while IFS=$'\t' read -r events new
    do
        make $cmd fname=$new -B &
        processes="$processes $!"
        nchild=`echo $processes | wc -w`
        p_a=( $processes )
        echo "$nchild": $processes
        if [ ! $nchild -lt $n_jobs ] 
        then
            echo "waiting for ${p_a[0]}"
            wait ${p_a[0]}
            processes=`echo $processes|sed "s/${p_a[0]}//g"`
        fi
    done

