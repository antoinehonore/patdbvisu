#!/bin/bash

usage="notif.sh <folder>"

if test "$#" -ne 1; then
    echo "Usage:" $usage
    exit
fi

WATCHED=$1
processes=

inotifywait -mre CLOSE_WRITE,MOVED_TO --format $'%e\t%w%f' $WATCHED |
    while IFS=$'\t' read -r events new
    do
		log_fname="log/patdbvisu_upload_`date +%Y%m%d`.log"
		echo "Uploading " $new "..."
		pyenv/bin/python bin/upload.py -i $new 2>> $log_fname
    done

