#!/bin/bash
WATCHED=tmp

inotifywait -mre CLOSE_WRITE,MOVED_TO --format $'%e\t%w%f' $WATCHED |
    while IFS=$'\t' read -r events new
    do
        if [[ "$new" =~ .*xlsx$ ]]; then
            echo $new;
            sbatch --mem=1G --ntasks 1 --cpus-per-task 1 ./error-filecheck.sh $new
        fi
    done
