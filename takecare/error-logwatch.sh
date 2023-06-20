#!/bin/bash
WATCHED=tmp

inotifywait -mre CLOSE_WRITE,MOVED_TO --format $'%e\t%w%f' $WATCHED |
    while IFS=$'\t' read -r events new
    do  
        if [[ "$new" =~ .*patlog$ ]]; then
            cat $WATCHED/.*.patlog > $WATCHED/.error.log
            ./error-logformat.sh $WATCHED/.error.log $WATCHED/errorfile.md
        fi
    done
