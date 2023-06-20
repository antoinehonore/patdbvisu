#!/bin/bash
# This formats the error.log file into a markdown table.

error_in=$1
error_out=$2
folder_name=`basename \`dirname $error_in\``
error_kind="patientID typo category date"

date > $error_out

for e in $error_kind; do
    echo "" >> $error_out;
    echo -e "# $e errors in $folder_name/\n" >> $error_out;
    echo -e "| Filename | PatientID | Date | Event | Specificities | Notes |" >> $error_out;
    echo -e "| --------- |--------- | ---- | ----  | --------------| ----- |" >> $error_out;
    cat $error_in | grep "$e|" | grep -v "10000" | grep -v "12345" | grep -v "11111" | sed -e "s/$e|/|/g" -e "s/|$e/|/g" >> $error_out;
done

