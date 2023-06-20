#!/bin/bash
#SBATCH --output /dev/null
fname=$1
thedirname=`dirname $fname`
thebasename=`basename $fname`

nom_fname=nomenclature/Nomenclature.xlsx
PYTHON=../../../virtualenvs/pypatdb/bin/python

$PYTHON xlsx2csv_takecare.py -i $fname -nom $nom_fname > /dev/null 2> ${thedirname}/.${thebasename%.*}_error.patlog
