#!/bin/bash

if [ -z "$1" ]
  then
    echo "No argument supplied - should be file to process"
    exit
fi

if [ -z "$2" ]
  then
    echo "No argument supplied - should be the expansion type, c or s"
    exit
fi

if [ -z "$3" ]
  then
    echo "No argument supplied - should be the offset"
    exit
fi

mkdir results
sudo apt-get -y install python-simplejson
./get_google_results.py $1 $2 $3 > results.log &
