#!/bin/bash

## It is really irritating to have to push a new version of code up to every server individually
## This script takes the local copy of 'get_google_results.py' and pushes it up to every EC2 and
## Google Compute instance, as well as my desktop.

## To you use you will need to update the aws_clients and google_clients arrays ... and stay away
## from my desktop.

aws_clients=( "54.193.45.34" "54.194.215.124" "54.207.31.187" "54.254.176.240" "54.206.25.34" )
google_clients=( "corpusag" "corpusah" )

for i in "${aws_clients[@]}"
do
    scp -i aws-key ./get_google_results.py admin@$i:~ || die
done

for i in "${google_clients[@]}"
do
    gcutil push $i ./get_google_results.py ~ || die
done

echo "Put in password for fox.fuller.edu"
scp ./get_google_results.py jharwell@fox.fuller.edu:~/bin/python/search_expansion/
