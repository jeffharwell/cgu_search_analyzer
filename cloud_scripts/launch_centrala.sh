#!/bin/bash

## In this case $1 is the name of the file containing the queries that we want
## to search for. Using gcutil I name the Google Compute instance after the filename
## which makes it easier to remember what is going on later.
## So commands like gcutil push $1 ./$1 push the file ./$1 to the server $1 ... see :)

gcutil addinstance $1 --zone us-central1-a --machine_type f1-micro --image debian-7
sleep 10
gcutil push $1 ./get_google_results.py ~ || (sleep 5 && gcutil push $1 ./get_google_results.py ~)
gcutil push $1 ./initialize.sh ~
gcutil push $1 ./$1 ~
gcutil ssh $1
