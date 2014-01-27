#!/bin/bash

aws_clients=( "54.199.174.21" "54.193.45.34" "54.194.215.124" "54.207.31.187" "54.254.176.240" "54.206.25.34" )

for i in "${aws_clients[@]}"
do
    ssh -i aws-key admin@$i || exit 1
done
echo "Your Done!! Whew"
