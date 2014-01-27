#!/bin/bash

exit(0)

## This isn't really a script, more a set of notes
## it could be one day through ...

## To create a new key, only do this once
ssh-keygen -t rsa -b 2048 -f aws-key

## I used the debian images, the AMI (image id) differs from 
## region to region, here is the debian website that has 
## the latest AMIs for each region
# https://wiki.debian.org/Cloud/AmazonEC2Image

## To initialize a new region ... only do this once per reason
ec2-import-keypair aws-key --public-key-file ./aws-key.pub --region ap-northeast-1
ec2-authorize --region ap-northeast-1 default -p 22

## To launch a new instance
ec2-run-instances ami-17a0c216 -k aws-key --instance-type t1.micro --region ap-northeast-1
ec2-describe-instances --region ap-northeast-1

## Now log in with the IP that ec2-describe-instances returns
ssh -i aws-key admin@XXX.XXX.XXX.XXX

## Once in the ami you must update the repositories (they are empty) and then
## install simplejson
sudo apt-get update
sudo apt-get install python-simplejson
