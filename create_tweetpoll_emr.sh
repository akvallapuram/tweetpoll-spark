#! /bin/bash

aws emr create-cluster --ec2-attributes KeyName="assignment" \
--release-label=emr-5.28.0 \
--applications Name=Hadoop Name=Spark \
--log-uri="s3://tweetpoll-backend/logs" \
--configurations file://emr_config.json \
--bootstrap-actions \
Name="Install Textblob",Path="s3://tweetpoll-backend/emr_bootstrap.sh" \
--instance-count=3 \
--instance-type=m1.large \
--name="TweetpollEMRCluster" \
--use-default-roles 
