#!/bin/bash
# update docker container
docker build -t debs-acm-flink-work ../.
docker tag debs-acm-flink-work omariani24/debs-acm-flink-work:latest
docker push omariani24/debs-acm-flink-work:latest
