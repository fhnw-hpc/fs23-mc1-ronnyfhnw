#!/bin/bash
docker build -f DockerFile.binance_processor -t binance_processor:latest .
docker build -f DockerFile.binance_producer -t binance_producer:latest .
docker build -f DockerFile.twitter_processor -t twitter_processor:latest .
docker build -f DockerFile.twitter_producer -t twitter_producer:latest .
