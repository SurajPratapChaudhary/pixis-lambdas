#!/bin/bash
AWSAccountId=767397866624
DOCKER_REGISTRY=$AWSAccountId.dkr.ecr.us-east-1.amazonaws.com
REPOSITORY_URI="$DOCKER_REGISTRY"/queue-worker-scaling

aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin $DOCKER_REGISTRY
docker pull $REPOSITORY_URI:latest || true
docker build --cache-from $REPOSITORY_URI:latest --tag $REPOSITORY_URI:latest -f queue-worker-scaling/Dockerfile .
docker push $REPOSITORY_URI:latest
