#!/bin/bash

# Default values
AWS_ACCOUNT_ID="767397866624"
DOCKER_REGISTRY="$AWS_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com"
REPOSITORY_TYPE="${1:-sp}"  # Use the first argument if provided, otherwise default to "sp"

# Set repository name based on the provided parameter or fallback to default
case "$REPOSITORY_TYPE" in
    sp)
        REPOSITORY_NAME="sp-api-clean-and-load"
        DOCKERFILE_PATH="amazon-selling-partners-api/CleanAndLoad/Dockerfile"
        ;;
    ads)
        REPOSITORY_NAME="ads-api-clean-and-load"
        DOCKERFILE_PATH="amazon-ads-api/CleanAndLoad/Dockerfile"
        ;;
    *)
        REPOSITORY_NAME="sp-api-clean-and-load"  # Default
        DOCKERFILE_PATH="amazon-selling-partners-api/CleanAndLoad/Dockerfile"
        ;;
esac

REPOSITORY_URI="$DOCKER_REGISTRY/$REPOSITORY_NAME"

# Authenticate with ECR and Docker
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin "$DOCKER_REGISTRY"

# Pull latest image (if available) and build, tag, and push the Docker image
docker pull "$REPOSITORY_URI:latest" || true
docker build --cache-from "$REPOSITORY_URI:latest" --tag "$REPOSITORY_URI:latest" -f "$DOCKERFILE_PATH" .
docker push "$REPOSITORY_URI:latest"

# Uncomment this line if you want to update a Lambda function with the latest image
# aws lambda update-function-code --function-name fe-authorization-AuthorizationLambdaFunction-anuTK6CTEHVy --image-uri "$REPOSITORY_URI:latest"
