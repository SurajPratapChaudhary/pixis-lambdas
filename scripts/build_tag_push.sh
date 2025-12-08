#!/bin/bash
set -euo pipefail

# --- Parse arguments ---
usage() {
  echo "Usage: $0 -r <ECR_REPOSITORY> -t <IMAGE_TAG> -f <DOCKER_FILE> -n <LAMBDA_FUNCTION>"
  exit 1
}

ECR_REPOSITORY=""
IMAGE_TAG=""
DOCKER_FILE=""
LAMBDA_FUNCTION=""

while getopts ":r:t:f:n:" opt; do
  case ${opt} in
    r ) ECR_REPOSITORY=$OPTARG ;;
    t ) IMAGE_TAG=$OPTARG ;;
    f ) DOCKER_FILE=$OPTARG ;;
    n ) LAMBDA_FUNCTION=$OPTARG ;;
    * ) usage ;;
  esac
done
shift $((OPTIND -1))

if [[ -z "$ECR_REPOSITORY" || -z "$IMAGE_TAG" || -z "$DOCKER_FILE" || -z "$LAMBDA_FUNCTION" ]]; then
  echo "❌ Missing required parameters."
  usage
fi

# --- Build & Push ---
echo "🚀 Building Docker image for $LAMBDA_FUNCTION ..."
docker build --cache-from "$ECR_REPOSITORY:latest" --tag "$ECR_REPOSITORY:$IMAGE_TAG" -f "$DOCKER_FILE" .

echo "🏷️  Tagging image as latest..."
docker tag "$ECR_REPOSITORY:$IMAGE_TAG" "$ECR_REPOSITORY:latest"

echo "📤 Pushing image to ECR..."
docker push "$ECR_REPOSITORY:$IMAGE_TAG"
docker push "$ECR_REPOSITORY:latest"

# --- Deploy to Lambda ---
echo "🔍 Deploying to Lambda function: $LAMBDA_FUNCTION"

if aws lambda get-function --function-name "$LAMBDA_FUNCTION" >/dev/null 2>&1; then
  aws lambda update-function-code \
    --function-name "$LAMBDA_FUNCTION" \
    --image-uri "$ECR_REPOSITORY:$IMAGE_TAG" \
    --region us-east-1
  echo "✅ Successfully updated Lambda: $LAMBDA_FUNCTION"
else
  echo "❌ Lambda function '$LAMBDA_FUNCTION' does not exist in AWS!"
  echo "➡️  Please create it manually the first time using this image:"
  echo "   $ECR_REPOSITORY:$IMAGE_TAG"
  exit 1
fi
