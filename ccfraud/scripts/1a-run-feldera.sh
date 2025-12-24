#!/bin/bash

set -euo pipefail

# Parse command-line arguments
ACTION="${1:-start}"

if [ "$ACTION" != "start" ] && [ "$ACTION" != "stop" ]; then
  echo "Usage: $0 [start|stop]"
  echo "  start - Start Feldera Docker container (default)"
  echo "  stop  - Stop Feldera Docker container"
  exit 1
fi

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Handle stop action
if [ "$ACTION" == "stop" ]; then
  echo "Stopping Feldera container..."
  CONTAINER_ID=$(docker ps -q --filter ancestor=ghcr.io/feldera/pipeline-manager:latest)

  if [ -z "$CONTAINER_ID" ]; then
    echo "No running Feldera container found."
    exit 0
  fi

  docker stop "$CONTAINER_ID"
  echo "Feldera container stopped successfully."
  exit 0
fi

# Start action - load environment and start container
# Check if Feldera is already running
RUNNING_CONTAINER=$(docker ps -q --filter ancestor=ghcr.io/feldera/pipeline-manager:latest)
if [ -n "$RUNNING_CONTAINER" ]; then
  echo "Feldera container is already running (ID: $RUNNING_CONTAINER)"
  echo "Use '$0 stop' to stop it first, or connect to the running instance."
  exit 0
fi

# Load environment variables from ../.env (relative to ccfraud directory)
ENV_FILE="${SCRIPT_DIR}/../../.env"

if [ -f "$ENV_FILE" ]; then
  echo "Loading environment from $ENV_FILE"
  set -a
  source "$ENV_FILE"
  set +a
else
  echo "Warning: .env file not found at $ENV_FILE"
fi

# Check FELDERA_DIR is set
if [ -z "${FELDERA_DIR+x}" ]; then
  echo "FELDERA_DIR environment variable is not set. Please set it in ../.env file."
  exit 1
fi

# Check HOPSWORKS_HOST is set
if [ -z "${HOPSWORKS_HOST+x}" ]; then
  echo "HOPSWORKS_HOST is not set. Please set it in ../.env file."
  exit 1
fi

# Check HOPSWORKS_PROJECT is set
if [ -z "${HOPSWORKS_PROJECT+x}" ]; then
  echo "HOPSWORKS_PROJECT is not set. Please set it in ../.env file."
  exit 1
fi

echo "Using FELDERA_DIR: $FELDERA_DIR"
echo "Using HOPSWORKS_HOST: $HOPSWORKS_HOST"
echo "Using HOPSWORKS_PROJECT: $HOPSWORKS_PROJECT"

cd "${FELDERA_DIR}"
# make sure the certs directory exists
mkdir -p /tmp/${HOPSWORKS_HOST}/${HOPSWORKS_PROJECT}

# Prevent the local host from deleting /tmp/${HOPSWORKS_HOST}
chmod 555 /tmp/${HOPSWORKS_HOST}

# Docker doesn't like mounting /tmp dirs into containers, so we create a symlink in the users' homedir that we will mount instead
# Only create symlink if it doesn't exist or points to wrong location
SYMLINK_PATH="${HOME}/${HOPSWORKS_HOST}"
TARGET_PATH="/tmp/${HOPSWORKS_HOST}"

if [ -L "$SYMLINK_PATH" ]; then
  # Symlink exists - check if it points to the correct location
  CURRENT_TARGET=$(readlink "$SYMLINK_PATH")
  if [ "$CURRENT_TARGET" != "$TARGET_PATH" ]; then
    echo "Symlink exists but points to wrong location ($CURRENT_TARGET). Recreating..."
    rm -f "$SYMLINK_PATH"
    ln -s "$TARGET_PATH" "$SYMLINK_PATH"
  fi
elif [ -e "$SYMLINK_PATH" ]; then
  # Path exists but is not a symlink
  echo "Error: $SYMLINK_PATH exists but is not a symlink. Please remove it manually."
  exit 1
else
  # Symlink doesn't exist - create it
  ln -s "$TARGET_PATH" "$SYMLINK_PATH"
fi

# Docker doesn't like mounting to /tmp, so mount to /mnt/certs and symlink to /tmp/c.app.hopsworks.ai
#docker run -p 8080:8080 -v ${HOME}/${HOPSWORKS_HOST}:/tmp/${HOPSWORKS_HOST} \
docker run -p 8080:8080 -v /tmp:/opt/${HOPSWORKS_HOST} \
	--tty --rm -it ghcr.io/feldera/pipeline-manager:latest
#docker run -p 8080:8080 -v /tmp:/home/ubuntu/certs \
#  --tty --rm -it pipeline-manager-with-symlink
