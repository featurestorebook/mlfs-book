#!/bin/bash

if [ -z "${FELDERA_DIR+x}" ]; then
  echo "FELDERA_DIR environment variable is not set. Please set it to the directory containing feldera before running this script."
  exit 1
fi

	

if [ -z "${HOPSWORKS_HOST+x}" ]; then
  echo "HOPSWORKS_HOST is not set. Please set it to your Hopsworks cluster before running this script."
  exit 1
fi

cd ${FELDERA_HOME}

rm -f ${HOME}/${HOPSWORKS_HOST} 
# make sure the certs directory exists
mkdir -p /tmp/${HOPSWORKS_HOST}/${HOPSWORKS_PROJECT}

# Prevent the local host from deleting /tmp/${HOPSWORKS_HOST}
chmod 555 /tmp/${HOPSWORKS_HOST}

# Docker doesn't like mounting /tmp dirs into containers, so we create a symlink in the users' homedir that we will mount instead
ln -s  /tmp/${HOPSWORKS_HOST} ${HOME}/${HOPSWORKS_HOST}
#ln -s  /tmp ${HOME}/${HOPSWORKS_HOST}

# Docker doesn't like mounting to /tmp, so mount to /mnt/certs and symlink to /tmp/c.app.hopsworks.ai
#docker run -p 8080:8080 -v ${HOME}/${HOPSWORKS_HOST}:/tmp/${HOPSWORKS_HOST} \
docker run -p 8080:8080 -v /tmp:/opt/${HOPSWORKS_HOST} \
	--tty --rm -it ghcr.io/feldera/pipeline-manager:latest 
#docker run -p 8080:8080 -v /tmp:/home/ubuntu/certs \
#  --tty --rm -it pipeline-manager-with-symlink
