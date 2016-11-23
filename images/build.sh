#!/bin/bash

set -ex

# create the image
SRC="/tmp/ceph-daemon"
mkdir $SRC || true
cd $SRC

# pull base image from ceph (we will layer on top of this)
wget https://raw.githubusercontent.com/systemslab/docker-cephdev/master/aliases.sh
. aliases.sh
rm aliases.sh
docker pull ceph/daemon:tag-build-master-jewel-ubuntu-14.04
docker tag ceph/daemon:tag-build-master-jewel-ubuntu-14.04 ceph/daemon:jewel

dmake \
  -e SHA1_OR_REF="f628bac31aa0b5b2696b8defc4aa18d4bc2ef757" \
  -e GIT_URL="https://github.com/ceph/ceph.git" \
  -e BUILD_THREADS=`grep processor /proc/cpuinfo | wc -l` \
  -e CONFIGURE_FLAGS="-DWITH_TESTS=OFF" \
  -e RECONFIGURE="true" \
  cephbuilder/ceph:latest build-cmake

docker tag ceph-f628bac31aa0b5b2696b8defc4aa18d4bc2ef757-base ceph/daemon:f628bac
cd -

# build against libcephfs in $SRC 
docker build -t tmp .
mkdir /tmp/curl || true
docker run --name tmp -it -v $SRC:/ceph -v /tmp/curl:/curl -v /tmp/root/mantle-docker:/mantledock tmp
docker commit --change='ENTRYPOINT ["/usr/bin/mantledock"]' tmp ceph/mantledock:f628bac
docker rm -f tmp

echo "Mantle Docker image is ceph/mantledock:f628bac; Ceph daemon image is ceph/daemon:f628bac"

docker tag ceph/mantledock:f628bac piha.soe.ucsc.edu:5000/ceph/mantledock:f628bac 
docker push piha.soe.ucsc.edu:5000/ceph/mantledock:f628bac
