#!/bin/bash

set -ex

# this needs to be done here because we link against a ceph directory
git clone https://github.com/michaelsevilla/mantle-docker.git /mantle-docker
cd /mantle-docker
make
cp mantledock /usr/bin/
cp /ceph/build/boost/lib/*.so* /usr/local/lib/
