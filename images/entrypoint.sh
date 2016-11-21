#!/bin/bash

set -ex

# this needs to be done here because we link against a ceph directory
cd /mantle-docker
make
cp mantledock /usr/bin/
cp /ceph/build/boost/lib/*.so* /usr/local/lib/
