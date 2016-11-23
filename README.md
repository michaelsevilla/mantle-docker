mantle-docker
=============

This is code for a daemon that monitors Ceph subtrees and migrates Docker
containers when its inode moves.

Check out [this
blog](http://programmability.us/mantle/blog4-containers-implementation) for
more info.

Requirements:
- ssh
- jsoncpp
- curl version > 7.40
- docker daemon listening on all nodes
