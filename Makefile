mantledock: mantledock.cc
	g++ -Werror -std=c++11 -o mantledock mantledock.cc -lcurl -lrados -lboost_system -lboost_filesystem -lboost_program_options -lcephfs -ljsoncpp -I/ceph/src/ -L/ceph/build/lib/ -I/usr/include/jsoncpp -I/ceph/src/boost/ -L/ceph/build/boost/lib/ -I/ceph/build/boost/include/ -I/usr/local/include/ -L/usr/local/lib/

clean:
	rm mantledock
