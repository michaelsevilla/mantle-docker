FROM ceph/daemon:f628bac

RUN echo "==> Install deps" && \
    apt-get update -y && \
    apt-get install -y libjsoncpp-dev ssh make g++ libssl-dev git mlocate && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* debian/ && \
    echo "==> Install cURL so we can talk to Docker" && \
    wget https://curl.haxx.se/download/curl-7.51.0.tar.gz && \
    tar xzvf curl-7.51.0.tar.gz && \
    cd curl-7.51.0 && \
    make && \
    make install && \
    echo "==> Get Mantle Docker" && \
    git clone https://github.com/michaelsevilla/mantle-docker.git /mantle-docker

ADD entrypoint.sh /
ENV LD_LIBRARY_PATH=/usr/local/lib
ENTRYPOINT ["/entrypoint.sh"]

