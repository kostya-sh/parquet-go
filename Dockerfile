FROM ubuntu:trusty

# Make sure the package repository is up to date.
# Install maven
RUN apt-get update -qq
RUN apt-get install -y build-essential
RUN apt-get install -y wget

RUN wget http://protobuf.googlecode.com/files/protobuf-2.5.0.tar.gz
RUN tar xzf protobuf-2.5.0.tar.gz -C /
WORKDIR /protobuf-2.5.0
RUN ./configure
RUN make
RUN make install
RUN ldconfig

RUN apt-get install -qq libboost-dev libboost-test-dev libboost-program-options-dev libevent-dev automake libtool flex bison pkg-config g++ libssl-dev

# Install OpenJDK 7 (latest version of it) and Ant
RUN apt-get install -y --no-install-recommends openjdk-7-jdk
RUN apt-get install -y maven
RUN apt-get install -y ant

RUN wget -nv http://archive.apache.org/dist/thrift/0.7.0/thrift-0.7.0.tar.gz
RUN which javac
RUN which java
RUN tar zxf thrift-0.7.0.tar.gz
WORKDIR thrift-0.7.0
RUN chmod +x ./configure
RUN ./configure --disable-gen-erl --disable-gen-hs --without-ruby --without-haskell --without-erlang --vithout-tests
RUN make install

ENV HADOOP_PROFILE default
ENV HADOOP_PROFILE hadoop-2

RUN mvn install --batch-mode -DskipTests=true -Dmaven.javadoc.skip=true -Dsource.skip=true > mvn_install.log || mvn install --batch-mode -DskipTests=true -Dmaven.javadoc.skip=true -Dsource.skip=true > mvn_install.log || (cat mvn_install.log && false)
RUN mvn test -P $HADOOP_PROFILE
