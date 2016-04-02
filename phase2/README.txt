Run thrift code stub generator:
thrift -out . --gen java tetrisched.thrift
thrift -out . --gen cpp tetrisched.thrift

Setup library path:
export LD_LIBRARY_PATH=/usr/local/lib/

Build java server:
javac -cp lib/libthrift-0.9.1.jar:lib/slf4j-api-1.7.7.jar Server.java YARNTetrischedHandler.java tetrisched/YARNTetrischedService.java

Build java client:
javac -cp lib/libthrift-0.9.1.jar:lib/slf4j-api-1.7.7.jar Client.java tetrisched/TetrischedService.java tetrisched/job_t.java

Build c++ server and client:
make

Run java server:
java -cp .:lib/libthrift-0.9.1.jar:lib/slf4j-api-1.7.7.jar:lib/slf4j-simple-1.7.7.jar Server

Run cpp client:
./YARNTetrischedService_client

OR

Run cpp server:
./TetrischedService_server

Run java client:
java -cp .:lib/libthrift-0.9.1.jar:lib/slf4j-api-1.7.7.jar:lib/slf4j-simple-1.7.7.jar Client

