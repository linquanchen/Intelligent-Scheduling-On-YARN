#include "TetrischedService.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>

#include "YARNTetrischedService.h"
#include <thrift/transport/TSocket.h>                                                                                             
#include <thrift/transport/TTransportUtils.h>  

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

using namespace std;
using namespace alsched;

class TetrischedServiceHandler : virtual public TetrischedServiceIf
{

private:

public:

    TetrischedServiceHandler()
    {
        // Your initialization goes here
        printf("init\n");
    }

    void AddJob(const JobID jobId, const job_t::type jobType, const int32_t k, const int32_t priority, const double duration, const double slowDuration)
    {
        // Your implementation goes here
        printf("AddJob\n");

        int yarnport = 9090;
        shared_ptr<TTransport> socket(new TSocket("localhost", yarnport));
        shared_ptr<TTransport> transport(new TBufferedTransport(socket));
        shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
        YARNTetrischedServiceClient client(protocol);
        // try to allocate some nodes
        try {
            transport->open();
            set<int32_t> machines;
            machines.insert(1);
            machines.insert(2);
            machines.insert(3);
            machines.insert(4);
            client.AllocResources(jobId, machines);

            transport->close();
        } catch (TException& tx) {
            printf("ERROR calling YARN : %s\n", tx.what());
        }
    }

    void FreeResources(const std::set<int32_t> & machines)
    {
        // Your implementation goes here
        printf("FreeResources\n");
    }

};

int main(int argc, char **argv)
{
    //create a listening server socket
    int alschedport = 9091;
    shared_ptr<TetrischedServiceHandler> handler(new TetrischedServiceHandler());
    shared_ptr<TProcessor> processor(new TetrischedServiceProcessor(handler));
    shared_ptr<TServerTransport> serverTransport(new TServerSocket(alschedport));
    shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

    TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
    server.serve();
    return 0;
}
