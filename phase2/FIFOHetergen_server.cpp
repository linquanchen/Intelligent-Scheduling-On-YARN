/** @file TetrischedService_server
 *  @brief This file contains implementation of a Tetrischeduler server
 *
 *  @author Ke Wu <kewu@andrew.cmu.edu>
 *  @author Linquan Chen <linquanc@andrew.cmu.edu>
 *
 *  @bug No known bugs.
 */
#include "TetrischedService.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <deque>
#include <vector>
#include <fstream>
#include <string>
#include "rapidjson/document.h"

#include "YARNTetrischedService.h"
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>
using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

using namespace alsched;
#ifdef DEBUG
# define dbg_printf(...) printf(__VA_ARGS__)
#else
# define dbg_printf(...)
#endif

#define GPU_RACK_INDEX 0
#define MAX_MACHINES_PER_RACK 6

class TetrischedServiceHandler : virtual public TetrischedServiceIf
{
private:
    struct QueueJob {
        JobID jobId;
        int32_t k;
        job_t::type jobType;
        QueueJob(JobID jobId, int32_t k, job_t::type jobType) {
            this->jobId = jobId;
            this->k = k;
            this->jobType = jobType;
        }
    };

    struct MachineResource{
        int machineID;
        bool isFree;
        MachineResource(int machineID) {
            this->machineID = machineID;
            this->isFree = true;
        }
    };

    /** @brief The queue for job that waiting for allocating resources */
    std::deque<QueueJob> queue;

    /** @brief The racks and machines array */
    std::vector<std::vector<MachineResource> > racks;

    /** @brief Read config-mini config file for topology information
     *  @return A vector which size is the number of racks, each value is the 
     *          number of machines on each rack 
     */
    std::vector<int> ReadConfigFile() {
        std::ifstream t("../config-mini");

        char c;
        std::string str;
        while(t.get(c)) {
            str += c;
        }
        const char *cstr = str.c_str();
        rapidjson::Document d;
        d.Parse(cstr);
        
        // Using a reference for consecutive access is handy and faster.
        const rapidjson::Value& a = d["rack_cap"]; 
        
        std::vector<int> rv;
        for (rapidjson::SizeType i = 0; i < a.Size(); i++) { 
            // rapidjson uses SizeType instead of size_t.
            rv.push_back(a[i].GetInt());
        }
    
        return rv;
    }

    /** @brief Get the total number of free machines */
    int GetFreeMachinesNum() {
        int count = 0;
        for (unsigned int i = 0; i < racks.size(); i++)
            for (unsigned int j = 0; j < racks[i].size(); j++)
                if (racks[i][j].isFree)
                    count++;
        return count;
    }

    /** @brief Get the id of the next free machines, 
     *         mark the machine as allocated
     */
    int GetNextFreeMachine() {
        for (unsigned int i = 0; i < racks.size(); i++)
            for (unsigned int j = 0; j < racks[i].size(); j++)
                if (racks[i][j].isFree) {
            racks[i][j].isFree = false;
                    return racks[i][j].machineID;
        }
        return -1;
    }

    std::vector<int> GetFreeMachines() {
        std::vector<int> freeMachines; 
        printf("Free machines per rack: ");
        for (unsigned int i = 0; i < racks.size(); i++) {
            int num = 0;
            for (unsigned int j = 0; j < racks[i].size(); j++) {
                if (racks[i][j].isFree) {
                    num++;
                }
            }
            printf("%d, ", num);
            freeMachines.push_back(num);
        }
        printf("\n");
        return freeMachines;
    }

    void AddJobsByRack(std::set<int> &machines, int k, int index) {
        for (unsigned int i = 0; k != 0 && i < racks[index].size(); i++) {
            if (racks[index][i].isFree) {
                machines.insert(racks[index][i].machineID);
                printf("Allocate VM %d: r%dh%d\n", racks[index][i].machineID, index+1, i+1);
                racks[index][i].isFree = false;
                k--;
            }
        }
    }
    
    // All machines
    int FindMinRack(std::vector<int> &freeMachines) {
        int min = MAX_MACHINES_PER_RACK;
        int index = -1;
        for (unsigned int i = 0; i < freeMachines.size(); i++) {
            int num = freeMachines[i];
            if (num != 0 && num <= min) {
                min = num;
                index = i;
            }
        }
        return index;
    }

    bool GetMachinesForMPI(std::set<int> &machines, int k) {
        std::vector<int> freeMachines = GetFreeMachines();
        
        int min = MAX_MACHINES_PER_RACK + 1;
        int index = -1;
        for (unsigned int i = 1; i < freeMachines.size(); i++) {
            int num = freeMachines[i];
            if (num >= k) {
                if (num < min) {
                    min = num;
                    index = i;
                }
            }        
        }
        if (index != -1) {
            printf("Allocate all to rack %d\n", index);
            AddJobsByRack(machines, k, index);
            return true;
        }
        
        if (freeMachines[0] >= k) {
            printf("Allocate all to rack 0, GPU rack!!!!\n");
            AddJobsByRack(machines, k, 0);
            return true;
        }
        
        while (k != 0) {
            index = FindMinRack(freeMachines);
            printf("Allocate part to rack %d\n", index);
            if (freeMachines[index] <= k) {
                AddJobsByRack(machines, freeMachines[index], index);
                k -= freeMachines[index];
                freeMachines[index] = 0;
            }
            else {
                AddJobsByRack(machines, k, index);
                k = 0;
            }
        }
        return false;
    }

    bool GetMachinesForGPU(std::set<int> &machines, int k) {
        std::vector<int> freeMachines = GetFreeMachines();
         
        if (freeMachines[0] >= k) {
            printf("Allocate all to GPU rack......\n");
            AddJobsByRack(machines, k, 0);
            return true;
        }
        
        int index;
        while (k != 0) {
            index = FindMinRack(freeMachines);
            printf("Allocate part to rack %d\n", index);
            if (freeMachines[index] <= k) {
                AddJobsByRack(machines, freeMachines[index], index);
                k -= freeMachines[index];
                freeMachines[index] = 0;
            }
            else {
                AddJobsByRack(machines, k, index);
                k = 0;
            }
        }
        return false;
    } 

    bool GetMachines(std::set<int> &machines, job_t::type jobType, int k) {
        switch(jobType) {
            case 0:
                return GetMachinesForMPI(machines, k);
                break;
            case 2:   
                return GetMachinesForGPU(machines, k);
                break;
            default:
                return false;
        } 
    }
    
    /** @brief Mark a machine as free */
    void FreeMachine(unsigned int id) {
        unsigned int rackID = 0;
        while (id >= racks[rackID].size()) {
            id -= racks[rackID].size();
            rackID++;
        }
        racks[rackID][id].isFree = true;
    }

    /** @brief Wrapper for allocate resources
     *  @param jobId The id of the job to allocate resources
     *  @param machines The set of machines that will be allocated to the job
     */
    void AllocResourcesWrapper(int jobId, std::set<int32_t> & machines) {
        int yarnport = 9090;
        shared_ptr<TTransport> socket(new TSocket("localhost", yarnport));
        shared_ptr<TTransport> transport(new TBufferedTransport(socket));
        shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
        YARNTetrischedServiceClient client(protocol);
        // try to allocate some nodes
        try {
            transport->open();
            client.AllocResources(jobId, machines);
            transport->close();
        } catch (TException& tx) {
            printf("ERROR calling YARN : %s\n", tx.what());
        }
    }

public:
    /** @brief Initilize Tetri server, read rack config info */
    TetrischedServiceHandler() {
        std::vector<int> rackInfo = ReadConfigFile();

        int count = 0;
        for (unsigned int i = 0; i < rackInfo.size(); i++) {
            std::vector<MachineResource> rack;
            for(int j = 0; j < rackInfo[i]; j++)
                rack.push_back(MachineResource(count + j));
            racks.push_back(rack);
            count += rackInfo[i];
        }
    }

    /** @brief A job is added to scheduler, waiting for allocating resources
     *  @param jobId The id of the job
     *  @param jobType The type of the job
     *  @param k The number of machines that the job is asking
     *  @param priority The priority of the job
     *  @param duration The estimated time of the job if all containers are
     *                  running on the same rack
     *  @param slowDuration The estimated time of the job if all containers are
     *                      running on different racks
     */
    void AddJob(const JobID jobId, const job_t::type jobType, const int32_t k, 
                const int32_t priority, const double duration, 
                const double slowDuration)
    {   
        // if there is enough resources and no job ahead of this job, then 
        // resources can be allocated to this jon directly
        if (queue.empty() && GetFreeMachinesNum() >= k) {
            std::set<int32_t> machines;
            printf("Add job %d, jobType: %d, VM: %d\n", jobId, jobType, k); 
            GetMachines(machines, jobType, k);
            AllocResourcesWrapper(jobId, machines);
            printf("Finish Add job %d.....\n", jobId);
        } else {
            // not enough resources or there are some jobs ahead of this job, 
            // this job must enqueue
            printf("Push job %d to queue, jobType: %d, VM: %d\n", jobId, jobType, k);
            queue.push_back(QueueJob(jobId, k, jobType));
        }
    }

    /** @brief Free some resources
     *  @param machines The set of machines that will be freed
     */
    void FreeResources(const std::set<int32_t> & machines)
    {
        // first free machine resources
        for (std::set<int32_t>::iterator it=machines.begin(); 
                                                    it!=machines.end(); ++it){
            FreeMachine(*it);
            printf("Free VM %d...........\n", *it);
        }

        // check the head of the queue to see if there is enough resource
        while (!queue.empty() && GetFreeMachinesNum() >= queue.front().k) {
            int jobId = queue.front().jobId;
            int k = queue.front().k;
            job_t::type jobType = queue.front().jobType;
            queue.pop_front();

            std::set<int32_t> machines;
            
            printf("Add job %d, jobType: %d, VM: %d\n", jobId, jobType, k); 
            GetMachines(machines, jobType, k);    
            AllocResourcesWrapper(jobId, machines);
            printf("Finish Add job %d.....\n", jobId);
        }
    }
};

int main(int argc, char **argv)
{
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

