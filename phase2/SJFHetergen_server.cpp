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

class TetrischedServiceHandler : virtual public TetrischedServiceIf
{
private:
    struct ListJob {
        JobID jobId;
        job_t::type jobType;
        int32_t k;
        double duration, slowDuration;
        ListJob(JobID jobId, job_t::type jobType, int32_t k, double duration, double slowDuration) {
            this->jobId = jobId;
            this->jobType = jobType;
            this->k = k;
            this->duration = duration;
            this->slowDuration = slowDuration;
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

    /** @brief The list for job that waiting for allocating resources */
    std::list<ListJob> list;

    /** @brief The racks and machines array */
    std::vector<std::vector<MachineResource> > racks;

    int maxMachinesPerRack = 0;

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

            if (maxMachinesPerRack < a[i].GetInt())
                maxMachinesPerRack = a[i].GetInt();
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

    std::vector<int> GetFreeMachines() {
        std::vector<int> freeMachines; 
        for (unsigned int i = 0; i < racks.size(); i++) {
            int num = 0;
            for (unsigned int j = 0; j < racks[i].size(); j++) {
                if (racks[i][j].isFree) {
                    num++;
                }
            }
            freeMachines.push_back(num);
        }
        return freeMachines;
    }

    void AddJobsByRack(std::set<int> & machines, int k, int index) {
        for (unsigned int i = 0; k != 0 && i < racks[index].size(); i++) {
            if (racks[index][i].isFree) {
                machines.insert(racks[index][i].machineID);
                k--;
                racks[index][i].isFree = false;
            }
        }
        if (k != 0) {
            printf("Something wrong with AddJobsByRack()");
        }
    }
    
    // All machines
    int FindMinRack(std::vector<int> & freeMachines) {
        int min = maxMachinesPerRack;
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

    bool GetMachinesForMPI(std::set<int> & machines, int k) {
        std::vector<int> freeMachines = GetFreeMachines();
        
        int min = maxMachinesPerRack + 1;
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
            AddJobsByRack(machines, k, index);
            return true;
        }
        
        if (freeMachines[0] >= k) {
            AddJobsByRack(machines, k, 0);
            return true;
        }
        
        while (k != 0) {
            index = FindMinRack(freeMachines);
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

    bool GetMachinesForGPU(std::set<int> & machines, int k) {
        std::vector<int> freeMachines = GetFreeMachines();
         
        if (freeMachines[0] >= k) {
            AddJobsByRack(machines, k, 0);
            return true;
        }
        
        int index;
        while (k != 0) {
            index = FindMinRack(freeMachines);
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

    bool GetBestMachines(job_t::type jobType, int k, std::set<int32_t> &machines) {
        switch(jobType) {
            case job_t::JOB_MPI:
                return GetMachinesForMPI(machines, k);
            case job_t::JOB_GPU:   
                return GetMachinesForGPU(machines, k);
            default:{
                printf("Unknown job type:%d", jobType);
                return false;
            }
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
     *  @param duration The estimated time of the job if on job's preferred allocation
     *  @param slowDuration The estimated time of the job if not on job's preferred allocation
     */
    void AddJob(const JobID jobId, const job_t::type jobType, const int32_t k, 
                const int32_t priority, const double duration, 
                const double slowDuration)
    {   
        // if there is enough resources and no job ahead of this job, then 
        // resources can be allocated to this job directly
        if (list.empty() && GetFreeMachinesNum() >= k) {
            std::set<int32_t> machines;
            GetBestMachines(jobType, k, machines);
            AllocResourcesWrapper(jobId, machines);
        } else {
            // not enough resources or there are some jobs ahead of this job, 
            // this job must push to list
            if (duration <= 0 || slowDuration <= 0) {
                printf("Parameter check failed, duration should be positive");
            }

            list.push_back(ListJob(jobId, jobType, k, duration, slowDuration));
        }
    }

    /** @brief Free some resources
     *  @param machines The set of machines that will be freed
     */
    void FreeResources(const std::set<int32_t> & machines)
    {
        // first free machine resources
        for (std::set<int32_t>::iterator it=machines.begin(); 
                                                    it!=machines.end(); ++it)
            FreeMachine(*it);

        while (true) {
            std::list<ListJob>::iterator bestJobToRun;
            double shortestTime = -1, tmpTime;
            std::set<int32_t> bestMachines, tmpMachines;

            int freeMachineNum = GetFreeMachinesNum();
            for (std::list<ListJob>::iterator i=list.begin(); i != list.end(); ++i){
                if (freeMachineNum >= (*i).k) {
                    // try to find a optimal configuration 
                    bool isBest = GetBestMachines((*i).jobType, (*i).k, tmpMachines);
                    
                    tmpTime = isBest ? (*i).duration : (*i).slowDuration;

                    // update schedule solution
                    if (shortestTime < 0 || shortestTime > tmpTime) {
                        // free machine resources of the last schedule solution
                        if (shortestTime > 0) {
                            for (std::set<int32_t>::iterator it=bestMachines.begin(); 
                                                        it!=bestMachines.end(); ++it)
                                FreeMachine(*it);
                        }
                        
                        bestJobToRun = i;
                        shortestTime = tmpTime;
                        bestMachines = tmpMachines;
                        tmpMachines.clear();
                    }  
                }
            }

            if (shortestTime > 0) {
                list.erase(bestJobToRun);
                AllocResourcesWrapper((*bestJobToRun).jobId, bestMachines);
            } else
                break;
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

