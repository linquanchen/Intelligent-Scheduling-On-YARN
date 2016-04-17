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

using namespace::apache::thrift;
using namespace::apache::thrift::protocol;
using namespace::apache::thrift::transport;
using namespace::apache::thrift::server;

using boost::shared_ptr;

using namespace alsched;

#ifdef MY_DEBUG
# define dbg_printf(...) printf(__VA_ARGS__)
#else
# define dbg_printf(...)
#endif

class TetrischedServiceHandler : virtual public TetrischedServiceIf
{
private:
    struct ListJob {
        JobID jobId;
        job_t::type jobType;
        int32_t k;
        double duration, slowDuration;
        ListJob(JobID jobId, job_t::type jobType, int32_t k, double duration, 
                                                        double slowDuration) {
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

    /** @brief This array is used to record that which machines are on the
               same rack and allocated for the same job. The index of array
               is machine id. The value of each element is a pointer that 
               points to a set which includes all machines that are on the
               same rack and allocated for the same job with this machine.*/
    std::set<int32_t> **sameRackVMs;

    /** @brief The max number of machines on the same rack */
    int maxMachinesPerRack;
    
    /** @brief Read config-mini config file for topology information
     *  @return A vector which size is the number of racks, each value is the 
     *          number of machines on each rack 
     */
    std::vector<int> ReadConfigFile() {
        std::ifstream t("config-none-timex1-c2x1-g6-h6-rho0.60");

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

    /** @brief Mark a set of machines as allocated
     *  @param machines The set of machines that will be marked as allocated
     */
    void AllocateBestMachines(std::set<int32_t> & machines) {
        for (std::set<int32_t>::iterator it=machines.begin(); 
                                                    it!=machines.end(); ++it) {
            unsigned int id = *it;
            unsigned int rackID = 0;
            while (id >= racks[rackID].size()) {
                id -= racks[rackID].size();
                rackID++;
            }
            racks[rackID][id].isFree = false;
        }
    }

    /** @brief Wrapper for allocate resources
     *  @param jobId The id of the job to allocate resources
     *  @param machines The set of machines that will be allocated to the job
     */
    void AllocResourcesWrapper(int jobId, std::set<int32_t> & machines) {
        dbg_printf("Allocate %d machines for %d\n", (int)machines.size(),jobId);
        printRackInfo();

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
            dbg_printf("ERROR calling YARN : %s\n", tx.what());
        }
    }

    /** @brief Get free VM number of every rack
     *  @return free VM number of every rack
     */
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

    /** @brief Allocate VMs to the job based on number k and rack index
     *  @param machines The set of machines that will be allocated to the job  
     *  @param k The number of machines that the job is asking
     *  @param index The rack will be allocateed to job
     */
    void AddJobsByRack(std::set<int> & machines, int k, int index) {
        for (unsigned int i = 0; k != 0 && i < racks[index].size(); i++) {
            if (racks[index][i].isFree) {
                machines.insert(racks[index][i].machineID);
                k--;
            }
        }
        if (k != 0) {
            dbg_printf("Something wrong with AddJobsByRack()\n");
        }
    }
    
    /** @brief Get the rack number which has minimum free VMs
     *  @param freemachines the free VM number of every rack
     *  @return the index of the rack
     */
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

    /** @brief Get machines for MPI job.
     *  @param machines The set of machines that will be allocated to the job 
     *  @param k The number of machines that the job is asking 
     *  @return true if on job's preferred allocation, else false
     */
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

        /* When one rack(no GPU) has enough VMs for MPI jobs */
        if (index != -1) {
            AddJobsByRack(machines, k, index);    
            return true;
        }
        
        /* When GPU rack has enough VMs for MPI jobs */
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

    /** @brief Get machines for GPU job.
     *  @param machines The set of machines that will be allocated to the job 
     *  @param k The number of machines that the job is asking 
     *  @return true if on job's preferred allocation, else false
     */
    bool GetMachinesForGPU(std::set<int> & machines, int k) {
        std::vector<int> freeMachines = GetFreeMachines();
         
        /* When the GPU rack has enough VMs for GPU jobs */
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


    /** @brief Get machines for specific job.
     *  @param machines The set of machines that will be allocated to the job 
     *  @param jobType The type of the job
     *  @param k The number of machines that the job is asking 
     *  @return true if on job's preferred allocation, else false
     */
    bool GetBestMachines(job_t::type jobType, int k, 
                                                std::set<int32_t> &machines) {
        switch(jobType) {
            case job_t::JOB_MPI:
                return GetMachinesForMPI(machines, k);
            case job_t::JOB_GPU:   
                return GetMachinesForGPU(machines, k);
            default:{
                dbg_printf("Unknown job type:%d\n", jobType);
                return false;
            }
        } 
    }

    /** @brief print current resources allocation information */
    void printRackInfo() {  
        dbg_printf("=============================================\n");
        dbg_printf("rack\t");
        for(int i = 0; i < maxMachinesPerRack; i++)
            dbg_printf("h%d\t", i);
        dbg_printf("total\n");

        for (unsigned int i = 0; i < racks.size(); i++) {
            dbg_printf("r%d\t", i);
            int num = 0;
            for (unsigned j = 0; j < racks[i].size(); j++) {
                int flag = racks[i][j].isFree ? 0 : 1;
                dbg_printf("%d\t", flag);
                num += (1-flag);
            }
            for (unsigned j = racks[i].size(); 
                                        j < (unsigned)maxMachinesPerRack; j++)
                dbg_printf("N\t");
            dbg_printf("%d\n", num);
        }
        dbg_printf("=============================================\n");
    }

    /** @brief print current queued job information */
    void printJobInfo() {
        dbg_printf("=============================================\n");
        dbg_printf("Id\tType\tk\tfast\t\tslow\n");
        for (std::list<ListJob>::iterator i=list.begin(); i != list.end(); ++i){
            dbg_printf("%d\t%d\t%d\t%f\t%f\n", (*i).jobId, (*i).jobType, 
                                    (*i).k, (*i).duration, (*i).slowDuration);
        }
        dbg_printf("=============================================\n");
    }

    /** @brief Free one machine and try to allocate resources for new jobs
     *  @param machine The machine that will be freed
     */
    void FreeResource(int32_t machine) {
        FreeMachine(machine);

        printRackInfo();

        printJobInfo();

        // check if this machine belongs to a set of machines that were 
        // on the same rack and allocated to the same job
        if (sameRackVMs[machine] != NULL) {
            std::set<int32_t>* sameRackVM = sameRackVMs[machine];
            sameRackVMs[machine] = NULL;

            (*sameRackVM).erase((*sameRackVM).find(machine));
            // if there are some other machines that were on the same rack and 
            // allocated to the same job, do not allocate this machine until
            // all machines are freed 
            if (!(*sameRackVM).empty())
                return;

            delete sameRackVM;
        }

        // try to allocate resources for one or more jobs
        while (true) {
            std::list<ListJob>::iterator bestJobToRun;
            double shortestTime = -1, tmpTime;
            std::set<int32_t> bestMachines, tmpMachines;
            bool isBestisPrefered;

            int freeMachineNum = GetFreeMachinesNum();

            for (std::list<ListJob>::iterator i=list.begin(); 
                                                        i != list.end(); ++i){
                if (freeMachineNum >= (*i).k) {
                    // try to find the best (preferred) machine allocation
                    bool isPrefered = 
                            GetBestMachines((*i).jobType, (*i).k, tmpMachines);
                    
                    tmpTime = isPrefered ? (*i).duration : (*i).slowDuration;

                    // if find a job with shorter running time, 
                    // update schedule solution
                    if (shortestTime < 0 || shortestTime > tmpTime) {
                        bestJobToRun = i;
                        shortestTime = tmpTime;
                        bestMachines = tmpMachines;
                        isBestisPrefered = isPrefered;
                        tmpMachines.clear();
                    }  
                }
            }

            if (shortestTime > 0) {
                dbg_printf("Choose job %d to run\n", (*bestJobToRun).jobId);
                // if this job is a MPI job and all allocated resources are on 
                // the same rack, record this information
                if (isBestisPrefered && 
                    (*bestJobToRun).jobType == job_t::JOB_MPI) {

                    std::set<int32_t> *sameRackVM = 
                                            new std::set<int32_t>(bestMachines);

                    for (std::set<int32_t>::iterator it=bestMachines.begin(); 
                            it!=bestMachines.end(); ++it) {
                        sameRackVMs[*it] = sameRackVM;
                    }        
                }
                AllocateBestMachines(bestMachines);
                AllocResourcesWrapper((*bestJobToRun).jobId, bestMachines);
                list.erase(bestJobToRun);
            } else
                // not job can be satisfied with current left resource
                break;
        }
    }

public:
    /** @brief Initilize Tetri server, read rack config info */
    TetrischedServiceHandler() {
        maxMachinesPerRack = 0;

        std::vector<int> rackInfo = ReadConfigFile();

        int count = 0;
        for (unsigned int i = 0; i < rackInfo.size(); i++) {
            std::vector<MachineResource> rack;
            for(int j = 0; j < rackInfo[i]; j++)
                rack.push_back(MachineResource(count + j));
            racks.push_back(rack);
            count += rackInfo[i];
        }

        sameRackVMs = new std::set<int32_t>*[count];
        for (int i = 0; i < count; i++)
            sameRackVMs[i] = NULL;
    }

    /** @brief A job is added to scheduler, waiting for allocating resources
     *  @param jobId The id of the job
     *  @param jobType The type of the job
     *  @param k The number of machines that the job is asking
     *  @param priority The priority of the job
     *  @param duration The estimated time of the job if on job's 
     *                  preferred allocation
     *  @param slowDuration The estimated time of the job if not on job's 
     *                      preferred allocation
     */
    void AddJob(const JobID jobId, const job_t::type jobType, const int32_t k, 
                const int32_t priority, const double duration, 
                const double slowDuration)
    {   
        dbg_printf("a new job comming: id:%d, type:%d, k:%d\n", jobId, 
                                                                    jobType, k);

        // if there is enough resources and no job ahead of this job, then 
        // resources can be allocated to this job directly
        if (list.empty() && GetFreeMachinesNum() >= k) {
            std::set<int32_t> machines;
            // try to find the best (preferred) machine allocation
            bool isPrefered = GetBestMachines(jobType, k, machines);

            // if this job is a MPI job and all allocated resources are on the
            // same rack, record this information
            if (isPrefered && jobType == job_t::JOB_MPI) {
                std::set<int32_t> *sameRackVM = new std::set<int32_t>(machines);
                for (std::set<int32_t>::iterator it=machines.begin(); 
                        it!=machines.end(); ++it) {
                    sameRackVMs[*it] = sameRackVM;
                }        
            }

            AllocateBestMachines(machines);
            AllocResourcesWrapper(jobId, machines);
        } else {
            // not enough resources or there are some jobs ahead of this job, 
            // this job must push to list
            if (duration <= 0 || slowDuration <= 0) {
                dbg_printf("Parameter check failed, \
                                                duration should be positive\n");
            }

            list.push_back(ListJob(jobId, jobType, k, duration, slowDuration));
        }
    }

    /** @brief Free some machine resources
     *  @param machines The set of machines that will be freed
     */
    void FreeResources(const std::set<int32_t> & machines)
    {   
        dbg_printf("free %d machines\n", (int)machines.size());

        // free machine resource one by one
        for (std::set<int32_t>::iterator it=machines.begin(); 
                                                    it!=machines.end(); ++it)
            FreeResource(*it);
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

