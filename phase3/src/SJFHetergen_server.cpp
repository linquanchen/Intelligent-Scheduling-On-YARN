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
#include <ctime>


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

    /** @brief The list for job that waiting for allocating resources */
    std::list<MyJob*> pendingJobList;

    /** @brief The list for job that running */
    std::priority_queue<MyJob*, vector<MyJob*>, JobComparison> runningJobList;


    /** @brief The racks and machines array */
    std::vector<std::vector<MyMachine> > racks;

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

    MyMachine* GetMachineByID(unsigned int id) {
        unsigned int rackID = 0;
        while (id >= racks[rackID].size()) {
            id -= racks[rackID].size();
            rackID++;
        }
        return &(racks[rackID][id]);
    }

    
    /** @brief Mark a set of machines as allocated
     *  @param machines The set of machines that will be marked as allocated
     */
    void AllocateBestMachines(MyJob* job, std::set<int32_t> & machines) {
        for (std::set<int32_t>::iterator it=machines.begin(); 
                                                    it!=machines.end(); ++it) {
            GetMachineByID(*it)->assignJob(job);
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
                int flag = racks[i][j].IsFree() ? 0 : 1;
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
        for (std::list<MyJob*>::iterator i=pendingJobList.begin(); i != pendingJobList.end(); ++i){
            dbg_printf("%d\t%d\t%d\t%f\t%f\n", (*i)->jobId, (*i)->jobType, 
                                    (*i)->k, (*i)->duration, (*i)->slowDuration);
        }
        dbg_printf("=============================================\n");
    }

    MyJob* getPendingJobByID(int jobID) {
        for (std::list<MyJob*>::iterator i=pendingJobList.begin(); 
                i != pendingJobList.end(); ++i) {
            if ((int)((*i)->JobID) == jobID) {
                return &i;
            }
        }
        return NULL;
    }

    bool Schedule() {
        printRackInfo();

        printJobInfo();

        std::vector<std::vector<int>> schedule;
        
        for (int i = 0; i < schedule.size(); i++) {
            std::vector<int> oneJob = schedule[i];
            
            int jobID = oneJob[0];
            bool isPrefered = (oneJob[1] == 1);
            
            MyJob *scheduledJob = getPendingJobByID(jobID);
            
            std::set<int32_t> machines; 
            for (int j = 2; j < oneJob.size(); j++) {
                machines.insert(oneJob[j]);
                //GetMachineByID(oneJob[j])->AssignJob(scheduledJob);
            }
            AllocateBestMachines(scheduledJob, machines);
            scheduledJob->Start(machines, isPrefered);
            
            AllocResourcesWrapper(jobID, machines);
            
            pendingJobList.erase(scheduledJob);
            runningJobList.push(scheduledJob);
        }
        /*
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
        */

        // try to allocate resources for one or more jobs
        while (true) {
            std::list<MyJob*>::iterator bestJobIter;
            double shortestTime = -1, tmpTime;
            std::set<int32_t> bestMachines, tmpMachines;
            bool isBestisPrefered;

            int freeMachineNum = GetFreeMachinesNum();

            for (std::list<MyJob*>::iterator i=pendingJobList.begin(); 
                                                        i != pendingJobList.end(); ++i){
                if (freeMachineNum >= (*i)->k) {
                    // try to find the best (preferred) machine allocation
                    bool isPrefered = 
                            GetBestMachines((*i)->jobType, (*i)->k, tmpMachines);
                    
                    tmpTime = isPrefered ? (*i)->duration : (*i)->slowDuration;

                    // if find a job with shorter running time, 
                    // update schedule solution
                    if (shortestTime < 0 || shortestTime > tmpTime) {
                        bestJobIter = i;
                        shortestTime = tmpTime;
                        bestMachines = tmpMachines;
                        isBestisPrefered = isPrefered;
                        tmpMachines.clear();
                    }  
                }
            }

            if (shortestTime > 0) {
                dbg_printf("Choose job %d to run\n", (*bestJobIter)->jobId);
                
                /*
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
                */

                AllocateBestMachines(*bestJobIter, bestMachines);
                (*bestJobIter)->Start(isBestisPrefered);
                pendingJobList.erase(bestJobIter);
                runningJobList.push(belongedJob);


                AllocResourcesWrapper((*bestJobIter)->jobId, bestMachines);
                
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
            std::vector<MyMachine> rack;
            for(int j = 0; j < rackInfo[i]; j++)
                rack.push_back(MyMachine(count + j));
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
        if (duration <= 0 || slowDuration <= 0) {
            dbg_printf("Parameter check failed, \
                                            duration should be positive\n");
        }

        dbg_printf("a new job comming: id:%d, type:%d, k:%d\n", jobId, 
                jobType, k);

        pendingJobList.push(
                new MyJob(jobId, jobType, k, duration, slowDuration));
        
        Schedule();
    }

    /** @brief Free some machine resources
     *  @param machines The set of machines that will be freed
     */
    void FreeResources(const std::set<int32_t> & machines)
    {   
        dbg_printf("free %d machines\n", (int)machines.size());

        // free machine resource one by one
        for (std::set<int32_t>::iterator it=machines.begin(); 
                it!=machines.end(); ++it) {
            MyMachine *machine = GetMachineByID(machineID);
            machine->Free();

            MyJob *job = machine->belongedJob;
            job->FreeMachine(machineID);
            
            if (job->IsFinished()) {
                std::vector<MyJob*> tmpJobs;
                while (!runningJobList.empty()) {
                    if (runningJobList.top()->jobId == job->jobId) {
                        delete runningJobList.pop();
                        break;
                    }
                    else {
                        tmpJobs.push_back(runningJobList.pop());
                    }
                }
                for (int i = 0; i < tmpJobs.size(); i++) {
                    runningJobList.push(tmpJobs[i]);
                }
            }
        }

        Schedule();
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

