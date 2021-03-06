/** @file Ultimate_server.cpp
 *  @brief This file contains implementation of a Tetrischeduler server
 *
 *  @author Ke Wu <kewu@andrew.cmu.edu>
 *  @author Linquan Chen <linquanc@andrew.cmu.edu>
 *
 *  @bug No known bugs.
 */

#include <deque>
#include <vector>
#include <fstream>
#include <string>
#include <string.h>
#include "rapidjson/document.h"
#include <ctime>
#include <list>
#include <set>
#include "inter.h"
#include <queue>
#include <stdio.h>

#include <unistd.h>


class TetrischedServiceHandler : virtual public TetrischedServiceIf
{
private:
    /** @brief Policy using for the scheduler, default is soft */
    enum {
        none,
        hard,
        soft
    } policy;

    /** @brief The list for job that waiting for allocating resources */
    std::list<MyJob*> pendingJobList;

    /** @brief The list for job that running */
    std::priority_queue<MyJob*, std::vector<MyJob*>, JobComparison> runningJobList;

    /** @brief The racks and machines array */
    std::vector<std::vector<MyMachine> > racks;

    /** @brief The max number of machines on the same rack */
    int maxMachinesPerRack;

    /** @brief Read config-mini config file for topology information
     *  @return A vector which size is the number of racks, each value is the 
     *          number of machines on each rack 
     */
    std::vector<int> ReadConfigFile() {
        std::ifstream t(configFilePath);

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

        const rapidjson::Value& b = d["simtype"];
        if (strcmp(b.GetString(), "none") == 0) {
            policy = none;
            dbg_printf("Using none policy\n");
        } else if (strcmp(b.GetString(), "soft") == 0) {
            policy = soft;
            dbg_printf("Using soft policy\n");
        } else if (strcmp(b.GetString(), "hard") == 0) {
            policy = hard;
            dbg_printf("Using hard policy\n");
        } else {
            policy = soft;
            dbg_printf("Not specify policy, using soft policy\n");
        }
    
        return rv;
    }

    /** @brief Return the machine given its id */
    MyMachine* GetMachineByID(uint32_t id) {
        uint32_t rackID = 0;
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
            GetMachineByID(*it)->AssignJob(job);
        }

    }

    /** @brief Wrapper for allocate resources
     *  @param jobId The id of the job to allocate resources
     *  @param machines The set of machines that will be allocated to the job
     */
    void AllocResourcesWrapper(int jobId, std::set<int32_t> & machines) {
        dbg_printf("Allocate %d machines for %d\n", (int)machines.size(),jobId);

        
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


    /** @brief Get the id of the random free machines,
     *         For non (random) policy
     */
    int GetRandomFreeMachine() {
        int rackIndex, machineIndex;
        int rackNum = racks.size(), machineNum;
        while(1) {
            rackIndex = rand() % rackNum;
            machineNum = racks[rackIndex].size();
            machineIndex = rand() % machineNum;
            if (racks[rackIndex][machineIndex].IsFree()) {
                return racks[rackIndex][machineIndex].machineID;
            }
        }
        return -1;
    }


    /** @brief Get the total number of free machines 
     *         For non (random) policy  
     */
    int GetFreeMachinesNum() {
        int count = 0;
        for (unsigned int i = 0; i < racks.size(); i++)
            for (unsigned int j = 0; j < racks[i].size(); j++)
                if (racks[i][j].IsFree())
                    count++;
        return count;
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
        dbg_printf("===================================================================================\n");
        dbg_printf("Id\tType\tk\tfast\t\tslow\t\tfast utility\tslow utility\n");
        for (std::list<MyJob*>::iterator i=pendingJobList.begin(); i != pendingJobList.end(); ++i){
            dbg_printf("%d\t%d\t%d\t%f\t%f\t%f\t%f\n", (*i)->jobId, (*i)->jobType, 
                                    (*i)->k, (*i)->duration, (*i)->slowDuration, 
                                    (*i)->CalUtility(time(NULL), true), (*i)->CalUtility(time(NULL), false));
        }
        dbg_printf("==================================================================================\n");
    }

    /** @brief Return the pending job given its id */
    MyJob* getPendingJobByID(int jobID) {
        for (std::list<MyJob*>::iterator i=pendingJobList.begin(); 
                i != pendingJobList.end(); ++i) {
            if ((int)((*i)->jobId) == jobID) {
                return *i;
            }
        }
        return NULL;
    }

    /** @brief Schedule 0, 1 or more jobs that are pending, given current free resources */
    void Schedule() {
        if (policy == none) {
            // for none policy, just using random FIFO
            while (GetFreeMachinesNum() >= pendingJobList.front()->k) {
                MyJob* scheduledJob = pendingJobList.front();
                pendingJobList.pop_front();
                
                int count = scheduledJob->k;
                std::set<int32_t> machines;
                while (count > 0) {
                    int32_t machineID = GetRandomFreeMachine();
                    GetMachineByID(machineID)->AssignJob(scheduledJob);
                    machines.insert(machineID);
                    count--;
                }
                
                scheduledJob->Start(machines, false);
            
                AllocResourcesWrapper(scheduledJob->jobId, machines);
            
                runningJobList.push(scheduledJob);
            }

            return;
        }

        // for hard policy and soft policy, create a snapshot of 
        // the current scheduler and do scheduling
        Cluster* cluster = new Cluster(racks, pendingJobList, runningJobList, 
                maxMachinesPerRack, (policy == soft));
        // The result is a vector where each element represents a schduled job 
        // with format <jobId, isPrefered, machine0, machine1, machine2, ...>
        std::vector<std::vector<int> > schedule = cluster->Schedule();
        
        for (unsigned int i = 0; i < schedule.size(); i++) {
            std::vector<int> oneJob = schedule[i];
            
            int jobID = oneJob[0];
            bool isPrefered = (oneJob[1] == 1);
            
            MyJob *scheduledJob = getPendingJobByID(jobID);
            if (scheduledJob == NULL) {
                dbg_printf("something wrong in Schedule() of sheculer\n");
            }
            
            std::set<int32_t> machines; 
            for (unsigned int j = 2; j < oneJob.size(); j++) {
                machines.insert(oneJob[j]);
            }
            AllocateBestMachines(scheduledJob, machines);
            scheduledJob->Start(machines, isPrefered);
            
            AllocResourcesWrapper(jobID, machines);
            
            pendingJobList.remove(scheduledJob);
            runningJobList.push(scheduledJob);
        }

        cluster->Clear();
        delete cluster;

        dbg_printf("After schedule\n");
        printRackInfo();
        printJobInfo();
    }

public:
    /** @brief The path of the config file */
    static char* configFilePath;

    /** @brief Initilize Tetri server, read rack config info */
    TetrischedServiceHandler() {
        maxMachinesPerRack = 0;
        
        std::vector<int> rackInfo;
        // Read rack info and policy from con/fig file.
        if (configFilePath != NULL) {
            rackInfo = ReadConfigFile();
        }
        // Using default rack info and policy.
        else {
            int rack[4] = {4, 6, 6, 6};
            rackInfo.assign(&rack[0], &rack[0]+4);
            policy = soft;
            maxMachinesPerRack = 6;
        }

        int count = 0;
        for (unsigned int i = 0; i < rackInfo.size(); i++) {
            std::vector<MyMachine> rack;
            for(int j = 0; j < rackInfo[i]; j++)
                rack.push_back(MyMachine(count + j));
            racks.push_back(rack);
            count += rackInfo[i];
        }
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

        dbg_printf("a new job comming: id:%d, type:%d, k:%d, fast:%f, slow:%f\n", jobId, 
                jobType, k, duration, slowDuration);

        pendingJobList.push_back(
                new MyJob(jobId, jobType, k, duration, slowDuration, time(NULL)));
        
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
            
            int machineID = *it;
            

            MyMachine *machine = GetMachineByID(machineID);
            
            MyJob *job = machine->belongedJob;
            
            machine->Free();
            
            job->FreeMachine(machineID);
            
            if (job->IsFinished()) {
                std::vector<MyJob*> tmpJobs;
                while (!runningJobList.empty()) {
                    if (runningJobList.top()->jobId == job->jobId) {
                        MyJob *tmp = runningJobList.top();
                        runningJobList.pop();
                        dbg_printf("A job %d is finished, real time: %f, expected time: %f\n", 
                                tmp->jobId, difftime(time(NULL), tmp->startTime), 
                                tmp->isPrefered ? tmp->duration : tmp->slowDuration);
                        delete tmp;
                        break;
                    }
                    else {
                        tmpJobs.push_back(runningJobList.top());
                        runningJobList.pop();
                    }
                }
                for (unsigned int i = 0; i < tmpJobs.size(); i++) {
                    runningJobList.push(tmpJobs[i]);
                }
            }
        }

        Schedule();
    }

};

char* TetrischedServiceHandler::configFilePath = NULL;

int main(int argc, char **argv)
{   
    // Read the path of the config file
    if ((argc == 3) && (strcmp(argv[1], "-c") == 0)) {
        TetrischedServiceHandler::configFilePath = argv[2];
        printf("Read rack info and policy from config file....\n");
    }
    else {
        TetrischedServiceHandler::configFilePath = NULL;
        printf("Using the default rack info and policy....\n");
        printf("Rack Info: [4, 6, 6, 6]; Policy: soft.\n");
    }

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


