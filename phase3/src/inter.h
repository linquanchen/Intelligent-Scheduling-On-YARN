/** @file inter.h
 *  @brief This file contains implementation of a Tetrischeduler server
 *
 *  @author Ke Wu <kewu@andrew.cmu.edu>
 *  @author Linquan Chen <linquanc@andrew.cmu.edu>
 *
 *  @bug No known bugs.
 */

#ifndef _INTER_H_
#define _INTER_H_

#include "TetrischedService.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include "YARNTetrischedService.h"
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

#include <queue>
#include <list>
#include <set>
#include <vector>
#include <stdint.h>
#include <ctime>

using namespace::apache::thrift;
using namespace::apache::thrift::protocol;
using namespace::apache::thrift::transport;
using namespace::apache::thrift::server;
using namespace alsched;

using boost::shared_ptr;

#define MY_DEBUG

#ifdef MY_DEBUG
# define dbg_printf(...) printf(__VA_ARGS__)
#else
# define dbg_printf(...)
#endif

#define SEARCH_STEP  5
#define EXTRA_SEARCH_STEP 7

class MyMachine;

class MyJob {
public:
    /** @brief The job id. */
    JobID jobId;
    
    /** @brief The job type. */
    job_t::type jobType;

    /** @brief The number of machines that the job needs. */
    int32_t k;

    /** @brief The fast duration and slow duration that the job runs. */
    double duration, slowDuration;

    /** @brief The arrive and start time of the job. */
    time_t arriveTime, startTime;

    /** @brief True if the job is allocated to preferrd resources, else false. */
    bool isPrefered;

    /** @brief The set of machines that allocate to the job. */
    std::set<int32_t> assignedMachines;

    MyJob(JobID jobId, job_t::type jobType, int32_t k, double duration, 
                double slowDuration, time_t arriveTime);
    
    MyJob(MyJob* job);
    
    void Start(std::set<int32_t> & machines, bool isPrefered);
    
    void FreeMachine(int machineID);
    
    bool IsFinished();
    
    double CalUtility(time_t curTime, bool isPrefered);
    
    time_t GetFinishedTime();
};

class MyMachine{
public:
    /** @brief The machine id. */
    int machineID;

    /** @brief The job that the machine allocates to. */
    MyJob* belongedJob;
    
    MyMachine(int machineID);

    void AssignJob(MyJob* job);
    
    void Free();
    
    bool IsFree();
};

/** @brief A comparator used for priority queue(runningjoblist) 
 *         based on the running time of the job. 
 */
struct JobComparison {
    bool operator() (const MyJob* job1, const MyJob* job2) const {
        int endTime1, endTime2;
        double duration;
        duration = job1->isPrefered ? job1->duration : job1->slowDuration;
        endTime1 = (job1->startTime) + (int)duration;
     
        duration = job2->isPrefered ? job2->duration : job2->slowDuration;
        endTime2 = (job2->startTime) + (int)duration;
         
        return endTime1 > endTime2;
    }
 };


class Cluster {
private:
    /** @brief true if the policy is soft, else false */
    bool isSoft;

    /** @brief The list for job that waiting for allocating resources */
    std::list<MyJob*> pendingJobList;

    /** @brief The list for job that running */
    std::priority_queue<MyJob*, std::vector<MyJob*>, JobComparison> runningJobList;

    /** @brief The racks and machines array */
    std::vector<std::vector<MyMachine> > racks;

    /** @brief The max number of machines on the same rack */
    int maxMachinesPerRack;
    
    MyMachine* GetMachineByID(unsigned int id);

    int GetFreeMachinesNum();

    void AllocateMachinesToJob(MyJob* job, std::set<int32_t> & machines, bool isPrefered);

    void FreeMachinesByJob(MyJob* job);

    std::vector<int> GetFreeMachines();

    void GetMachineByRack(std::set<int> & machines, int k, int index);

    int FindMinRack(std::vector<int> & freeMachines);

    bool GetMachinesForMPI(std::set<int> & machines, int k);

    bool GetMachinesForGPU(std::set<int> & machines, int k);

    bool GetBestMachines(job_t::type jobType, int k, std::set<int32_t> &machines);

    std::vector<std::vector<int> > Search(int step, int searchEndJobId, time_t curTime, double & resultUtility);

    std::vector<std::vector<int> > SimulateNext(int step, int searchEndJobId, time_t curTime, double & resultUtility);

    std::vector<std::vector<int> > constructResult(std::vector<MyJob*> & jobs);

public:
    Cluster(std::vector<std::vector<MyMachine> > & racks, 
            std::list<MyJob*> & pendingJobList,
            std::priority_queue<MyJob*, std::vector<MyJob*>, JobComparison> & runningJobList,
            int maxMachinesPerRack, bool isSoft);

    void Clear();

    std::vector<std::vector<int> > Schedule();
};

#endif
