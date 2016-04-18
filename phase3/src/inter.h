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


using boost::shared_ptr;

using namespace alsched;

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
    JobID jobId;
    job_t::type jobType;
    int32_t k;
    double duration, slowDuration;
    time_t arriveTime, startTime;
    bool isPrefered;
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
    int machineID;
    MyJob* belongedJob;
    
    MyMachine(int machineID);
    void AssignJob(MyJob* job);
    void Free();
    bool IsFree();
};


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

    /** @brief Get the total number of free machines */
    int GetFreeMachinesNum();

    /** @brief Mark a set of machines as allocated
     *  @param machines The set of machines that will be marked as allocated
     */
    void AllocateMachinesToJob(MyJob* job, std::set<int32_t> & machines,
            bool isPrefered);

    void FreeMachinesByJob(MyJob* job);

    /** @brief Get free VM number of every rack
     *  @return free VM number of every rack
     */
    std::vector<int> GetFreeMachines();

    /** @brief Get k VMs from rack index
     *  @param machines The set of machines to store k VMs
     *  @param k The number of machines that the is asking
     *  @param index The rack to get k VMs
     */
    void GetMachineByRack(std::set<int> & machines, int k, int index);

    
    /** @brief Get the rack number which has minimum free VMs
     *  @param freemachines the free VM number of every rack
     *  @return the index of the rack
     */
    int FindMinRack(std::vector<int> & freeMachines);

    /** @brief Get machines for MPI job.
     *  @param machines The set of machines that will be allocated to the job 
     *  @param k The number of machines that the job is asking 
     *  @return true if on job's preferred allocation, else false
     */
    bool GetMachinesForMPI(std::set<int> & machines, int k);

    /** @brief Get machines for GPU job.
     *  @param machines The set of machines that will be allocated to the job 
     *  @param k The number of machines that the job is asking 
     *  @return true if on job's preferred allocation, else false
     */
    bool GetMachinesForGPU(std::set<int> & machines, int k);


    /** @brief Get machines for specific job.
     *  @param machines The set of machines that will be allocated to the job 
     *  @param jobType The type of the job
     *  @param k The number of machines that the job is asking 
     *  @return true if on job's preferred allocation, else false
     */
    bool GetBestMachines(job_t::type jobType, int k, 
                                                std::set<int32_t> &machines);

    double CalAddedUtility(int delayJobNum);

    std::vector<std::vector<int> > constructResult(std::vector<MyJob*> & jobs);

public:
    Cluster(std::vector<std::vector<MyMachine> > & racks, 
            std::list<MyJob*> & pendingJobList,
            std::priority_queue<MyJob*, std::vector<MyJob*>, JobComparison> & runningJobList,
            int maxMachinesPerRack, bool isSoft);

    void Clear();

    // for each vector, 0 is jobID, 1 indicates if is prefered, 
    // 2...n is machine ID
    std::vector<std::vector<int> > Schedule();
};


#endif