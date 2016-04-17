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
                double slowDuration, double arriveTime);
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


class Cluster {
private:
    /** @brief The list for job that waiting for allocating resources */
    std::list<MyJob*> pendingJobList;

    /** @brief The list for job that running */
    std::list<MyJob*> runningJobList;

    /** @brief The racks and machines array */
    std::vector<std::vector<MyMachine> > racks;

    /** @brief The max number of machines on the same rack */
    int maxMachinesPerRack;
    
    Cluster(std::vector<std::vector<MyMachine> > & racks, 
            std::list<MyJob*> & pendingJobList,
            std::list<MyJob*> & runningJobList,
            int maxMachinesPerRack);

    Clear();

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

    // for each vector, 0 is jobID, 1 indicates if is prefered, 
    // 2...n is machine ID
    std::vector<std::vector<int> > Schedule();
};
