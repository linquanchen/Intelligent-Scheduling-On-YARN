class MyMachine;

class MyJob {
public:
    JobID jobId;
    job_t::type jobType;
    int32_t k;
    double duration, slowDuration;
    time_t arriveTime, startTime;
    bool isPrefered;
    std::set<int> assignedMachines;

    MyJob(JobID jobId, job_t::type jobType, int32_t k, double duration, 
                double slowDuration, double arriveTime);
    void Start(std::set<int32_t> & machines);
    void FreeMachine(int machineID);
    bool IsFinished();
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


