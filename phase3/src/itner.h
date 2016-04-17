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

struct JobComparison {
    //bool operator() (const MyJob& job1, const MyJob& job2) const {
    bool operator() (const MyJob* job1, const MyJob* job2) const {
        double endTime1, endTime2;
        double durtion;
        duration = job1->isPrefered ? job1->duration : job1->slowDuration;
        endTime1 = static_cast<double>(job1->startTime(NULL)) + duration;
        
        duration = job2->isPrefered ? job2->duration : job2->slowDuration;
        endTime2 =  static_cast<double>(job2->startTime(NULL)) + duration;
        
        return endTime1 > endTime2;
    }
}
